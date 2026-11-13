use std::collections::BTreeMap;
use std::convert::Infallible;

use bytes::Bytes;
use futures_util::stream;
use multer::{Constraints, Multipart, SizeLimit};
use serde_json::{Map, Value};
use webhook_async_core::{ParameterSpec, ValueSource};
use xmltree::{Element, XMLNode};

use crate::HookEngineError;
use crate::extract::{extract_as_string, set_dot_path};

/// Normalized HTTP request context used for trigger evaluation and argument/env extraction.
/// Raw request bytes are preserved untouched for signature validation and `raw-request-body`.
#[derive(Debug, Clone, PartialEq)]
pub struct RequestContext {
    pub request_id: String,
    pub method: String,
    pub remote_addr: Option<String>,
    pub headers: BTreeMap<String, Value>,
    pub query: BTreeMap<String, Value>,
    pub payload: Option<Value>,
    pub raw_body: Vec<u8>,
}

impl RequestContext {
    #[must_use]
    pub fn new(
        request_id: impl Into<String>,
        method: impl Into<String>,
        remote_addr: Option<String>,
        raw_body: Vec<u8>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            method: method.into(),
            remote_addr,
            headers: BTreeMap::new(),
            query: BTreeMap::new(),
            payload: None,
            raw_body,
        }
    }

    pub fn insert_header(&mut self, name: &str, value: impl Into<String>) {
        self.headers
            .insert(canonical_header_name(name), Value::String(value.into()));
    }

    pub fn insert_query(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.query.insert(name.into(), Value::String(value.into()));
    }

    /// Parses `raw_body` as JSON, wrapping top-level JSON arrays under `{"root": [...]}`
    /// to match `adnanh/webhook` v2.8.3 (`ParseJSONPayload`).
    pub fn parse_json_payload(&mut self) -> Result<(), HookEngineError> {
        let trimmed = self
            .raw_body
            .iter()
            .copied()
            .skip_while(u8::is_ascii_whitespace)
            .count();
        if trimmed == 0 {
            self.payload = None;
            return Ok(());
        }
        let parsed: Value = serde_json::from_slice(&self.raw_body)
            .map_err(|err| HookEngineError::PayloadParse(err.to_string()))?;
        match parsed {
            Value::Object(_) => self.payload = Some(parsed),
            Value::Array(_) => {
                let mut root = Map::new();
                root.insert("root".into(), parsed);
                self.payload = Some(Value::Object(root));
            }
            other => {
                return Err(HookEngineError::PayloadParse(format!(
                    "expected JSON object or array payload, got {}",
                    json_type_name(&other)
                )));
            }
        }
        Ok(())
    }

    /// Parses `raw_body` as `application/x-www-form-urlencoded`, preserving the first value
    /// for each key to match `adnanh/webhook` v2.8.3 (`ParseFormPayload`).
    pub fn parse_form_payload(&mut self) -> Result<(), HookEngineError> {
        let mut map = Map::new();
        for (k, v) in form_urlencoded::parse(&self.raw_body) {
            map.entry(k.into_owned())
                .or_insert_with(|| Value::String(v.into_owned()));
        }
        self.payload = Some(Value::Object(map));
        Ok(())
    }

    /// Parses `raw_body` as XML using `mxj` conventions (`-attr` for attributes, `#text` for
    /// mixed/attributed element text, arrays for repeated sibling tags).
    pub fn parse_xml_payload(&mut self) -> Result<(), HookEngineError> {
        if self.raw_body.iter().all(u8::is_ascii_whitespace) {
            self.payload = None;
            return Ok(());
        }
        let root = Element::parse(self.raw_body.as_slice()).map_err(|err| {
            HookEngineError::PayloadParse(format!("error parsing XML payload: {err}"))
        })?;
        let mut map = Map::new();
        map.insert(root.name.clone(), xml_element_to_value(&root));
        self.payload = Some(Value::Object(map));
        Ok(())
    }

    /// Parses `raw_body` as `multipart/form-data`, preserving `raw_body` intact for signature
    /// checks while enforcing `max_mem_bytes` across the stream.
    pub async fn parse_multipart_payload(
        &mut self,
        content_type: &str,
        json_params: &[ParameterSpec],
        max_mem_bytes: usize,
    ) -> Result<(), HookEngineError> {
        let boundary = multer::parse_boundary(content_type).map_err(|err| {
            HookEngineError::PayloadParse(format!("invalid multipart boundary: {err}"))
        })?;

        let stream =
            stream::once(async { Ok::<_, Infallible>(Bytes::copy_from_slice(&self.raw_body)) });
        let constraints =
            Constraints::new().size_limit(SizeLimit::new().whole_stream(max_mem_bytes as u64));
        let mut multipart = Multipart::with_constraints(stream, boundary, constraints);

        let mut payload = match self.payload.take() {
            Some(Value::Object(m)) => m,
            _ => Map::new(),
        };

        while let Some(field) = multipart.next_field().await.map_err(|err| {
            HookEngineError::PayloadParse(format!("error parsing multipart form: {err}"))
        })? {
            let Some(name) = field.name().map(str::to_owned) else {
                continue;
            };
            let is_file = field.file_name().is_some();
            if is_file {
                let forced_json = json_params
                    .iter()
                    .any(|p| p.source == ValueSource::Payload && p.name == name);
                let is_json_ct = field
                    .content_type()
                    .is_some_and(|ct| ct.essence_str().eq_ignore_ascii_case("application/json"));
                if forced_json || is_json_ct {
                    let bytes = field.bytes().await.map_err(|err| {
                        HookEngineError::PayloadParse(format!(
                            "error reading multipart file '{name}': {err}"
                        ))
                    })?;
                    if let Ok(parsed) = serde_json::from_slice::<Value>(&bytes) {
                        payload.entry(name).or_insert(parsed);
                    }
                }
            } else {
                let text = field.text().await.map_err(|err| {
                    HookEngineError::PayloadParse(format!(
                        "error reading multipart field '{name}': {err}"
                    ))
                })?;
                payload.entry(name).or_insert(Value::String(text));
            }
        }

        self.payload = Some(Value::Object(payload));
        Ok(())
    }

    /// Implements `parse-parameters-as-json`: decodes selected string parameters
    /// in-place into structured JSON values in `headers`, `query`, or `payload`.
    /// Processes all specs even if an earlier parameter fails, returning the first error if any.
    pub fn parse_parameters_as_json(
        &mut self,
        specs: &[ParameterSpec],
    ) -> Result<(), HookEngineError> {
        let mut first_err: Option<HookEngineError> = None;
        for spec in specs {
            if let Err(err) = self.parse_single_parameter_as_json(spec)
                && first_err.is_none()
            {
                first_err = Some(err);
            }
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn parse_single_parameter_as_json(
        &mut self,
        spec: &ParameterSpec,
    ) -> Result<(), HookEngineError> {
        let raw_str = extract_as_string(self, spec)?;
        let parsed: Value = serde_json::from_str(&raw_str).map_err(|err| {
            HookEngineError::PayloadParse(format!(
                "failed to parse parameter '{}' from '{}' as JSON: {err}",
                spec.name,
                spec.source.as_str()
            ))
        })?;
        match spec.source {
            ValueSource::Header => {
                let canonical = canonical_header_name(&spec.name);
                if self.headers.contains_key(&canonical) {
                    self.headers.insert(canonical, parsed);
                } else if let Some((head, tail)) = spec.name.split_once('.') {
                    let canonical_head = canonical_header_name(head);
                    if let Some(entry) = self.headers.get_mut(&canonical_head) {
                        set_dot_path(entry, tail, parsed)?;
                    } else {
                        self.headers.insert(canonical, parsed);
                    }
                } else {
                    self.headers.insert(canonical, parsed);
                }
            }
            ValueSource::Query => {
                set_map_or_dot_path(&mut self.query, &spec.name, parsed)?;
            }
            ValueSource::Payload => {
                let root = self
                    .payload
                    .get_or_insert_with(|| Value::Object(Map::new()));
                set_dot_path(root, &spec.name, parsed)?;
            }
            other => {
                return Err(HookEngineError::Extraction(format!(
                    "parse-parameters-as-json does not support source '{}'",
                    other.as_str()
                )));
            }
        }
        Ok(())
    }
}

/// Converts HTTP header names to MIME canonical form (matching Go's `textproto.CanonicalMIMEHeaderKey`).
#[must_use]
pub fn canonical_header_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper_next = true;
    for ch in name.chars() {
        if ch == '-' {
            out.push('-');
            upper_next = true;
        } else if upper_next {
            out.push(ch.to_ascii_uppercase());
            upper_next = false;
        } else {
            out.push(ch.to_ascii_lowercase());
        }
    }
    out
}

fn set_map_or_dot_path(
    map: &mut BTreeMap<String, Value>,
    path: &str,
    new_val: Value,
) -> Result<(), HookEngineError> {
    if map.contains_key(path) {
        map.insert(path.to_string(), new_val);
        return Ok(());
    }
    if let Some((head, tail)) = path.split_once('.')
        && let Some(entry) = map.get_mut(head)
    {
        return set_dot_path(entry, tail, new_val);
    }
    map.insert(path.to_string(), new_val);
    Ok(())
}

const fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn xml_element_to_value(elem: &Element) -> Value {
    let mut text = String::new();
    let mut children: BTreeMap<String, Vec<Value>> = BTreeMap::new();

    for node in &elem.children {
        match node {
            XMLNode::Text(s) | XMLNode::CData(s) => text.push_str(s),
            XMLNode::Element(child) => {
                children
                    .entry(child.name.clone())
                    .or_default()
                    .push(xml_element_to_value(child));
            }
            XMLNode::Comment(_) | XMLNode::ProcessingInstruction(_, _) => {}
        }
    }

    if elem.attributes.is_empty() && children.is_empty() {
        return Value::String(text);
    }

    let mut map = Map::new();
    for (k, v) in &elem.attributes {
        map.insert(format!("-{k}"), Value::String(v.clone()));
    }
    for (name, mut vals) in children {
        let entry = if vals.len() == 1 {
            vals.pop().unwrap_or(Value::Null)
        } else {
            Value::Array(vals)
        };
        map.insert(name, entry);
    }
    if !text.trim().is_empty() {
        map.insert("#text".into(), Value::String(text));
    }
    Value::Object(map)
}
