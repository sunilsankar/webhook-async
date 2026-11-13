use std::collections::BTreeMap;

use serde_json::{Map, Value};
use webhook_async_core::{ParameterSpec, ValueSource};

use crate::HookEngineError;
use crate::context::{RequestContext, canonical_header_name};

pub fn extract_value(ctx: &RequestContext, spec: &ParameterSpec) -> Result<Value, HookEngineError> {
    match spec.source {
        ValueSource::Header => {
            let canonical = canonical_header_name(&spec.name);
            if let Some(exact) = ctx.headers.get(&canonical) {
                return Ok(exact.clone());
            }
            if let Some((head, tail)) = spec.name.split_once('.') {
                let canonical_head = canonical_header_name(head);
                if let Some(sub) = ctx.headers.get(&canonical_head)
                    && let Some(found) = lookup_dot_path(sub, tail)
                {
                    return Ok(found);
                }
            }
            Err(HookEngineError::ParameterMissing {
                value_source: ValueSource::Header,
                name: spec.name.clone(),
            })
        }
        ValueSource::Query => {
            lookup_in_btree(&ctx.query, &spec.name, ValueSource::Query, &spec.name)
        }
        ValueSource::Payload => {
            let root = ctx
                .payload
                .as_ref()
                .ok_or_else(|| HookEngineError::ParameterMissing {
                    value_source: ValueSource::Payload,
                    name: spec.name.clone(),
                })?;
            lookup_dot_path(root, &spec.name).ok_or_else(|| HookEngineError::ParameterMissing {
                value_source: ValueSource::Payload,
                name: spec.name.clone(),
            })
        }
        ValueSource::RawRequestBody => Ok(Value::String(
            String::from_utf8_lossy(&ctx.raw_body).into_owned(),
        )),
        ValueSource::Request => match spec.name.to_ascii_lowercase().as_str() {
            "remote-addr" => ctx.remote_addr.clone().map(Value::String).ok_or_else(|| {
                HookEngineError::ParameterMissing {
                    value_source: ValueSource::Request,
                    name: spec.name.clone(),
                }
            }),
            "method" => Ok(Value::String(ctx.method.clone())),
            _ => Err(HookEngineError::ParameterMissing {
                value_source: ValueSource::Request,
                name: spec.name.clone(),
            }),
        },
        ValueSource::StringLiteral => Ok(Value::String(spec.name.clone())),
        ValueSource::EntirePayload => Ok(ctx
            .payload
            .clone()
            .unwrap_or_else(|| Value::Object(Map::new()))),
        ValueSource::EntireHeaders => Ok(btree_to_value(&ctx.headers)),
        ValueSource::EntireQuery => Ok(btree_to_value(&ctx.query)),
    }
}

pub fn extract_as_string(
    ctx: &RequestContext,
    spec: &ParameterSpec,
) -> Result<String, HookEngineError> {
    let val = extract_value(ctx, spec)?;
    value_to_string(&val, spec.source, &spec.name)
}

fn value_to_string(
    val: &Value,
    source: ValueSource,
    name: &str,
) -> Result<String, HookEngineError> {
    match val {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(val).map_err(|err| HookEngineError::Extraction(err.to_string()))
        }
        Value::Null => Err(HookEngineError::ParameterMissing {
            value_source: source,
            name: name.to_string(),
        }),
    }
}

fn lookup_in_btree(
    map: &BTreeMap<String, Value>,
    path: &str,
    source: ValueSource,
    original_name: &str,
) -> Result<Value, HookEngineError> {
    if let Some(exact) = map.get(path) {
        return Ok(exact.clone());
    }
    if let Some((head, tail)) = path.split_once('.')
        && let Some(sub) = map.get(head)
        && let Some(found) = lookup_dot_path(sub, tail)
    {
        return Ok(found);
    }
    Err(HookEngineError::ParameterMissing {
        value_source: source,
        name: original_name.to_string(),
    })
}

/// Traverses `root` using dot notation while checking literal keys (`"a.b"`) before
/// splitting on `.`, and supporting non-negative integer indices for JSON arrays.
#[must_use]
pub fn lookup_dot_path(root: &Value, path: &str) -> Option<Value> {
    if path.is_empty() {
        return Some(root.clone());
    }
    if let Value::Object(obj) = root
        && let Some(literal) = obj.get(path)
    {
        return Some(literal.clone());
    }
    let (head, tail) = match path.split_once('.') {
        Some((h, t)) => (h, Some(t)),
        None => (path, None),
    };

    let next = match root {
        Value::Object(obj) => obj.get(head)?,
        Value::Array(arr) => {
            let idx: usize = head.parse().ok()?;
            arr.get(idx)?
        }
        _ => return None,
    };

    match tail {
        Some(rest) => lookup_dot_path(next, rest),
        None => Some(next.clone()),
    }
}

pub(crate) fn set_dot_path(
    root: &mut Value,
    path: &str,
    new_val: Value,
) -> Result<(), HookEngineError> {
    if let Value::Object(obj) = root {
        if obj.contains_key(path) {
            obj.insert(path.to_string(), new_val);
            return Ok(());
        }
        if let Some((head, tail)) = path.split_once('.') {
            if let Some(child) = obj.get_mut(head) {
                return set_dot_path(child, tail, new_val);
            }
            return Err(HookEngineError::ParameterMissing {
                value_source: ValueSource::Payload,
                name: path.to_string(),
            });
        }
        obj.insert(path.to_string(), new_val);
        return Ok(());
    }
    if let Value::Array(arr) = root
        && let Some((head, tail)) = path
            .split_once('.')
            .map_or_else(|| Some((path, None)), |(h, t)| Some((h, Some(t))))
        && let Ok(idx) = head.parse::<usize>()
        && let Some(slot) = arr.get_mut(idx)
    {
        return match tail {
            Some(rest) => set_dot_path(slot, rest, new_val),
            None => {
                *slot = new_val;
                Ok(())
            }
        };
    }

    Err(HookEngineError::ParameterMissing {
        value_source: ValueSource::Payload,
        name: path.to_string(),
    })
}

fn btree_to_value(map: &BTreeMap<String, Value>) -> Value {
    let mut obj = Map::new();
    for (k, v) in map {
        obj.insert(k.clone(), v.clone());
    }
    Value::Object(obj)
}
