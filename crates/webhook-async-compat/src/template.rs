use crate::CompatError;
use minijinja::{Environment, Value};
use std::path::Path;

pub fn render_template(input: &str) -> Result<String, CompatError> {
    let mut env = Environment::new();

    env.add_function(
        "getenv",
        |name: String, default_val: Option<String>| -> String {
            match std::env::var(&name) {
                Ok(v) if !v.is_empty() => v,
                _ => default_val.unwrap_or_default(),
            }
        },
    );

    env.add_function("cat", |path: String| -> String {
        std::fs::read_to_string(&path)
            .map(|s| s.trim_end_matches(['\r', '\n']).to_string())
            .unwrap_or_default()
    });

    env.add_function("credential", |name: String| -> String {
        if let Ok(dir) = std::env::var("CREDENTIALS_DIRECTORY") {
            let p = Path::new(&dir).join(&name);
            if let Ok(s) = std::fs::read_to_string(p) {
                return s.trim_end_matches(['\r', '\n']).to_string();
            }
        }
        std::env::var(&name).unwrap_or_default()
    });

    env.add_filter("js", |val: Value| -> Value {
        if let Some(s) = val.as_str() {
            Value::from(serde_json::to_string(s).unwrap_or_else(|_| format!("\"{}\"", s)))
        } else {
            let json_str = serde_json::to_string(&val).unwrap_or_default();
            Value::from(json_str)
        }
    });

    let tmpl = env
        .template_from_str(input)
        .map_err(|e| CompatError::Template {
            details: e.to_string(),
        })?;

    tmpl.render(minijinja::context!())
        .map_err(|e| CompatError::Template {
            details: e.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_getenv_and_js_filter() {
        // SAFETY: single-threaded test context
        unsafe { std::env::set_var("TEST_SECRET_VAR", "my-secret-key-123") };
        let tmpl = r#"{"secret": {{ getenv("TEST_SECRET_VAR") | js }}}"#;
        let rendered = render_template(tmpl).unwrap();
        assert_eq!(rendered, r#"{"secret": "my-secret-key-123"}"#);
    }

    #[test]
    fn test_cat_function() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "file-token-456").unwrap();
        let path = tmp.path().to_str().unwrap();

        let tmpl = format!(r#"{{"token": {{{{ cat("{}") | js }}}}}}"#, path);
        let rendered = render_template(&tmpl).unwrap();
        assert_eq!(rendered, r#"{"token": "file-token-456"}"#);
    }

    #[test]
    fn test_credential_function_fallback() {
        unsafe { std::env::set_var("MY_CRED", "cred-from-env") };
        let tmpl = r#"{"cred": {{ credential("MY_CRED") | js }}}"#;
        let rendered = render_template(tmpl).unwrap();
        assert_eq!(rendered, r#"{"cred": "cred-from-env"}"#);
    }
}
