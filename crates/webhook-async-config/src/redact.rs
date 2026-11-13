/// Masks credentials in connection URLs (e.g. `redis://user:pass@host:6379` ->
/// `redis://***@host:6379`) so secrets are never written to logs or Debug output.
#[must_use]
pub fn redact_url_credentials(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let Some((_userinfo, host_and_path)) = rest.split_once('@') else {
        return url.to_string();
    };
    format!("{scheme}://***@{host_and_path}")
}
