use webhook_async_core::{JobId, JobState};

/// Hash-tagged key layout (`{prefix}:...`) so every key for an instance shares one hash slot
/// on Redis/Valkey Cluster while acting as normal keys on standalone servers.
#[derive(Debug, Clone)]
pub(crate) struct KeyLayout {
    tag: String,
}

impl KeyLayout {
    pub(crate) fn new(prefix: &str) -> Self {
        let cleaned = prefix.trim();
        let base = if cleaned.is_empty() {
            "webhook_async"
        } else {
            cleaned
        };
        Self {
            tag: format!("{{{base}}}"),
        }
    }

    pub(crate) fn job_prefix(&self) -> String {
        format!("{}:job:", self.tag)
    }

    pub(crate) fn job(&self, id: JobId) -> String {
        format!("{}:job:{id}", self.tag)
    }

    pub(crate) fn attempts(&self, id: JobId) -> String {
        format!("{}:attempts:{id}", self.tag)
    }

    pub(crate) fn idempotency(&self, hook_id: &str, key: &str) -> String {
        format!(
            "{}:idemp:{}:{}",
            self.tag,
            hex_encode(hook_id.as_bytes()),
            hex_encode(key.as_bytes())
        )
    }

    pub(crate) fn idempotency_unused(&self) -> String {
        format!("{}:idemp:_unused", self.tag)
    }

    pub(crate) fn priorities(&self) -> String {
        format!("{}:priorities", self.tag)
    }

    pub(crate) fn priority_queue_prefix(&self) -> String {
        format!("{}:q:", self.tag)
    }

    pub(crate) fn priority_queue(&self, priority: i32) -> String {
        format!("{}:q:{priority}", self.tag)
    }

    pub(crate) fn retry_wait(&self) -> String {
        format!("{}:retry_wait", self.tag)
    }

    pub(crate) fn leases(&self) -> String {
        format!("{}:leases", self.tag)
    }

    pub(crate) fn all_jobs(&self) -> String {
        format!("{}:all_jobs", self.tag)
    }

    pub(crate) fn state(&self, state: JobState) -> String {
        format!("{}:state:{}", self.tag, state.as_str())
    }

    pub(crate) fn hook(&self, hook_id: &str) -> String {
        format!("{}:hook:{}", self.tag, hex_encode(hook_id.as_bytes()))
    }
}

/// Translates `valkey://` and `valkeys://` URLs to `redis://` and `rediss://` for the shared
/// RESP client while preserving credentials, host, port, db, and query parameters intact.
pub(crate) fn normalize_resp_url(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("valkey://") {
        format!("redis://{rest}")
    } else if let Some(rest) = url.strip_prefix("valkeys://") {
        format!("rediss://{rest}")
    } else {
        url.to_string()
    }
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{KeyLayout, normalize_resp_url};

    #[test]
    fn translates_valkey_schemes_for_resp_client() {
        assert_eq!(
            normalize_resp_url("valkey://:secret@127.0.0.1:6379/0"),
            "redis://:secret@127.0.0.1:6379/0"
        );
        assert_eq!(
            normalize_resp_url("valkeys://cache.internal:6380/2"),
            "rediss://cache.internal:6380/2"
        );
        assert_eq!(
            normalize_resp_url("redis://127.0.0.1:6379/0"),
            "redis://127.0.0.1:6379/0"
        );
    }

    #[test]
    fn key_layout_wraps_prefix_in_cluster_hash_tag() {
        let layout = KeyLayout::new("prod_hooks");
        assert_eq!(layout.all_jobs(), "{prod_hooks}:all_jobs");
        assert_eq!(layout.retry_wait(), "{prod_hooks}:retry_wait");
    }
}
