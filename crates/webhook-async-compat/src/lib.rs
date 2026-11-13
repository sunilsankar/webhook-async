//! Upstream `adnanh/webhook` v2.8.3 compatibility config parser and defaults.

pub mod error;
pub mod parser;
pub mod raw;
pub mod template;

pub use error::CompatError;
pub use parser::{
    HookFileFormat, parse_hooks_file, parse_hooks_file_templated, parse_hooks_files,
    parse_hooks_files_templated, parse_hooks_json, parse_hooks_str, parse_hooks_yaml,
};
pub use template::render_template;

/// Upstream-compatible server defaults documented for `adnanh/webhook` v2.8.3.
pub struct UpstreamDefaults;

impl UpstreamDefaults {
    pub const IP: &'static str = "0.0.0.0";
    pub const PORT: u16 = 9000;
    pub const URL_PREFIX: &'static str = "hooks";
    pub const HOOKS_FILE: &'static str = "hooks.json";
    pub const CERT_FILE: &'static str = "cert.pem";
    pub const KEY_FILE: &'static str = "key.pem";
    pub const TLS_MIN_VERSION: &'static str = "1.2";
    pub const MAX_MULTIPART_MEM_BYTES: usize = 1_048_576;
}
