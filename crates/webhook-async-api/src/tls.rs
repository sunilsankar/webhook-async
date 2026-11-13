use std::path::Path;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub fn create_tls_acceptor(
    cert_path: Option<&Path>,
    key_path: Option<&Path>,
) -> Result<TlsAcceptor, Box<dyn std::error::Error + Send + Sync>> {
    let (certs, key) = match (cert_path, key_path) {
        (Some(cp), Some(kp)) if cp.exists() && kp.exists() => {
            let cert_file = std::fs::File::open(cp)?;
            let mut cert_reader = std::io::BufReader::new(cert_file);
            let certs: Vec<CertificateDer<'static>> =
                rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;

            let key_file = std::fs::File::open(kp)?;
            let mut key_reader = std::io::BufReader::new(key_file);
            let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no private key found in key file",
                )
            })?;

            (certs, key)
        }
        _ => {
            // Upstream adnanh/webhook self-signed fallback when cert/key do not exist
            let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
            let certified_key = rcgen::generate_simple_self_signed(subject_alt_names)?;
            let cert_der = certified_key.cert.der().to_vec();
            let key_der = certified_key.signing_key.serialize_der();
            let cert = CertificateDer::from(cert_der);
            let key = PrivateKeyDer::Pkcs8(key_der.into());
            (vec![cert], key)
        }
    };

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

pub struct TlsListener<L> {
    inner: L,
    acceptor: TlsAcceptor,
}

impl<L> TlsListener<L> {
    pub fn new(inner: L, acceptor: TlsAcceptor) -> Self {
        Self { inner, acceptor }
    }
}

impl<L> axum::serve::Listener for TlsListener<L>
where
    L: axum::serve::Listener,
{
    type Io = tokio_rustls::server::TlsStream<L::Io>;
    type Addr = L::Addr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = self.inner.accept().await;
            match self.acceptor.accept(stream).await {
                Ok(tls_stream) => return (tls_stream, addr),
                Err(e) => {
                    tracing::debug!("tls handshake error: {e}");
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_self_signed_acceptor_fallback() {
        let acceptor = create_tls_acceptor(None, None).expect("self-signed acceptor");
        let _ = acceptor;
    }
}
