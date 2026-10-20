use std::{
    error::Error,
    fs::File,
    io::BufReader,
    path::Path,
};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub type BoxError = Box<dyn Error>;

pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, BoxError> {
    let mut reader = open(path)?;
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!("{}: no certificates found", path.display()).into());
    }
    Ok(certs)
}

pub fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, BoxError> {
    let mut reader = open(path)?;
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("{}: {e}", path.display()))?
        .ok_or_else(|| format!("{}: no private key found", path.display()).into())
}

fn open(path: &Path) -> Result<BufReader<File>, BoxError> {
    let file = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(BufReader::new(file))
}
