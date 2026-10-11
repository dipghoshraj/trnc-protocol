use std::fs::File;
use std::io::{self, BufReader, Error, ErrorKind};
use rustls_pemfile::{certs, pkcs8_private_keys};

use rustls_pki_types::{CertificateDer, PrivateKeyDer};

pub fn load_certs(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(File::open(path)?);
    certs(&mut reader)
        .map(|cert| cert.map(CertificateDer::from))
        .collect()
}

pub fn load_private_key(path: &str) -> io::Result<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(File::open(path)?);
    pkcs8_private_keys(&mut reader)
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "no PKCS#8 private key found"))?
        .map(PrivateKeyDer::from)
}