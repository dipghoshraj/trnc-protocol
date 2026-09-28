use std::fs::File;
use std::io::BufReader;
use rustls_pemfile::{certs, pkcs8_private_keys};

use rustls_pki_types::{CertificateDer, PrivateKeyDer};


pub fn load_certs(path: &str) -> Vec<CertificateDer<'static>> {
    let certfile = File::open(path).expect("cannot open certificate file");
    let mut reader = BufReader::new(certfile);
    certs(&mut reader)
        .map(|cert| cert.expect("failed to parse certs"))
        .map(CertificateDer::from)
        .collect()
}

pub fn load_private_key(path: &str) -> PrivateKeyDer<'static> {
    let keyfile = File::open(path).expect("cannot open private key file");
    let mut reader = BufReader::new(keyfile);
    let mut keys = pkcs8_private_keys(&mut reader)
        .map(|cert| cert.expect("malformed private key"))
        .map(PrivateKeyDer::from)
        .into_iter()
        .collect::<Vec<_>>();
    if keys.is_empty() {
        panic!("no private key found");
    }
    keys.remove(0)
}