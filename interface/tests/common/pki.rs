//! Throwaway CA and leaf certificates generated in memory for TLS tests.
//! Nothing here is written to disk.

use interface::tls::{TlsClientProvider, TlsServerProvider};
use rcgen::{
    date_time_ymd, BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, PublicKeyData,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use sha2::{Digest, Sha256};

/// Validity window as (year, month, day) pairs.
#[derive(Clone, Copy)]
pub struct Window {
    pub from: (i32, u8, u8),
    pub until: (i32, u8, u8),
}

pub const CURRENT: Window = Window { from: (2020, 1, 1), until: (2099, 1, 1) };
pub const EXPIRED: Window = Window { from: (2000, 1, 1), until: (2001, 1, 1) };
pub const NOT_YET_VALID: Window = Window { from: (2090, 1, 1), until: (2099, 1, 1) };

#[derive(Clone)]
pub struct Leaf {
    pub cert: CertificateDer<'static>,
    /// PKCS#8 DER bytes; converted to `PrivateKeyDer` when a provider is built.
    pub key_pkcs8: Vec<u8>,
    /// SHA-256 of the SubjectPublicKeyInfo, the value `--pin` expects.
    pub spki_sha256: [u8; 32],
}

pub struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    pub cert: CertificateDer<'static>,
}

fn apply_window(params: &mut CertificateParams, window: Window) {
    let (y, m, d) = window.from;
    params.not_before = date_time_ymd(y, m, d);
    let (y, m, d) = window.until;
    params.not_after = date_time_ymd(y, m, d);
}

fn leaf_params(names: &[&str], window: Window) -> CertificateParams {
    let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
    let mut params = CertificateParams::new(names.clone()).expect("valid SANs");
    params.distinguished_name.push(DnType::CommonName, names[0].as_str());
    apply_window(&mut params, window);
    params
}

fn leaf_from(cert_der: &CertificateDer<'_>, key: &KeyPair) -> Leaf {
    Leaf {
        cert: cert_der.clone().into_owned(),
        key_pkcs8: key.serialize_der(),
        spki_sha256: Sha256::digest(key.subject_public_key_info()).into(),
    }
}

impl TestCa {
    pub fn new(common_name: &str) -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("empty SANs");
        params.distinguished_name.push(DnType::CommonName, common_name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        apply_window(&mut params, CURRENT);

        let key = KeyPair::generate().expect("CA key");
        let cert = params.self_signed(&key).expect("self-signed CA");
        Self {
            cert: cert.der().clone(),
            issuer: Issuer::new(params, key),
        }
    }

    /// Issues a leaf for `names` (DNS names or IP literals) signed by this CA.
    pub fn issue_leaf(&self, names: &[&str], window: Window) -> Leaf {
        let key = KeyPair::generate().expect("leaf key");
        let cert = leaf_params(names, window)
            .signed_by(&key, &self.issuer)
            .expect("CA-signed leaf");
        leaf_from(cert.der(), &key)
    }
}

/// A leaf that is signed by its own key rather than by any CA.
pub fn self_signed_leaf(names: &[&str], window: Window) -> Leaf {
    let key = KeyPair::generate().expect("leaf key");
    let cert = leaf_params(names, window)
        .self_signed(&key)
        .expect("self-signed leaf");
    leaf_from(cert.der(), &key)
}

pub fn server_provider(leaf: &Leaf) -> TlsServerProvider {
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(leaf.key_pkcs8.clone()));
    TlsServerProvider::new(vec![leaf.cert.clone()], key)
        .expect("server TLS config")
}

pub fn client_provider(trusted: &[&TestCa], pin: Option<[u8; 32]>) -> TlsClientProvider {
    let roots = trusted.iter().map(|ca| ca.cert.clone()).collect();
    TlsClientProvider::new(roots, pin).expect("client TLS config")
}
