//! Payload of the initiator `Hello` frame: the node credential and a session-bound proof.
//!
//! Wire layout: `u32 BE credential_len | credential | u16 BE proof_len | proof`.

use rustls::ConnectionCommon;
use uuid::Uuid;

use super::credential::NodeCredential;
use crate::errors::TransportError;

/// TLS exporter label used to derive the session secret that the initiator signs.
pub const AUTH_LABEL: &[u8] = b"trench-peer-auth";
/// Length of the exported session secret.
pub const SESSION_SECRET_LEN: usize = 32;

const MAX_CREDENTIAL_LEN: usize = 4096;
const MAX_PROOF_LEN: usize = 256;

/// Derives the session secret for a TLS connection.
///
/// `context_node_id` is the acceptor's node id. The initiator takes it from the verified server
/// credential and the acceptor uses its own, so both sides derive the same secret.
pub fn session_secret<D>(
    conn: &ConnectionCommon<D>,
    context_node_id: &Uuid,
) -> Result<[u8; SESSION_SECRET_LEN], TransportError> {
    let mut secret = [0u8; SESSION_SECRET_LEN];
    conn.export_keying_material(&mut secret, AUTH_LABEL, Some(context_node_id.as_bytes()))
        .map_err(TransportError::TlsError)?;
    Ok(secret)
}

pub fn encode_hello(credential: &NodeCredential, proof: &[u8]) -> Result<Vec<u8>, TransportError> {
    let credential_bytes = credential.encode()?;
    if credential_bytes.len() > MAX_CREDENTIAL_LEN {
        return Err(TransportError::InvalidCredential("credential too large".into()));
    }
    if proof.len() > MAX_PROOF_LEN {
        return Err(TransportError::InvalidCredential("auth proof too large".into()));
    }

    let mut out = Vec::with_capacity(6 + credential_bytes.len() + proof.len());
    out.extend_from_slice(&(credential_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&credential_bytes);
    out.extend_from_slice(&(proof.len() as u16).to_be_bytes());
    out.extend_from_slice(proof);
    Ok(out)
}

pub fn decode_hello(payload: &[u8]) -> Result<(NodeCredential, Vec<u8>), TransportError> {
    let (credential_len, rest) = split_len(payload, 4)?;
    let credential_len = credential_len as usize;
    if credential_len > MAX_CREDENTIAL_LEN || rest.len() < credential_len {
        return Err(malformed("credential length out of range"));
    }
    let (credential_bytes, rest) = rest.split_at(credential_len);

    let (proof_len, rest) = split_len(rest, 2)?;
    let proof_len = proof_len as usize;
    if proof_len > MAX_PROOF_LEN || rest.len() != proof_len {
        return Err(malformed("proof length does not match payload"));
    }

    let credential = NodeCredential::decode(credential_bytes)?;
    Ok((credential, rest.to_vec()))
}

/// Reads a big-endian length prefix of `width` bytes (2 or 4) and returns it with the remainder.
fn split_len(data: &[u8], width: usize) -> Result<(u64, &[u8]), TransportError> {
    if data.len() < width {
        return Err(malformed("Hello payload truncated"));
    }
    let (head, rest) = data.split_at(width);
    let value = head.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b));
    Ok((value, rest))
}

fn malformed(msg: &str) -> TransportError {
    TransportError::InvalidCredential(format!("malformed Hello payload: {msg}"))
}
