# TLS Implementation Plan for TRNC

> Goal: add TLS 1.3 to the `transport` crate so that TRNC connections are encrypted **and** the peer is authenticated using the network-signed credential model from [`doc/communication/trust.md`](../communication/trust.md). We are **not** building a traditional Web-PKI/mTLS system; we are building a trust model that prevents TLS termination and MITM attacks by binding the TLS session to a stable node identity.

---

## 1. What we are protecting against

| Attack | How it would look | What stops it in this design |
|--------|-------------------|------------------------------|
| **Passive eavesdrop** | Attacker reads frames on the wire | TLS 1.3 encryption |
| **TLS termination / MITM proxy** | Load balancer or malicious node terminates TLS and presents its own cert | Client validates the server's `NodeCredential` against trusted issuers or the local trust cache; a proxy cannot produce a credential signed by a trusted network issuer for the target node |
| **Impersonating an initiator** | Attacker opens a TLS connection to a node and claims to be Node A | Acceptor requires the initiator to present a `NodeCredential` **plus** a session-bound signature in the `Hello` frame; the signature needs Node A's private key |
| **Credential replay** | Attacker replays a stolen credential without the private key | The `Hello` auth proof is signed over TLS exported keying material, so it is bound to this exact TLS session and cannot be replayed onto another connection |
| **Downgrade** | Attacker forces TLS 1.2 or weak ciphers | `rustls` config rejects anything older than TLS 1.3 |

---

## 2. High-level design

```text
Application
     │
     ▼
StreamManager<T>          ── Hello (with credential + proof)
     │
     ▼
Connection<T>             ── TRNC frames
     │
     ▼
TlsStream<TcpStream>      ── TLS 1.3 record layer
     │
     ▼
TcpStream
```

1. **Server-side TLS**: the acceptor presents an X.509 certificate whose public key is the node's transport key. The certificate embeds the node's signed `NodeCredential` in a custom X.509 extension.
2. **Client-side TLS verification**: the initiator uses a custom `rustls` `ServerCertVerifier`. It extracts the credential from the certificate, checks it against the local trust cache or trusted issuer set, verifies expiry/revocation, and extracts the peer's `node_id`.
3. **Application-level initiator authentication**: because we are not using client certificates, the initiator sends its credential and a session-bound signature inside the existing `Hello` frame on stream 0. The acceptor validates the credential and the signature before sending `Welcome`.
4. **Trust propagation**: after a credential is validated once against a trusted issuer, it is stored in the local trust cache. Future connections to that node can be validated from the cache without re-contacting an issuer.

---

## 3. New types and modules

Introduce a new module tree under `transport/src/tls/`:

```text
transport/src/tls/
├── mod.rs                 # public exports
├── pem.rs                 # keep existing cert/key PEM loading
├── server_config.rs       # build ServerConfig (rename from current client.rs)
├── client_config.rs       # build ClientConfig with custom verifier
├── credential.rs          # NodeCredential, PublicKey, signature/verify
├── verifier.rs            # NetworkCredentialVerifier (ServerCertVerifier)
├── trust_cache.rs         # TrustCache trait + in-memory impl
└── identity.rs            # PeerIdentity extracted after handshake
```

### 3.1 `NodeCredential`

A signed statement that binds a stable node identity to a public key.

```rust
pub struct NodeCredential {
    pub node_id: Uuid,              // stable node identity (must be UUID v4)
    pub public_key: PublicKey,      // the node's long-term transport public key
    pub issued_at: u64,             // seconds since UNIX epoch
    pub expires_at: u64,            // seconds since UNIX epoch
    pub issuer_id: Uuid,            // the node/network signer that issued this credential
    pub signature: Vec<u8>,         // signature over the canonical bytes above
}

pub struct PublicKey {
    pub algorithm: KeyAlgorithm,    // e.g. Ed25519, ECDSA_P256
    pub bytes: Vec<u8>,
}
```

- The canonical signed payload is a deterministic encoding of all fields except `signature` (e.g. `node_id || algorithm || public_key || issued_at || expires_at || issuer_id`).
- The credential is signed by an issuer's signing key, **not** by the node itself.
- A bootstrap node may self-issue its own credential.

### 3.2 `PeerIdentity`

Extracted from the verified peer credential and stored on the connection.

```rust
pub struct PeerIdentity {
    pub node_id: Uuid,
    pub cert_fingerprint: [u8; 32], // SHA-256 of DER-encoded TLS certificate
}
```

`Connection<T>` and `StreamManager<T>` gain an optional `peer_identity: Option<PeerIdentity>` so the application layer can log and assert who is on the other end.

### 3.3 `TrustCache`

```rust
pub trait TrustCache: Send + Sync {
    fn is_trusted(&self, node_id: &Uuid) -> bool;
    fn get(&self, node_id: &Uuid) -> Option<NodeCredential>;
    fn insert(&self, credential: NodeCredential);
    fn issuers(&self) -> Vec<PublicKey>; // trusted network signers
}
```

- Default implementation: `InMemoryTrustCache` backed by a `RwLock<HashMap<Uuid, NodeCredential>>`.
- Persistence and revocation are out of scope for the first milestone but the interface must leave room for both.

---

## 4. TLS configuration

### 4.1 Server config (`tls/server_config.rs`)

- TLS 1.3 only.
- `with_no_client_auth()`. We do **not** request client certificates; initiator authentication happens inside the TRNC `Hello` frame.
- `with_single_cert(certs, key)` where the end-entity certificate contains the node's credential extension.

```rust
pub fn build_server_config(
    cert_path: &str,
    key_path: &str,
) -> Arc<ServerConfig> { ... }
```

> Note: the existing [`transport/src/tls/client.rs`](../../transport/src/tls/client.rs) actually builds a `ServerConfig` and should be renamed to `server_config.rs`.

### 4.2 Client config (`tls/client_config.rs`)

- TLS 1.3 only.
- Uses a custom `ServerCertVerifier` that performs credential-based verification instead of Web-PKI.
- The verifier needs a reference to the `TrustCache` so it can check cached credentials and trusted issuers.

```rust
pub fn build_client_config(
    trust_cache: Arc<dyn TrustCache>,
) -> Arc<ClientConfig> { ... }
```

---

## 5. Custom certificate verification

Implement `rustls::client::danger::ServerCertVerifier` in `tls/verifier.rs`.

For every server certificate chain presented during a TLS handshake:

1. **Structural checks**
   - Exactly one end-entity certificate.
   - Certificate is not expired.
   - Valid for TLS server authentication.

2. **Extract credential**
   - Parse the end-entity certificate with `x509-parser`.
   - Read the custom Trench credential extension (OID TBD).
   - Deserialize the extension bytes into a `NodeCredential`.

3. **Validate credential**
   - `node_id` is a valid UUID v4.
   - Credential has not expired.
   - Either:
     - the credential is in the local trust cache and matches exactly, **or**
     - the credential's signature verifies against one of the trusted issuer public keys from the cache.
   - The certificate's Subject Public Key Info (SPKI) matches the `public_key` inside the credential.

4. **Return**
   - `ServerCertVerified::assertion()` on success.
   - `rustls::Error` on failure, which becomes `TransportError::TlsError`.
   - The extracted `PeerIdentity` is captured and stored on the connection via the `StreamManager`.

This guarantees that the initiator's TLS session terminates at the node that owns the credential; a proxy with a different certificate cannot satisfy the credential check.

---

## 6. Initiator authentication without client certs

After the TLS handshake, the TRNC handshake on stream 0 authenticates the initiator to the acceptor.

### 6.1 Extended `Hello` payload

The current `Hello` frame has no payload. Extend it with:

```rust
pub struct Hello {
    pub version: u8,
    pub min_version: u8,
    pub capabilities: u32,
    pub max_frame_size: u32,
    pub initial_window: u32,

    // new fields
    pub node_id: Uuid,
    pub credential: NodeCredential,
    pub auth_proof: Vec<u8>, // signature over exported TLS keying material
}
```

### 6.2 `auth_proof` computation

Both sides call `rustls::Connection::export_keying_material` after the TLS handshake:

```rust
const LABEL: &str = "trench-peer-auth";
let context = peer_node_id.as_bytes();
let mut secret = [0u8; 32];
conn.export_keying_material(&mut secret, LABEL.as_bytes(), Some(context))?;
auth_proof = signing_key.sign(&secret);
```

- The initiator signs the exported secret.
- The acceptor verifies the signature with the public key from the initiator's credential.
- Because the secret is unique to this TLS session, a replayed credential from another session will fail signature verification.

### 6.3 Acceptor `Hello` handling

In `StreamManager::handle_hello`:

1. Parse the `Hello` payload.
2. Verify the `NodeCredential` the same way the custom verifier does (cache or trusted issuer).
3. Recompute the exported keying material and verify `auth_proof`.
4. If any step fails, call `fail_handshake` with `ErrorCode::HandshakeRejected` and close the TLS connection.
5. On success, store `PeerIdentity` and send `Welcome`.

---

## 7. Wiring into the existing connection path

### 7.1 `ResilientClient` ([`transport/src/client/resilient_client.rs`](../../transport/src/client/resilient_client.rs))

```rust
pub async fn build_stream(&mut self) -> Result<(), Box<dyn std::error::Error>> {
    let tcp = TcpStream::connect(self.sockaddr).await?;
    let tls_stream = TlsConnector::from(self.client_config.clone())
        .connect(self.server_name.clone(), tcp)
        .await?;
    self.tcp_stream = Some(tls_stream.into_inner().0); // or keep TlsStream wrapper
    Ok(())
}
```

Better: keep the `TlsStream` as the inner stream of `Connection<T>`, because `TlsStream<TcpStream>` implements `AsyncRead + AsyncWrite + Unpin`.

### 7.2 `ResilientServer` ([`transport/src/server/resilient_server.rs`](../../transport/src/server/resilient_server.rs))

```rust
let (tcp, peer) = listener.accept().await?;
let tls_stream = self.tls_acceptor.accept(tcp).await?;
let mut manager = StreamManager::new(Connection::new(tls_stream), Role::Acceptor);
```

### 7.3 `Connection<T>` and `StreamManager<T>`

No API change is needed for `Connection<T>` because `TlsStream` satisfies the same `AsyncRead + AsyncWrite + Unpin` bounds as `TcpStream` ([`transport/src/tcp/connection.rs`](../../transport/src/tcp/connection.rs) §8 already anticipates this).

`StreamManager<T>`:

- Stores `peer_identity: Option<PeerIdentity>`.
- `start_handshake` (initiator) serializes the extended `Hello` including credential and auth proof.
- `handle_hello` (acceptor) validates the extended `Hello`.

### 7.4 `TransportError`

Uncomment and use the existing `TlsError(rustls::Error)` variant in [`transport/src/errors.rs`](../../transport/src/errors.rs). Add:

```rust
UntrustedPeer(Uuid),
InvalidCredential(String),
CredentialExpired(Uuid),
```

---

## 8. Configuration file additions

`config.trench` and the node config parser should accept:

```ini
# Transport security
TlsCertPath=/etc/trench/node.crt
TlsKeyPath=/etc/trench/node.key
TrustedIssuerKeys=/etc/trench/issuers/
TrustCachePath=/var/lib/trench/trust_cache
CredentialPath=/etc/trench/node.credential
```

- `TrustedIssuerKeys`: directory of issuer public key files used for first-time validation.
- `TrustCachePath`: persisted validated peer credentials.
- `CredentialPath`: this node's signed credential to send in `Hello`.

---

## 9. Implementation phases

### Phase 0 — Cleanup
- Rename [`transport/src/tls/client.rs`](../../transport/src/tls/client.rs) to `server_config.rs` and add `client_config.rs`.
- Add missing dependencies: `x509-parser`, a signature crate (`ed25519-dalek` or `p256`), `uuid` if not already available.

### Phase 1 — Credential primitives
- Implement `NodeCredential`, `PublicKey`, signing, verification, canonical encoding.
- Implement `InMemoryTrustCache`.
- Add unit tests for sign/verify and cache lookup.

### Phase 2 — One-way TLS + custom verifier
- Build server config that loads the credential-bearing certificate.
- Build client config with `NetworkCredentialVerifier`.
- Update `TransportError`.
- Test: a server with a credential signed by a trusted issuer is accepted; an untrusted self-signed cert is rejected.

### Phase 3 — Application-level mutual authentication
- Extend `Hello` / `handle_hello` with credential and `auth_proof`.
- Use `export_keying_material` to bind the proof to the session.
- Test: an initiator with a valid credential and correct proof is accepted; a replayed or unsigned credential is rejected.

### Phase 4 — Bootstrap/cache integration
- Persist and load `TrustCache` from disk.
- Add the bootstrap admission flow (new node requests credential from bootstrap peer).
- Test bootstrap -> peer connection -> cache-only validation.

### Phase 5 — Integration and hardening
- Wire TLS into `ResilientClient` and `ResilientServer`.
- Add operational logging of `PeerIdentity` after handshake.
- Add revocation hooks to `TrustCache`.
- Update [`doc/protocol.md`](../protocol.md) and [`doc/transport/architecture.md`](architecture.md).

---

## 10. How we confirm "no TLS termination / no MITM"

1. **Automated tests**
   - `connect_to_trusted_peer_succeeds`: server presents a trusted credential; client completes handshake and reports the expected `node_id`.
   - `untrusted_server_is_rejected`: client receives a cert with no valid credential; TLS handshake fails before any TRNC frame.
   - `mitm_proxy_is_rejected`: a middle process with its own certificate terminates TLS; client verifier rejects it because the credential is missing or untrusted.
   - `credential_replay_fails`: attacker forwards a valid `Hello` credential but cannot produce a valid session-bound `auth_proof`; acceptor rejects.

2. **Operational checks**
   - After every connection, log `peer_identity.node_id` and assert it matches the expected node for the configured address.
   - If a load balancer terminates TLS, the logged `node_id` will be missing or wrong and the connection will be closed.

3. **Invariants**
   - TLS 1.3 is mandatory; downgrade attempts abort.
   - No static CA bundle is required; trust comes from the local cache + trusted network issuers.
   - Bootstrap authority is used only for admission, not for every connection.

---

## 11. Open decisions to confirm

1. **Signature algorithm**: Ed25519 keeps credentials small and is well-supported, but ECDSA P-256 may fit better with `rustls`/aws-lc-rs. Which should we use?
2. **Credential storage format**: binary (e.g. CBOR/byteser) or JSON? `byteser` is already used in the project but credentials may be easier to debug as JSON.
3. **TLS certificate generation**: should nodes generate their own self-signed TLS cert and embed the credential, or should the network issuer sign the TLS cert directly? Embedding a self-signed cert is simpler and matches the no-static-CA model.
4. **UUID enforcement**: `config.trench` currently uses `"xyz"` as an ID. Do we migrate IDs to UUID v4 now, or keep arbitrary strings?
5. **Revocation**: should the first milestone implement expiry only, or also a signed revocation list?
