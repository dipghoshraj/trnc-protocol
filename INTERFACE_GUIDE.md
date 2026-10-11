# Transport Protocol Interface Testing Guide

## Overview
Implemented a comprehensive testing interface for the transport protocol based on the `trench-db` patterns. The interface provides both server and client components using the `ResilientClient` and `ResilientServer` from the transport layer.

## Files Created/Modified

### Transport Layer Files
- **`transport/src/frame/decoder.rs`**: Deserialize frame bytes to Frame objects
- **`transport/src/frame/encoder.rs`**: Serialize Frame objects to bytes
- **`transport/src/frame/mod.rs`**: Frame module exports and organization
- **`transport/src/tls/endpoint.rs`**: `ClientTls` and `ServerTls` entry points
- **`transport/src/tls/credential.rs`**: Node credentials and Ed25519 key handling
- **`transport/src/tls/identity.rs`**: Node identity and self-signed TLS certificate
- **`transport/src/tls/verifier.rs`**: Peer certificate verification
- **`transport/src/tls/hello.rs`**: Application-level `Hello` authentication
- **`transport/src/tls/server_config.rs`** / **`client_config.rs`**: rustls configuration
- **`transport/src/tls/mod.rs`**: TLS module exports
- **`transport/src/tls/pem.rs`**: PEM certificate handling
- **`transport/src/errors.rs`**: TransportError type definitions
- **`transport/src/lib.rs`**: Core module exports
- **`transport/src/client/resilient_client.rs`**: Connection pooling and resilience
- **`transport/src/server/resilient_server.rs`**: Concurrent connection handling
- **`transport/src/tcp/manager.rs`**: TCP connection lifecycle management
- **`transport/Cargo.toml`**: Transport crate configuration
- **`transport/Cargo.lock`**: Dependency lock file

### Interface Layer Files
- **`interface/Cargo.toml`**: Package configuration with dependencies (clap, tokio, byteser, async-trait)
- **`interface/src/lib.rs`**: Module exports for server and client
- **`interface/src/server.rs`**: Server implementation with Actions and Handlers
- **`interface/src/client.rs`**: Client implementation using ResilientClient
- **`interface/src/bin/server.rs`**: Server CLI binary
- **`interface/src/bin/client.rs`**: Client CLI binary

## Implementation Details

### Frame Protocol
The transport layer uses a binary frame protocol for efficient message delivery:
- **Encoder**: Converts Frame objects to bytes with header metadata
- **Decoder**: Reconstructs Frame objects from byte streams
- **Frame Types**: Data, Close, Reset, Error
- **Header Format**: Contains frame type and payload length information

### TLS Support
Every connection is protected by TLS 1.3 with mutual authentication:
- **Node credentials**: Each node holds an Ed25519 credential signed by a trusted issuer
- **Mutual checks**: Both peers must present a credential that chains to a trusted issuer
- **Application handshake**: The `Hello` exchange re-verifies both credentials and proves key possession
- **Transparent**: TLS wraps TCP connections without protocol changes
- **Async**: Full async support via tokio-rustls

### TLS Identity Files
The interface binaries take three flags. All are required:

| Flag | Contents |
|------|----------|
| `--credential <file>` | Encoded node credential (`NodeCredential::encode()` bytes) |
| `--key <file>` | The node's Ed25519 private key, PKCS#8 DER |
| `--trusted-issuer <file>` | Raw 32-byte Ed25519 public key of a trusted issuer. Repeat the flag for more issuers. |

The credential and key must belong to the same Ed25519 key pair. Nothing in this repository issues credentials yet, so the files come from your issuer tooling.

### Data Structures
```rust
#[derive(Debug, ByteSerializable)]
struct UserMessage {
    message: String,
}

#[derive(Debug, ByteSerializable)]
struct ServerResponse {
    response: String,
    user: User,
}

#[derive(Debug, ByteSerializable)]
struct User {
    name: String,
    age: u32,
}
```

### Server Features
- **ResilientServer**: Handles incoming TCP connections
- **Actions Registry**: Maps action names to handlers
- **EchoHandler**: Sample handler that echoes back messages with user data
- **Async Spawning**: Each client connection is spawned in a separate task

### Client Features
- **ResilientClient**: Manages resilient client connections
- **Message Serialization**: Uses ByteSerializable for structured data
- **Request/Response Pattern**: Communicates via RequestEnvelope and ResponseEnvelope

## Testing Instructions

### 1. Build the Interface
```bash
cd e:\trnc-protocol\interface
cargo build --release
```

### 2. Run the Server
Start the server on default address `127.0.0.1:5000` with its identity files:
```bash
cargo run --bin server -- --credential server.cred --key server.key --trusted-issuer issuer.pub
# Or with custom address:
cargo run --bin server -- --addr 127.0.0.1:8080 --credential server.cred --key server.key --trusted-issuer issuer.pub
```

### 3. Run the Client (in another terminal)
```bash
cargo run --bin client -- --credential client.cred --key client.key --trusted-issuer issuer.pub
# Or with custom parameters:
cargo run --bin client -- --addr 127.0.0.1:8080 --message "Your message here" --credential client.cred --key client.key --trusted-issuer issuer.pub
```

### Expected Output

**Server Terminal:**
```
[server] listening on 127.0.0.1:5000
[server] accepted connection from 127.0.0.1:xxxxx
```

**Client Terminal:**
```
[client] connected to 127.0.0.1:5000
[client] opened stream 1
[client] message sent, waiting for response...
[client] response: ECHO: Your message here
[client] user: Alice (30)
```

## Key Features

✅ **Serialization**: Uses `byteser` for binary serialization of structured data  
✅ **Async Runtime**: Built on Tokio for concurrent connections  
✅ **CLI Interface**: Both server and client have CLI argument parsing via `clap`  
✅ **Error Handling**: Proper error handling with `TransportError`  
✅ **Trait-based Design**: Handler trait for extensible request processing  
✅ **Resilient Communication**: Uses ResilientClient/ResilientServer abstractions

## Architecture Flow

```
┌─────────────────────────────────────────────────────────┐
│                  Client Application                      │
│  (Uses ResilientClient with ByteSerializable)           │
└──────────────────┬──────────────────────────────────────┘
                   │
                   │ RequestEnvelope with serialized payload
                   ▼
┌─────────────────────────────────────────────────────────┐
│           Transport Layer (StreamManager)               │
│  - Frame encoding/decoding                             │
│  - Stream management                                   │
│  - TCP/TLS connection handling                         │
└──────────────────┬──────────────────────────────────────┘
                   │
                   │ TCP/TLS frames over network
                   ▼
┌─────────────────────────────────────────────────────────┐
│           Transport Layer (StreamManager)               │
│  - Frame decoding                                      │
│  - Stream receiving                                    │
│  - Connection management                              │
└──────────────────┬──────────────────────────────────────┘
                   │
                   │ RequestEnvelope
                   ▼
┌─────────────────────────────────────────────────────────┐
│                  Server Application                      │
│  - Dispatcher routes to handlers                        │
│  - Handler deserializes and processes request          │
│  - Returns ResponseEnvelope with serialized response   │
│  - Concurrent task per client connection              │
└─────────────────────────────────────────────────────────┘
```

## Advanced Configuration

### Custom Server Address
```bash
cargo run --bin server -- --addr 0.0.0.0:8080 --credential server.cred --key server.key --trusted-issuer issuer.pub
```

### Custom Client Parameters
```bash
cargo run --bin client -- --addr 192.168.1.100:5000 --message "Custom message" --credential client.cred --key client.key --trusted-issuer issuer.pub
```

### Environment-based Configuration
Set default values via environment variables before compilation:
```bash
export TRANSPORT_ADDR="0.0.0.0:9000"
cargo run --bin server
```

## Troubleshooting

### Connection Refused
- Ensure server is running: `cargo run --bin server -- --credential server.cred --key server.key --trusted-issuer issuer.pub`
- Check address and port: Default is `127.0.0.1:5000`
- Verify firewall allows TCP connections

### Serialization Errors
- Ensure all types implement `ByteSerializable`
- Check that field types are supported by byteser
- Review byteser documentation for custom serialization

### TLS Connection Issues
- `Untrusted peer`: the other node's credential was not issued by an issuer listed in `--trusted-issuer`
- `certificate public key does not match credential`: the TLS certificate was not produced by the key in the credential, which usually means a copied or spoofed certificate
- `CredentialExpired` / `certificate expired`: re-issue the credential; it is valid only before `expires_at`
- `received fatal alert: HandshakeFailure` on the server: the client rejected the server or sent no valid credential
- Check that `--credential` and `--key` belong to the same Ed25519 key pair

### Timeout Issues
- Increase Tokio runtime timeout settings
- Check network connectivity
- Verify server is accepting connections

## Security Considerations

✅ **Use TLS in Production**: Enable encrypted connections  
✅ **Validate Input**: Deserialize with proper error handling  
✅ **Rate Limiting**: Consider adding rate limits for handlers  
✅ **Authentication**: Implement authentication in custom handlers  
✅ **Authorization**: Add authorization checks before processing  
✅ **Logging**: Log all connection attempts and errors  

## Extending with Custom Handlers

To add a new handler:

1. Define your message types:
```rust
#[derive(Debug, ByteSerializable)]
struct MyRequest { /* ... */ }

#[derive(Debug, ByteSerializable)]
struct MyResponse { /* ... */ }
```

2. Implement Handler trait:
```rust
struct MyHandler;

#[async_trait]
impl Handler for MyHandler {
    async fn call(&self, payload: Vec<u8>) -> Result<Vec<u8>, TransportError> {
        // Deserialize, process, serialize response
    }
}
```

3. Register in server:
```rust
actions.register_action("my_action", MyHandler);
```

## Dependencies
- `transport` - Local transport protocol library with:
  - ResilientClient/Server for connection management
  - Frame encoder/decoder for binary protocol
  - TLS support via rustls and tokio-rustls
  - Error handling layer
- `tokio` - Async runtime with full features (tokio 1.52+)
- `byteser` - Binary serialization framework
- `byteser_derive` - Derive macros for ByteSerializable
- `async-trait` - Async trait support
- `clap` - CLI argument parsing with derive
- `rustls` - TLS encryption (0.23.43+)
- `tokio-rustls` - Async TLS runtime (0.26.4+)

## Git History

### Recent Commits (7 new additions)
1. `8b1655e` - docs: add comprehensive interface testing guide
2. `097eaff` - feat: add frame module encoder and decoder implementations
3. `db6a485` - feat: add TLS module for secure transport connections
4. `4922ded` - chore: update transport dependencies and configuration
5. `402904b` - feat: add transport error handling and module exports
6. `f7bc585` - feat: implement resilient client with reconnection support
7. `2898994` - feat: add resilient server and TCP connection management

### Complete Implementation
All changes have been committed with organized commit messages documenting:
- Frame protocol implementation (encoding/decoding)
- TLS security layer
- Dependency management and configuration
- Error handling infrastructure
- Client-side resilience features
- Server-side concurrent handling

Plus 20 transport protocol commits cherry-picked from E:\trench-db repository, providing the foundation for:
- Connection pooling and management
- Stream lifecycle handling
- Request/Response envelope pattern
- Handler-based action dispatching
