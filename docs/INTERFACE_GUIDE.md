# Transport Protocol Interface Testing Guide

## Overview
Implemented a comprehensive testing interface for the transport protocol based on the `trench-db` patterns. The interface provides both server and client components using the `ResilientClient` and `ResilientServer` from the transport layer.

## Files Created/Modified

### Transport Layer Files
- **`transport/src/frame/decoder.rs`**: Deserialize frame bytes to Frame objects
- **`transport/src/frame/encoder.rs`**: Serialize Frame objects to bytes
- **`transport/src/frame/mod.rs`**: Frame module exports and organization
- **`transport/src/secure.rs`**: `SecureStreamProvider` trait, the seam between the protocol and any secure channel
- **`transport/src/testing.rs`**: `PlaintextProvider`, test-only (behind `test-support` or `cfg(test)`)
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
- **`interface/src/tls/`**: Application-side TLS adapter (`TlsClientProvider`, `TlsServerProvider`, PEM loading, trust policy)
- **`interface/src/bin/server.rs`**: Server CLI binary (TLS)
- **`interface/src/bin/client.rs`**: Client CLI binary (TLS)
- **`interface/tests/`**: Integration tests for the plaintext and TLS providers, including rejection cases

## Implementation Details

### Frame Protocol
The transport layer uses a binary frame protocol for efficient message delivery:
- **Encoder**: Converts Frame objects to bytes with header metadata
- **Decoder**: Reconstructs Frame objects from byte streams
- **Frame Types**: Data, Close, Reset, Error
- **Header Format**: Contains frame type and payload length information

### Secure Transport
`transport` knows nothing about TLS. `ResilientClient<P>` and `ResilientServer<P>` are generic over
`SecureStreamProvider`, which turns a `TcpStream` into a secure stream. The protocol runs over
whatever stream the provider returns, and the Hello/Welcome handshake runs once per connection
after the secure handshake completes.

The TLS provider lives in the application (`interface/src/tls/`):
- **Client** (`TlsClientProvider`): TLS 1.3 only. Trust is an explicit root CA set. An empty set is an error. WebPKI verifies the chain, validity window, and server name. An optional SPKI SHA-256 pin (`--pin`) is checked after WebPKI succeeds.
- **Server** (`TlsServerProvider`): TLS 1.3 only, no client certificates.
- **PEM**: `--cert` (leaf chain), `--key` (private key), and `--ca` (trusted roots) are read from disk. Load errors name the file.

A peer that fails the TLS handshake is dropped before any protocol frame is read or written.
The server's 10s handshake timeout covers only the secure accept. The client's timeout covers the secure connect and the Hello/Welcome exchange.

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
The server requires a PEM leaf certificate chain and its private key. The default address is `127.0.0.1:5000`:
```bash
cargo run --bin server -- --cert server.pem --key server-key.pem
# Or with custom address:
cargo run --bin server -- --addr 127.0.0.1:8080 --cert server.pem --key server-key.pem
```

### 3. Run the Client (in another terminal)
The client requires the PEM root CA that signed the server certificate. The server certificate must list the host the client connects to as a SAN (an IP literal for an address like `127.0.0.1`). `--pin <hex SHA-256 of the server SPKI>` is optional and adds a key pin.
```bash
cargo run --bin client -- --addr 127.0.0.1:5000 --ca ca.pem
# Or with custom parameters:
cargo run --bin client -- --addr 127.0.0.1:8080 --ca ca.pem --message "Your message here"
```

Certificates are not generated by the repo. The integration tests build throwaway CA and leaf certificates in memory (`interface/tests/common/pki.rs`).

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

### Running the Tests
```bash
cd transport && cargo test --all-features
cd interface && cargo test
```
The `plaintext` test runs the protocol over the test-only `PlaintextProvider`. The `tls` and `tls_rejections` tests run it over the TLS adapter. The rejection tests check that a wrong CA, self-signed leaf, expired or not-yet-valid leaf, hostname mismatch, SPKI pin mismatch, TLS 1.2-only peer, plaintext peer, and garbage input all fail before the handler runs.

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
cargo run --bin server -- --addr 0.0.0.0:8080 --cert server.pem --key server-key.pem
```

### Custom Client Parameters
```bash
cargo run --bin client -- --addr 192.168.1.100:5000 --ca ca.pem --message "Custom message"
```

### Environment-based Configuration
Set default values via environment variables before compilation:
```bash
export TRANSPORT_ADDR="0.0.0.0:9000"
cargo run --bin server
```

## Troubleshooting

### Connection Refused
- Ensure server is running: `cargo run --bin server`
- Check address and port: Default is `127.0.0.1:5000`
- Verify firewall allows TCP connections

### Serialization Errors
- Ensure all types implement `ByteSerializable`
- Check that field types are supported by byteser
- Review byteser documentation for custom serialization

### TLS Connection Issues
- `Secure handshake failed: invalid peer certificate: UnknownIssuer`: the `--ca` file does not contain the root that signed the server certificate.
- `certificate not valid for name ...`: the server certificate has no SAN for the host the client connects to. IP addresses need IP SANs.
- `certificate expired` or `not valid yet`: check the certificate validity window.
- `SPKI pin` mismatch (`ApplicationVerificationFailure`): `--pin` does not match the server public key.
- `tls handshake eof` or `Connection closed`: the peer is not a TLS 1.3 endpoint, or it speaks plaintext.
- `no certificates found` or `no private key found`: the PEM file is missing or in the wrong format. The error names the file.


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
  - ResilientClient/Server for connection management, generic over `SecureStreamProvider`
  - Frame encoder/decoder for binary protocol
  - Error handling layer
- `interface` - Adds the TLS adapter with:
  - `rustls` and `tokio-rustls` (TLS 1.3, WebPKI verification)
  - `rustls-pemfile` and `x509-parser` (PEM loading, SPKI pinning)
  - `sha2` (SPKI SHA-256 pins)
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
