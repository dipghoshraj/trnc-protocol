# trnc-protocol

A robust, async-first transport protocol implementation in Rust with resilient client-server communication, TLS support, and comprehensive testing interfaces.

## 🎯 Project Overview

`trnc-protocol` provides a production-ready transport layer with:

- **Resilient Client/Server**: Automatic reconnection and connection pooling
- **Binary Frame Protocol**: Efficient frame encoding/decoding with headers
- **TLS Support**: Secure encrypted connections using rustls
- **Async Architecture**: Built on Tokio for high-concurrency scenarios
- **Type-Safe Serialization**: ByteSerializable pattern for request/response messages
- **Handler Pattern**: Extensible action-based request dispatching

## 📁 Repository Structure

```
trnc-protocol/
├── transport/                  # Core transport protocol library
│   ├── src/
│   │   ├── client/            # ResilientClient implementation
│   │   ├── server/            # ResilientServer & dispatcher
│   │   ├── frame/             # Frame encoder/decoder
│   │   ├── tcp/               # TCP connection management
│   │   ├── tls/               # TLS support
│   │   ├── errors.rs          # Error definitions
│   │   └── lib.rs             # Module exports
│   ├── test/                  # Integration tests
│   └── Cargo.toml
├── interface/                 # Testing interface with CLI tools
│   ├── src/
│   │   ├── server.rs          # Sample server implementation
│   │   ├── client.rs          # Sample client implementation
│   │   ├── lib.rs             # Module exports
│   │   └── bin/               # CLI binaries (server, client)
│   └── Cargo.toml
├── INTERFACE_GUIDE.md         # Comprehensive testing guide
└── README.md                  # This file
```

## 🚀 Quick Start

### Build
```bash
cd e:\trnc-protocol
cargo build --release
```

### Run Server
```bash
cd interface
cargo run --bin server -- --addr 127.0.0.1:5000
```

### Run Client (in another terminal)
```bash
cd interface
cargo run --bin client -- --addr 127.0.0.1:5000 --message "Hello Protocol"
```

### Expected Output
```
# Server:
[server] listening on 127.0.0.1:5000
[server] accepted connection

# Client:
[client] connected to 127.0.0.1:5000
[client] opened stream 1
[client] response: ECHO: Hello Protocol
[client] user: Alice (30)
```

## 📚 Key Components

### Transport Layer (`transport/`)

**ResilientClient**: Connection pooling and automatic reconnection
- Manages TCP connections with fallback
- Sends RequestEnvelope messages
- Handles ResponseEnvelope responses
- Stream lifecycle management

**ResilientServer**: Concurrent connection handling
- Accepts multiple client connections
- Routes requests via Dispatcher to Handlers
- Manages stream lifecycle per connection
- Returns structured responses

**Frame Protocol**: Binary message framing
- Encoder: Serializes Frame objects to bytes
- Decoder: Deserializes bytes back to Frames
- Header: Frame metadata (type, length)
- Types: Data, Close, Reset, Error

**TLS Module**: Secure connections
- rustls-based encryption
- Certificate/PEM handling
- Transparent to application layer

### Interface Layer (`interface/`)

**Server Implementation**:
- ResilientServer with Actions registry
- EchoHandler for testing
- Deserializes UserMessage, returns ServerResponse
- Concurrent client handling

**Client Implementation**:
- ResilientClient connection wrapper
- Serializes UserMessage to RequestEnvelope
- Deserializes ResponseEnvelope back to ServerResponse
- Clean error handling

**CLI Tools**:
- `server` binary: Start listening on custom address
- `client` binary: Connect and send test messages

## 🔧 Technology Stack

| Component | Library | Version |
|-----------|---------|---------|
| Async Runtime | tokio | 1.52+ |
| Serialization | byteser | 0.1.1 |
| Async Traits | async-trait | 0.1.68 |
| CLI Parsing | clap | 4.4+ |
| TLS | rustls | 0.23.43 |
| TLS Runtime | tokio-rustls | 0.26.4 |

## 📖 Documentation

- **[INTERFACE_GUIDE.md](INTERFACE_GUIDE.md)** - Comprehensive testing guide with examples and extension patterns
- **[transport/src/lib.rs](transport/src/lib.rs)** - API documentation
- **Code comments** - Inline documentation throughout

## ✨ Features

✅ **Async/Await**: Full async support with Tokio  
✅ **Error Handling**: Comprehensive TransportError types  
✅ **Binary Protocol**: Efficient frame-based messaging  
✅ **TLS Ready**: Secure connections out of the box  
✅ **Handler Pattern**: Extensible request processing  
✅ **Type Safety**: Rust's type system prevents data corruption  
✅ **CLI Tools**: Easy testing with built-in binaries  
✅ **Well Documented**: Extensive guides and examples  

## 🎓 Custom Handler Development

Add new handlers by:

1. Define request/response types with `#[derive(ByteSerializable)]`
2. Implement `Handler` trait with `async fn call()`
3. Register in server: `actions.register_action("name", handler)`

See [INTERFACE_GUIDE.md](INTERFACE_GUIDE.md#extending-with-custom-handlers) for detailed examples.

## 📊 Recent Changes

### Latest Commits (7 new commits)
- `2898994` - feat: add resilient server and TCP connection management
- `f7bc585` - feat: implement resilient client with reconnection support
- `402904b` - feat: add transport error handling and module exports
- `4922ded` - chore: update transport dependencies and configuration
- `db6a485` - feat: add TLS module for secure transport connections
- `097eaff` - feat: add frame module encoder and decoder implementations
- `8b1655e` - docs: add comprehensive interface testing guide
- `435adde` - feat: implement comprehensive interface for transport protocol testing

Plus 20 transport protocol commits cherry-picked from trench-db repository.

## 🧪 Testing

Run tests with:
```bash
cargo test --all
```

Integration test available at: `transport/test/ping_pong.rs`

## 📝 License

See LICENSE file for details.

## 🤝 Contributing

Please ensure:
- Code compiles without warnings: `cargo check`
- All tests pass: `cargo test`
- Documentation is updated for new features

## 🔗 Related Projects

- **trench-db**: Database transport implementation (source of cherry-picked commits)
- **transport protocol**: Core messaging layer for distributed systems