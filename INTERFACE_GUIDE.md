# Transport Protocol Interface Testing Guide

## Overview
Implemented a comprehensive testing interface for the transport protocol based on the `trench-db` patterns. The interface provides both server and client components using the `ResilientClient` and `ResilientServer` from the transport layer.

## Files Created/Modified

### New Files
- **`interface/Cargo.toml`**: Package configuration with dependencies (clap, tokio, byteser, async-trait)
- **`interface/src/lib.rs`**: Module exports for server and client
- **`interface/src/server.rs`**: Server implementation with Actions and Handlers
- **`interface/src/bin/server.rs`**: Server CLI binary
- **`interface/src/bin/client.rs`**: Client CLI binary

### Updated Files
- **`interface/src/client.rs`**: Refactored to use ResilientClient and ByteSerializable patterns

## Implementation Details

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
Start the server on default address `127.0.0.1:5000`:
```bash
cargo run --bin server
# Or with custom address:
cargo run --bin server -- --addr 127.0.0.1:8080
```

### 3. Run the Client (in another terminal)
```bash
cargo run --bin client
# Or with custom parameters:
cargo run --bin client -- --addr 127.0.0.1:8080 --message "Your message here"
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
│  - TCP connection handling                             │
└──────────────────┬──────────────────────────────────────┘
                   │
                   │ TCP frames over network
                   ▼
┌─────────────────────────────────────────────────────────┐
│           Transport Layer (StreamManager)               │
│  - Frame decoding                                      │
│  - Stream receiving                                    │
└──────────────────┬──────────────────────────────────────┘
                   │
                   │ RequestEnvelope
                   ▼
┌─────────────────────────────────────────────────────────┐
│                  Server Application                      │
│  - Dispatcher routes to handlers                        │
│  - EchoHandler deserializes and processes              │
│  - Returns ResponseEnvelope with serialized payload    │
└─────────────────────────────────────────────────────────┘
```

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
- `transport` - Local transport protocol library
- `tokio` - Async runtime
- `byteser` - Binary serialization
- `async-trait` - Async trait support
- `clap` - CLI argument parsing

## Git History
All changes have been committed with message:
```
feat: implement comprehensive interface for transport protocol testing
```

This commit includes 20 transport protocol commits merged from E:\trench-db plus the new interface implementation.
