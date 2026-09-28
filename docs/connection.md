# Connection (`transport::tcp::connection`)

`Connection<T>` is the lowest-level async I/O wrapper in the transport crate. It converts between raw bytes on a stream and `Frame` values in memory, with internal read and write buffers to minimise system calls.

> Part of the [transport layer documentation](README.md).

---

## Table of Contents

1. [Responsibility](#1-responsibility)
2. [Struct layout](#2-struct-layout)
3. [Buffer strategy — writes](#3-buffer-strategy--writes)
4. [Buffer strategy — reads](#4-buffer-strategy--reads)
5. [Method reference](#5-method-reference)
6. [Buffer constants](#6-buffer-constants)
7. [Error conditions](#7-error-conditions)
8. [Generic parameter `T`](#8-generic-parameter-t)
9. [Usage example](#9-usage-example)

---

## 1. Responsibility

`Connection<T>` has exactly one job: **move `Frame` values across an async byte stream**.

It does not know about streams, flow control, stream IDs, or multiplexing. Those concerns belong to `StreamManager`. `Connection` is purely about serialisation, buffering, and I/O.

---

## 2. Struct layout

```rust
pub struct Connection<T> {
    stream:       T,           // the underlying async byte stream
    read_buffer:  BytesMut,    // bytes received but not yet decoded
    write_buffer: BytesMut,    // frames encoded but not yet flushed
}
```

Both buffers start at `MIN_BUFFER_SIZE` (64 KiB) and grow dynamically within their respective caps.

---

## 3. Buffer strategy — writes

Outgoing frames are **accumulated** in `write_buffer` and not written to the stream until `flush()` is called. This allows the caller to batch several frames into a single `write_all` syscall.

```
buffer_frame(&frame)          → appends encoded bytes to write_buffer
flush()                       → write_all(write_buffer) then clear
send_frame(&frame)            → buffer_frame + flush (convenience)
```

`buffer_frame` returns `TransportError::BufferOverflow` if adding the encoded frame would push `write_buffer` beyond `MAX_BUFFER_SIZE` (256 KiB). This prevents unbounded memory growth if the caller batches frames faster than the network drains them.

### Write path in detail

```
Frame
  │
  ▼ encoder::encode()
Bytes  (header[16] + payload[N])
  │
  ▼ write_buffer.extend_from_slice()
write_buffer (BytesMut, up to 256 KiB)
  │
  ▼ flush() → stream.write_all() + stream.flush()
TCP stream
```

---

## 4. Buffer strategy — reads

`recv_frame()` runs a **decode–read loop**:

```
loop:
  1. try decode(&read_buffer[..])
     ├── Ok((frame, consumed))  → advance read_buffer by consumed; return frame
     ├── Err(NeedMoreData)      → fall through to read step
     └── Err(other)             → return error (close connection)
  2. guard: if read_buffer.len() >= MAX_READ_BUFFER_SIZE → FrameTooLarge
  3. reserve CHUNK_SIZE bytes in read_buffer
  4. stream.read_buf(&mut read_buffer)
     ├── 0 bytes (EOF)          → ConnectionClosed
     └── N bytes                → loop back to step 1
```

`NeedMoreData` is never surfaced to callers — it is an internal signal between the decoder and this loop.

The read buffer is capped at `MAX_FRAME_SIZE + HEADER_SIZE` (~16 MiB + 16 B). If the buffer exceeds this before a valid frame can be decoded, the connection is torn down with `FrameTooLarge` to prevent a malicious peer from forcing gigabytes of allocation.

### Read path in detail

```
TCP stream
  │
  ▼ stream.read_buf() — 8 KiB at a time
read_buffer (BytesMut, grows up to ~16 MiB)
  │
  ▼ decoder::decode(&read_buffer[..])
Frame + consumed count
  │
  ▼ read_buffer.advance(consumed)
read_buffer trimmed; Frame returned to caller
```

---

## 5. Method reference

### Constructor

```rust
Connection::new(stream: T) -> Connection<T>
```

Wraps any `T: AsyncRead + AsyncWrite + Unpin`. Both buffers are allocated at `MIN_BUFFER_SIZE` (64 KiB).

### Write methods

| Method                          | Description                                                  |
|---------------------------------|--------------------------------------------------------------|
| `buffer_frame(&frame)`          | Encode `frame` and append to `write_buffer`. **Does not flush.** Returns `BufferOverflow` if the buffer would exceed 256 KiB. |
| `flush()`                       | `write_all(write_buffer)` + `stream.flush()`. No-op if buffer is empty. |
| `send_frame(&frame)`            | `buffer_frame` + `flush` in one call. Returns the first error from either step. |
| `write_buf_len()`               | Bytes currently in `write_buffer` (encoded but unflushed).   |

### Read methods

| Method          | Description                                                        |
|-----------------|--------------------------------------------------------------------|
| `recv_frame()`  | Block until one complete `Frame` is decoded and return it.         |
| `read_buf_len()`| Bytes currently in `read_buffer` (received but not yet consumed).  |

---

## 6. Buffer constants

| Constant               | Value             | Purpose                                               |
|------------------------|-------------------|-------------------------------------------------------|
| `MIN_BUFFER_SIZE`      | 65,536 (64 KiB)   | Initial capacity for both read and write buffers      |
| `MAX_BUFFER_SIZE`      | 262,144 (256 KiB) | Hard cap on write buffer; `buffer_frame` enforces this|
| `CHUNK_SIZE`           | 8,192 (8 KiB)     | Bytes requested per `read_buf` call                   |
| `MAX_READ_BUFFER_SIZE` | ~16 MiB + 16 B    | Hard cap on read buffer; prevents allocation exhaustion|

---

## 7. Error conditions

| Error                       | Trigger                                                         |
|-----------------------------|-----------------------------------------------------------------|
| `TransportError::BufferOverflow`    | `buffer_frame` would push write buffer past 256 KiB    |
| `TransportError::ConnectionClosed` | `read_buf` returned 0 bytes (peer closed the connection) |
| `TransportError::FrameTooLarge`    | Read buffer exceeded `MAX_READ_BUFFER_SIZE` before a valid frame appeared |
| `TransportError::InvalidMagic`     | Decoded header has wrong magic bytes                    |
| `TransportError::InvalidVersion`   | Decoded header has unsupported version number           |
| `TransportError::InvalidFrame`     | Unknown frame type, bad flags, or `FLAG_CONTROL` mismatch |
| `TransportError::Io(_)`            | Any underlying `std::io::Error` from read or write      |

---

## 8. Generic parameter `T`

`T` must implement `AsyncRead + AsyncWrite + Unpin`. In production this is `tokio::net::TcpStream`. For tests, `tokio::io::duplex` creates a pair of in-memory streams that satisfy the same bounds, allowing full round-trip testing without a network.

The TLS layer (planned) will slot in here: `rustls`'s async wrapper also implements `AsyncRead + AsyncWrite + Unpin`, so no changes to `Connection` are required when TLS is added.

---

## 9. Usage example

```rust
use tokio::net::TcpStream;
use transport::tcp::connection::Connection;
use transport::frame::frame::{Frame, Frametype};
use transport::frame::header::FLAG_CONTROL;

let stream = TcpStream::connect("127.0.0.1:4200").await?;
let mut conn = Connection::new(stream);

// Send a Ping (single frame, flush immediately)
let ping = Frame::empty(Frametype::Ping, FLAG_CONTROL, 0);
conn.send_frame(&ping).await?;

// Batch two frames, then flush once
conn.buffer_frame(&frame_a)?;
conn.buffer_frame(&frame_b)?;
conn.flush().await?;

// Receive the next frame
let reply = conn.recv_frame().await?;
```
