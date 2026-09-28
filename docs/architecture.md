# Transport Layer — Architecture

> This is one of several transport documents. See
> [`README.md`](README.md) for the consolidated, holistic view of the whole
> layer (including the `ResilientClient`/`ResilientServer` request/response
> layer, documented in [`resilient.md`](resilient.md)).

---

## Table of Contents

1. [Overview](#1-overview)
2. [Layer Stack](#2-layer-stack)
3. [Module Map](#3-module-map)
4. [Wire Format — TRNC Framing Protocol](#4-wire-format--trnc-framing-protocol)
5. [Frame Types](#5-frame-types)
6. [Header Flags](#6-header-flags)
7. [Frame Module (`frame/`)](#7-frame-module-frame)
8. [TCP Module (`tcp/`)](#8-tcp-module-tcp)
   - [Connection](#81-connection)
   - [Stream](#82-stream)
   - [StreamManager](#83-streammanager)
   - [Receiver](#84-receiver)
9. [Flow Control](#9-flow-control)
10. [Stream Lifecycle](#10-stream-lifecycle)
11. [Error Model](#11-error-model)
12. [Design Constraints](#12-design-constraints)
13. [What Is Not Implemented Yet](#13-what-is-not-implemented-yet)
14. [Resilient client/server layer](#14-resilient-clientserver-layer)

---

## 1. Overview

The `transport` crate is the binary framing and connection management layer for TrenchDB. It provides:

- A **binary framing protocol** (TRNC) for length-prefixed, magic-prefixed, versioned frames.
- An **async TCP connection wrapper** (`Connection<T>`) that sends and receives frames over any `AsyncRead + AsyncWrite` stream.
- **Logical stream multiplexing** (`StreamManager<T>`) over a single TCP connection, with per-stream flow control.

The crate is intentionally **application-agnostic**. It moves opaque byte payloads. It knows nothing about SQL, replication, or cluster state.

---

## 2. Layer Stack

```
┌───────────────────────────────────────────────┐
│              Application Layer                │
│   (replication, queries, cluster messages)    │
├───────────────────────────────────────────────┤
│           StreamManager<T>                    │
│   open_stream / send_data / recv_data         │
│   close_stream / reset_stream / send_ping     │
├───────────────────────────────────────────────┤
│           Stream (state + flow control)       │
│   StreamState machine  │  send/recv windows   │
├───────────────────────────────────────────────┤
│           Connection<T>                       │
│   buffer_frame / flush / recv_frame           │
├───────────────────────────────────────────────┤
│        Frame Encoder / Decoder                │
│   encode() → Bytes   │   decode() → Frame     │
├───────────────────────────────────────────────┤
│              TRNC Wire Format                 │
│   16-byte header + opaque payload             │
├───────────────────────────────────────────────┤
│         TLS (rustls) — planned                │
├───────────────────────────────────────────────┤
│              TCP (tokio)                      │
└───────────────────────────────────────────────┘
```

---

## 3. Module Map

```
transport/src/
├── lib.rs              — crate root; re-exports frame, tcp, errors
├── errors.rs           — TransportError, ErrorCode, ErrorPayload
│
├── frame/
│   ├── mod.rs          — re-exports header, frame, encoder, decoder
│   ├── header.rs       — Header struct, constants (magic, version, sizes, flags)
│   ├── frame.rs        — Frame struct, Frametype enum
│   ├── encoder.rs      — encode(frame) → Bytes
│   └── decoder.rs      — decode(&mut BytesMut) → Result<Frame>
│
└── tcp/
    ├── mod.rs          — re-exports connection, manager, stream, receiver
    ├── connection.rs   — Connection<T>: buffered frame I/O
    ├── stream.rs       — Stream: state machine + flow-control windows
    ├── manager.rs      — StreamManager<T>: multiplexer, public API
    └── receiver.rs     — stateless inbound frame dispatch handlers
```

---

## 4. Wire Format — TRNC Framing Protocol

Every frame begins with a **fixed 16-byte header** followed immediately by `payload_length` bytes of opaque payload.

```
Byte offset   Field            Size    Encoding
───────────   ──────────────   ─────   ─────────────────────────
0 – 3         magic            4 B     ASCII "TRNC" = 0x54 0x52 0x4E 0x43
4             version          1 B     u8, currently 1
5 – 6         flags            2 B     u16 big-endian  (see §6)
7 – 10        stream_id        4 B     u32 big-endian
11 – 14       payload_length   4 B     u32 big-endian
15            frame_type       1 B     u8  (see §5)
──────────────────────────────────────
              TOTAL HEADER     16 B
[16 …]        payload          payload_length bytes
```

**Constants**

| Constant              | Value      | Purpose                                       |
|-----------------------|------------|-----------------------------------------------|
| `FRAME_MAGIC`         | `"TRNC"`   | Stream alignment guard                        |
| `CURRENT_VERSION`     | `1`        | Version written into every outgoing frame     |
| `MIN_SUPPORTED_VERSION` | `1`      | Oldest version accepted from a peer           |
| `MAX_FRAME_SIZE`      | 16 MiB     | Maximum payload; enforced before allocation   |
| `HEADER_SIZE`         | 16 bytes   | Fixed header length                           |

A received frame whose magic bytes do not match is rejected with `TransportError::InvalidMagic` before any further parsing takes place.

---

## 5. Frame Types

Frame type occupies byte 15 of the header (the `frame_type` field). Values are **stable wire constants** — changing them is a breaking protocol change.

| Byte | Variant    | Direction      | Description                                               |
|------|------------|----------------|-----------------------------------------------------------|
| 1    | `Open`     | client → server | Open a new logical stream; `stream_id` must be unique    |
| 2    | `Data`     | both           | Carry opaque application payload on an established stream |
| 3    | `Close`    | both           | Half-close a stream (FIN); no more Data after this        |
| 4    | `Reset`    | both           | Abortively terminate a stream; discard buffered data      |
| 5    | `Ping`     | both           | Liveness probe; payload SHOULD be empty                   |
| 6    | `Pong`     | both           | Echo reply to a `Ping`; carries same payload              |
| 7    | `Window`   | both           | Increment the peer's send window for a stream             |
| 8    | `Error`    | both           | Signal a stream-level or connection-level error           |
| 9    | `Settings` | both           | Exchange connection-level configuration parameters        |
| 10   | `Hello`    | client → server | Initiate the version handshake                           |
| 11   | `Welcome`  | server → client | Accept the version handshake                             |

`Frametype::from_u8` returns `None` for unknown values, allowing a well-formed `InvalidFrame` error rather than a panic.

---

## 6. Header Flags

The `flags` field is a 16-bit little-endian bitfield. Three bits are currently defined:

| Bit | Constant        | Meaning                                                                  |
|-----|-----------------|--------------------------------------------------------------------------|
| 0   | `FLAG_FIN`      | Final frame on this stream; sender will send no more `Data` frames       |
| 1   | `FLAG_ACK`      | Acknowledgement — used in handshake and flow-control exchanges           |
| 2   | `FLAG_CONTROL`  | Control frame (`Ping`, `Pong`, `Settings`, `Hello`, `Welcome` MUST set this) |

Data-plane frames (`Data`, `Open`, `Close`, `Reset`) MUST NOT set `FLAG_CONTROL`.

---

## 7. Frame Module (`frame/`)

### `header.rs`
Defines `Header` (the 16-byte struct) and all wire constants. `Header::validate()` checks magic, version range, and payload size before any frame body is read.

### `frame.rs`
Defines `Frame { header: Header, payload: Vec<u8> }` and the `Frametype` enum. `Frame::new` and `Frame::empty` are the two constructors.

### `encoder.rs`
`encode(frame: &Frame) -> Result<Bytes, TransportError>`

Serialises a `Frame` to a contiguous `Bytes` buffer:
1. Write 4-byte magic.
2. Write version, flags (big-endian u16), stream_id (big-endian u32), payload_length (big-endian u32), frame_type byte.
3. Append payload bytes.

### `decoder.rs`
`decode(buf: &mut BytesMut) -> Result<Frame, TransportError>`

Attempts to extract one complete frame from the front of a `BytesMut` buffer:
1. Return `NeedMoreData` if fewer than 16 bytes are available.
2. Validate magic, version, and payload_length.
3. Return `NeedMoreData` if the full payload has not arrived yet.
4. Advance the buffer cursor and return the `Frame`.

`NeedMoreData` is an internal sentinel — it is never surfaced to callers; `Connection::recv_frame` loops until a full frame arrives.

---

## 8. TCP Module (`tcp/`)

### 8.1 `Connection<T>`

```
Connection<T: AsyncRead + AsyncWrite + Unpin>
  ├── stream: T
  ├── read_buffer: BytesMut  (initial 64 KiB, max 16 MiB + 16 B)
  └── write_buffer: BytesMut (initial 64 KiB, max 256 KiB)
```

**Buffering strategy — writes**

Outgoing frames are accumulated in the write buffer via `buffer_frame`. They are NOT sent until `flush` is called. `send_frame` is the convenience method that does both in one call. This batching reduces system call overhead when multiple frames are sent in sequence.

`buffer_frame` returns `TransportError::BufferOverflow` if adding the frame would exceed 256 KiB.

**Buffering strategy — reads**

`recv_frame` reads 8 KiB chunks from the stream into `read_buffer` in a loop, calling `decode` after each chunk. It returns as soon as one complete frame is decoded.

If the read buffer grows beyond `MAX_FRAME_SIZE + HEADER_SIZE` before a decodable frame appears, the connection is torn down with `FrameTooLarge` to prevent memory exhaustion by a malicious peer.

**Key buffer constants**

| Constant              | Value    |
|-----------------------|----------|
| `MIN_BUFFER_SIZE`     | 64 KiB   |
| `MAX_BUFFER_SIZE`     | 256 KiB  |
| `CHUNK_SIZE`          | 8 KiB    |
| `MAX_READ_BUFFER_SIZE`| ~16 MiB  |

---

### 8.2 `Stream`

Represents a single logical stream within a connection.

```
Stream {
  id:          u32
  state:       StreamState
  send_window: i64   (bytes the remote peer permits us to send)
  recv_window: i64   (bytes we permit the remote peer to send)
  recv_queue:  VecDeque<Bytes>
}
```

**Default window:** 64 KiB (`DEFAULT_WINDOW = 65_536`). Can be overridden at construction via `Stream::with_window`.

Key methods:

| Method                    | Description                                               |
|---------------------------|-----------------------------------------------------------|
| `check_send_window(n)`    | Returns `FlowControlViolation` if `send_window < n`       |
| `consume_send_window(n)`  | Deducts `n` from `send_window` after a Data frame is sent |
| `apply_window_increment(n)` | Adds `n` to `send_window` when a Window frame arrives   |
| `push_recv(payload)`      | Enqueues inbound data; decrements `recv_window`           |
| `pop_recv()`              | Dequeues next payload; restores `recv_window`             |
| `on_local_close()`        | State transition on sending a `Close` frame               |
| `on_remote_close()`       | State transition on receiving a `Close` frame             |
| `on_reset()`              | Immediately moves state to `Reset`                        |

---

### 8.3 `StreamManager<T>`

The primary public API of the transport crate. Wraps a `Connection<T>` and a `HashMap<u32, Stream>`.

```
StreamManager<T> {
  conn:          Connection<T>
  streams:       HashMap<u32, Stream>
  next_local_id: u32
  role:          Role
}
```

**Role and stream ID parity**

Stream IDs are 32-bit unsigned integers. Parity encodes the originating side:

| Role        | Owns IDs    | Starting ID |
|-------------|-------------|-------------|
| `Initiator` (client) | Odd  | 1          |
| `Acceptor`  (server) | Even | 2          |

IDs increment by 2. `StreamIdExhausted` is returned when the counter overflows.

**Auto-flush threshold**

`buffer_and_maybe_flush` buffers a frame, then calls `flush` automatically if the write buffer reaches or exceeds 32 KiB (`FLUSH_THRESHOLD`). This prevents unbounded buffer growth under sustained write load while still batching small bursts.

**Public API summary**

| Method                          | Description                                                  |
|---------------------------------|--------------------------------------------------------------|
| `open_stream()`                 | Allocate an ID, send `Open`, return the stream ID            |
| `send_data(id, payload)`        | Flow-control check, encode `Data` frame, buffer + maybe flush |
| `close_stream(id)`              | Send `Close(FIN)`, advance state machine                     |
| `reset_stream(id)`              | Send `Reset`, remove stream immediately                      |
| `recv_frame()`                  | Read next frame, dispatch to receiver handlers               |
| `recv_data(id)`                 | Pop next payload from stream's receive queue; sends `Window` |
| `send_ping(payload)`            | Send `Ping(CONTROL)` and flush immediately                   |
| `flush()`                       | Force-flush the write buffer                                 |
| `stream_count()`                | Number of currently tracked streams                          |
| `stream_state(id)`              | Current `StreamState` for a stream                           |
| `send_window(id)`               | Remaining send-window bytes for a stream                     |

**Ping/Pong handling in `recv_frame`**

When a `Ping` arrives, `StreamManager` immediately replies with a `Pong` carrying the same payload (using `send_frame`, which flushes inline) before returning the `Ping` frame to the caller.

---

### 8.4 `Receiver`

`receiver.rs` contains pure, stateless functions that mutate the `streams` map. They are called by `StreamManager::recv_frame` after each inbound frame is decoded.

| Function          | Frame     | Action                                                       |
|-------------------|-----------|--------------------------------------------------------------|
| `handle_open`     | `Open`    | Validate ID parity, reject duplicates, insert new `Stream`   |
| `handle_data`     | `Data`    | Check `can_receive()`, enqueue payload via `push_recv`        |
| `handle_close`    | `Close`   | Call `on_remote_close()`; remove stream if now fully closed  |
| `handle_reset`    | `Reset`   | Call `on_reset()`; remove stream from map                    |
| `handle_window`   | `Window`  | Parse 4-byte big-endian increment, call `apply_window_increment` |

---

## 9. Flow Control

Flow control is per-stream and credit-based. Both sides maintain independent windows:

```
Sender side                          Receiver side
──────────────────────────────────   ──────────────────────────────────
send_window (i64)                    recv_window (i64)
  ↓ decremented on each Data sent      ↓ decremented on each Data received
  ↑ incremented when Window arrives    ↑ restored when pop_recv() called
```

**Sending data**

1. `StreamManager::send_data` calls `check_send_window(payload.len())`.
2. If `send_window < payload.len()` → `TransportError::FlowControlViolation`.
3. Otherwise `consume_send_window(payload.len())` deducts the bytes and the `Data` frame is buffered.

**Receiving data and sending Window frames**

`StreamManager::recv_data` pops the next payload from the stream's receive queue. After popping, if the stream can still receive, it sends a `Window` frame back to the peer with the number of bytes just freed. This lets the peer know it can send more data.

**Window frame payload**

A `Window` frame carries a 4-byte big-endian `u32` increment in its payload. The receiver calls `apply_window_increment(increment)` on the target stream, which uses `saturating_add` to guard against i64 overflow from a malicious or buggy peer.

**Default window**

Both `send_window` and `recv_window` start at **64 KiB** (65,536 bytes). They can be overridden via `Stream::with_window` during handshake negotiation (not yet implemented).

---

## 10. Stream Lifecycle

```
                   open_stream() / handle_open()
                           │
                           ▼
                        ┌──────┐
                        │ Open │
                        └──┬───┘
              ┌────────────┴────────────┐
    send Close(FIN)            receive Close(FIN)
    on_local_close()           on_remote_close()
              │                          │
              ▼                          ▼
    ┌──────────────────┐     ┌───────────────────┐
    │ HalfClosedLocal  │     │ HalfClosedRemote  │
    └────────┬─────────┘     └─────────┬─────────┘
  receive Close(FIN)         send Close(FIN)
  on_remote_close()          on_local_close()
              │                          │
              └────────────┬─────────────┘
                           ▼
                        ┌────────┐
                        │ Closed │  ← stream removed from map
                        └────────┘

    Any state + RESET (either side)
              │
              ▼
           ┌───────┐
           │ Reset │  ← stream removed from map immediately
           └───────┘
```

`StreamState` helpers:

| Method          | True when state is …                          |
|-----------------|-----------------------------------------------|
| `can_send()`    | `Open` or `HalfClosedRemote`                  |
| `can_receive()` | `Open` or `HalfClosedLocal`                   |
| `is_closed()`   | `Closed` or `Reset`                           |

---

## 11. Error Model

All fallible operations return `Result<_, TransportError>`. The enum covers both internal sentinels and wire-level errors:

| Variant                   | Cause                                                        |
|---------------------------|--------------------------------------------------------------|
| `InvalidMagic`            | First 4 bytes of header ≠ `"TRNC"`                          |
| `InvalidVersion`          | `version` field outside `[MIN_SUPPORTED_VERSION, CURRENT_VERSION]` |
| `InvalidFrame(String)`    | Unknown frame type, parity error, bad flags, etc.            |
| `FrameTooLarge`           | Declared payload_length > `MAX_FRAME_SIZE`                   |
| `ConnectionClosed`        | EOF from the underlying stream                               |
| `StreamClosed(u32)`       | Operation on a closed stream                                 |
| `StreamReset(u32)`        | Stream was reset by the remote peer                          |
| `UnknownStream(u32)`      | Frame or operation targeted a stream that does not exist     |
| `StreamNotWritable(u32)`  | Attempt to send data on a non-writable stream state          |
| `FlowControlViolation`    | Payload exceeds the remaining send window                    |
| `HandshakeRejected`       | Handshake failed (code + message)                            |
| `StreamIdExhausted`       | All stream IDs for this role have been used                  |
| `BufferOverflow`          | Write buffer would exceed 256 KiB                            |
| `Timeout`                 | Operation did not complete in time                           |
| `NeedMoreData`            | *Internal only* — decoder needs more bytes; never exposed    |
| `Io(std::io::Error)`      | Underlying I/O error                                         |

Wire-level error codes (transmitted inside `Error` frames as `u16` big-endian):

`Unknown(0)`, `ProtocolError(1)`, `InvalidVersion(2)`, `InvalidFrame(3)`, `FrameTooLarge(4)`, `StreamClosed(5)`, `StreamReset(6)`, `FlowControlViolation(7)`, `HandshakeRejected(8)`, `Timeout(9)`, `InternalError(10)`.

---

## 12. Design Constraints

- **Application-agnostic.** The crate knows nothing about query language, replication protocol, or cluster topology. Applications pass and receive `Vec<u8>` / `Bytes`.
- **Zero unsafe.** All code is safe Rust.
- **Minimal dependencies.** Only `bytes` and `tokio` are runtime dependencies.
- **Generic over I/O.** `Connection<T>` and `StreamManager<T>` work over any `T: AsyncRead + AsyncWrite + Unpin`, enabling easy testing with in-memory duplex streams.
- **No panics on malformed input.** All wire parsing returns `Result`; unknown frame types and bad magic are errors, not panics.
- **Memory-bounded.** Read buffer and write buffer are capped; payload size is validated before allocation.

---

## 13. What Is Not Implemented Yet

| Feature                  | Status          | Notes                                                      |
|--------------------------|-----------------|------------------------------------------------------------|
| TLS + network-verified peer identity (rustls) | Planned         | `TlsError` variant is commented out in `errors.rs`        |
| Handshake (`Hello`/`Welcome`) | Implemented | `Hello`/`Welcome` handshake state machine is implemented in `StreamManager` |
| `Settings` frame         | Partial         | Connection-level settings are accepted and validated; no application-specific behavior yet |
| `Error` frame dispatch   | Implemented     | `ErrorPayload` is decoded and received `Error` frames become `TransportError` |
| `Pong`/`Settings`/`Hello`/`Welcome` receive handling | Partial | `recv_frame` handles handshake and error frames; `Pong` is echoed, `Settings` is accepted |
| Back-pressure signaling  | Partial         | `recv_window` is tracked; no async wake-up to the application when window is exhausted |
| Configurable timeouts    | Planned         | `Timeout` error exists; no actual timeout wiring yet       |

---

## 14. Resilient client/server layer

Above everything described in this document, `transport::client::resilient_client::ResilientClient`
and `transport::server::ResilientServer` implement a concrete request/response
pattern on top of `StreamManager`: one stream per request, a `RequestEnvelope`/
`ResponseEnvelope` wire pair, and server-side routing by action name through
`Dispatcher`/`Actions`/`Handler`.

This layer, its sequence diagrams, and its current limitations (no
retry/reconnect, no timeouts, one in-flight request per client) are fully
documented in [`resilient.md`](resilient.md). For the single consolidated view
of the entire transport stack — from `ResilientClient` down to raw TCP — see
[`README.md`](README.md).

