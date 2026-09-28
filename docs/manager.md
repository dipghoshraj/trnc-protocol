# StreamManager (`transport::tcp::manager`)

`StreamManager<T>` is the primary public API of the `transport` crate. It multiplexes any number of logical streams over a single `Connection<T>`, enforces stream ID parity, manages per-stream flow control, and dispatches inbound frames to the appropriate `Stream` via `receiver`.

> Part of the [transport layer documentation](README.md).

---

## Table of Contents

1. [Responsibility](#1-responsibility)
2. [Struct layout](#2-struct-layout)
3. [Role and stream ID parity](#3-role-and-stream-id-parity)
4. [Buffering and auto-flush](#4-buffering-and-auto-flush)
5. [Public API](#5-public-api)
   - [Construction](#51-construction)
   - [Opening and closing streams](#52-opening-and-closing-streams)
   - [Sending data](#53-sending-data)
   - [Receiving frames and data](#54-receiving-frames-and-data)
   - [Ping / Pong](#55-ping--pong)
   - [Inspection](#56-inspection)
6. [Inbound frame dispatch (`recv_frame`)](#6-inbound-frame-dispatch-recv_frame)
7. [Flow-control loop](#7-flow-control-loop)
8. [Error conditions](#8-error-conditions)
9. [What is not yet handled](#9-what-is-not-yet-handled)

---

## 1. Responsibility

`StreamManager` sits between application code and the raw `Connection`. It answers three questions:

- **Which stream does this frame belong to?** — routes inbound frames to the right `Stream` by `stream_id`.
- **Are we allowed to send right now?** — enforces `StreamState` and flow-control windows before buffering any `Data` frame.
- **When should we flush?** — auto-flushes when the write buffer crosses a threshold, while still allowing explicit batching.

Everything application-agnostic: the manager moves bytes; it never inspects payload content.

---

## 2. Struct layout

```rust
pub struct StreamManager<T> {
    conn:          Connection<T>,       // buffered frame I/O
    streams:       HashMap<u32, Stream>, // all live streams keyed by stream_id
    next_local_id: u32,                  // next ID to allocate for open_stream()
    role:          Role,                 // Initiator (client) or Acceptor (server)
}
```

`streams` holds only live streams. A stream is removed from the map as soon as it enters `Closed` or `Reset` state.

---

## 3. Role and stream ID parity

Stream ID parity encodes which side originally opened the stream. This prevents both sides from accidentally choosing the same ID independently.

```
Role::Initiator (client)  → owns odd  IDs: 1, 3, 5, 7, …
Role::Acceptor  (server)  → owns even IDs: 2, 4, 6, 8, …
```

`next_local_id` starts at `1` for `Initiator` and `2` for `Acceptor` and increments by `2` on each `open_stream()` call. If the counter would overflow `u32::MAX`, `open_stream()` returns `TransportError::StreamIdExhausted`.

The `receiver::handle_open` function validates parity on incoming `Open` frames: a frame opening a stream with the wrong parity for its direction is rejected as `InvalidFrame`.

---

## 4. Buffering and auto-flush

`StreamManager` uses an internal helper for all frame writes:

```rust
async fn buffer_and_maybe_flush(&mut self, frame: &Frame) -> Result<(), TransportError>
```

This buffers the frame via `Connection::buffer_frame`, then calls `Connection::flush` **only if** `write_buf_len() >= FLUSH_THRESHOLD` (32 KiB).

This means:
- **Small bursts** are batched automatically — multiple `send_data` / `close_stream` calls in quick succession result in one `write_all` syscall.
- **Sustained writes** flush themselves once the threshold is crossed — the buffer never grows without bound.
- **Explicit flush** is always available via `StreamManager::flush()` when the application needs to guarantee delivery before waiting for a response.

`reset_stream` and `send_ping` bypass `buffer_and_maybe_flush` and call `Connection::send_frame` directly (immediate flush), because these are urgent control messages.

```
FLUSH_THRESHOLD = 32 KiB   (32 * 1024 bytes)
```

---

## 5. Public API

### 5.1 Construction

```rust
StreamManager::new(conn: Connection<T>, role: Role) -> StreamManager<T>
```

`role` must match the application's position in the connection:
- Client initiates → `Role::Initiator`
- Server accepts  → `Role::Acceptor`

### 5.2 Opening and closing streams

#### `open_stream() -> Result<u32, TransportError>`

1. Allocate the next local ID (`next_local_id`).
2. Increment `next_local_id` by 2 (saturating check → `StreamIdExhausted`).
3. Insert a new `Stream::new(id)` into `streams`.
4. Buffer an `Open` frame (`stream_id = id`, no payload) via `buffer_and_maybe_flush`.
5. Return `id`.

The `Open` frame tells the remote peer that this stream ID is now in use. The peer's `receiver::handle_open` registers the stream on their side.

#### `close_stream(stream_id) -> Result<(), TransportError>`

1. Look up the stream; error if not found.
2. Return `StreamClosed` if `state.is_closed()` already.
3. Call `stream.on_local_close()` — advances state to `HalfClosedLocal` or `Closed`.
4. Buffer a `Close` frame with `FLAG_FIN`.
5. If state is now `Closed`, remove the stream from the map.

The remote peer mirrors this by calling `stream.on_remote_close()` inside `receiver::handle_close`, which may also reach `Closed` and remove the stream on their side.

#### `reset_stream(stream_id) -> Result<(), TransportError>`

1. Look up the stream; error if not found.
2. Call `stream.on_reset()` — state becomes `Reset`.
3. Send a `Reset` frame immediately (bypasses `buffer_and_maybe_flush`; calls `conn.send_frame` for immediate flush).
4. Remove the stream from the map.

### 5.3 Sending data

#### `send_data(stream_id, payload: Vec<u8>) -> Result<(), TransportError>`

1. Return `Ok(())` immediately if `payload` is empty.
2. Return `FrameTooLarge` if `payload.len() > MAX_FRAME_SIZE` (16 MiB).
3. Look up the stream; error if not found.
4. Return `StreamNotWritable` if `!state.can_send()`.
5. Call `stream.check_send_window(payload.len())` — return `FlowControlViolation` if the window is exhausted.
6. Call `stream.consume_send_window(payload.len())` — deduct the bytes from the send window.
7. Build a `Data` frame and buffer it via `buffer_and_maybe_flush`.

### 5.4 Receiving frames and data

#### `recv_frame() -> Result<Frame, TransportError>`

Reads the next frame from the connection, dispatches it internally, and returns the raw `Frame` to the caller. See [§6](#6-inbound-frame-dispatch-recv_frame) for the dispatch table.

The returned `Frame` is useful for inspecting `frame_type`, `stream_id`, and `flags`. For `Data` frames the payload is also accessible in the returned frame — the same bytes are simultaneously queued in the stream's receive queue.

#### `recv_data(stream_id) -> Result<Option<Bytes>, TransportError>`

1. Look up the stream; error if not found.
2. Pop the next payload from `stream.pop_recv()`.
3. If a payload was returned and the stream can still receive, build a `Window` frame containing the byte count as a big-endian `u32` and buffer it via `buffer_and_maybe_flush`. This automatically replenishes the peer's send window.
4. Return `Ok(Some(bytes))` or `Ok(None)` if the queue was empty.

### 5.5 Ping / Pong

#### `send_ping(payload: Vec<u8>) -> Result<(), TransportError>`

Builds a `Ping` frame with `FLAG_CONTROL` and calls `conn.send_frame` directly (immediate flush). The remote `StreamManager` will echo the payload back in a `Pong` frame the next time `recv_frame` is called.

Pong handling inside `recv_frame`: when a `Pong` arrives the manager currently passes it through to the caller without additional processing (`Pong` is in the "later phase" no-op arm).

### 5.6 Inspection

| Method                   | Returns                                                   |
|--------------------------|-----------------------------------------------------------|
| `stream_count()`         | Number of currently live streams in the map               |
| `stream_state(id)`       | `Option<StreamState>` for the given stream                |
| `send_window(id)`        | `Option<i64>` — remaining send window for a stream        |
| `flush()`                | Force-flush `Connection`'s write buffer immediately       |

---

## 6. Inbound frame dispatch (`recv_frame`)

```
recv_frame()
  │
  ▼ conn.recv_frame() → Frame
  │
  ├── Open    → receiver::handle_open(streams, role, stream_id)
  ├── Data    → receiver::handle_data(streams, stream_id, payload.clone())
  ├── Close   → receiver::handle_close(streams, stream_id)
  ├── Reset   → receiver::handle_reset(streams, stream_id)
  ├── Window  → receiver::handle_window(streams, stream_id, &payload)
  ├── Ping    → build Pong(same payload), conn.send_frame (immediate flush)
  └── Pong / Settings / Hello / Welcome / Error
              → (no-op — handled in a later phase)
  │
  ▼ return Frame to caller
```

After dispatch the raw `Frame` is always returned to the caller so it can inspect `frame_type`, `stream_id`, and payload for application-level logic.

---

## 7. Flow-control loop

The full credit loop between two peers, step by step:

```
  Sender (Initiator)                    Receiver (Acceptor)
  ──────────────────                    ──────────────────────
  send_data(id, 4096 bytes)
    check_send_window(4096)  ✓
    consume_send_window(4096)
    buffer Data frame
    (auto-flush if ≥ 32 KiB)
                              ──── Data(stream=id, 4096 B) ────▶

                                         recv_frame()
                                           handle_data → push_recv
                                         recv_data(id)
                                           pop_recv() → Some(bytes)
                                           send Window(id, 4096)
                              ◀──── Window(stream=id, payload=[0,0,16,0]) ────

  recv_frame()
    handle_window → apply_window_increment(4096)
    send_window restored by 4096
```

The loop is automatic — the application only calls `send_data` and `recv_data`; the manager handles `Window` frame generation and consumption transparently.

---

## 8. Error conditions

| Error                          | Method          | Cause                                              |
|--------------------------------|-----------------|----------------------------------------------------|
| `StreamIdExhausted`            | `open_stream`   | `next_local_id` would overflow `u32`               |
| `UnknownStream(id)`            | several         | Frame or call targets a stream not in the map      |
| `StreamClosed(id)`             | `close_stream`  | Stream is already in `Closed` or `Reset` state     |
| `StreamNotWritable(id)`        | `send_data`     | Stream state does not allow sending                |
| `FlowControlViolation`         | `send_data`     | `payload.len() > send_window`                      |
| `FrameTooLarge`                | `send_data`     | `payload.len() > MAX_FRAME_SIZE` (16 MiB)          |
| `InvalidFrame`                 | `recv_frame`    | Parity error, duplicate stream ID, etc.            |
| `ConnectionClosed`             | `recv_frame`    | Underlying stream returned EOF                     |
| `BufferOverflow`               | any send path   | Write buffer in `Connection` would exceed 256 KiB  |
| `Io(_)`                        | any             | Underlying `std::io::Error`                        |

---

## 9. What is not yet handled

The following `Frametype` variants are received and returned to the caller but not processed internally by `recv_frame`:

| Frame type  | Status   | Planned action                                         |
|-------------|----------|--------------------------------------------------------|
| `Pong`      | Partial  | Application inspects the returned frame; ping is still echoed immediately |
| `Settings`  | Partial  | Connection-level settings are accepted; validation is implemented, but no application-level behavior is defined yet |
| `Hello`     | Implemented | Drives the connection handshake state machine on the acceptor |
| `Welcome`   | Implemented | Server reply to `Hello`; completes handshake on initiator |
| `Error`     | Implemented | Decodes `ErrorPayload` and converts inbound `Error` frames into `TransportError` |
