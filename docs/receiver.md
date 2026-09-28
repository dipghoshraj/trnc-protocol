# Receiver (`transport::tcp::receiver`)

`receiver.rs` contains a set of pure, stateless functions that mutate the `streams` map in response to inbound frames. These functions are called exclusively by `StreamManager::recv_frame` immediately after a `Frame` is decoded off the wire.

> Part of the [transport layer documentation](README.md).

---

## Table of Contents

1. [Design intent](#1-design-intent)
2. [Dispatch table](#2-dispatch-table)
3. [Function reference](#3-function-reference)
   - [`handle_open`](#31-handle_open)
   - [`handle_data`](#32-handle_data)
   - [`handle_close`](#33-handle_close)
   - [`handle_reset`](#34-handle_reset)
   - [`handle_window`](#35-handle_window)
4. [What the receiver does not do](#4-what-the-receiver-does-not-do)
5. [Error conditions](#5-error-conditions)

---

## 1. Design intent

Separating the receiver functions from `StreamManager` keeps two things clean:

- **`StreamManager`** focuses on outbound operations, the auto-flush policy, and returning frames to callers.
- **`receiver`** is a set of focused, easily testable functions that each handle one frame type.

Every function takes `&mut HashMap<u32, Stream>` as its first argument rather than `&mut StreamManager`. This means the functions can be unit-tested with a bare `HashMap` without constructing a full TCP connection.

---

## 2. Dispatch table

`StreamManager::recv_frame` dispatches inbound frames as follows:

```
Frame type   Handler function                              Immediate action
───────────  ────────────────────────────────────────────  ──────────────────────────────────────────
Open         receiver::handle_open(streams, role, id)      Register new stream or return InvalidFrame
Data         receiver::handle_data(streams, id, payload)   Enqueue payload in stream's recv_queue
Close        receiver::handle_close(streams, id)           Advance half-close state; remove if Closed
Reset        receiver::handle_reset(streams, id)           Remove stream unconditionally
Window       receiver::handle_window(streams, id, &bytes)  Increment stream's send_window
Ping         (handled inline in StreamManager)             Reply with Pong immediately
Pong / Settings / Hello / Welcome / Error
             (no-op — future phases)
```

---

## 3. Function reference

### 3.1 `handle_open`

```rust
pub fn handle_open(
    streams: &mut HashMap<u32, Stream>,
    role: Role,
    stream_id: u32,
) -> Result<(), TransportError>
```

Called when a remote peer sends an `Open` frame to create a new logical stream.

**Steps:**

1. Determine the expected parity for remotely-opened streams:
   - If we are `Initiator`, the remote is `Acceptor` → remote IDs must be **even** (`stream_id % 2 == 0`).
   - If we are `Acceptor`, the remote is `Initiator` → remote IDs must be **odd** (`stream_id % 2 == 1`).
2. If parity is wrong → `TransportError::InvalidFrame` ("remote opened stream N but parity is wrong").
3. If `stream_id` already exists in `streams` → `TransportError::InvalidFrame` ("remote opened stream N but it already exists").
4. Insert `Stream::new(stream_id)` into `streams`.

**Why parity matters:** Both sides allocate IDs independently. Without parity enforcement, both peers could choose ID `1` simultaneously. The odd/even split ensures that each side's IDs are disjoint.

---

### 3.2 `handle_data`

```rust
pub fn handle_data(
    streams: &mut HashMap<u32, Stream>,
    stream_id: u32,
    payload: Vec<u8>,
) -> Result<(), TransportError>
```

Called when a `Data` frame arrives on an established stream.

**Steps:**

1. Look up `stream_id`; return `TransportError::UnknownStream` if not found.
2. Check `stream.state.can_receive()`:
   - `false` → `TransportError::InvalidFrame` ("Data frame on non-receivable stream N").
3. Call `stream.push_recv(Bytes::from(payload))`:
   - Appends the payload to `stream.recv_queue`.
   - Decrements `stream.recv_window` by `payload.len()` (clamped to 0).

The payload is passed by value; `StreamManager` clones it from the original `Frame` before calling this function so the original `Frame` (with its payload) can still be returned to the application.

---

### 3.3 `handle_close`

```rust
pub fn handle_close(
    streams: &mut HashMap<u32, Stream>,
    stream_id: u32,
) -> Result<(), TransportError>
```

Called when a `Close(FIN)` frame arrives, indicating the remote peer will send no more `Data` frames on this stream.

**Steps:**

1. Look up `stream_id`; return `TransportError::UnknownStream` if not found.
2. Call `stream.on_remote_close()`:
   - `Open` → `HalfClosedRemote`
   - `HalfClosedLocal` → `Closed`
3. If state is now `Closed`, remove `stream_id` from `streams`.

If `Closed` is reached it means both sides have half-closed — the stream is fully done and is cleaned up immediately.

---

### 3.4 `handle_reset`

```rust
pub fn handle_reset(
    streams: &mut HashMap<u32, Stream>,
    stream_id: u32,
)
```

Called when a `Reset` frame arrives. This is an unconditional, abortive termination.

**Steps:**

1. Remove `stream_id` from `streams` (no validation; if the stream is unknown it simply does not exist and nothing happens — `HashMap::remove` is a no-op for missing keys).

`handle_reset` is the only receiver function that does not return `Result` — there is no meaningful error to report for a reset.

---

### 3.5 `handle_window`

```rust
pub fn handle_window(
    streams: &mut HashMap<u32, Stream>,
    stream_id: u32,
    payload: &[u8],
) -> Result<(), TransportError>
```

Called when a `Window` frame arrives, advertising that the remote peer has freed receive buffer space and can now accept more data.

**Steps:**

1. Look up `stream_id`; return `TransportError::UnknownStream` if not found.
2. Parse `payload` as a 4-byte big-endian `u32`:
   - If `payload.len() != 4` → `TransportError::InvalidFrame` ("Window frame payload must be exactly 4 bytes").
3. Call `stream.apply_window_increment(increment)`:
   - `stream.send_window = stream.send_window.saturating_add(increment as i64)`

The `saturating_add` guards against integer overflow from a malicious or buggy peer that sends repeated large `Window` frames.

---

## 4. What the receiver does not do

- **Does not send any frames.** The receiver only mutates in-memory state. Any outgoing frames (e.g. the `Window` frame sent in response to consuming data via `recv_data`) are generated by `StreamManager`, not by these functions.
- **Does not handle `Ping`** — that is handled inline in `StreamManager::recv_frame` because it requires sending a `Pong` back.
- **Does not handle `Pong`, `Settings`, `Hello`, `Welcome`, `Error`** — those are handled by `StreamManager` itself; `receiver.rs` remains focused on stream-level frame handling.

---

## 5. Error conditions

| Error                             | Function         | Cause                                                   |
|-----------------------------------|------------------|---------------------------------------------------------|
| `InvalidFrame("parity is wrong")` | `handle_open`    | Remote opened a stream with the wrong ID parity         |
| `InvalidFrame("already exists")`  | `handle_open`    | Remote reused an existing stream ID                     |
| `UnknownStream(id)`               | `handle_data`, `handle_close`, `handle_window` | Frame targeted a stream not in the map |
| `InvalidFrame("non-receivable")`  | `handle_data`    | `Data` frame arrived on a half-closed or closed stream  |
| `InvalidFrame("4 bytes")`         | `handle_window`  | `Window` payload was not exactly 4 bytes                |
