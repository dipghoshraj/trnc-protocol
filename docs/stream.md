# Stream (`transport::tcp::stream`)

A `Stream` represents one logical, ordered, bidirectional byte channel multiplexed over a single TCP connection. It is the unit of flow control and lifecycle tracking inside `StreamManager`.

> Part of the [transport layer documentation](README.md).

---

## Table of Contents

1. [What a stream is](#1-what-a-stream-is)
2. [Struct layout](#2-struct-layout)
3. [State machine](#3-state-machine)
4. [Flow-control windows](#4-flow-control-windows)
5. [Receive queue](#5-receive-queue)
6. [Method reference](#6-method-reference)
7. [Constants](#7-constants)
8. [Invariants](#8-invariants)

---

## 1. What a stream is

A stream is identified by a `u32` stream ID. Multiple streams can be open simultaneously on the same `Connection`. Each stream has:

- A **state** (Open → half-closed → Closed or Reset).
- A **send window** — how many bytes the remote peer currently allows us to send.
- A **receive window** — how many more bytes we are willing to accept from the remote peer.
- An in-memory **receive queue** of payload chunks waiting for the application to consume.

Streams are owned exclusively by `StreamManager`. Application code never constructs or holds a `Stream` directly; it interacts through `StreamManager`'s API (`send_data`, `recv_data`, `close_stream`, etc.).

---

## 2. Struct layout

```rust
pub struct Stream {
    pub id:          u32,
    pub state:       StreamState,
    pub send_window: i64,   // bytes we may still send to the remote peer
    pub recv_window: i64,   // bytes the remote peer may still send to us
    recv_queue:      VecDeque<Bytes>,
}
```

`send_window` and `recv_window` are `i64` even though they conceptually cannot be negative. The signed type simplifies saturating arithmetic and defensive clamping without needing casts.

---

## 3. State machine

```
              open_stream() / handle_open()
                          │
                          ▼
                       ┌──────┐
                       │ Open │  ← can_send() == true, can_receive() == true
                       └──┬───┘
             ┌────────────┴─────────────┐
   send Close(FIN)              receive Close(FIN)
   on_local_close()             on_remote_close()
             │                           │
             ▼                           ▼
   ┌──────────────────┐      ┌───────────────────┐
   │ HalfClosedLocal  │      │ HalfClosedRemote  │
   │ can_send=false   │      │ can_receive=false │
   │ can_receive=true │      │ can_send=true     │
   └────────┬─────────┘      └────────┬──────────┘
 receive Close(FIN)          send Close(FIN)
 on_remote_close()           on_local_close()
             │                           │
             └────────────┬──────────────┘
                          ▼
                       ┌────────┐
                       │ Closed │  ← is_closed() == true
                       └────────┘
                    (removed from map)

  Any state + RESET (either direction)
                          │
                          ▼
                       ┌───────┐
                       │ Reset │  ← is_closed() == true
                       └───────┘
                    (removed from map)
```

### Transitions

| Event                        | Method             | `Open`              | `HalfClosedLocal`   | `HalfClosedRemote`  |
|------------------------------|--------------------|---------------------|---------------------|---------------------|
| We send `Close(FIN)`         | `on_local_close()` | → `HalfClosedLocal` | (no-op)             | → `Closed`          |
| We receive `Close(FIN)`      | `on_remote_close()`| → `HalfClosedRemote`| → `Closed`          | (no-op)             |
| Either side sends/receives `Reset` | `on_reset()` | → `Reset`           | → `Reset`           | → `Reset`           |

Once a stream reaches `Closed` or `Reset`, `StreamManager` removes it from the `streams` map immediately.

### State helpers

```rust
state.can_send()    // true if Open or HalfClosedRemote
state.can_receive() // true if Open or HalfClosedLocal
state.is_closed()   // true if Closed or Reset
```

---

## 4. Flow-control windows

The transport uses a **credit-based flow control** model. Each side independently tracks two numbers:

```
Our perspective:

  send_window  — credits granted to us by the remote peer.
                 Decremented when we send a Data frame.
                 Incremented when we receive a Window frame.

  recv_window  — credits we have granted to the remote peer.
                 Decremented when we receive a Data frame (push_recv).
                 Restored when the application consumes data (pop_recv),
                 and a Window frame is sent back by StreamManager.
```

**Default window:** 65,536 bytes (64 KiB). Can be customised via `Stream::with_window`.

### Sending

Before buffering a `Data` frame, `StreamManager::send_data` calls:

```rust
stream.check_send_window(payload.len())?;  // Err if send_window < payload.len()
stream.consume_send_window(payload.len()); // send_window -= payload.len()
```

If the send window is exhausted the call returns `TransportError::FlowControlViolation` immediately — no data is buffered or sent.

### Receiving a Window frame

When a `Window` frame arrives from the peer:

```rust
stream.apply_window_increment(increment); // send_window += increment (saturating)
```

`saturating_add` prevents integer overflow if a misbehaving peer sends pathologically large increments.

### Receiving data

When a `Data` frame arrives:

```rust
stream.push_recv(payload); // recv_window -= payload.len() (clamped to 0)
```

When the application calls `StreamManager::recv_data` and pops a payload:

```rust
stream.pop_recv(); // recv_window += payload.len()
```

`StreamManager` then immediately sends a `Window` frame to the peer advertising the freed credit:

```
Window frame payload = 4-byte big-endian u32(payload.len())
```

This keeps the flow-control loop running automatically without any application-level intervention.

---

## 5. Receive queue

```rust
recv_queue: VecDeque<Bytes>
```

Inbound payloads are appended to the tail of `recv_queue` in arrival order (`push_recv`). `pop_recv` removes and returns the head. The queue is unbounded in size — back-pressure is provided by the receive window shrinking as data accumulates. A zero `recv_window` stops the remote peer from sending more.

---

## 6. Method reference

### Constructors

| Method                            | Description                                               |
|-----------------------------------|-----------------------------------------------------------|
| `Stream::new(id)`                 | Create with default window (`DEFAULT_WINDOW = 65_536`)    |
| `Stream::with_window(id, window)` | Create with a custom initial window (for handshake use)   |

### Flow control

| Method                             | Description                                                   |
|------------------------------------|---------------------------------------------------------------|
| `check_send_window(n)`             | Returns `Err(FlowControlViolation)` if `send_window < n`      |
| `consume_send_window(n)`           | `send_window -= n`, clamped to 0 (defensive)                  |
| `apply_window_increment(increment)`| `send_window = send_window.saturating_add(increment as i64)`  |

### Receive queue

| Method             | Description                                           |
|--------------------|-------------------------------------------------------|
| `push_recv(bytes)` | Enqueue payload; `recv_window -= bytes.len()` (clamped) |
| `pop_recv()`       | Dequeue next payload; `recv_window += payload.len()`; returns `Option<Bytes>` |
| `recv_queue_len()` | Number of payloads waiting in the queue               |

### State transitions

| Method              | Description                                                          |
|---------------------|----------------------------------------------------------------------|
| `on_local_close()`  | We sent a `Close(FIN)` frame                                         |
| `on_remote_close()` | We received a `Close(FIN)` frame                                     |
| `on_reset()`        | Either side sent or received a `Reset` frame; immediately terminal   |

---

## 7. Constants

| Constant         | Value    | Meaning                              |
|------------------|----------|--------------------------------------|
| `DEFAULT_WINDOW` | `65_536` | Initial send and receive window (64 KiB) |

---

## 8. Invariants

The following invariants are assumed to hold at all times while a `Stream` is alive inside `StreamManager`:

1. `payload.len()` matches `header.payload_length` on every frame that touches the stream (enforced by the encoder).
2. `send_window` is never negative after `consume_send_window` (clamped defensively).
3. A `Reset` state is terminal — `on_local_close` and `on_remote_close` are no-ops once `Reset` is reached (the state is simply left as-is by the `other => other` arm).
4. A stream in `Closed` or `Reset` state is removed from `StreamManager::streams` and will never be accessed again.
5. `recv_window` is always >= 0 after `push_recv` (clamped defensively).
