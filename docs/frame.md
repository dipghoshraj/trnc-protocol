# Frame Module (`transport::frame`)

The `frame` module owns everything that touches the TRNC wire format: the data types that represent a frame in memory, the constants that define the protocol, and the functions that convert between bytes and those types.

> Part of the [transport layer documentation](README.md).

---

## Table of Contents

1. [Module layout](#1-module-layout)
2. [Wire format recap](#2-wire-format-recap)
3. [`header.rs` — Header struct and wire constants](#3-headerrs--header-struct-and-wire-constants)
4. [`frame.rs` — Frame and Frametype](#4-framers--frame-and-frametype)
5. [`encoder.rs` — Serialisation](#5-encoderrs--serialisation)
6. [`decoder.rs` — Deserialisation](#6-decoderrs--deserialisation)
7. [Validation rules](#7-validation-rules)
8. [Adding a new frame type](#8-adding-a-new-frame-type)

---

## 1. Module layout

```
frame/
├── mod.rs       — re-exports header, frame, encoder, decoder
├── header.rs    — Header struct + all wire constants + Header::validate()
├── frame.rs     — Frame struct + Frametype enum + Frame constructors
├── encoder.rs   — encode(frame) → Bytes
└── decoder.rs   — decode(&[u8]) → Result<(Frame, usize)>
```

`mod.rs` re-exports all four sub-modules so callers can write `frame::header::Header`, `frame::frame::Frame`, etc.

---

## 2. Wire format recap

Every TRNC message on the wire is a frame. A frame is a fixed 16-byte header immediately followed by zero or more payload bytes.

```
Offset   Field            Size   Type / Encoding
──────   ──────────────   ────   ───────────────────────────
0–3      magic            4 B    [u8; 4]  — always "TRNC" (0x54 0x52 0x4E 0x43)
4        version          1 B    u8       — currently 1
5–6      flags            2 B    u16 big-endian
7–10     stream_id        4 B    u32 big-endian
11–14    payload_length   4 B    u32 big-endian
15       frame_type       1 B    u8
─────────────────────────────────────────
         TOTAL HEADER     16 B
16+      payload          payload_length bytes   (opaque)
```

All multi-byte integers are **big-endian**.

---

## 3. `header.rs` — Header struct and wire constants

### Constants

| Constant                | Value        | Purpose                                                       |
|-------------------------|--------------|---------------------------------------------------------------|
| `FRAME_MAGIC`           | `[0x54,0x52,0x4E,0x43]` (`"TRNC"`) | Written into every outgoing frame; checked on every incoming frame |
| `CURRENT_VERSION`       | `1`          | Version byte written into all outgoing frames                 |
| `MIN_SUPPORTED_VERSION` | `1`          | Lowest version accepted from a peer                           |
| `MAX_FRAME_SIZE`        | `16_777_216` (16 MiB) | Maximum allowed `payload_length`; enforced before any allocation |
| `HEADER_SIZE`           | `16`         | Fixed byte length of the header                               |
| `FLAG_FIN`              | `0x0001`     | Bit 0 — final frame on a stream                               |
| `FLAG_ACK`              | `0x0002`     | Bit 1 — acknowledgement                                      |
| `FLAG_CONTROL`          | `0x0004`     | Bit 2 — frame is a control frame                             |

### `Header` struct

```rust
pub struct Header {
    pub magic:          [u8; 4],
    pub version:        u8,
    pub flags:          u16,
    pub stream_id:      u32,
    pub payload_length: u32,
    pub frame_type:     frame::frame::Frametype,
}
```

### `Header::validate()`

Called by the decoder immediately after parsing. Performs five checks in order:

1. `magic == FRAME_MAGIC` — else `TransportError::InvalidMagic`.
2. `MIN_SUPPORTED_VERSION <= version <= CURRENT_VERSION` — else `TransportError::InvalidVersion`.
3. `payload_length <= MAX_FRAME_SIZE` — else `TransportError::FrameTooLarge`.
4. `frame_type` is a known variant (exhaustive match that fails to compile if a new variant is added without updating `validate`).
5. No undefined flag bits are set — else `TransportError::InvalidFrame`.
6. `FLAG_CONTROL` consistency: control frame types (`Ping`, `Pong`, `Settings`, `Hello`, `Welcome`) **must** have `FLAG_CONTROL` set; data-plane types (`Open`, `Data`, `Close`, `Reset`, `Window`, `Error`) **must not**.

### Flag helpers

```rust
header.is_fin()     // flags & FLAG_FIN     != 0
header.is_ack()     // flags & FLAG_ACK     != 0
header.is_control() // flags & FLAG_CONTROL != 0
```

---

## 4. `frame.rs` — Frame and Frametype

### `Frame`

```rust
pub struct Frame {
    pub header:  Header,
    pub payload: Vec<u8>,
}
```

`payload.len()` must always equal `header.payload_length as usize`. The encoder enforces this; constructors set it automatically.

**Constructors**

| Constructor              | When to use                                                   |
|--------------------------|---------------------------------------------------------------|
| `Frame::new(ft, flags, stream_id, payload)` | Any frame with a non-empty payload          |
| `Frame::empty(ft, flags, stream_id)`        | Control and signalling frames with no payload (`Open`, `Close`, `Reset`, `Ping`) |

Both constructors build a valid `Header` for you — `payload_length` is derived from the `payload` argument.

### `Frametype`

```rust
#[repr(u8)]
pub enum Frametype {
    Open     = 1,
    Data     = 2,
    Close    = 3,
    Reset    = 4,
    Ping     = 5,
    Pong     = 6,
    Window   = 7,
    Error    = 8,
    Settings = 9,
    Hello    = 10,
    Welcome  = 11,
}
```

The discriminant values are **stable wire constants**. Any change is a breaking protocol change.

`Frametype::from_u8(byte)` returns `Option<Frametype>`. A `None` result means the byte is unknown; the decoder turns this into `TransportError::InvalidFrame` rather than panicking.

---

## 5. `encoder.rs` — Serialisation

```rust
pub fn encode(frame: &Frame) -> Result<Bytes, TransportError>
```

Writes the header fields then the payload into a single pre-allocated `BytesMut` and returns a frozen `Bytes` handle — a zero-copy slice.

**Step-by-step**

1. Pre-flight: assert `frame.payload.len() == frame.header.payload_length as usize`. Returns `TransportError::InvalidFrame` on mismatch.
2. Allocate `BytesMut` with capacity `HEADER_SIZE + payload.len()`.
3. Write 4 magic bytes.
4. Write `version` (1 byte).
5. Write `flags` as big-endian u16 (2 bytes).
6. Write `stream_id` as big-endian u32 (4 bytes).
7. Write `payload_length` as big-endian u32 (4 bytes).
8. Write `frame_type as u8` (1 byte).
9. Append `payload` bytes.
10. `freeze()` → return `Bytes`.

The output is directly writable to any `AsyncWrite` stream.

---

## 6. `decoder.rs` — Deserialisation

```rust
pub fn decode(buffer: &[u8]) -> Result<(Frame, usize), TransportError>
```

Attempts to extract **one** complete `Frame` from the front of a byte slice. Returns the frame and the number of bytes consumed (`HEADER_SIZE + payload_length`). The caller advances its buffer by `consumed`.

**Step-by-step**

1. If `buffer.len() < HEADER_SIZE` → `Err(NeedMoreData)`.
2. Parse all header fields from bytes 0–15.
3. Construct a `Frametype` via `from_u8`; unknown byte → `InvalidFrame`.
4. Call `Header::validate()` — magic, version, size, flags all checked here.
5. If `buffer.len() < HEADER_SIZE + payload_length` → `Err(NeedMoreData)`.
6. Copy bytes `[HEADER_SIZE .. HEADER_SIZE + payload_length]` into `payload: Vec<u8>`.
7. Return `Ok((Frame { header, payload }, total_consumed))`.

**`NeedMoreData` is an internal sentinel.** It is never returned to application code. `Connection::recv_frame` catches it, reads another 8 KiB chunk, and retries.

**The decoder never advances the buffer itself.** The caller (`Connection::recv_frame`) calls `BytesMut::advance(consumed)` after a successful decode to discard the processed bytes.

---

## 7. Validation rules

The table below summarises every check and where it is enforced:

| Rule                                          | Enforced in          | Error                  |
|-----------------------------------------------|----------------------|------------------------|
| First 4 bytes == `"TRNC"`                    | `Header::validate`   | `InvalidMagic`         |
| `version` in `[1, 1]`                         | `Header::validate`   | `InvalidVersion`       |
| `payload_length <= 16 MiB`                    | `Header::validate`   | `FrameTooLarge`        |
| `frame_type` byte is a known variant          | `decode` + `validate`| `InvalidFrame`         |
| No undefined flag bits                        | `Header::validate`   | `InvalidFrame`         |
| Control types carry `FLAG_CONTROL`            | `Header::validate`   | `InvalidFrame`         |
| Data types do not carry `FLAG_CONTROL`        | `Header::validate`   | `InvalidFrame`         |
| `payload.len() == header.payload_length`      | `encode`             | `InvalidFrame`         |

---

## 8. Adding a new frame type

1. Add a new variant to `Frametype` in `frame.rs` with the next unused `u8` discriminant.
2. Add the discriminant to `Frametype::from_u8` in `frame.rs`.
3. Add the variant to the exhaustive match inside `Header::validate` in `header.rs` (the compiler will refuse to compile until you do).
4. Decide whether the new type is a control frame or a data-plane frame and update the `is_control_type` match in `Header::validate` accordingly.
5. Add a handler in `receiver.rs` (or `manager.rs`) for inbound frames of this type.
6. Update `architecture.md` and this file.
