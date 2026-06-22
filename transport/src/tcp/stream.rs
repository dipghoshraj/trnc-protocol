
// ── State machine ────────────────────────────────────────────────────────────
//
//                   Open
//                  /    \
//   send CLOSE(FIN)      receive CLOSE(FIN)
//        /                        \
// HalfClosedLocal          HalfClosedRemote
//        \                        /
//   receive CLOSE(FIN,ACK)  send CLOSE(FIN)
//         \                      /
//                  Closed
//
//    any state + RESET (either side) → Reset (immediately)

use bytes::Bytes;
use std::collections::VecDeque;
use crate::{errors::TransportError};


pub const DEFAULT_WINDOW: i64 = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamState {
    Open,
    HalfClosedLocal,
    HalfClosedRemote,
    Closed,
    Reset,
}

impl StreamState {
    pub fn can_send(&self) -> bool {
        matches!(self, StreamState::Open | StreamState::HalfClosedRemote)
    }
    pub fn can_receive(&self) -> bool {
        matches!(self, StreamState::Open | StreamState::HalfClosedLocal)
    }
    pub fn is_closed(&self) -> bool {
        matches!(self, StreamState::Closed | StreamState::Reset)
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stream {
    pub id: u32,
    pub state: StreamState,
    pub send_window: i64,
    pub recv_window: i64, 
    recv_queue: VecDeque<Bytes>,
}

impl Stream {

    pub fn new(id: u32) -> Self {
        Stream {
            id,
            state:       StreamState::Open,
            send_window: DEFAULT_WINDOW,
            recv_window: DEFAULT_WINDOW,
            recv_queue:  VecDeque::new(),
        }
    }

    // Create with a custom initial window (set during handshake).
    pub fn with_window(id: u32, initial_window: i64) -> Self {
        Stream {
            id,
            state:       StreamState::Open,
            send_window: initial_window,
            recv_window: initial_window,
            recv_queue:  VecDeque::new(),
        }
    }

    // Check if we can send `byte_count` bytes right now.
    // Returns Ok if yes, FlowControlViolation if the window is exhausted.
    pub fn check_send_window(&self, byte_count: usize) -> Result<(), TransportError> {
        if self.send_window < byte_count as i64 {
            return Err(TransportError::FlowControlViolation { stream_id: self.id });
        }
        Ok(())
    }

    // Deduct `byte_count` from send_window after a Data frame is sent.
    // Clamped to 0 on underflow (defensive; check_send_window should prevent this path, but we never let send_window go negative).
    pub fn consume_send_window(&mut self, byte_count: usize) {
        self.send_window -= byte_count as i64;
        if self.send_window < 0 {
            self.send_window = 0;
        }
    }

    // Add `increment` to send_window when a Window frame arrives from remote.increment is a u32 from the wire; we widen to i64 before adding.
    // saturating_add prevents i64 overflow from a malicious peer sending repeated Window frames.
    pub fn apply_window_increment(&mut self, increment: u32) {
        self.send_window = self.send_window.saturating_add(increment as i64);
    }

    // Enqueue an inbound payload for the application to consume. Called by StreamManager when a Data frame arrives for this stream.
    pub fn push_recv(&mut self, payload: Bytes) {
        self.recv_window -= payload.len() as i64;
        if self.recv_window < 0 {
            self.recv_window = 0;
        }
        self.recv_queue.push_back(payload);
    }

    // Pop the next payload for the application. Returns None if the queue is empty.
    pub fn pop_recv(&mut self) -> Option<Bytes> {
        let payload = self.recv_queue.pop_front()?;
        self.recv_window += payload.len() as i64;
        Some(payload)
    }

    pub fn recv_queue_len(&self) -> usize {
        self.recv_queue.len()
    }

    // ── State transitions ──────────────────────────────────────────────────────

    // We sent a CLOSE(FIN) frame.
    pub fn on_local_close(&mut self) {
        self.state = match self.state {
            StreamState::Open => StreamState::HalfClosedLocal,
            StreamState::HalfClosedRemote => StreamState::Closed,
            other => other,
        };
    }

    // We received a CLOSE(FIN) frame from the remote.
    pub fn on_remote_close(&mut self) {
        self.state = match self.state {
            StreamState::Open => StreamState::HalfClosedRemote,
            StreamState::HalfClosedLocal => StreamState::Closed,
            other => other,
        };
    }

    // Either side sent or received a RESET frame.
    // Immediately terminal regardless of current state.
    pub fn on_reset(&mut self) {
        self.state = StreamState::Reset;
    }


    
}