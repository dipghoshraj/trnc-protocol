

use crate::tcp::connection::{Connection};
use crate::tcp::stream::{Stream};
use std::collections::HashMap;

const FLUSH_THRESHOLD: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Initiator, // client side
    Acceptor, // server side
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamManager<T> {
    conn: Connection<T>,
    streams: HashMap<u32, Stream>,
    next_local_id: u32,
    role: Role,
}

