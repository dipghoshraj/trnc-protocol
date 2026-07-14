use byteser_derive::ByteSerializable;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use crate::errors::TransportError;
use byteser::ByteSerializable;

#[derive(ByteSerializable)]
pub struct RequestEnvelope {
    pub action: String,
    pub payload: Vec<u8>,
}

#[derive(ByteSerializable)]
pub struct ResponseEnvelope {
    pub payload: Vec<u8>,
}

pub struct Actions {
    action_map: HashMap<String, Arc<dyn Handler>>,
}


#[async_trait]
pub trait Handler: Send + Sync {
    async fn call(&self,payload: Vec<u8>,) -> Result<Vec<u8>, TransportError>;
}


#[async_trait]
pub trait TypedHandler: Send +Sync {
    type Request: ByteSerializable + Send;
    type Response: ByteSerializable + Send;
    async fn handle(&self,request: Self::Request,) -> Result<Self::Response, TransportError>;
}

pub struct HandlerAdapter<H> {
    inner: H,
}