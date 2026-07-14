use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use crate::errors::TransportError;
pub struct Actions {
    action_map: HashMap<String, Arc<dyn Handler>>,
}


#[async_trait]
pub trait Handler: Send + Sync {
    async fn call(&self,payload: Vec<u8>,) -> Result<Vec<u8>, TransportError>;
}



impl Actions {
    pub fn new() -> Self {
        Self {
            action_map: HashMap::new(),
        }
    }

    pub fn register_action<H>(&mut self, action_name: &str, handler: H)
    where
        H: Handler + 'static,
    {
        self.action_map.insert(action_name.to_string(), Arc::new(handler));
        // self
    }

    pub fn get_handler(&self, action_name: &str) -> Option<Arc<dyn Handler>> {
        self.action_map.get(action_name).cloned()
    }
}