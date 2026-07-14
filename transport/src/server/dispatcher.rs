
use std::sync::Arc;
use super::actions::Actions;
use crate::errors::TransportError;
use super::resilient_server::{RequestEnvelope, ResponseEnvelope};

pub struct Dispatcher {
    actions: Arc<Actions>,
}

impl Dispatcher {
    
    pub fn new(actions: Arc<Actions>) -> Self {
        Dispatcher { actions: actions }
    }

    pub async fn dispatch(
        &self,
        request: RequestEnvelope,
    ) -> Result<ResponseEnvelope, TransportError> {
        let action_name = request.action.clone();
        if let Some(handler) = self.actions.get_handler(&action_name) {
            let response_payload = handler.call(request.payload).await?;
            Ok(ResponseEnvelope {
                payload: response_payload,
            })
        } else {
            Err(TransportError::ActionNotFound(action_name))
        }
    }
}