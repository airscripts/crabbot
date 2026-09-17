use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Plugin handshake failed: {0}")]
    Handshake(String),
    #[error("Unsupported protocol: {0}")]
    Protocol(String),
    #[error("Denied: {0}")]
    Denied(String),
    #[error("Turn limit reached: {0}")]
    Limit(String),
}

pub type Result<T> = std::result::Result<T, Error>;
