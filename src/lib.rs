mod filter;
mod scalable_filter;
pub mod server;

pub type AsyncResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
