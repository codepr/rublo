mod filter;
mod scalable_filter;
pub mod server;

use serde::Deserialize;
use serde_yaml;

pub type AsyncResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, PartialEq, Deserialize)]
pub struct Config {
    listen_on: String,
    #[serde(default = "scalable_filter::ScaleFactor::small_scale_size")]
    scale_factor: scalable_filter::ScaleFactor,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
        let f = std::fs::File::open(path)?;
        let config: Config = serde_yaml::from_reader(f)?;
        return Ok(config);
    }

    pub fn listen_on(&self) -> &str {
        &self.listen_on
    }

    pub fn scale_factor(&self) -> &scalable_filter::ScaleFactor {
        &self.scale_factor
    }
}
