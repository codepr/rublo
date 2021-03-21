mod filter;
pub mod server;

use chrono::Local;
use log::{Level, LevelFilter, Metadata, Record, SetLoggerError};
use serde::Deserialize;
use serde_yaml;

pub type AsyncResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, PartialEq, Deserialize)]
pub struct Config {
    listen_on: String,
    #[serde(default = "filter::ScaleFactor::small_scale_size")]
    scale_factor: filter::ScaleFactor,
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

    pub fn scale_factor(&self) -> &filter::ScaleFactor {
        &self.scale_factor
    }
}

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!(
                "{} - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLogger = SimpleLogger;

pub fn init_logging() -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Info))
}
