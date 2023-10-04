pub mod ethereum;
pub mod mockchain;

use super::config::CommandConfig;

#[derive(Debug, PartialEq, Eq)]
pub enum IngestorMode {
    Full,
    NoProducer,
    NoResumer,
    StdOut,
}

impl From<&CommandConfig> for IngestorMode {
    fn from(cfg: &CommandConfig) -> Self {
        match cfg {
            CommandConfig {
                resumer: Some(_),
                producer: Some(_),
                ..
            } => IngestorMode::Full,
            CommandConfig {
                resumer: Some(_),
                producer: None,
                ..
            } => IngestorMode::NoProducer,
            CommandConfig {
                resumer: None,
                producer: Some(_),
                ..
            } => IngestorMode::NoResumer,
            CommandConfig {
                resumer: None,
                producer: None,
                ..
            } => IngestorMode::StdOut,
        }
    }
}
