#![allow(dead_code)]

use common_libs::tokio::time::error::Elapsed;

#[derive(Debug, Clone)]
pub enum ClientError {
    Initialization(String),
    Serialization(String),
    DataNotFound(String),
    BadData(String),
    RateLimitExceeded(String),
    UnsupportedRequest(String),
    ServerUnavailable(String),
    Unknown(String),
    Timeout(String),
    BadConfig(String),
}

#[derive(Debug)]
pub enum ResumerError {
    Initialization(String),
    OperationFailed(String),
}

#[derive(Debug)]
pub enum ConfigStoreError {
    Initialization(String),
    OperationFailed(String),
    BadData(String),
    Disconnected(String),
}

#[derive(Debug)]
pub enum ProducerError {
    Initialization(String),
    Publish(String),
    HandleReorg(String),
    Operation(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ChainPieceError {
    DuplicateBlockNumber(u64),
    ReorgTooDeep,
    Interrupted(Option<u64>),
    StepBack(u64),
}

#[derive(Debug)]
pub enum IngestorError {
    Client(ClientError),
    Producer(ProducerError),
    Resumer(ResumerError),
    ConfigStore(ConfigStoreError),
    Undeterministic(String),
    ChainPiece(ChainPieceError),
}

impl From<ClientError> for IngestorError {
    fn from(e: ClientError) -> IngestorError {
        IngestorError::Client(e)
    }
}

impl From<ResumerError> for IngestorError {
    fn from(e: ResumerError) -> IngestorError {
        IngestorError::Resumer(e)
    }
}

impl From<ProducerError> for IngestorError {
    fn from(e: ProducerError) -> IngestorError {
        IngestorError::Producer(e)
    }
}

impl From<ConfigStoreError> for IngestorError {
    fn from(e: ConfigStoreError) -> IngestorError {
        IngestorError::ConfigStore(e)
    }
}

impl From<ChainPieceError> for IngestorError {
    fn from(e: ChainPieceError) -> IngestorError {
        IngestorError::ChainPiece(e)
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(value: serde_json::Error) -> Self {
        ClientError::Serialization(value.to_string())
    }
}

impl From<Elapsed> for ClientError {
    fn from(value: Elapsed) -> Self {
        ClientError::Timeout(format!("Too long (> {:?})", value))
    }
}
