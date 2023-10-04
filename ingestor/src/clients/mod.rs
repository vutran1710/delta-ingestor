mod ethereum;
mod mockchain;

pub use ethereum::Client as EthereumClient;
pub use ethereum::NodeType;
pub use ethereum::RRClient as RREthClient;

pub use mockchain::BlockID;
pub use mockchain::MockChain;
pub use mockchain::MockClient;
