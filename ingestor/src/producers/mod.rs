mod delta;
mod stdout;
mod nats;

use delta::DeltaLakeProducer;
use stdout::StdOutProducer;
use strum::Display;

use crate::config::CommandConfig;
use crate::core::ProducerTrait;
use crate::errors::ProducerError;
use crate::name_svc::NameService;
use crate::proto::BlockTrait;

use common_libs::async_trait::async_trait;
use common_libs::log::info;

#[derive(Display)]
pub enum Producer<B: BlockTrait> {
    #[strum(serialize = "Stdout-Producer")]
    StdOut(StdOutProducer<B>),
    #[strum(serialize = "Delta-Producer")]
    DeltaLake(DeltaLakeProducer),
}

#[async_trait]
impl<B: BlockTrait> ProducerTrait<B> for Producer<B> {
    async fn publish_blocks(&self, blocks: Vec<B>) -> Result<(), ProducerError> {
        match self {
            Producer::StdOut(producer) => producer.publish_blocks(blocks).await,
            Producer::DeltaLake(producer) => producer.publish_blocks(blocks).await,
        }
    }
}

pub async fn create_producer<B: BlockTrait>(
    cfg: &CommandConfig,
    _name_service: &NameService,
) -> Result<Producer<B>, ProducerError> {
    let producer_type = cfg.producer.clone().unwrap_or("unspecified".to_string());
    match producer_type.as_str() {
        "delta" => {
            info!("Setting up DeltaLake Producer");
            let producer = DeltaLakeProducer::new(cfg).await?;
            Ok(Producer::DeltaLake(producer))
        }
        _ => {
            info!("Unknown Producer type, using stdout as Producer");
            let producer = StdOutProducer::new();
            Ok(Producer::StdOut(producer))
        }
    }
}
