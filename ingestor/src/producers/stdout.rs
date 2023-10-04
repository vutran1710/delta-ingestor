use crate::core::ProducerTrait;
use crate::errors::ProducerError;
use crate::proto::BlockTrait;
use common_libs::async_trait::async_trait;
use common_libs::log::warn;
use common_libs::tokio::sync::Mutex;
use common_libs::tokio::time::sleep;
use common_libs::tokio::time::Duration;
use std::sync::Arc;

pub struct StdOutProducer<B: BlockTrait> {
    pub blocks: Arc<Mutex<Vec<B>>>,
}

impl<B: BlockTrait> StdOutProducer<B> {
    pub fn new() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(Vec::<B>::new())),
        }
    }
}

#[async_trait]
impl<B: BlockTrait> ProducerTrait<B> for StdOutProducer<B> {
    async fn publish_blocks(&self, blocks: Vec<B>) -> Result<(), ProducerError> {
        let count = blocks.len();
        let block_numbers = blocks.iter().map(|b| b.get_number()).collect::<Vec<u64>>();
        warn!(
            "------ @@StdOutProducer: received {count} blocks, taking 2 secs -> {:?}",
            block_numbers
        );

        let mut block_queue = self.blocks.lock().await;
        block_queue.append(&mut blocks.clone());
        sleep(Duration::from_secs(2)).await;
        Ok(())
    }
}
