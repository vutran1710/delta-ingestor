use crate::clients::BlockID;
use crate::clients::MockChain;
use crate::core::ClientTrait;
use crate::errors::ClientError;
use crate::proto::mockchain::Block;
use common_libs::async_trait::async_trait;
use std::sync::Arc;

#[derive(Clone)]
pub struct FakeClient {
    pub block_store: Arc<MockChain>,
}

impl FakeClient {
    pub async fn new() -> Self {
        let block_store = Arc::new(MockChain::new().await);
        Self { block_store }
    }

    pub async fn fake_block_generator(&self, stop: u64, sleep_time: u8) {
        self.block_store
            .run(u64::from(sleep_time) * 1000, Some(stop))
            .await;
    }

    pub async fn get_block_store(&self) -> Vec<Block> {
        let head = (self.block_store.count().await - 1) as u64;
        self.block_store.get_blocks((0..head).collect()).await
    }
}

#[async_trait]
impl ClientTrait<Block> for FakeClient {
    async fn get_latest_block_number(&self) -> Result<u64, ClientError> {
        let block = self.block_store.get_block(BlockID::Latest).await.unwrap();
        Ok(block.block_number)
    }

    async fn get_light_blocks(
        &self,
        sorted_block_numbers: Vec<u64>,
    ) -> Result<Vec<Block>, ClientError> {
        self.get_full_blocks(sorted_block_numbers).await
    }

    async fn get_full_blocks(
        &self,
        sorted_block_numbers: Vec<u64>,
    ) -> Result<Vec<Block>, ClientError> {
        let blocks = self.block_store.get_blocks(sorted_block_numbers).await;
        Ok(blocks)
    }

    fn set_blocking_round(&self, _blocking_round: u8) {}
}
