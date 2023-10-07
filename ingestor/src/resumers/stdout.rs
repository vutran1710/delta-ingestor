use crate::core::ResumerTrait;
use crate::errors::ResumerError;
use crate::proto::BlockTrait;
use common_libs::async_trait::async_trait;
use common_libs::log::warn;
use common_libs::tokio::sync::RwLock;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct StdOutResumer<B: BlockTrait> {
    pub checkpoints: AtomicI64,
    pub blocks: Arc<RwLock<Vec<B>>>,
}

impl<B: BlockTrait> Default for StdOutResumer<B> {
    fn default() -> Self {
        Self {
            checkpoints: AtomicI64::new(-1),
            blocks: Arc::new(RwLock::new(Vec::<B>::new())),
        }
    }
}

#[async_trait]
impl<B: BlockTrait> ResumerTrait<B> for StdOutResumer<B> {
    async fn save_latest_blocks(&self, blocks: &[B]) -> Result<(), ResumerError> {
        self.checkpoints.load(Ordering::SeqCst);
        let latest = blocks.last().unwrap().get_number();
        let block_numbers = blocks.iter().map(|b| b.get_number()).collect::<Vec<u64>>();
        warn!(
            "------ @@StdOutCheckpoint: saving blocks {:?}",
            block_numbers
        );
        self.checkpoints.store(latest as i64, Ordering::SeqCst);
        let mut list = self.blocks.write().await;
        let mut new_blocks = blocks.to_vec();
        list.clear();
        list.append(&mut new_blocks);
        Ok(())
    }

    async fn get_latest_blocks(&self) -> Result<Vec<B>, ResumerError> {
        Ok(self.blocks.read().await.clone())
    }
}
