mod redis;
mod stdout;

use common_libs::async_trait::async_trait;
use redis::RedisStore;
use stdout::StdOutResumer;

use crate::config::CommandConfig;
use crate::core::ResumerTrait;
use crate::errors::ResumerError;
use crate::name_svc::NameService;
use crate::proto::BlockTrait;

pub enum Resumer<B: BlockTrait> {
    Redis(RedisStore),
    StdOut(StdOutResumer<B>),
}

#[async_trait]
impl<B: BlockTrait> ResumerTrait<B> for Resumer<B> {
    async fn save_latest_blocks(&self, blocks: &[B]) -> Result<(), ResumerError> {
        match self {
            Resumer::Redis(resumer) => resumer.save_latest_blocks(blocks).await,
            Resumer::StdOut(resumer) => resumer.save_latest_blocks(blocks).await,
        }
    }

    async fn get_latest_blocks(&self) -> Result<Vec<B>, ResumerError> {
        match self {
            Resumer::Redis(resumer) => resumer.get_latest_blocks().await,
            Resumer::StdOut(resumer) => resumer.get_latest_blocks().await,
        }
    }
}

pub async fn create_resumer<B: BlockTrait>(
    cfg: &CommandConfig,
    name_service: &NameService,
) -> Result<Resumer<B>, ResumerError> {
    match cfg.resumer.clone() {
        Some(uri) => {
            let resumer = RedisStore::new(uri, name_service.resumer_key.to_owned()).await?;
            Ok(Resumer::Redis(resumer))
        }
        None => {
            let resumer = StdOutResumer::default();
            Ok(Resumer::StdOut(resumer))
        }
    }
}
