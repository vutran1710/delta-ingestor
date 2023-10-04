use crate::core::ResumerTrait;
use crate::errors::ResumerError;
use crate::proto::BlockTrait;
use common_libs::async_trait::async_trait;
use common_libs::redis::aio::Connection;
use common_libs::redis::AsyncCommands;
use common_libs::redis::Client;
use std::collections::HashMap;

#[derive(Clone)]
pub struct RedisStore {
    client: Client,
    resumer_key: String,
}

impl RedisStore {
    pub async fn new(redis_uri: String, resumer_key: String) -> Result<Self, ResumerError> {
        let client = Client::open(redis_uri.as_str())
            .map_err(|e| ResumerError::Initialization(e.to_string()))?;
        let store = RedisStore {
            client,
            resumer_key,
        };

        Ok(store)
    }

    async fn get_con(&self) -> Result<Connection, ResumerError> {
        self.client
            .get_async_connection()
            .await
            .map_err(|e| ResumerError::OperationFailed(e.to_string()))
    }

    pub async fn clear(&self) -> Result<(), ResumerError> {
        let mut conn = self.get_con().await?;
        conn.del(self.resumer_key.clone())
            .await
            .map_err(|e| ResumerError::OperationFailed(e.to_string()))?;
        Ok(())
    }

    async fn read_blocks_from_redis<B: BlockTrait>(&self) -> Result<Vec<B>, ResumerError> {
        let mut con = self.get_con().await?;
        let raw_data: HashMap<u64, String> = con
            .hgetall(self.resumer_key.clone())
            .await
            .map_err(|e| ResumerError::OperationFailed(e.to_string()))?;
        let blocks = self.convert_redis_values_to_blocks(raw_data);
        Ok(blocks)
    }

    fn convert_redis_values_to_blocks<B: BlockTrait>(
        &self,
        values: HashMap<u64, String>,
    ) -> Vec<B> {
        if values.is_empty() {
            return vec![];
        }
        let mut block_numbers = values.keys().cloned().collect::<Vec<u64>>();
        block_numbers.sort();

        let mut blocks = Vec::new();
        for block_number in block_numbers.into_iter() {
            let hashes = values.get(&block_number).unwrap().split(':');
            let block_hash = hashes.clone().next().unwrap().to_string();
            let parent_hash = hashes.clone().nth(1).unwrap().to_string();
            let block = B::from((block_number, block_hash, parent_hash));
            blocks.push(block)
        }
        blocks
    }
}

#[async_trait]
impl<B: BlockTrait> ResumerTrait<B> for RedisStore {
    async fn save_latest_blocks(&self, blocks: &[B]) -> Result<(), ResumerError> {
        // Reset before saving!
        self.clear().await?;
        let mut con = self.get_con().await?;

        let values = blocks
            .iter()
            .map(|b| {
                let hash = b.get_hash();
                let parent_hash = b.get_parent_hash();
                let arg_value = format!("{hash}:{parent_hash}");
                (b.get_number(), arg_value)
            })
            .collect::<Vec<(u64, String)>>();

        con.hset_multiple(self.resumer_key.clone(), &values)
            .await
            .map_err(|e| ResumerError::OperationFailed(e.to_string()))?;
        Ok(())
    }

    async fn get_latest_blocks(&self) -> Result<Vec<B>, ResumerError> {
        self.read_blocks_from_redis().await.map(Ok)?
    }
}
