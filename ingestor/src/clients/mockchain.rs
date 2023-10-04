use crate::config::CommandConfig;
use crate::core::ClientTrait;
use crate::errors::ClientError;
use crate::proto::mockchain::Block;
use crate::proto::BlockTrait;
use common_libs::async_trait::async_trait;
use common_libs::envy;
use common_libs::futures::future::join_all;
use common_libs::log::info;
use common_libs::log::warn;
use common_libs::tokio::sync::Mutex;
use common_libs::tokio::time::sleep;
use common_libs::tokio::time::Duration;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BlockID {
    Latest,
    Number(u64),
}

#[derive(Debug)]
pub struct ChainFork {
    pub fork_id: u8,
    pub blocks: Arc<Mutex<Vec<Block>>>,
}

impl ChainFork {
    pub fn new(fork_id: u8) -> Self {
        Self {
            fork_id,
            blocks: Arc::new(Mutex::new(vec![])),
        }
    }

    pub async fn mine(&self, start_block: Option<Block>) -> Block {
        let data = format!("this is fork#{}", self.fork_id);
        let mut blocks = self.blocks.lock().await;
        let block = match (blocks.last().map(|b| b.to_owned()), start_block) {
            (Some(prev_block), _) | (None, Some(prev_block)) => {
                let number = prev_block.get_number() + 1;
                let parent_hash = prev_block.get_hash();
                Block::mock_new(number, data, Some(parent_hash))
            }
            (None, None) => Block::mock_new(0, data, None),
        };
        blocks.push(block);
        blocks.last().cloned().unwrap()
    }

    pub async fn mine_until(&self, chain_head: u64, start_block: Option<Block>) {
        while self.mine(start_block.clone()).await.block_number != chain_head {}
    }

    pub async fn get_block(&self, block_id: BlockID) -> Option<Block> {
        let blocks = self.blocks.lock().await;
        if blocks.is_empty() {
            return None;
        }
        match block_id {
            BlockID::Latest => blocks.iter().last().cloned(),
            BlockID::Number(number) => blocks.iter().find(|b| b.get_number() == number).cloned(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct MockChainEnvars {
    pub mockchain_premine: Option<u64>,
}

#[derive(Clone)]
pub struct MockChain {
    pub chain: Arc<Mutex<Vec<u8>>>,
    pub forks: Arc<Mutex<Vec<ChainFork>>>,
}

impl MockChain {
    pub async fn new() -> Self {
        let genesis_fork = ChainFork::new(0);
        genesis_fork.mine(None).await;
        warn!("Mined the Genesis block#0");
        let forks = Arc::new(Mutex::new(vec![genesis_fork]));
        let chain = Arc::new(Mutex::new(vec![0]));
        Self { chain, forks }
    }

    pub async fn get_block(&self, block_id: BlockID) -> Option<Block> {
        let chain = self.chain.lock().await;
        let forks = self.forks.lock().await;
        let fork_idx = match block_id {
            BlockID::Latest => chain.iter().last().unwrap().to_owned(),
            BlockID::Number(number) => chain.iter().nth(number as usize)?.to_owned(),
        };
        let fork = forks.iter().nth(fork_idx as usize).unwrap();
        fork.get_block(block_id).await
    }

    pub async fn get_blocks(&self, block_ids: Vec<u64>) -> Vec<Block> {
        let queries = block_ids
            .into_iter()
            .map(|b| self.get_block(BlockID::Number(b)));
        let result = join_all(queries).await;
        result.into_iter().flatten().collect()
    }

    pub async fn count(&self) -> usize {
        self.chain.lock().await.len()
    }

    #[cfg(test)]
    pub async fn are_blocks_on_chain(&self, blocks: &[Block]) -> Vec<bool> {
        let block_numbers = blocks.iter().map(|b| b.get_number()).collect::<Vec<u64>>();
        let on_chain_blocks = self.get_blocks(block_numbers).await;
        blocks
            .iter()
            .zip(on_chain_blocks)
            .map(|(input_block, on_chain_block)| {
                input_block.get_hash() == on_chain_block.get_hash()
            })
            .collect::<Vec<bool>>()
    }

    pub async fn mine(&self) {
        let latest_block = self.get_block(BlockID::Latest).await;
        let forks = self.forks.lock().await;
        let fork = forks.iter().last().unwrap();
        fork.mine(latest_block).await;
        let mut chain = self.chain.lock().await;
        chain.push(fork.fork_id);
    }

    pub async fn mine_until(&self, stop: u64) {
        while self.count().await < stop.try_into().unwrap() {
            self.mine().await;
        }
    }

    pub async fn run(&self, block_time_as_ms: u64, stop: Option<u64>) {
        warn!("MockChain start running...");
        loop {
            self.mine().await;
            let block = self.get_block(BlockID::Latest).await.unwrap();
            let block_number = block.get_number();
            let block_hash = block.get_hash();
            warn!("Mined 1 block: #{block_number} ({block_hash})",);
            if stop
                .map(|stop_number| stop_number == block_number)
                .unwrap_or(false)
            {
                warn!("MockChain finished mining!");
                return;
            }
            sleep(Duration::from_millis(block_time_as_ms)).await;
        }
    }

    pub async fn reorg(&self, at_block: u64) {
        assert!(at_block > 0, "Cannot reorg genesis block");
        warn!("Reorg at block=#{}", at_block);
        let prev_block = self.get_block(BlockID::Number(at_block - 1)).await;
        let latest_block = self.get_block(BlockID::Latest).await.unwrap();
        let mut forks = self.forks.lock().await;
        let mut chain = self.chain.lock().await;

        let new_fork_idx = forks.len();
        let new_fork = ChainFork::new(new_fork_idx as u8);
        new_fork
            .mine_until(latest_block.get_number(), prev_block)
            .await;
        forks.push(new_fork);

        for i in (at_block as usize)..chain.len() {
            chain[i] = new_fork_idx as u8;
        }

        warn!(
            "Reorg done {} -> {}, forkId={}",
            at_block,
            chain.len() - 1,
            new_fork_idx
        );
    }
}

pub struct MockClient {
    pub chain: MockChain,
    delay_request: u64,
}

impl MockClient {
    pub async fn new(cfg: &CommandConfig) -> Self {
        let start_block = cfg.start_block;
        let stop_block = cfg.stop_block;
        let chain = MockChain::new().await;
        let envars = envy::from_env::<MockChainEnvars>().unwrap();

        let pre_mine = envars
            .mockchain_premine
            .unwrap_or(stop_block.unwrap_or(start_block * 2).max(10));

        info!("Pre-mining up to block (minimum = max(10, start_block x 2)): #{pre_mine}");
        chain.mine_until(pre_mine).await;
        info!("Pre-mining finished");
        Self {
            chain,
            delay_request: cfg.block_time.wrapping_div(2) as u64,
        }
    }
}

#[async_trait]
impl ClientTrait<Block> for MockClient {
    async fn get_latest_block_number(&self) -> Result<u64, ClientError> {
        let block = self.chain.get_block(BlockID::Latest).await.unwrap();
        sleep(Duration::from_secs(self.delay_request)).await;
        Ok(block.block_number)
    }

    async fn get_full_blocks(
        &self,
        sorted_block_numbers: Vec<u64>,
    ) -> Result<Vec<Block>, ClientError> {
        sleep(Duration::from_secs(self.delay_request)).await;
        Ok(self.chain.get_blocks(sorted_block_numbers).await)
    }

    async fn get_light_blocks(
        &self,
        sorted_block_numbers: Vec<u64>,
    ) -> Result<Vec<Block>, ClientError> {
        sleep(Duration::from_secs(self.delay_request)).await;
        Ok(self.chain.get_blocks(sorted_block_numbers).await)
    }

    fn set_blocking_round(&self, _block_round: u8) {}
}
