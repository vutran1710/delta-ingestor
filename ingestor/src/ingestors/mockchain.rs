use crate::clients::BlockID;
use crate::clients::MockChain;
use crate::clients::MockClient;
use crate::config::CommandConfig;
use crate::config::Config;
use crate::core::Ingestor;
use crate::errors::IngestorError;
use crate::name_svc::NameService;
use crate::producers::create_producer;
use crate::proto::mockchain::Block;
use crate::proto::BlockChain;
use crate::resumers::create_resumer;
use common_libs::log;
use common_libs::log::error;
use common_libs::warp;
use common_libs::warp::reject::Reject;
use common_libs::warp::reply::Response;
use common_libs::warp::Filter;
use metrics::Registry;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

impl warp::Reply for BlockID {
    fn into_response(self) -> warp::reply::Response {
        match self {
            BlockID::Latest => Response::new("latest".to_string().into()),
            BlockID::Number(block) => Response::new(format!("{block}").into()),
        }
    }
}

pub struct MockChainServer {
    chain: MockChain,
    reorg_threshold: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReorgResponse {
    block: u64,
    hash_before: String,
    hash_after: String,
}

#[derive(Debug)]
struct ReorgTooDeep;

#[derive(Debug)]
struct BlockNotFound;

#[derive(Debug)]
struct BadBlockRange;

impl Reject for ReorgTooDeep {}
impl Reject for BlockNotFound {}
impl Reject for BadBlockRange {}

impl MockChainServer {
    async fn new(chain: MockChain, reorg_threshold: u16) -> Self {
        Self {
            chain,
            reorg_threshold,
        }
    }

    async fn handle_get_block(
        block_id: BlockID,
        chain: MockChain,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        let block = chain.get_block(block_id).await;

        if let Some(block) = block {
            return Ok(warp::reply::json(&block));
        }

        Err(warp::reject::custom(BlockNotFound))
    }

    async fn handle_manual_reorg(
        reorg_block: u64,
        chain: MockChain,
        reorg_threshold: u16,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        let block = chain.get_block(BlockID::Latest).await;

        if block.is_none() {
            return Err(warp::reject::custom(BlockNotFound));
        }

        let latest_block_number = block.unwrap().block_number;

        if latest_block_number < reorg_block {
            return Err(warp::reject::custom(BlockNotFound));
        }

        let minium_reorg_possible = latest_block_number - reorg_threshold as u64;

        if reorg_block < minium_reorg_possible {
            return Err(warp::reject::custom(ReorgTooDeep));
        }

        let block_before = chain.get_block(BlockID::Number(reorg_block)).await.unwrap();
        chain.reorg(reorg_block).await;
        let block_after = chain.get_block(BlockID::Number(reorg_block)).await.unwrap();

        let resp = ReorgResponse {
            block: reorg_block,
            hash_before: block_before.block_hash,
            hash_after: block_after.block_hash,
        };

        assert_ne!(
            resp.hash_after, resp.hash_before,
            "Hash must be different after reorg"
        );

        Ok(warp::reply::json(&resp))
    }

    async fn handle_get_blocks(
        from_block: u64,
        to_block: u64,
        chain: MockChain,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        if from_block > to_block {
            error!("from-block > to-block");
            return Err(warp::reject::custom(BadBlockRange));
        }

        let latest_block_number = chain
            .get_block(BlockID::Latest)
            .await
            .expect("Genesis block is None")
            .block_number;

        if from_block > latest_block_number {
            error!("from-block > latest_block_number");
            return Err(warp::reject::custom(BadBlockRange));
        }

        let block_range = from_block..(to_block + 1).min(latest_block_number);
        let blocks = chain.get_blocks(block_range.collect()).await;
        Ok(warp::reply::json(&blocks))
    }

    fn with_chain(
        &self,
    ) -> impl Filter<Extract = (MockChain,), Error = std::convert::Infallible> + Clone {
        let chain = self.chain.clone();
        warp::any().map(move || chain.clone())
    }

    fn with_reorg_threshold(
        &self,
    ) -> impl Filter<Extract = (u16,), Error = std::convert::Infallible> + Clone {
        let reorg_threshold = self.reorg_threshold;
        warp::any().map(move || reorg_threshold)
    }

    pub async fn run(&self) {
        let api_doc = r#"
Welcome to MockChainServer
------------
Available API:

* get latest block
- GET /block/latest

* get specific block by number
- GET /block/{block_number:u64}

* get multi blocks in range (from-block, to-block)
** if (to-block) > latest block, all blocks from from-block to latest will be returned
** if (from-block) > latest block, errors will be returned
- GET /blocks/{from_block:u64}/{to_block:u64}

* Manual reorg at block
- GET /reorg/{reorg_block:u64}
"#;
        let root = warp::path::end().map(move || api_doc);

        let block_path = warp::path("block");
        let get_latest_block = warp::path!("latest").map(|| BlockID::Latest);
        let get_specific_block = warp::path!(u64).map(BlockID::Number);

        let reorg_request_path = warp::path!("reorg" / u64)
            .and(self.with_chain())
            .and(self.with_reorg_threshold())
            .and_then(MockChainServer::handle_manual_reorg);

        let get_multi_blocks_path = warp::path!("blocks" / u64 / u64)
            .and(self.with_chain())
            .and_then(MockChainServer::handle_get_blocks);

        let router = warp::get().and(
            root.or(block_path
                .and(get_latest_block.or(get_specific_block))
                .unify()
                .and(self.with_chain())
                .and_then(MockChainServer::handle_get_block))
                .or(reorg_request_path)
                .or(get_multi_blocks_path),
        );

        log::info!("Running mock-chain-server API at: http://0.0.0.0:8081");
        warp::serve(router).run(([0, 0, 0, 0], 8081)).await;
    }
}

pub async fn create_ingestor(
    cfg: &CommandConfig,
    registry: &Registry,
) -> Result<(Ingestor<Block>, MockChain, MockChainServer), IngestorError> {
    let name_service = NameService::from((cfg, BlockChain::MockChain));
    let client = MockClient::new(cfg).await;
    let chain = client.chain.clone();
    let mock_server = MockChainServer::new(chain.clone(), cfg.reorg_threshold).await;
    let client = Arc::new(client);
    let producer = Arc::new(create_producer(cfg, &name_service).await?);
    let resumer = Arc::new(create_resumer(cfg, &name_service).await?);
    let config = Arc::new(Config::new(cfg, name_service.config_key.clone()).await?);
    let ingestor = Ingestor::new(client, producer, resumer, config, registry);
    Ok((ingestor, chain, mock_server))
}
