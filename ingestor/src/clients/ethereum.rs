use crate::core::ClientTrait;
use crate::errors::ClientError;
use crate::proto::ethereum as pb;
use common_libs::async_trait::async_trait;
use common_libs::load_balancer::RRBLoadBalancer;
use common_libs::log;
use common_libs::tokio::time::timeout as tokio_timeout;
use common_libs::tokio::time::Duration;
use common_libs::tokio::try_join;
use common_libs::tokio_retry::strategy::FixedInterval;
use common_libs::tokio_retry::Retry;
use metrics::ingestor::EthClientMetrics;
use metrics::Registry;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use web3::error::TransportError;
use web3::transports::Batch;
use web3::transports::Http;
use web3::types::Block;
use web3::types::BlockId;
use web3::types::BlockNumber;
use web3::types::FilterBuilder;
use web3::types::Log;
use web3::types::Transaction;
use web3::types::H256;
use web3::types::U64;
use web3::Web3;

// NOTE: for batch download, its either All-OK or Die!
type ClientResult<T> = Result<T, ClientError>;
type BatchResult<T> = ClientResult<Vec<Option<T>>>;

impl From<web3::Error> for ClientError {
    fn from(error: web3::Error) -> Self {
        match error {
            web3::Error::Unreachable => ClientError::ServerUnavailable(error.to_string()),
            web3::Error::Decoder(e) => ClientError::Serialization(e),
            web3::Error::InvalidResponse(e) => ClientError::BadData(e),
            web3::Error::Transport(err) => match err {
                TransportError::Code(code) => match code {
                    429 => ClientError::RateLimitExceeded(err.to_string()),
                    403..=503 => ClientError::ServerUnavailable(err.to_string()),
                    400 => ClientError::UnsupportedRequest(err.to_string()),
                    _ => ClientError::Unknown(err.to_string()),
                },
                TransportError::Message(msg) => ClientError::Unknown(msg),
            },
            web3::Error::Rpc(err) => {
                match err.code.code() {
                    -32700 => ClientError::Unknown(err.to_string()),
                    // Invalid request & Method not found
                    -32601..=-32000 => ClientError::UnsupportedRequest(err.to_string()),
                    _ => ClientError::Unknown(format!("rpc other code: {}", err)),
                }
            }
            _ => ClientError::Unknown(error.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum NodeType {
    Light,   // can get txs
    Archive, // can get logs (not guaranteed it can get txs)
}

impl From<String> for NodeType {
    fn from(endpoint: String) -> Self {
        if endpoint.starts_with("archive:") {
            return Self::Archive;
        }

        Self::Light
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    pub node_type: NodeType,
    web3_batch: Web3<Batch<Http>>,
    web3: Web3<Http>,
    endpoint: String,
}

impl Client {
    pub fn new(endpoint: String, node_type: NodeType) -> Result<Self, ClientError> {
        let transport = Http::new(&endpoint)?;
        let batch = Batch::new(transport.clone());
        let web3_batch = Web3::new(batch);
        let web3 = Web3::new(transport);
        let client = Client {
            web3,
            web3_batch,
            endpoint,
            node_type,
        };
        Ok(client)
    }

    pub async fn get_chain_id(&self) -> ClientResult<u64> {
        self.web3
            .eth()
            .chain_id()
            .await
            .map(|r| r.as_u64())
            .map_err(ClientError::from)
    }

    /// Private helper methods
    async fn submit_batch_request(&self) -> ClientResult<Vec<web3::Result<Value>>> {
        self.web3_batch
            .transport()
            .submit_batch()
            .await
            .map_err(ClientError::from)
    }

    fn serialize_batch_result<T: serde::de::DeserializeOwned>(
        &self,
        batch_result: Vec<web3::Result<Value>>,
        accept_none: bool,
    ) -> BatchResult<T> {
        let mut results = Vec::new();
        for value in batch_result.into_iter() {
            if value.is_err() {
                // NOTE: result is err, we fail the whole batch
                return Err(ClientError::from(value.err().unwrap()));
            }

            // NOTE: this is safe to unwrap because value is already present
            let value = value.unwrap();
            match value {
                Value::Array(values) => {
                    for val in values {
                        let try_serialize = serde_json::from_value(val);
                        let result = try_serialize.map(Some)?;
                        results.push(result);
                    }
                }
                Value::Object(_) => {
                    let try_serialize = serde_json::from_value(value);
                    let result = try_serialize.map(Some)?;
                    results.push(result);
                }
                _ => {
                    /* NOTE: this part is hard
                    - it can be None when we are trying to fetch something like a future block, it's OK indeed
                    - it can be None when we are fetching a past block but the node is a light-node, which does
                    not have archived data - and that is not OK.
                    - thats why we need to be explicit about what we expect
                    */
                    if accept_none {
                        results.push(None);
                    } else {
                        return Err(ClientError::DataNotFound(value.to_string()));
                    }
                }
            }
        }

        Ok(results)
    }

    pub fn group_logs_by_block_hash(&self, logs: Vec<Log>) -> HashMap<H256, Vec<Log>> {
        let mut result = HashMap::new();
        for log in logs.into_iter().filter(|log| log.block_hash.is_some()) {
            let hash = log.block_hash.unwrap();
            result.entry(hash).or_insert(vec![]).push(log);
        }
        result
    }

    /// Public api call for fetching data
    pub async fn get_latest_block_number(&self) -> ClientResult<u64> {
        self.web3
            .eth()
            .block(BlockId::Number(BlockNumber::Latest))
            .await
            .map_err(ClientError::from)
            .map(|rs| match rs {
                Some(Block {
                    number: Some(number),
                    ..
                }) => Ok(number.as_u64()),
                _ => Err(ClientError::BadData(
                    "Unable to fetch latest-block-number".to_string(),
                )),
            })?
    }

    pub async fn get_blocks_no_txs(&self, block_numbers: &[u64]) -> BatchResult<Block<H256>> {
        block_numbers.iter().for_each(|number| {
            let block_id = BlockId::from(U64([*number]));
            self.web3_batch.eth().block(block_id);
        });

        log::debug!("__ request blocks-no-txs from: {}", self.endpoint);
        let results = self.submit_batch_request().await?;
        log::debug!(
            "client:{}#{:?} > get_multi_blocks_no_txs ({:?}, {} blocks)",
            self.endpoint,
            self.node_type,
            block_numbers,
            results.len()
        );
        self.serialize_batch_result(results, false)
    }

    pub async fn get_blocks(&self, block_numbers: &[u64]) -> BatchResult<Block<Transaction>> {
        block_numbers.iter().for_each(|number| {
            let block_id = BlockId::from(U64([*number]));
            self.web3_batch.eth().block_with_txs(block_id);
        });

        log::debug!("__ request blocks from: {}", self.endpoint);
        let results = self.submit_batch_request().await?;
        log::debug!(
            "client:{}#{:?} > get_multi_block_headers ({:?}, {} blocks)",
            self.endpoint,
            self.node_type,
            block_numbers,
            results.len()
        );
        self.serialize_batch_result(results, false)
    }

    pub async fn get_logs(&self, block_numbers: &[u64]) -> ClientResult<HashMap<H256, Vec<Log>>> {
        block_numbers.iter().for_each(|block| {
            let filter = FilterBuilder::default()
                .from_block(BlockNumber::from(*block))
                .to_block(BlockNumber::from(*block))
                .build();
            self.web3_batch.eth().logs(filter);
        });
        log::debug!("__ request logs from: {}", self.endpoint);
        let result = self.submit_batch_request().await?;
        let logs = self.serialize_batch_result::<Log>(result, false)?;
        let logs = logs.into_iter().flatten().collect::<Vec<Log>>();

        log::debug!(
            "client:{}#{:?} > get_logs_from_block_range ({:?}, {} logs)",
            self.endpoint,
            self.node_type,
            block_numbers,
            logs.len()
        );

        let result = self.group_logs_by_block_hash(logs);
        Ok(result)
    }
}

pub struct RRClient {
    pub chain_id: u64,
    archives: RRBLoadBalancer<Client>,
    lights: RRBLoadBalancer<Client>,
    metrics: EthClientMetrics,
    timeout: u8,
    lb_blocking: AtomicU8,
}

impl RRClient {
    pub fn new(
        clients: Vec<Client>,
        registry: &Registry,
        timeout: u8,
        lb_blocking: u8,
        chain_id: u64,
    ) -> Self {
        let archive_clients = clients
            .clone()
            .into_iter()
            .filter(|c| matches!(c.node_type, NodeType::Archive))
            .map(|c| (c.endpoint.clone(), c, 1))
            .collect::<Vec<(String, Client, u8)>>();

        let light_clients = clients
            .into_iter()
            .filter(|c| matches!(c.node_type, NodeType::Light))
            .map(|c| (c.endpoint.clone(), c, 1))
            .collect::<Vec<(String, Client, u8)>>();

        log::info!(
            "RoundRobin-Ethereum-Client: {} light-nodes, {} archives-nodes, timeout={:?}, load-blancer-blocking={}",
            light_clients.len(),
            archive_clients.len(),
            timeout,
            lb_blocking,
        );

        let archives = RRBLoadBalancer::new(archive_clients, true);
        let lights = RRBLoadBalancer::new(light_clients, true);

        let metrics = EthClientMetrics::new(registry);
        Self {
            archives,
            lights,
            metrics,
            timeout,
            chain_id,
            lb_blocking: AtomicU8::new(lb_blocking),
        }
    }

    fn get(&self, node: NodeType) -> &Client {
        match node {
            NodeType::Archive => self.archives.get(),
            NodeType::Light => self.lights.get(),
        }
    }

    async fn get_latest_block_number(&self) -> ClientResult<u64> {
        let client = self.get(NodeType::Light);
        tokio_timeout(
            Duration::from_secs(self.timeout as u64),
            client.get_latest_block_number(),
        )
        .await?
    }

    fn update_request_counter(&self, client: &Client, method: &str, status: &str) {
        self.metrics
            .request_count
            .with_label_values(&[
                &client.endpoint,
                method,
                &format!("{:?}", client.node_type),
                status,
            ])
            .inc();
    }

    async fn get_blocks_no_txs(&self, blocks: &[u64]) -> BatchResult<Block<H256>> {
        let client = self.get(NodeType::Light);
        let result = tokio_timeout(
            Duration::from_secs(self.timeout as u64),
            client.get_blocks_no_txs(blocks),
        )
        .await?;

        if let Err(e) = result {
            log::error!("{} get_light_blocks_error: {:?}", client.endpoint, e);
            self.lights.block(
                client.endpoint.clone(),
                self.lb_blocking.load(Ordering::SeqCst) as u32,
            );
            return Err(e);
        }

        result
    }

    async fn get_blocks(&self, blocks: &[u64]) -> BatchResult<Block<Transaction>> {
        let client = self.get(NodeType::Light);
        let result = tokio_timeout(
            Duration::from_secs(self.timeout as u64),
            client.get_blocks(blocks),
        )
        .await?;

        if let Err(e) = result {
            self.update_request_counter(client, "get_blocks", "error");
            log::error!("{} get_blocks_error: {:?}", client.endpoint, e);
            self.lights.block(
                client.endpoint.clone(),
                self.lb_blocking.load(Ordering::SeqCst) as u32,
            );
            return Err(e);
        }

        self.update_request_counter(client, "get_blocks", "success");
        result
    }

    async fn get_logs(&self, blocks: &[u64]) -> ClientResult<HashMap<H256, Vec<Log>>> {
        let client = self.get(NodeType::Archive);
        let result = tokio_timeout(
            Duration::from_secs(self.timeout as u64),
            client.get_logs(blocks),
        )
        .await?;

        if let Err(e) = result {
            self.update_request_counter(client, "get_logs", "error");
            log::error!("{}, get_logs_error: {:?}", client.endpoint, e);
            self.archives.block(
                client.endpoint.clone(),
                self.lb_blocking.load(Ordering::SeqCst) as u32,
            );
            return Err(e);
        }

        self.update_request_counter(client, "get_logs", "success");
        result
    }

    fn map_to_protobuf(
        &self,
        blocks: Vec<Block<Transaction>>,
        logs_group: HashMap<H256, Vec<Log>>,
    ) -> Vec<pb::Block> {
        blocks
            .into_iter()
            .map(|block| {
                let block_hash = block.hash.unwrap();
                let logs = logs_group.get(&block_hash).unwrap_or(&Vec::new()).to_vec();
                pb::Block::from((block, logs, self.chain_id))
            })
            .collect()
    }
}

#[async_trait]
impl ClientTrait<pb::Block> for RRClient {
    async fn get_latest_block_number(&self) -> ClientResult<u64> {
        let retry_strategy = FixedInterval::from_millis(5);
        let get_latest_block = || self.get_latest_block_number();
        Retry::spawn(retry_strategy.clone(), get_latest_block).await
    }

    async fn get_light_blocks(
        &self,
        sorted_block_numbers: Vec<u64>,
    ) -> ClientResult<Vec<pb::Block>> {
        let get_light_blocks = || self.get_blocks_no_txs(&sorted_block_numbers);
        let retry_strategy = FixedInterval::from_millis(5);
        let light_blocks = Retry::spawn(retry_strategy.clone(), get_light_blocks).await?;
        let blocks = light_blocks
            .into_iter()
            .flatten()
            // NOTE: we dont care about chain-id here because this data will not be sent
            .map(pb::Block::from)
            .collect::<Vec<pb::Block>>();
        Ok(blocks)
    }

    async fn get_full_blocks(
        &self,
        sorted_block_numbers: Vec<u64>,
    ) -> ClientResult<Vec<pb::Block>> {
        // NOTE: by Ingestor's Core logic, we are always keep the range
        // less than or equal the latest block number
        // so we will `retry` until either we die or the RPC die
        let retry_strategy = FixedInterval::from_millis(5);

        let get_blocks = || self.get_blocks(&sorted_block_numbers);
        let retry_get_blocks = Retry::spawn(retry_strategy.clone(), get_blocks);

        let get_logs = || self.get_logs(&sorted_block_numbers);
        let retry_get_logs = Retry::spawn(retry_strategy.clone(), get_logs);

        let (blocks, logs_group) = try_join! {
            retry_get_blocks,
            retry_get_logs,
        }?;
        let blocks = blocks.into_iter().flatten().collect();
        let pb_blocks = self.map_to_protobuf(blocks, logs_group);

        return Ok(pb_blocks);
    }

    fn set_blocking_round(&self, block_round: u8) {
        let current_blocking_round = self.lb_blocking.load(Ordering::SeqCst);
        if block_round != current_blocking_round {
            self.lb_blocking.store(block_round, Ordering::SeqCst);
        }
    }
}
