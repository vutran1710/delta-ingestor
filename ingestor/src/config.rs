use crate::core::ConfigTrait;
use crate::core::RemoteConfigValue;
use crate::errors::ConfigStoreError;
use crate::proto::BlockChain;
use common_libs::async_trait::async_trait;
use common_libs::clap;
use common_libs::clap::Parser;
use common_libs::log;
use common_libs::redis::aio::Connection;
use common_libs::redis::AsyncCommands;
use common_libs::redis::Client;
use common_libs::redis::RedisError;
use common_libs::utils::load_file;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::prelude::Read;

#[derive(Parser, Debug, Clone, Deserialize)]
#[command()]
pub struct CommandConfig {
    #[clap(long, env = "CHAIN", default_value = "ethereum")]
    #[arg(long = "chain", default_value = "ethereum")]
    pub chain: BlockChain,
    #[clap(long, env = "START_BLOCK", default_value_t = 0)]
    #[arg(long = "start-block", default_value_t = 0)]
    pub start_block: u64,
    #[clap(long, env = "STOP_BLOCK")]
    #[arg(long = "stop-block")]
    pub stop_block: Option<u64>,
    #[clap(long, env = "RESUMER")]
    #[arg(long = "resumer")]
    pub resumer: Option<String>,
    #[clap(long, env = "PRODUCER")]
    #[arg(long = "producer")]
    pub producer: Option<String>,
    #[clap(long, env = "KEY_PREFIX", default_value = "bedrock")]
    #[arg(long = "key-prefix", default_value = "bedrock")]
    pub key_prefix: String,
    #[clap(long, env = "CONFIG_KEY")]
    #[arg(long = "config-key")]
    pub config_key: Option<String>,
    #[clap(long, env = "RESUMER_KEY")]
    #[arg(long = "resumer-key")]
    pub resumer_key: Option<String>,
    #[clap(long, env = "BATCH", default_value_t = 5)]
    #[arg(long = "batch", default_value_t = 5)]
    pub batch: u8,
    #[clap(long, env = "TASK_LIMIT", default_value_t = 30)]
    #[arg(long = "task-limit", default_value_t = 30)]
    pub task_limit: u8,
    #[clap(long, env = "METRICS_PORT", default_value_t = 8060)]
    #[arg(long = "metrics-port", default_value_t = 8060)]
    pub metrics_port: u16,
    #[clap(long, env = "CHANNEL_SIZE", default_value_t = 2)]
    #[arg(long = "channel-size", default_value_t = 2)]
    pub channel_size: u16,
    #[clap(long, env = "REQUEST_TIMEOUT", default_value_t = 2)]
    #[arg(long = "request-timeout", default_value_t = 2)]
    pub request_timeout: u8,
    #[clap(long, env = "LOAD_BALANCER_BLOCKING", default_value_t = 10)]
    #[arg(long = "lb-blocking", default_value_t = 10)]
    pub lb_blocking: u8,
    #[clap(long, env = "CONFIG_FILE", default_value = "config.toml")]
    #[arg(long = "config", default_value = "config.toml")]
    pub config_file: String,
    #[clap(long, env = "REORG_THRESHOLD", default_value_t = 200)]
    #[arg(long = "reorg-threshold", default_value_t = 200)]
    pub reorg_threshold: u16,
    #[clap(long, env = "BLOCK_TIME", default_value_t = 20)]
    #[arg(long = "block-time", default_value_t = 20)]
    pub block_time: u16,
    #[clap(long, env = "BLOCK_DESCRIPTOR")]
    #[arg(long = "block-descriptor")]
    pub block_descriptor: Option<String>,
}

impl Default for CommandConfig {
    fn default() -> Self {
        Self {
            chain: BlockChain::Ethereum,
            task_limit: 2,
            start_block: 1,
            stop_block: None,
            batch: 2,
            channel_size: 2,
            metrics_port: 0,
            config_file: "".to_string(),
            request_timeout: 2,
            lb_blocking: 10,
            config_key: None,
            reorg_threshold: 5,
            block_time: 10,
            key_prefix: "bedrock".to_string(),
            resumer_key: None,
            resumer: None,
            producer: None,
            block_descriptor: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Rpc {
    pub endpoint: String,
    pub weight: u8,
}

#[derive(Deserialize)]
pub struct Ethereum {
    pub rpc: Vec<Rpc>,
}

#[derive(Deserialize)]
pub struct ConfigTOML {
    pub ethereum: Option<Ethereum>,
}

impl ConfigTOML {
    pub fn from_file(path: &str) -> Self {
        let mut file = load_file(path);
        let mut text = String::new();
        file.read_to_string(&mut text).expect("bad content");
        toml::from_str::<Self>(&text).expect("Bad format!")
    }
}

pub struct Config {
    config_cmd: CommandConfig,
    config_key: String,
    redis: Option<Client>,
}

impl Config {
    pub async fn new(
        config_cmd: &CommandConfig,
        config_key: String,
    ) -> Result<Self, ConfigStoreError> {
        let mut redis: Option<Client> = None;

        if let Some(redis_uri) = config_cmd.resumer.clone() {
            log::info!(
                "Init dynamic Config using redis: config-key={:?}",
                config_key
            );
            let redis_client = Client::open(redis_uri)
                .map_err(|e| ConfigStoreError::Initialization(e.to_string()))?;
            redis = Some(redis_client);
        } else {
            log::info!("Init static Config");
        }

        let store = Self {
            config_cmd: config_cmd.to_owned(),
            config_key,
            redis,
        };

        if store.redis.is_some() {
            log::info!("Reset Config' remote config");
            store.reset_config_key().await?;
            log::info!("Set Config' remote config = initial-config");
            store.set_default_remote_config().await?;
        }

        Ok(store)
    }

    async fn get_con(&self) -> Result<Option<Connection>, ConfigStoreError> {
        if let Some(redis) = self.redis.clone() {
            let conn = redis
                .get_async_connection()
                .await
                .map_err(|e| ConfigStoreError::Disconnected(e.to_string()))?;
            return Ok(Some(conn));
        }
        Ok(None)
    }

    pub async fn reset_config_key(&self) -> Result<(), ConfigStoreError> {
        // NOTE: reset config, different ingestor should use different
        // config keys
        let mut conn = self.get_con().await?.unwrap();
        conn.del(self.config_key.clone())
            .await
            .map_err(|e| ConfigStoreError::OperationFailed(e.to_string()))?;
        Ok(())
    }

    async fn set_default_remote_config(&self) -> Result<(), ConfigStoreError> {
        // NOTE: we are supposed to run this only once at the initialization
        let value = RemoteConfigValue {
            task_limit: self.config_cmd.task_limit,
            batch_size: self.config_cmd.batch,
            load_balancer_blocking: self.config_cmd.lb_blocking,
        };

        if let Some(mut conn) = self.get_con().await? {
            let values: Vec<(String, u8)> = vec![
                ("task-limit".to_string(), value.task_limit),
                ("batch-size".to_string(), value.batch_size),
                ("lb-block".to_string(), value.load_balancer_blocking),
            ];
            conn.hset_multiple(self.config_key.to_owned(), &values)
                .await
                .map_err(|e| ConfigStoreError::OperationFailed(e.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl ConfigTrait for Config {
    fn get_start_block(&self) -> u64 {
        self.config_cmd.start_block
    }

    fn get_stop_block(&self) -> Option<u64> {
        self.config_cmd.stop_block
    }

    fn get_channel_size(&self) -> u16 {
        assert!(self.config_cmd.channel_size > 0, "Bad channel size");
        self.config_cmd.channel_size
    }

    fn get_reorg_threshold(&self) -> u16 {
        self.config_cmd.reorg_threshold
    }

    fn get_block_time(&self) -> u16 {
        self.config_cmd.block_time
    }

    async fn get_remote_config(&self) -> RemoteConfigValue {
        let mut remote_config_value = RemoteConfigValue {
            task_limit: self.config_cmd.task_limit,
            batch_size: self.config_cmd.batch,
            load_balancer_blocking: self.config_cmd.lb_blocking,
        };

        if let Ok(Some(mut redis)) = self.get_con().await {
            let result: Result<HashMap<String, u8>, RedisError> =
                redis.hgetall(self.config_key.to_owned()).await;
            if let Ok(values) = result {
                for (key, value) in values {
                    match key.as_str() {
                        "task-limit" => {
                            remote_config_value.task_limit = value.max(1);
                        }
                        "batch-size" => {
                            remote_config_value.batch_size = value.max(1);
                        }
                        "lb-block" => {
                            remote_config_value.load_balancer_blocking = value;
                        }
                        _ => (),
                    }
                }
            } else {
                log::error!("Failed to get remote config value: {:?}", result);
            }
        }

        remote_config_value
    }
}
