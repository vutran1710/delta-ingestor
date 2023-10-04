use crate::config::CommandConfig;
use crate::proto::BlockChain;

#[derive(Debug)]
pub struct NameService {
    pub resumer_key: String,
    pub config_key: String,
}

impl From<(&CommandConfig, BlockChain)> for NameService {
    fn from(value: (&CommandConfig, BlockChain)) -> Self {
        let (cfg, block_chain) = value;

        let chain_prefix = format!("{block_chain}");

        let key_prefix = cfg.key_prefix.to_owned();
        let blocks_range = format!(
            "{}_{}",
            cfg.start_block,
            cfg.stop_block
                .map_or("latest".to_string(), |b| b.to_string())
        )
        .to_lowercase();
        let config_key = cfg.config_key.to_owned().unwrap_or(format!(
            "{key_prefix}_config__{chain_prefix}__{blocks_range}"
        ));
        let resumer_key = cfg.resumer_key.to_owned().unwrap_or(format!(
            "{key_prefix}_resume__{chain_prefix}__{blocks_range}"
        ));

        NameService {
            resumer_key,
            config_key,
        }
    }
}
