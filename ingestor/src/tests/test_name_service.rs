use crate::config::CommandConfig;
use crate::name_svc::NameService;
use crate::proto::BlockChain;
use common_libs::env_logger;

#[test]
fn test_name_service() {
    env_logger::try_init().unwrap_or_default();
    let mut config = CommandConfig::default();
    let chain_type = BlockChain::Ethereum;

    let name_set = NameService::from((&config, chain_type));

    assert_eq!(name_set.config_key, "bedrock_config__ethereum__1_latest");
    assert_eq!(name_set.resumer_key, "bedrock_resume__ethereum__1_latest");
    config.stop_block = Some(2);

    config.config_key = Some("bedrock_config_eth".to_string());
    let chain_type = BlockChain::Ethereum;
    let name_set = NameService::from((&config, chain_type.clone()));
    assert_eq!(name_set.config_key, "bedrock_config_eth");

    config.resumer_key = Some("my-custom-checkpoint".to_string());
    let name_set = NameService::from((&config, chain_type));
    assert_eq!(name_set.resumer_key, "my-custom-checkpoint");
}
