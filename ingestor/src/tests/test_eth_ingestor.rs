use crate::config::CommandConfig;
use crate::ingestors::ethereum as eth;
use crate::ingestors::IngestorMode;
use common_libs::tokio;
use metrics::Registry;

#[tokio::test]
async fn test_init_ingestor_stdouts() {
    let registry = Registry::new();
    let mut cfg = CommandConfig::default();
    cfg.key_prefix = "stdouts".to_string();
    cfg.config_file = "../secrets/sample_eth_config.toml".to_string();
    assert_eq!(IngestorMode::from(&cfg), IngestorMode::StdOut);
    eth::create_ingestor(&cfg, &registry).await.unwrap();
}

#[tokio::test]
async fn test_init_ingestor_no_bus() {
    let registry = Registry::new();
    let mut cfg = CommandConfig::default();
    cfg.key_prefix = "stdouts".to_string();
    cfg.resumer = Some("redis://localhost:6379".to_string());
    cfg.config_file = "../secrets/ethereum.toml".to_string();
    assert_eq!(IngestorMode::from(&cfg), IngestorMode::NoProducer);
    eth::create_ingestor(&cfg, &registry).await.unwrap();
}
