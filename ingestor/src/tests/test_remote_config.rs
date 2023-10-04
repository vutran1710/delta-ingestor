use crate::config::{CommandConfig, Config};
use crate::core::ConfigTrait;
use common_libs::env_logger;
use common_libs::tokio;

#[tokio::test]
async fn test_config_01() {
    env_logger::try_init().unwrap_or_default();
    let cfg = CommandConfig::default();
    let config = Config::new(&cfg, "some-key".to_string()).await.unwrap();
    let remote_config = config.get_remote_config().await;
    assert_eq!(remote_config.task_limit, cfg.task_limit);
    assert_eq!(remote_config.batch_size, cfg.batch);
    assert_eq!(remote_config.load_balancer_blocking, cfg.lb_blocking);
}

#[tokio::test]
async fn test_config_02() {
    env_logger::try_init().unwrap_or_default();
    let mut cfg = CommandConfig::default();
    cfg.resumer = Some("redis://localhost:6379".to_string());
    let config = Config::new(&cfg, "some-key".to_string()).await.unwrap();
    config.reset_config_key().await.unwrap();
    let remote_config = config.get_remote_config().await;
    assert_eq!(remote_config.task_limit, cfg.task_limit);
    assert_eq!(remote_config.batch_size, cfg.batch);
    assert_eq!(remote_config.load_balancer_blocking, cfg.lb_blocking);
}
