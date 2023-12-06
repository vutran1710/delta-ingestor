use super::mock_client::FakeClient;
use crate::config::CommandConfig;
use crate::config::Config;
use crate::core::ClientTrait;
use crate::core::Ingestor;
use crate::errors::IngestorError;
use crate::ingestors::IngestorMode;
use crate::name_svc::NameService;
use crate::producers::create_producer;
use crate::producers::Producer;
use crate::proto::BlockChain;
use crate::proto::BlockTrait;
use crate::resumers::create_resumer;
use crate::resumers::Resumer;
use common_libs::tokio::sync::RwLock;
use metrics::Registry;
use std::sync::Arc;

pub async fn create_fake_ethereum_ingestor<B: BlockTrait>(
    start_block: u64,
    stop_block: u64,
    reorg_threshold: u16,
) -> Result<
    (
        Ingestor<B>,
        Arc<FakeClient>,
        Arc<Resumer<B>>,
        Arc<RwLock<Vec<B>>>,
    ),
    IngestorError,
>
where
    FakeClient: ClientTrait<B>,
{
    let mut cfg = CommandConfig::default();
    cfg.start_block = start_block;
    cfg.stop_block = Some(stop_block);
    cfg.reorg_threshold = reorg_threshold;

    assert_eq!(IngestorMode::from(&cfg), IngestorMode::StdOut);

    let registry = Registry::default();
    let client = Arc::new(FakeClient::new().await);
    let client1 = client.clone();
    let name_service = NameService::from((&cfg, BlockChain::Ethereum));

    let stdout_producer = create_producer(&cfg, &name_service).await?;
    let mut blocks = Arc::new(RwLock::new(vec![]));

    if let Producer::StdOut(p) = &stdout_producer {
        blocks = p.blocks.clone();
    };

    let producer = Arc::new(stdout_producer);

    let checkpoint = Arc::new(create_resumer(&cfg, &name_service).await?);
    let checkpoint1 = checkpoint.clone();

    let config_store = Arc::new(Config::new(&cfg, "dont-matter".to_string()).await?);

    let ingestor = Ingestor::new(client, producer, checkpoint, config_store, &registry);
    Ok((ingestor, client1, checkpoint1, blocks))
}
