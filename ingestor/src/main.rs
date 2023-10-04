mod chain_piece;
mod clients;
mod config;
mod core;
mod errors;
mod ingestors;
mod name_svc;
mod producers;
mod proto;
mod resumers;
mod tests;

use crate::config::CommandConfig;
use crate::core::IngestorTrait;
use crate::errors::IngestorError;
use crate::ingestors::ethereum as eth;
use crate::ingestors::mockchain as mock;
use crate::ingestors::IngestorMode;
use crate::proto::BlockChain;

use common_libs::clap::Parser;
use common_libs::env_logger;
use common_libs::log;
use common_libs::log::info;
use common_libs::tokio;

use metrics::default_registry;
use metrics::run_metric_server;

#[tokio::main]
async fn main() {
    env_logger::init();
    let cfg = CommandConfig::parse();
    let mode = IngestorMode::from(&cfg);

    info!("Ingestor Mode = {:?}", mode);

    let registry = default_registry();
    info!("Running ingestor for chain: {}", cfg.chain);

    tokio::select! {
        result = async {
            match cfg.chain {
                BlockChain::Ethereum => {
                    let ingestor = eth::create_ingestor(&cfg, registry)
                        .await
                        .expect("Failed to create ingestor");
                    ingestor.run().await
                }
                BlockChain::MockChain => {
                    let block_time = cfg.block_time;
                    let (ingestor, chain, server) = mock::create_ingestor(&cfg, registry)
                        .await
                        .expect("Failed to create mock ingestor");
                    let run_chain = async move {
                        log::info!("Wait for premining before starting mock-chain");
                        tokio::time::sleep(tokio::time::Duration::from_secs(block_time as u64)).await;
                        log::info!("Running mock-chain with block time = {}", block_time);
                        chain.run((block_time * 1000) as u64, None).await;
                        Ok::<(), IngestorError>(())
                    };
                    let run_server = async move {
                        server.run().await;
                        Ok::<(), IngestorError>(())
                    };
                    tokio::try_join!(ingestor.run(), run_chain, run_server).map(|r| r.1)
                }
            }
        } => {
            info!("Ingestor finished with result = {:?}", result);
        },
        _ = run_metric_server(cfg.metrics_port) => (),
    };
}
