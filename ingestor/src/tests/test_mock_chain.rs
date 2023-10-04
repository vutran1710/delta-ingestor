use crate::clients::BlockID;
use crate::clients::MockChain;
use crate::proto::BlockTrait;
use common_libs::env_logger;
use common_libs::log::warn;
use common_libs::tokio;
use common_libs::tokio::time::sleep;
use common_libs::tokio::time::Duration;
use std::str::FromStr;
use std::sync::Arc;
use web3::types::H128;

#[tokio::test]
async fn test_mock_chain() {
    env_logger::try_init().unwrap_or_default();
    let chain = Arc::new(MockChain::new().await);
    let c1 = chain.clone();
    let c2 = chain.clone();

    let reorg = tokio::spawn(async move {
        let mut reorg1 = false;
        let mut reorg2 = false;
        let mut reorg3 = false;
        loop {
            if let Some(block) = c2.get_block(BlockID::Latest).await {
                if block.get_number() > 20 && !reorg1 {
                    warn!("Lets reorg -------------------------");
                    c2.reorg(10).await;
                    reorg1 = true
                }
                if block.get_number() > 25 && !reorg2 {
                    warn!("Lets reorg -------------------------");
                    c2.reorg(20).await;
                    reorg2 = true
                }
                if block.get_number() > 35 && !reorg3 {
                    warn!("Lets reorg -------------------------");
                    c2.reorg(15).await;
                    reorg3 = true
                }
            }
            sleep(Duration::from_millis(300)).await;
        }
    });

    tokio::select! {
        _ = c1.run(100, Some(50)) => (),
        _ = reorg => (),
    }

    let block = chain.get_block(BlockID::Latest).await.unwrap();
    warn!("Asserting block count & hashes");
    assert_eq!(block.block_number, 50);

    let block = chain.get_block(BlockID::Number(0)).await.unwrap();
    assert_eq!(
        H128::from_str(&block.get_parent_hash()).unwrap(),
        H128::zero()
    );
    warn!("Yahoooooooooo!");
}
