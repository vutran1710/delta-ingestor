use std::sync::Arc;

use super::mock_ingestor::create_fake_ethereum_ingestor;
use crate::core::create_ranges;
use crate::core::ClientTrait;
use crate::core::IngestorTrait;
use crate::core::ResumerTrait;
use crate::proto::BlockTrait;
use crate::resumers::Resumer;
use common_libs::env_logger;
use common_libs::log;
use common_libs::tokio;
use common_libs::tokio::time::sleep;
use common_libs::tokio::time::Duration;

async fn resumer_last_block_number<B: BlockTrait>(resumer: &Arc<Resumer<B>>) -> Option<u64> {
    let blocks = resumer.get_latest_blocks().await.unwrap();
    blocks.last().map(|b| b.get_number())
}

#[test]
fn test_create_ranges() {
    env_logger::try_init().unwrap_or_default();
    let ranges = create_ranges(1, 1, 1, 5);
    let expected = vec![(1, 2)];
    assert_eq!(ranges, expected);

    let ranges = create_ranges(1, 10, 1, 5);
    let expected = vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)];
    assert_eq!(ranges, expected);

    let ranges = create_ranges(1, 10, 3, 5);
    let expected = vec![(1, 4), (4, 7), (7, 10), (10, 11)];
    assert_eq!(ranges, expected);

    let ranges = create_ranges(1, 12, 5, 3);
    let expected = vec![(1, 6), (6, 11), (11, 13)];
    assert_eq!(ranges, expected);
}

#[tokio::test]
async fn test_ingestor_get_start_block() {
    env_logger::try_init().unwrap_or_default();

    let (ingestor, ..) = create_fake_ethereum_ingestor(0, 5, 0).await.unwrap();
    assert_eq!(ingestor.get_start_block(None), 0);
    assert_eq!(ingestor.get_start_block(Some(0)), 1);
    assert_eq!(ingestor.get_start_block(Some(10)), 11);

    let (ingestor, ..) = create_fake_ethereum_ingestor(10, 100, 0).await.unwrap();
    assert_eq!(ingestor.get_start_block(None), 10);
    assert_eq!(ingestor.get_start_block(Some(12)), 13);
    assert_eq!(ingestor.get_start_block(Some(10)), 11);
}

#[tokio::test]
async fn test_ingestor_core_01() {
    env_logger::try_init().unwrap_or_default();

    let (ingestor, client, resumer, published_blocks) =
        create_fake_ethereum_ingestor(0, 5, 0).await.unwrap();

    log::info!("Components init successful");
    let c1 = client.clone();
    let block_generator = |stop: u64| c1.fake_block_generator(stop, 1);
    let latest_block_number = client.get_latest_block_number().await.unwrap();
    assert_eq!(latest_block_number, 0);

    log::info!("Run block_generator for 10 sec, expect to generate 5 blocks");
    tokio::select! {
        _ = block_generator(5) => (),
        _ = sleep(Duration::from_secs(10)) => (),
    };

    log::info!(
        "=>> BlockStore: {:?}",
        client
            .get_block_store()
            .await
            .into_iter()
            .map(|b| b.block_number)
            .collect::<Vec<u64>>()
    );

    let latest_block_number = client.get_latest_block_number().await.unwrap();
    assert_eq!(latest_block_number, 5);

    /* --------------------------------------------------------------------
    Produce up to block#20
    - time needed for produce up to block #20 = 20s (because block-generator
    add new block every 1 second)
    - In 20(secs) ingestor can publish 10 blocks, because mock-producer
    need 2 secs to publish a batch of block, but stop_block is 5!
     ---------------------------------------------------------------------- */
    log::info!(
        "Run block_generator for 20 sec, expect to generate 15 more blocks, published 5 blocks"
    );
    assert_eq!(resumer_last_block_number(&resumer).await, None);

    let _ = tokio::join! {
        block_generator(20),
        ingestor.run(),
    };

    log::info!(
        "=>> BlockStore: {:?}",
        client
            .get_block_store()
            .await
            .into_iter()
            .map(|b| b.block_number)
            .collect::<Vec<u64>>()
    );

    log::info!(
        "=>> published: {:?}",
        published_blocks
            .read()
            .await
            .iter()
            .map(|b| b.get_number())
            .collect::<Vec<u64>>()
    );
    let blocks_count = published_blocks.read().await.len();
    assert_eq!(blocks_count, 6);
    assert_eq!(resumer_last_block_number(&resumer).await, Some(5));
}

#[tokio::test]
async fn test_ingestor_core_02() {
    env_logger::try_init().unwrap_or_default();

    let (ingestor, client, resumer, _published_blocks) =
        create_fake_ethereum_ingestor(0, 100, 0).await.unwrap();

    let c1 = client.clone();
    let resumer1 = resumer.clone();
    let block_generator = |stop: u64| c1.fake_block_generator(stop, 1);
    let latest_block_number = client.get_latest_block_number().await.unwrap();
    assert_eq!(latest_block_number, 0);

    log::info!("Generate 10 more blocks");
    tokio::select! {
        _ = block_generator(10) => (),
        _ = sleep(Duration::from_secs(10)) => (),
    };

    log::info!(
        "=>> BlockStore: {:?}",
        client
            .get_block_store()
            .await
            .into_iter()
            .map(|b| b.block_number)
            .collect::<Vec<u64>>()
    );

    let latest_block_number = client.get_latest_block_number().await.unwrap();
    assert_eq!(latest_block_number, 10);

    let reorg_trigger = |cancel_block: u64| async move {
        while resumer_last_block_number(&resumer1)
            .await
            .map(|b| b < cancel_block)
            .unwrap_or(true)
        {
            sleep(Duration::from_secs(1)).await;
        }
        log::info!("Stopping ingestor");
    };

    tokio::select! {
        _ = ingestor.run() => (),
        // NOTE: cancel ingestor if published up to block #5
        _ = reorg_trigger.clone()(5) => (),
    };

    assert_eq!(resumer_last_block_number(&resumer).await, Some(7));

    log::info!("Resume ingestor");

    tokio::select! {
        _ = ingestor.run() => (),
        // NOTE: cancel ingestor if published up to block #9
        _ = reorg_trigger(9) => (),
    };
    assert_eq!(resumer_last_block_number(&resumer).await, Some(10));
}

#[tokio::test]
async fn test_ingestor_core_reorg_01() {
    env_logger::try_init().unwrap_or_default();
    let (ingestor, client, resumer, published_blocks) =
        create_fake_ethereum_ingestor(0, 100, 5).await.unwrap();

    let c1 = client.clone();
    let c2 = client.clone();
    let block_generator = |stop: u64| c1.fake_block_generator(stop, 3);

    let latest_block_number = client.get_latest_block_number().await.unwrap();
    assert_eq!(latest_block_number, 0);

    let reorg_trigger = || async move {
        let mut reorg = false;
        loop {
            let last_block = resumer_last_block_number(&resumer).await;
            if let Some(check_point) = last_block {
                if check_point > 10 {
                    return;
                }

                if check_point > 4 && !reorg {
                    log::warn!("________ Reorg as last_block={check_point}");
                    c2.block_store.reorg(4).await;
                    reorg = true;
                }
            }
            sleep(Duration::from_millis(300)).await;
        }
    };

    tokio::select! {
        _ = ingestor.run() => (),
        _ = block_generator(50) => (),
        _ = reorg_trigger() => (),
    };

    let published_block_numbers = published_blocks
        .read()
        .await
        .iter()
        .map(|b| b.get_number())
        .collect::<Vec<u64>>();
    let check_blocks_on_chain = client
        .block_store
        .are_blocks_on_chain(&published_blocks.read().await)
        .await;
    let published_blocks = published_block_numbers
        .into_iter()
        .zip(check_blocks_on_chain)
        .collect::<Vec<(u64, bool)>>();

    let expected = vec![
        (0, true),
        (1, true),
        (2, true),
        (3, true),
        (4, false), // reorg block
        (5, false), // reorg block
        (6, false), // reorg block
        (7, false), // reorg block
        (4, true),
        (5, true),
        (6, true),
        (7, true),
        (8, true),
        (9, true),
        (10, true),
        (11, true),
        (12, true),
        (13, true),
        (14, true),
    ];
    log::warn!("---------- published blocks: {:?}", published_blocks);
    assert_eq!(published_blocks, expected);
}

#[tokio::test]
async fn test_ingestor_core_reorg_02() {
    env_logger::try_init().unwrap_or_default();
    let (ingestor, client, resumer, published_blocks) =
        create_fake_ethereum_ingestor(0, 10, 0).await.unwrap();

    let c1 = client.clone();
    let block_generator = |stop: u64| c1.fake_block_generator(stop, 1);

    let latest_block_number = client.get_latest_block_number().await.unwrap();
    assert_eq!(latest_block_number, 0);

    tokio::select! {
        _ = ingestor.run() => (),
        _ = block_generator(50) => (),
    };

    let published_block_numbers = published_blocks
        .read()
        .await
        .iter()
        .map(|b| b.get_number())
        .collect::<Vec<u64>>();
    let check_blocks_on_chain = client
        .block_store
        .are_blocks_on_chain(&published_blocks.read().await)
        .await;
    let published_blocks = published_block_numbers
        .into_iter()
        .zip(check_blocks_on_chain)
        .collect::<Vec<(u64, bool)>>();

    let expected = vec![
        (0, true),
        (1, true),
        (2, true),
        (3, true),
        (4, true),
        (5, true),
        (6, true),
        (7, true),
        (8, true),
        (9, true),
        (10, true),
    ];
    assert_eq!(published_blocks, expected);
    assert_eq!(resumer_last_block_number(&resumer).await, Some(10));
}

#[tokio::test]
async fn test_ingestor_core_reorg_with_resume() {
    env_logger::try_init().unwrap_or_default();

    let (ingestor, client, resumer, published_blocks) =
        create_fake_ethereum_ingestor(0, 100, 5).await.unwrap();

    let c1 = client.clone();
    let c2 = client.clone();
    let resumer1 = resumer.clone();
    let block_generator = |stop: u64| c1.fake_block_generator(stop, 1);
    let chain_reorg = |block: u64| c2.block_store.reorg(block);

    let latest_block_number = client.get_latest_block_number().await.unwrap();
    assert_eq!(latest_block_number, 0);

    let cancel_ingestor = |cancel_block: u64| async move {
        while resumer_last_block_number(&resumer1)
            .await
            .map(|b| b < cancel_block)
            .unwrap_or(true)
        {
            sleep(Duration::from_millis(50)).await;
        }
        log::info!("Stopping ingestor");
    };

    block_generator(10).await;

    tokio::select! {
        _ = ingestor.run() => (),
        _ = cancel_ingestor.clone()(10) => (),
    };
    assert_eq!(resumer_last_block_number(&resumer).await, Some(10));

    block_generator(15).await;
    chain_reorg(8).await;
    log::info!("Resume ingestor");
    let checkpoint_blocks = resumer.get_latest_blocks().await.unwrap();
    let checkpoint_block_numbers = checkpoint_blocks
        .iter()
        .map(|b| b.get_number())
        .collect::<Vec<u64>>();
    assert_eq!(checkpoint_block_numbers.len(), 5);
    log::info!(">>> Checkpoint: {:?}", checkpoint_block_numbers);

    tokio::select! {
        _ = ingestor.run() => (),
        _ = cancel_ingestor(15) => (),
    };

    let published_block_numbers = published_blocks
        .read()
        .await
        .iter()
        .map(|b| b.get_number())
        .collect::<Vec<u64>>();
    let check_blocks_on_chain = client
        .block_store
        .are_blocks_on_chain(&published_blocks.read().await)
        .await;
    let published_blocks = published_block_numbers
        .into_iter()
        .zip(check_blocks_on_chain)
        .collect::<Vec<(u64, bool)>>();

    let expected = vec![
        (0, true),
        (1, true),
        (2, true),
        (3, true),
        (4, true),
        (5, true),
        (6, true),
        (7, true),
        (8, false),  // reorg block
        (9, false),  // reorg block
        (10, false), // reorg block
        (8, true),
        (9, true),
        (10, true),
        (11, true),
        (12, true),
        (13, true),
        (14, true),
        (15, true),
    ];
    assert_eq!(published_blocks, expected);
}
