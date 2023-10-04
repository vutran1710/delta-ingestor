use crate::chain_piece::ChainPiece;
use crate::chain_piece::MergeStatus;
use crate::clients::MockChain;
use crate::errors::ChainPieceError;
use crate::proto::mockchain::Block;
use common_libs::env_logger;
use common_libs::log;
use common_libs::tokio;

#[tokio::test]
async fn test_chain_piece_01() {
    env_logger::try_init().unwrap_or_default();
    log::info!("Init Reorg-Chain with threshold = 10");
    let threshold = 10;
    let mut chain_piece = ChainPiece::new(threshold);
    let mock_chain = MockChain::new().await;

    log::info!("Merge 0 blocks, expect MergeStatus::Nothing");
    let blocks: Vec<Block> = vec![];
    let merge = chain_piece.merge(&blocks).unwrap();
    assert_eq!(merge, MergeStatus::Nothing);

    mock_chain.run(10, Some(threshold as u64)).await;
    assert_eq!(mock_chain.count().await, 11);
    let blocks = mock_chain.get_blocks((0..=10).collect()).await;
    assert_eq!(blocks.last().unwrap().block_number, 10);

    log::info!("Merge blocks 0-10 from Chain to ChainPiece, expect OK");
    let merge = chain_piece.merge(&blocks).unwrap();
    assert_eq!(merge, MergeStatus::Ok(10));
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 10);
    assert_eq!(tail, 1);

    log::info!("Continue to mine up to Block#15");
    mock_chain.run(10, Some(15)).await;
    assert_eq!(mock_chain.count().await, 16);

    let blocks = mock_chain.get_blocks((5..=15).collect()).await;
    assert_eq!(blocks.last().unwrap().block_number, 15);
    assert_eq!(blocks.len(), 11);
    log::info!("Merge block 5->15 to ChainPiece, expect Ok");
    let merge = chain_piece.merge(&blocks).unwrap();
    assert_eq!(merge, MergeStatus::Ok(15));
    let (_, tail) = chain_piece.get_head_and_tail();
    assert_eq!(tail, 6);

    log::info!("Merge block 5->7 to ChainPiece, expect Ok, head & tail no change");
    let blocks = mock_chain.get_blocks((5..=7).collect()).await;
    assert_eq!(blocks.last().unwrap().block_number, 7);
    assert_eq!(blocks.len(), 3);
    let merge = chain_piece.merge(&blocks).unwrap();
    assert_eq!(merge, MergeStatus::Ok(15));
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(tail, 6);
    assert_eq!(head, 15);

    log::info!("Merge block 3->5 to ChainPiece, expect Err(ReorgTooDeep)");
    let blocks = mock_chain.get_blocks((3..=5).collect()).await;
    assert_eq!(blocks.last().unwrap().block_number, 5);
    assert_eq!(blocks.len(), 3);
    let merge = chain_piece.merge(&blocks).err().unwrap();
    assert_eq!(merge, ChainPieceError::ReorgTooDeep);

    log::info!("Merge block 3->8 to ChainPiece, expect OK");
    let blocks = mock_chain.get_blocks((3..=8).collect()).await;
    assert_eq!(blocks.last().unwrap().block_number, 8);
    assert_eq!(blocks.len(), 6);
    let merge = chain_piece.merge(&blocks).unwrap();
    assert_eq!(merge, MergeStatus::Ok(15));

    log::info!("Merge block 11->15 to ChainPiece, expect OK");
    let blocks = mock_chain.get_blocks((11..=15).collect()).await;
    assert_eq!(blocks.last().unwrap().block_number, 15);
    assert_eq!(blocks.len(), 5);
    let merge = chain_piece.merge(&blocks).unwrap();
    assert_eq!(merge, MergeStatus::Ok(15));
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 15);
    assert_eq!(tail, 6);

    log::info!("Continue to mine up to Block#20");
    mock_chain.run(10, Some(20)).await;
    assert_eq!(mock_chain.count().await, 21);

    log::info!("Merge block 17->20 to ChainPiece, expect Interrupted(15)");
    let blocks = mock_chain.get_blocks((17..=20).collect()).await;
    assert_eq!(blocks.last().unwrap().block_number, 20);
    assert_eq!(blocks.len(), 4);
    let merge = chain_piece.merge(&blocks).err().unwrap();
    assert_eq!(merge, ChainPieceError::Interrupted(Some(15)));
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 15);
    assert_eq!(tail, 6);

    log::info!("Merge block 16->20 to ChainPiece, expect OK");
    let blocks = mock_chain.get_blocks((16..=20).collect()).await;
    assert_eq!(blocks.last().unwrap().block_number, 20);
    assert_eq!(blocks.len(), 5);
    let merge = chain_piece.merge(&blocks).unwrap();
    assert_eq!(merge, MergeStatus::Ok(20));
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 20);
    assert_eq!(tail, 11);
}

#[tokio::test]
async fn test_chain_piece_02() {
    env_logger::try_init().unwrap_or_default();
    log::info!("Init Reorg-Chain with threshold = 10");
    let mut chain_piece = ChainPiece::new(10);
    let mock_chain = MockChain::new().await;

    log::info!("Mine 21 blocks 0->20");
    mock_chain.run(10, Some(20)).await;

    log::info!("Merge blocks 0-10 from Chain to ChainPiece, OK");
    let blocks = mock_chain.get_blocks((0..=10).collect()).await;
    chain_piece.merge(&blocks).unwrap();

    log::info!("Get block 11->15 before reorg");
    let mut blocks = mock_chain.get_blocks((11..=15).collect()).await;

    log::info!("Reorg at blocks 15");
    mock_chain.reorg(15).await;

    log::info!(
        "Get block 16->20 after reorg, concat with blocks 11->15 (creating a broken chain A)"
    );
    let after_reorg_blocks = mock_chain.get_blocks((16..=20).collect()).await;
    blocks.append(&mut after_reorg_blocks.clone());

    log::info!("Manually validate A, expect to fail");
    assert!(!chain_piece.check_blocks_continuity(&blocks));

    log::info!("Merge A to ChainPiece, expect BlockDisconnected(10)");
    log::info!("(broken chain failed the validation, return the current head of ChainPiece)");
    let merge = chain_piece.merge(&blocks).err().unwrap();
    assert_eq!(merge, ChainPieceError::Interrupted(Some(10)));
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 10);
    assert_eq!(tail, 1);

    log::info!("Retry get from the last head: 10->20, merge and expect OK");
    let blocks = mock_chain.get_blocks((10..=20).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap();
    assert_eq!(merge, MergeStatus::Ok(20));
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 20);
    assert_eq!(tail, 11);
}

#[tokio::test]
async fn test_chain_piece_03() {
    env_logger::try_init().unwrap_or_default();
    log::info!("Init Reorg-Chain with threshold = 10");
    let mut chain_piece = ChainPiece::new(10);
    let mock_chain = MockChain::new().await;

    log::info!("Mine 21 blocks 0->20");
    mock_chain.run(10, Some(20)).await;

    log::info!("Merge blocks 0-15 from Chain to ChainPiece, OK");
    let blocks = mock_chain.get_blocks((0..=15).collect()).await;
    chain_piece.merge(&blocks).unwrap();
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 15);
    assert_eq!(tail, 6);

    log::info!("Reorg at blocks 10");
    mock_chain.reorg(10).await;

    log::info!("Merge block 16->20, expect StepBack(15)");
    let blocks = mock_chain.get_blocks((16..=20).collect()).await;
    let merge = chain_piece.merge(&blocks).err().unwrap();
    assert_eq!(merge, ChainPieceError::StepBack(15));
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 15);
    assert_eq!(tail, 6);

    log::info!("StepBack & merge block 13->15, expect StepBack(12)");
    let blocks = mock_chain.get_blocks((13..=15).collect()).await;
    let merge = chain_piece.merge(&blocks).err().unwrap();
    assert_eq!(merge, ChainPieceError::StepBack(12));

    log::info!("StepBack & merge block 11->12, expect StepBack(10)");
    let blocks = mock_chain.get_blocks((11..=12).collect()).await;
    let merge = chain_piece.merge(&blocks).err().unwrap();
    assert_eq!(merge, ChainPieceError::StepBack(10));

    log::info!("StepBack & merge block 8->10, expect Reorg(10)");
    let blocks = mock_chain.get_blocks((8..=10).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap();
    assert_eq!(merge, MergeStatus::Reorg(10));
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 9);
    assert_eq!(tail, 6);
}

#[tokio::test]
async fn test_chain_piece_04() {
    env_logger::try_init().unwrap_or_default();
    log::info!("Init Reorg-Chain with threshold = 10");
    let mut chain_piece = ChainPiece::new(10);
    let mock_chain = MockChain::new().await;

    log::info!("Mine 21 blocks 0->20");
    mock_chain.run(10, Some(20)).await;

    log::info!("Merge blocks 0-15 from Chain to ChainPiece, OK");
    let blocks = mock_chain.get_blocks((0..=15).collect()).await;
    chain_piece.merge(&blocks).unwrap();
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 15);
    assert_eq!(tail, 6);

    log::info!("Reorg at blocks 6");
    mock_chain.reorg(6).await;

    let blocks = mock_chain.get_blocks((16..=20).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap_err();
    assert_eq!(merge, ChainPieceError::StepBack(15));

    let blocks = mock_chain.get_blocks((10..=15).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap_err();
    assert_eq!(merge, ChainPieceError::StepBack(9));

    let blocks = mock_chain.get_blocks((5..=9).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap_err();
    assert_eq!(merge, ChainPieceError::ReorgTooDeep);

    let blocks = mock_chain.get_blocks((8..=9).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap_err();
    assert_eq!(merge, ChainPieceError::StepBack(7));

    let blocks = mock_chain.get_blocks((7..=7).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap_err();
    assert_eq!(merge, ChainPieceError::ReorgTooDeep);

    let blocks = mock_chain.get_blocks((6..=7).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap_err();
    assert_eq!(merge, ChainPieceError::ReorgTooDeep);

    let blocks = mock_chain.get_blocks((5..=7).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap_err();
    assert_eq!(merge, ChainPieceError::ReorgTooDeep);

    let blocks = mock_chain.get_blocks((5..=6).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap_err();
    assert_eq!(merge, ChainPieceError::ReorgTooDeep);

    let blocks = mock_chain.get_blocks((4..=5).collect()).await;
    let merge = chain_piece.merge(&blocks).unwrap_err();
    assert_eq!(merge, ChainPieceError::ReorgTooDeep);
}

#[tokio::test]
async fn test_chain_piece_05() {
    env_logger::try_init().unwrap_or_default();
    log::info!("Init Reorg-Chain with threshold = 1");
    let mut chain_piece = ChainPiece::new(0);
    let mock_chain = MockChain::new().await;

    log::info!("Mine 21 blocks 0->20");
    mock_chain.run(10, Some(20)).await;

    log::info!("Merge blocks 0-15 from Chain to ChainPiece, OK");
    let blocks = mock_chain.get_blocks((0..=15).collect()).await;
    chain_piece.merge(&blocks).unwrap();
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 15);
    assert_eq!(tail, 15);

    log::info!("Merge blocks 14-16 from Chain to ChainPiece, OK");
    let blocks = mock_chain.get_blocks((14..=16).collect()).await;
    chain_piece.merge(&blocks).unwrap();
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 16);
    assert_eq!(tail, 16);

    log::info!("Merge blocks 14-16 (again) from Chain to ChainPiece, OK");
    let blocks = mock_chain.get_blocks((14..=16).collect()).await;
    chain_piece.merge(&blocks).unwrap();
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 16);
    assert_eq!(tail, 16);
}

#[tokio::test]
async fn test_chain_piece_06() {
    env_logger::try_init().unwrap_or_default();
    log::info!("Init Reorg-Chain with threshold = 1");
    let mut chain_piece = ChainPiece::new(0);
    let mock_chain = MockChain::new().await;

    log::info!("Mine 31 blocks 0->30");
    mock_chain.run(10, Some(30)).await;

    log::info!("Merge blocks 0-15 from Chain to ChainPiece, OK");
    let blocks = mock_chain.get_blocks((0..=15).collect()).await;
    chain_piece.merge(&blocks).unwrap();
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 15);
    assert_eq!(tail, 15);

    log::info!("Merge blocks 14-16 from Chain to ChainPiece, OK");
    let blocks = mock_chain.get_blocks((14..=16).collect()).await;
    chain_piece.merge(&blocks).unwrap();
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 16);
    assert_eq!(tail, 16);

    log::info!("Merge blocks 16-20 from Chain to ChainPiece, OK");
    let blocks = mock_chain.get_blocks((16..=20).collect()).await;
    chain_piece.merge(&blocks).unwrap();
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 20);
    assert_eq!(tail, 20);

    log::info!("Merge blocks 21-25 from Chain to ChainPiece, OK");
    let blocks = mock_chain.get_blocks((21..=25).collect()).await;
    chain_piece.merge(&blocks).unwrap();
    let (head, tail) = chain_piece.get_head_and_tail();
    assert_eq!(head, 25);
    assert_eq!(tail, 25);
}
