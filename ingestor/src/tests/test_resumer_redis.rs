use crate::config::CommandConfig;
use crate::core::get_block_numbers;
use crate::core::ResumerTrait;
use crate::name_svc::NameService;
use crate::proto::ethereum::Block;
use crate::proto::BlockChain;
use crate::proto::BlockTrait;
use crate::resumers::create_resumer;
use crate::resumers::Resumer;
use common_libs::env_logger;
use common_libs::log::info;
use common_libs::tokio;

async fn init_store<B: BlockTrait>() -> Resumer<B> {
    env_logger::try_init().unwrap_or_default();
    let mut cfg = CommandConfig::default();
    cfg.reorg_threshold = 10;
    cfg.resumer = Some(String::from("redis://localhost:6379"));
    let name_service = NameService::from((&cfg, BlockChain::Ethereum));
    let store = create_resumer::<B>(&cfg, &name_service).await.unwrap();
    if let Resumer::Redis(redis) = store {
        redis.clear().await.unwrap();
        return Resumer::Redis(redis);
    }
    panic!("not redis")
}

#[tokio::test]
async fn test_checkpoint_store_01() {
    let store = init_store().await;

    let last_blocks = store.get_latest_blocks().await.unwrap();
    info!("Before:\n {:?}", last_blocks);
    assert!(last_blocks.is_empty());

    let mut blocks = vec![Block::mock_new(0, "somedata".to_string(), None)];

    for number in 1..10 {
        let parent_hash = blocks.last().unwrap().get_parent_hash();
        let data = format!("Some data = {number}");
        let block = Block::mock_new(number, data, Some(parent_hash));
        blocks.push(block);
    }

    store.save_latest_blocks(&blocks).await.unwrap();

    let last_blocks = store.get_latest_blocks().await.unwrap();
    info!("After:\n {:?}", last_blocks);
    assert_eq!(last_blocks.last().unwrap().get_number(), 9);

    let actual_blocks = store.get_latest_blocks().await.unwrap();
    let expected_block_numbers = (0..=9).collect::<Vec<u64>>();
    assert_eq!(get_block_numbers(&actual_blocks), expected_block_numbers);
}
