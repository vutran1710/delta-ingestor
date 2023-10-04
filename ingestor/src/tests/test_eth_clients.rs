use crate::clients::EthereumClient;
use crate::clients::NodeType;
use crate::clients::RREthClient;
use crate::config::CommandConfig;
use crate::core::ClientTrait;
use crate::ingestors::ethereum::rpc_check;
use comfy_table::Table;
use common_libs::clap::Parser;
use common_libs::env_logger;
use common_libs::futures_util::future::join_all;
use common_libs::log;
use common_libs::log::info;
use common_libs::tokio;
use common_libs::tokio::time;
use metrics::default_registry;
use std::env::var;
use web3::types::Block;
use web3::types::Transaction;

pub fn get_public_rpc() -> Vec<String> {
    let public_rpcs = vec![
        "https://eth-mainnet.g.alchemy.com/v2/N7gZFcuMkhLTTpdsRLEcDXYIJssj6GsI",
        "https://eth-mainnet.g.alchemy.com/v2/demo",
        "https://api.securerpc.com/v1",
        "https://ethereum.publicnode.com",
        "https://rpc.builder0x69.io",
        "https://rpc.energyweb.org",
        "https://eth-rpc.gateway.pokt.network",
        "https://eth.llamarpc.com",
        "https://rpc.ankr.com/eth",
        "https://ethereum.blockpi.network/v1/rpc/public",
        "https://eth.rpc.blxrbdn.com",
        "https://eth.api.onfinality.io/public",
        "https://uk.rpc.blxrbdn.com",
        "https://singapore.rpc.blxrbdn.com",
        "https://virginia.rpc.blxrbdn.com",
        "https://eth-mainnet.nodereal.io/v1/1659dfb40aa24bbb8153a677b98064d7",
        "https://cloudflare-eth.com",
        "https://rpc.payload.de",
        "https://api.zmok.io/mainnet/oaen6dy8ff6hju9k",
        "https://eth.althea.net",
    ];
    public_rpcs.into_iter().map(|s| s.to_string()).collect()
}

fn categorize_node(url: String) -> (String, NodeType) {
    let node_type = NodeType::from(url.clone());
    let endpoint = match node_type {
        NodeType::Light => url,
        NodeType::Archive => {
            let parts = url.split(':').collect::<Vec<&str>>();
            parts[1..].to_owned().join(":")
        }
    };
    (endpoint, node_type)
}

#[tokio::test]
async fn read_public_rpcs_to_endpoint_and_node_type() {
    env_logger::try_init().unwrap_or_default();
    let config = CommandConfig::parse();
    let rpcs = get_public_rpc();
    let mut count = 0;

    for endpoint in rpcs {
        if let Ok((_, clients)) = rpc_check(endpoint, &config).await {
            info!("checked {} clients", clients.len());
            count += 1;
        }
    }
    assert!(count > 0);
}

fn create_eth_single_client() -> EthereumClient {
    env_logger::try_init().unwrap_or_default();
    let default_rpc = "https://eth.llamarpc.com".to_string();
    let (endpoint, node_type) = categorize_node(var("RPC").unwrap_or(default_rpc));
    EthereumClient::new(endpoint, node_type).unwrap()
}

async fn create_rr_eth_client() -> RREthClient {
    env_logger::try_init().unwrap_or_default();
    let registry = default_registry();
    let clients = get_public_rpc()
        .into_iter()
        .map(|rpc| {
            let (endpoint, node_type) = categorize_node(rpc);
            EthereumClient::new(endpoint, node_type).unwrap()
        })
        .collect();
    RREthClient::new(clients, registry, 2, 10, 1)
}

#[tokio::test]
async fn get_blocks_from_block_range() {
    let client = create_eth_single_client();
    let block_numbers: Vec<u64> = vec![5_000_000, 5_000_001, 5_000_002];
    let block_headers = client.get_blocks(&block_numbers).await.unwrap();

    let block_headers = block_headers
        .into_iter()
        .flatten()
        .collect::<Vec<Block<Transaction>>>();

    assert_eq!(block_headers.len(), 3);

    for block in block_headers {
        let actual_hash = format!("{:?}", block.hash.unwrap());
        let actual_tx_count = block.transactions.len();
        let expected_hash = match block.number.unwrap().as_u64() {
            5_000_000 => "0x7d5a4369273c723454ac137f48a4f142b097aa2779464e6505f1b1c5e37b5382",
            5_000_001 => "0x056bf449c33030c74b01cdfe2c76e05c25895340b68575e51225e5aadc7af6e1",
            5_000_002 => "0xc7faf8f637094c95c5f6cc131fc8aa3d8f031bcfb1df9b30b9a73f1c4532c417",
            _ => panic!(),
        };
        assert_eq!(actual_hash, expected_hash.to_string());

        let expected_tx_count = match actual_hash.as_str() {
            "0x7d5a4369273c723454ac137f48a4f142b097aa2779464e6505f1b1c5e37b5382" => 109,
            "0x056bf449c33030c74b01cdfe2c76e05c25895340b68575e51225e5aadc7af6e1" => 88,
            "0xc7faf8f637094c95c5f6cc131fc8aa3d8f031bcfb1df9b30b9a73f1c4532c417" => 109,
            "0x9a71a95be3fe957457b11817587e5af4c7e24836d5b383c430ff25b9286a457f" => 348,
            _ => panic!(),
        };

        assert_eq!(actual_tx_count, expected_tx_count);
    }

    let block_numbers: Vec<u64> = vec![5_000_000, 5_000_002];
    let block_headers = client.get_blocks(&block_numbers).await.unwrap();

    let block_headers = block_headers
        .into_iter()
        .flatten()
        .collect::<Vec<Block<Transaction>>>();

    assert_eq!(block_headers.len(), 2);
    let block_numbers = block_headers
        .into_iter()
        .map(|b| b.number.unwrap().as_u64())
        .collect::<Vec<u64>>();

    assert_eq!(block_numbers[0], 5_000_000);
    assert_eq!(block_numbers[1], 5_000_002);
}

#[tokio::test]
async fn get_logs_from_block_range() {
    let client = create_eth_single_client();
    let block_numbers: Vec<u64> = vec![5_000_000, 5_000_001, 5_000_002];
    let logs = client.get_logs(&block_numbers).await.unwrap();

    assert_eq!(logs.keys().len(), 3);

    for block_hash in logs.keys() {
        let hash_string = format!("{:?}", block_hash.clone());
        let actual_tx_count = logs.get(block_hash).unwrap().len();
        let expected_tx_count = match hash_string.as_str() {
            "0x7d5a4369273c723454ac137f48a4f142b097aa2779464e6505f1b1c5e37b5382" => 209,
            "0x056bf449c33030c74b01cdfe2c76e05c25895340b68575e51225e5aadc7af6e1" => 156,
            "0xc7faf8f637094c95c5f6cc131fc8aa3d8f031bcfb1df9b30b9a73f1c4532c417" => 96,
            _ => panic!(),
        };

        assert_eq!(actual_tx_count, expected_tx_count);
    }

    let block_numbers: Vec<u64> = vec![5_000_000, 5_000_003];
    let logs = client.get_logs(&block_numbers).await.unwrap();

    assert_eq!(logs.keys().len(), 2);
}

fn create_load_test_table() -> comfy_table::Table {
    let mut table = Table::new();
    table.set_header(vec![
        "download time",
        "from block",
        "to block",
        "number of blocks",
        "total txs",
        "total logs",
        "total traces",
    ]);
    table
}

#[tokio::test]
async fn load_test_with_rr_client() {
    use std::sync::Arc;
    use std::sync::Mutex;
    /*
    Run with command:
    ```
    LOAD_TEST=1 RUST_LOG=info cargo test tests::eth_clients::load_test_with_rr_client
    ```
    */

    if var("LOAD_TEST").is_err() {
        return;
    }

    let table = create_load_test_table();
    let table = Arc::new(Mutex::new(table));
    let client = Arc::new(create_rr_eth_client().await);
    log::info!("---warm up---");
    client.get_full_blocks((0..2).collect()).await.unwrap();
    log::warn!("-------------------------------------");

    let start_blocks: [u64; 5] = [0, 1_000_000, 5_000_000, 10_000_000, 16_000_000];
    let batch_sizes = [5, 10, 20, 50];
    let mut tasks = vec![];

    for size in batch_sizes {
        for start_block in start_blocks {
            let range = (start_block, start_block + size);
            let client = client.clone();
            let table = table.clone();
            let task = async move {
                let timer = time::Instant::now();
                let data = client
                    .get_full_blocks((range.0..range.1).collect())
                    .await
                    .unwrap();
                let elapsed = timer.elapsed();

                let block_count = data.len();
                let start_block = data.first().unwrap().block_number;
                let stop_block = data.last().unwrap().block_number;
                let mut total_txs = 0;
                let mut total_logs = 0;

                for block in data {
                    total_txs += block.transactions.len();
                    total_logs += block.logs.len();
                }

                if let Ok(mut inner_table) = table.try_lock() {
                    inner_table.add_row(vec![
                        format!("{:?}", elapsed),
                        format!("{:?}", start_block),
                        format!("{:?}", stop_block),
                        format!("{:?}", block_count),
                        // Detail count of each data
                        format!("{:?}", total_txs),
                        format!("{:?}", total_logs),
                    ]);
                };
            };

            tasks.push(task);
        }
    }

    join_all(tasks).await;
    log::info!("Summary: \n{}", table.try_lock().unwrap());
}
