use crate::config::Rpc;
use crate::config::{CommandConfig, Ethereum};
use crate::core::ClientTrait;
use crate::ingestors::ethereum::create_client;
use crate::proto::BlockTrait;
use common_libs::env_logger;
use common_libs::futures_util::future::try_join_all;
use common_libs::log::info;
use common_libs::tokio;
use metrics::default_registry;
use std::sync::Arc;

#[tokio::test]
async fn test_download_rre() {
    env_logger::try_init().unwrap_or_default();
    let mut cfg = CommandConfig::default();
    cfg.start_block = 10_000_000;
    cfg.stop_block = Some(10_000_100);
    cfg.reorg_threshold = 1;

    let endpoints = vec![
        "https://eth.llamarpc.com",
        "https://rpc.ankr.com/eth",
        "https://eth.api.onfinality.io/public",
        "https://eth-mainnet.nodereal.io/v1/1659dfb40aa24bbb8153a677b98064d7",
    ];
    let rpcs = endpoints
        .into_iter()
        .map(|endpoint| Rpc {
            endpoint: String::from(endpoint),
            weight: 1,
        })
        .collect();
    let ethereum_config = Ethereum { rpc: rpcs };
    let registry = default_registry();
    let client = create_client(&cfg, registry, ethereum_config)
        .await
        .unwrap();
    let client1 = Arc::new(client);

    for nth in 0..4 {
        info!("---------------------- ROUND: {nth} --------------------------");
        let client2 = client1.clone();
        let client3 = client1.clone();
        let client4 = client1.clone();

        let range1 = (10_000_000..10_000_003).collect();
        let range2 = (10_000_003..10_000_006).collect();
        let range3 = (10_000_006..10_000_010).collect();
        let range4 = (17_014_628..17_014_629).collect();

        let blocks_ranges = vec![
            client1.get_full_blocks(range1),
            client2.get_full_blocks(range2),
            client3.get_full_blocks(range3),
            client4.get_full_blocks(range4),
        ];
        let blocks = try_join_all(blocks_ranges)
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(blocks.len(), 11);

        blocks.iter().for_each(|b| {
            assert_eq!(b.chain_id, 1);
            let number = b.get_number();
            let tx_count = b.transactions.len();
            match number {
                10_000_000 => assert_eq!(tx_count, 103),
                10_000_001 => assert_eq!(tx_count, 108),
                10_000_002 => assert_eq!(tx_count, 201),
                10_000_003 => assert_eq!(tx_count, 116),
                10_000_004 => assert_eq!(tx_count, 127),
                10_000_005 => assert_eq!(tx_count, 194),
                10_000_006 => assert_eq!(tx_count, 152),
                10_000_007 => assert_eq!(tx_count, 0),
                10_000_008 => assert_eq!(tx_count, 174),
                10_000_009 => assert_eq!(tx_count, 170),
                17_014_628 => assert_eq!(tx_count, 110),
                _ => (),
            };
            if number == 10000009 {
                let header = b.header.clone().unwrap();
                info!(">> Validating header data");
                assert_eq!(header.logs_bloom.unwrap(), "0xcc10300081109104a1604240a140100460280a0614194880a0982800b46019e20708184c61121628e9600585000d870a4838d0804726a620a3e8c0981dfe100049743582a2d0c6094e84825844805a2e607639202244f12c024c4600832470351e418040221108574b48424410417a10200488031f88007a584a013bcc8831e403c2520600141082b924c102015584100e0158030381217550076a14441000a10e1590840512a294ac04a990014d8c914000005c998429a080ac12284c09ca45409c206600b4201010784404024e4704a82c7220918c0000a02a414998036326a090417191d01060452082040816e5150021230383c2205c000a1096088a34e8");
                assert_eq!(
                    header.author,
                    "0x5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c".to_lowercase()
                );

                assert_eq!(
                    header.extra_data,
                    "0x6574682d70726f2d687a672d74303032".to_lowercase()
                );

                assert_eq!(
                    header.state_root,
                    "0xe398b5c84256973960ff38b6158abffbdf23db619440b5ae4f64c87af9e5d73d"
                        .to_lowercase()
                );

                assert_eq!(header.size.unwrap(), 30_418);
                assert_eq!(header.difficulty, "2386193856182044");
                assert_eq!(header.total_difficulty, "15273321145620517592509");
                assert_eq!(header.gas_used, "9975920");
                assert_eq!(header.gas_limit, "9990202");
                assert_eq!(header.nonce, "0xd98ce64806532298");
                assert_eq!(header.timestamp, "1588598652");
                assert!(header.base_fee_per_gas.is_none());
            }

            if number == 17014628 {
                info!("test modern blocks");
                let header = b.header.clone().unwrap();
                assert_eq!(header.base_fee_per_gas.unwrap(), "21492254447");
                assert_eq!(header.gas_limit, "30000000");
                assert_eq!(header.total_difficulty, "58750003716598352816469");
                assert_eq!(
                    header.extra_data,
                    "0x68747470733a2f2f6574682d6275696c6465722e636f6d".to_lowercase()
                );
                assert_eq!(header.nonce, "0x0000000000000000");
            }

            for tx in b.transactions.iter() {
                let logs = b
                    .logs
                    .iter()
                    .filter(|l| l.transaction_hash.clone().unwrap() == tx.hash)
                    .collect::<Vec<_>>();
                let log_count = logs.len();
                match tx.hash.as_str() {
                    "0x4b4b366c4e76221293449ad9480cd8e60b42d950bfecbc4c576f11e69bb9ac08" => {
                        assert_eq!(
                            tx.from_address,
                            "0x63D6b930e65eb35d68c02F335B10A19F24A86F07".to_lowercase()
                        );
                        assert_eq!(
                            tx.to_address.clone().unwrap(),
                            "0x765F8a7993111016962cEaB0f1666157d1F56D77".to_lowercase()
                        );
                        assert_eq!(tx.input, "0xfeea0226");
                        assert_eq!(log_count, 25)
                    }
                    "0xa2bb7181b3e745b2462b409eba0ec6c13ae0b8f344bddf36c917884321456676" => {
                        assert_eq!(log_count, 10)
                    }
                    "0x90344c3d7e1e047d61d5818e6dfc8111d90d67857858311b824149fdda9de80f" => {
                        assert_eq!(log_count, 9)
                    }
                    "0xc65667c9efb96c98b0544e487c3a461565d6d135d0391856879facbcf8f26818" => {
                        info!("Validate log...");
                        assert_eq!(log_count, 3);
                        let log = logs[0];
                        assert_eq!(log.address, "0xd4a0e3ec2a937e7cca4a192756a8439a8bf4ba91");
                        assert_eq!(log.topics.len(), 1);
                        assert_eq!(log.topics[0], "0xf85e44c6c3597d176b8d59bfbf500dfdb2badfc8cf91e6d960b16583a5807e48");
                        assert_eq!(log.data, "0x000000000000000000000000000000000000000000000064caf88b01a3ca00000000000000000000000000000000000000000000000000000000000064335f90");

                        let log = logs[1];
                        assert_eq!(log.address, "0xcc88a9d330da1133df3a7bd823b95e52511a6962");
                        assert_eq!(log.topics.len(), 3);
                        assert_eq!(log.topics[0], "0x4cd53b0b754082a31f5a6f3dc965c36d1d901c309830e0b4c17949aff97f0b14");
                        assert_eq!(log.topics[1], "0x00000000000000000000000083533fdd3285f48204215e9cf38c785371258e76");
                        assert_eq!(log.topics[2], "0x000000000000000000000000a8e272fda85fc5593ead318a97bb3a6439ce9302");
                        assert_eq!(log.data, "0x000000000000000000000000008a9e053ae331b923e7f9b4e8eaa0e230000000");
                    }
                    _ => (),
                }
            }
        });
    }
}
