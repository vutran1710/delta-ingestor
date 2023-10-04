use crate::proto::ethereum as pb;
use crate::tests::helper::get_block_sample;
use crate::tests::helper::get_logs_sample;
use crate::tests::helper::get_txs;
use common_libs::env_logger;
use prost::Message;
use std::str::FromStr;
use web3::types::H256;

#[test]
fn test_block_serializer() {
    env_logger::try_init().unwrap_or_default();
    let header = get_block_sample();
    let txs = get_txs();
    let logs = get_logs_sample();
    // let trace_calls = get_trace_call();
    let block = pb::Block {
        transactions: txs
            .values()
            .map(|tx| pb::Transaction::from(tx.clone()))
            .collect(),
        logs: logs.clone().into_iter().map(pb::Log::from).collect(),
        ..pb::Block::from(header.clone())
    };
    assert_eq!(
        format!("{:?}", header.hash.unwrap_or_default()),
        block.block_hash
    );
    assert_eq!(txs.len(), block.transactions.len());
    assert_eq!(logs.len(), block.logs.len());
    assert_eq!(header.number.unwrap().as_u64(), block.block_number);
    assert_eq!(header.nonce.unwrap().to_low_u64_be(), 896361);
    assert_eq!(header.timestamp.as_u64(), 1680503255);
    for tx in block.transactions {
        assert!(txs.get(&H256::from_str(&tx.hash).unwrap()).is_some())
    }
    for log in block.logs {
        assert_eq!(
            H256::from_str(&log.block_hash.unwrap_or_default()).unwrap(),
            header.hash.unwrap()
        )
    }
}

/// ## Test Decode Data
#[test]
fn test_decode_block_bytes_to_struct() {
    let block = pb::Block::from(get_block_sample());
    let block_to_bytes = block.encode_to_vec();
    let block_from_bytes = pb::Block::decode(block_to_bytes.as_slice()).unwrap();

    assert_eq!(block.block_number, block_from_bytes.block_number);
    // assert_eq!(block.trace_calls.len(), block_from_bytes.trace_calls.len());
    assert_eq!(
        block.transactions.len(),
        block_from_bytes.transactions.len()
    );
    assert_eq!(block.logs.len(), block_from_bytes.logs.len());
    assert_eq!(
        block.header.unwrap().gas_used,
        block_from_bytes.header.unwrap().gas_used
    )
}

#[test]
fn test_decode_txs_bytes_to_struct() {
    let txs = get_txs()
        .values()
        .map(|tx| pb::Transaction::from(tx.clone()))
        .collect::<Vec<_>>();

    let txs_to_bytes = txs
        .clone()
        .into_iter()
        .map(|tx| tx.encode_to_vec())
        .collect::<Vec<_>>();

    let txs_decoded = txs_to_bytes
        .into_iter()
        .map(|tx| {
            pb::Transaction::decode(tx.as_slice()).expect("Decode bytes to Transaction error")
        })
        .collect::<Vec<_>>();

    assert_eq!(txs.len(), txs_decoded.len())
}

#[test]
fn test_encode_decode_logs() {
    let logs = get_logs_sample()
        .into_iter()
        .map(pb::Log::from)
        .collect::<Vec<_>>();

    let logs_to_bytes = logs
        .clone()
        .into_iter()
        .map(|log| log.encode_to_vec())
        .collect::<Vec<_>>();

    let logs_encoded = logs_to_bytes
        .into_iter()
        .map(|log| pb::Log::decode(log.as_slice()));
    assert_eq!(logs.len(), logs_encoded.len())
}

fn get_bytes_sample() -> Vec<u8> {
    vec![
        8, 1, 18, 66, 48, 120, 56, 56, 101, 57, 54, 100, 52, 53, 51, 55, 98, 101, 97, 52, 100, 57,
        99, 48, 53, 100, 49, 50, 53, 52, 57, 57, 48, 55, 98, 51, 50, 53, 54, 49, 100, 51, 98, 102,
        51, 49, 102, 52, 53, 97, 97, 101, 55, 51, 52, 99, 100, 99, 49, 49, 57, 102, 49, 51, 52, 48,
        54, 99, 98, 54, 26, 66, 48, 120, 100, 52, 101, 53, 54, 55, 52, 48, 102, 56, 55, 54, 97,
        101, 102, 56, 99, 48, 49, 48, 98, 56, 54, 97, 52, 48, 100, 53, 102, 53, 54, 55, 52, 53, 97,
        49, 49, 56, 100, 48, 57, 48, 54, 97, 51, 52, 101, 54, 57, 97, 101, 99, 56, 99, 48, 100, 98,
        49, 99, 98, 56, 102, 97, 51, 32, 1, 42, 249, 6, 10, 42, 48, 120, 48, 53, 97, 53, 54, 101,
        50, 100, 53, 50, 99, 56, 49, 55, 49, 54, 49, 56, 56, 51, 102, 53, 48, 99, 52, 52, 49, 99,
        51, 50, 50, 56, 99, 102, 101, 53, 52, 100, 57, 102, 18, 66, 48, 120, 100, 54, 55, 101, 52,
        100, 52, 53, 48, 51, 52, 51, 48, 52, 54, 52, 50, 53, 97, 101, 52, 50, 55, 49, 52, 55, 52,
        51, 53, 51, 56, 53, 55, 97, 98, 56, 54, 48, 100, 98, 99, 48, 97, 49, 100, 100, 101, 54, 52,
        98, 52, 49, 98, 53, 99, 100, 51, 97, 53, 51, 50, 98, 102, 51, 26, 66, 48, 120, 53, 54, 101,
        56, 49, 102, 49, 55, 49, 98, 99, 99, 53, 53, 97, 54, 102, 102, 56, 51, 52, 53, 101, 54, 57,
        50, 99, 48, 102, 56, 54, 101, 53, 98, 52, 56, 101, 48, 49, 98, 57, 57, 54, 99, 97, 100, 99,
        48, 48, 49, 54, 50, 50, 102, 98, 53, 101, 51, 54, 51, 98, 52, 50, 49, 34, 66, 48, 120, 53,
        54, 101, 56, 49, 102, 49, 55, 49, 98, 99, 99, 53, 53, 97, 54, 102, 102, 56, 51, 52, 53,
        101, 54, 57, 50, 99, 48, 102, 56, 54, 101, 53, 98, 52, 56, 101, 48, 49, 98, 57, 57, 54, 99,
        97, 100, 99, 48, 48, 49, 54, 50, 50, 102, 98, 53, 101, 51, 54, 51, 98, 52, 50, 49, 42, 1,
        48, 50, 4, 53, 48, 48, 48, 58, 52, 48, 120, 52, 55, 54, 53, 55, 52, 54, 56, 50, 102, 55,
        54, 51, 49, 50, 101, 51, 48, 50, 101, 51, 48, 50, 102, 54, 99, 54, 57, 54, 101, 55, 53, 55,
        56, 50, 102, 54, 55, 54, 102, 51, 49, 50, 101, 51, 52, 50, 101, 51, 50, 66, 130, 4, 48,
        120, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
        48, 48, 48, 48, 48, 48, 48, 48, 74, 10, 49, 52, 51, 56, 50, 54, 57, 57, 56, 56, 82, 11, 49,
        55, 49, 55, 49, 52, 56, 48, 53, 55, 54, 90, 11, 51, 52, 51, 53, 49, 51, 52, 57, 55, 54, 48,
        104, 153, 4, 122, 18, 48, 120, 53, 51, 57, 98, 100, 52, 57, 55, 57, 102, 101, 102, 49, 101,
        99, 52,
    ]
}

#[test]
fn test_get_block_from_bytes() {
    let bytes = get_bytes_sample();
    let block = pb::Block::decode(bytes.as_slice()).unwrap();
    assert_eq!(block.block_number, 1);
    assert_eq!(block.chain_id, 1);
    let expected_hash =
        H256::from_str("0x88e96d4537bea4d9c05d12549907b32561d3bf31f45aae734cdc119f13406cb6")
            .unwrap();
    let expected_parent_hash =
        H256::from_str("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
            .unwrap();
    assert_eq!(block.block_hash, format!("{:?}", expected_hash));
    assert_eq!(block.parent_hash, format!("{:?}", expected_parent_hash));
}
