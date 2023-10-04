#![allow(dead_code)]

use crate::clients::EthereumClient as Client;
use crate::clients::NodeType;
use common_libs::relative_path::RelativePath;
use common_libs::utils::load_file;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::env::current_dir;
use std::fs;
use std::io::BufReader;
use web3::types::Block;
use web3::types::Log;
use web3::types::Trace;
use web3::types::Transaction;
use web3::types::H256;

pub fn get_client() -> Client {
    Client::new(std::env::var("RPC").unwrap(), NodeType::Light).unwrap()
}

pub fn load_sample_data_from_json_file<T: DeserializeOwned>(path: &str) -> T {
    let file = load_file(&format!("src/tests/{path}"));
    serde_json::from_reader(BufReader::new(file)).unwrap()
}

pub fn get_block_sample() -> Block<H256> {
    load_sample_data_from_json_file("block_sample.json")
}

pub fn get_logs_sample() -> Vec<Log> {
    load_sample_data_from_json_file("logs_sample.json")
}

pub fn get_trace_call() -> Vec<Trace> {
    load_sample_data_from_json_file("trace_call_sample.json")
}

pub fn get_txs() -> HashMap<H256, Transaction> {
    let txs = load_sample_data_from_json_file::<Vec<Transaction>>("txs_sample.json");
    txs.into_iter().fold(HashMap::new(), |mut map, tx| {
        map.insert(tx.hash, tx);
        map
    })
}

pub fn save_temp_json<T: Serialize>(value: &T) -> Result<String, String> {
    let content = serde_json::to_string(value).map_err(|e| e.to_string())?;
    let dir = current_dir().unwrap();
    let path_rf = RelativePath::new("temp.json").to_path(dir);
    let os_string = path_rf.into_os_string();
    let full_path = os_string.to_str().unwrap();
    fs::write(full_path, content).map_err(|e| e.to_string())?;
    Ok(full_path.to_owned())
}

pub fn save_temp_binary(value: &[u8]) -> Result<String, String> {
    let dir = current_dir().unwrap();
    let path_rf = RelativePath::new("temp.bin").to_path(dir);
    let os_string = path_rf.into_os_string();
    let full_path = os_string.to_str().unwrap();
    fs::write(full_path, value).map_err(|e| e.to_string())?;
    Ok(full_path.to_owned())
}
