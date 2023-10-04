use super::sha256_hasing;
use crate::proto::mockchain::Block;
use crate::proto::mockchain::Transaction;
use crate::proto::BlockChain;
use crate::proto::BlockTrait;
use common_libs::log;
use lazy_static::lazy_static;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use web3::types::H128;

lazy_static! {
    static ref NAME_COLLECTION: Vec<&'static str> = vec!["quan", "vu", "mr.block", "mr.chain"];
}

pub fn mock_transactions(count: usize, block_number: u64) -> Vec<Transaction> {
    let mut rng = thread_rng();
    let txs_range = 0..count;
    txs_range
        .map(|nth| {
            let names = NAME_COLLECTION.choose_multiple(&mut rng, 2).cloned().collect::<Vec<_>>();
            let from_addr = names[0].to_owned();
            let to_addr = names[1].to_owned();
            let amount = thread_rng().gen_range(0..10);
            let hash = sha256_hasing(format!("{block_number}-{nth}-{from_addr}-{to_addr}-{amount}"));
            log::info!("New TX(#{block_number}, idx=#{nth}): [{from_addr} ---> {to_addr} (Amount={amount} coins)]");
            Transaction {
                from_addr,
                to_addr,
                amount,
                hash,
            }
        })
        .collect()
}

impl From<(u64, String, String)> for Block {
    fn from(value: (u64, String, String)) -> Self {
        Self {
            block_hash: value.1,
            parent_hash: value.2,
            block_number: value.0,
            transactions: vec![],
        }
    }
}

impl Block {
    pub fn mock_new(block_number: u64, data: String, parent_hash: Option<String>) -> Self {
        let transactions = if block_number > 0 {
            mock_transactions(thread_rng().gen_range(0..5), block_number)
        } else {
            vec![]
        };

        let txs_hashes = transactions
            .iter()
            .map(|tx| tx.hash.clone())
            .collect::<Vec<_>>()
            .join("");

        let block_hash = sha256_hasing(format!(
            "block-number={block_number}, parent-hash={:?}, data={data}, tx-collection={txs_hashes}",
            parent_hash.clone().unwrap_or("None".to_string())
        ));

        Block {
            block_hash,
            block_number,
            parent_hash: parent_hash.unwrap_or(format!("{:?}", H128::zero())),
            transactions,
        }
    }
}

impl BlockTrait for Block {
    fn get_blockchain(&self) -> BlockChain {
        BlockChain::MockChain
    }

    fn get_number(&self) -> u64 {
        self.block_number
    }

    fn get_hash(&self) -> String {
        self.block_hash.clone()
    }

    fn get_parent_hash(&self) -> String {
        self.parent_hash.clone()
    }
}
