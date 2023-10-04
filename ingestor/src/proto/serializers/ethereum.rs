use crate::proto::ethereum as pb;
use crate::proto::serializers::bytes_to_hex;
use crate::proto::serializers::hex_address;
use crate::proto::BlockChain;
use crate::proto::BlockTrait;
use web3::types as eth;

impl From<eth::AccessList> for pb::AccessList {
    fn from(al: eth::AccessList) -> Self {
        let item = al
            .into_iter()
            .map(|item| pb::AccessListItem {
                address: hex_address(item.address),
                storage_keys: item
                    .storage_keys
                    .into_iter()
                    .map(hex_address)
                    .collect::<Vec<_>>(),
            })
            .collect::<Vec<_>>();
        Self { item }
    }
}

impl<T> From<eth::Block<T>> for pb::Header {
    fn from(block: eth::Block<T>) -> Self {
        Self {
            author: hex_address(block.author),
            state_root: hex_address(block.state_root),
            transactions_root: hex_address(block.transactions_root),
            receipts_root: hex_address(block.receipts_root),
            gas_used: hex_address(block.gas_used),
            gas_limit: hex_address(block.gas_limit),
            extra_data: bytes_to_hex(block.extra_data),
            logs_bloom: block.logs_bloom.map(hex_address),
            timestamp: hex_address(block.timestamp),
            difficulty: hex_address(block.difficulty),
            total_difficulty: hex_address(block.total_difficulty.unwrap_or(eth::U256::zero())),
            seal_fields: block
                .seal_fields
                .into_iter()
                .map(hex_address)
                .collect::<Vec<_>>(),
            size: block.size.map(|s| s.as_u64()),
            base_fee_per_gas: block.base_fee_per_gas.map(hex_address),
            nonce: hex_address(block.nonce.unwrap_or(eth::H64::zero())),
        }
    }
}

impl From<eth::Transaction> for pb::Transaction {
    fn from(tx: eth::Transaction) -> Self {
        Self {
            hash: hex_address(tx.hash),
            nonce: tx.nonce.as_u64(),
            block_hash: tx.block_hash.map(hex_address),
            block_number: tx.block_number.map(|b| b.as_u64()),
            transaction_index: tx.transaction_index.map(|i| i.as_u64()),
            from_address: hex_address(tx.from.unwrap_or_default()),
            to_address: tx.to.map(hex_address),
            value: hex_address(tx.value),
            gas_price: tx.gas_price.map(hex_address),
            gas: hex_address(tx.gas),
            input: bytes_to_hex(tx.input),
            v: tx.v.unwrap_or_default().as_u64(),
            r: hex_address(tx.r.unwrap_or_default()),
            s: hex_address(tx.s.unwrap_or_default()),
            transaction_type: tx.transaction_type.map(|t| t.as_u32() as i32),
            access_list: tx.access_list.map(pb::AccessList::from),
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.map(hex_address),
            max_fee_per_gas: tx.max_fee_per_gas.map(hex_address),
        }
    }
}

impl From<eth::Log> for pb::Log {
    fn from(log: eth::Log) -> Self {
        Self {
            address: hex_address(log.address),
            topics: log.topics.iter().map(hex_address).collect(),
            data: bytes_to_hex(log.data),
            block_hash: log.block_hash.map(hex_address),
            block_number: log.block_number.map(|nb| nb.as_u64()),
            transaction_hash: log.transaction_hash.map(hex_address),
            transaction_index: log.transaction_index.map(|idx| idx.as_u64()),
            log_index: log.log_index.map(|idx| idx.as_u64()),
            transaction_log_index: log.transaction_log_index.map(|idx| idx.as_u64()),
            log_type: log.log_type,
            removed: log.removed,
        }
    }
}

impl From<eth::Block<eth::H256>> for pb::Block {
    fn from(block: eth::Block<eth::H256>) -> Self {
        Self {
            chain_id: 1,
            block_hash: hex_address(block.hash.unwrap()),
            parent_hash: hex_address(block.parent_hash),
            block_number: block.number.unwrap_or_default().as_u64(),
            header: Some(pb::Header::from(block)),
            transactions: vec![],
            logs: vec![],
        }
    }
}

impl From<eth::Block<eth::Transaction>> for pb::Block {
    fn from(block: eth::Block<eth::Transaction>) -> Self {
        let transactions = block
            .transactions
            .clone()
            .into_iter()
            .map(pb::Transaction::from)
            .collect();

        Self {
            chain_id: 1,
            block_hash: hex_address(block.hash.unwrap_or_default()),
            parent_hash: hex_address(block.parent_hash),
            block_number: block.number.unwrap_or_default().as_u64(),
            header: Some(pb::Header::from(block)),
            transactions,
            logs: vec![],
        }
    }
}

impl From<(eth::Block<eth::Transaction>, Vec<eth::Log>, u64)> for pb::Block {
    fn from((block, logs, chain_id): (eth::Block<eth::Transaction>, Vec<eth::Log>, u64)) -> Self {
        let mut pb_block = pb::Block::from(block);
        pb_block.chain_id = chain_id;
        pb_block.logs = logs.into_iter().map(pb::Log::from).collect::<Vec<_>>();
        pb_block
    }
}

impl From<(u64, String, String)> for pb::Block {
    fn from(values: (u64, String, String)) -> Self {
        let (block_number, block_hash, parent_hash) = values;
        let header = pb::Header::default();
        pb::Block {
            block_number,
            block_hash,
            parent_hash,
            header: Some(header),
            ..Default::default()
        }
    }
}

impl pb::Block {
    #[cfg(test)]
    pub fn mock_new(block_number: u64, data: String, parent_hash: Option<String>) -> Self {
        use crate::proto::serializers::sha256_hasing;
        let block_hash = sha256_hasing(format!("{block_number}|{data}|{:?}", parent_hash));
        let header = pb::Header::default();

        pb::Block {
            block_hash,
            block_number,
            parent_hash: parent_hash.unwrap_or(format!("{:?}", eth::H256::zero())),
            header: Some(header),
            ..Default::default()
        }
    }
}

impl BlockTrait for pb::Block {
    fn get_blockchain(&self) -> BlockChain {
        BlockChain::Ethereum
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
