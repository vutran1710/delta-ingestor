use crate::errors::ChainPieceError;
use crate::proto::BlockTrait;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::marker::PhantomData;

#[derive(Default)]
pub struct ChainPiece<B: BlockTrait> {
    _marker: PhantomData<B>,
    // An in-memory piece of chain that contains recent blocks
    // It helps deal with Block validation & Reorg
    // We use 2 data structures:
    // - a <number->hash> Hashmap
    number_idx: HashMap<u64, String>,
    // - a small queue to track block numbers
    queue: VecDeque<u64>,
    // threshold to determine how long the in-memory chain should extend, min=1
    threshold: u16,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MergeStatus {
    Nothing,
    Ok(u64),
    Reorg(u64),
}

impl<B: BlockTrait> ChainPiece<B> {
    pub fn new(threshold: u16) -> Self {
        Self {
            threshold: threshold.max(1),
            ..Default::default()
        }
    }

    fn is_on_chain(&self, block: &B) -> bool {
        let block_number = block.get_number();
        let block_hash = block.get_hash();

        self.number_idx
            .get(&block_number)
            .map(|hash| *hash == block_hash)
            .unwrap_or(false)
    }

    fn is_parent_on_chain(&self, block: &B) -> bool {
        let parent_number = block.get_number() - 1;
        let parent_hash = block.get_parent_hash();

        self.number_idx
            .get(&parent_number)
            .map(|hash| *hash == parent_hash)
            .unwrap_or(false)
    }

    pub fn get_head_and_tail(&self) -> (u64, u64) {
        let head = self.queue.back().expect("Missing chain head").to_owned();
        let tail = self.queue.front().expect("Missing chain tail").to_owned();
        (head, tail)
    }

    fn append_new_block(&mut self, block: &B) -> Result<(), ChainPieceError> {
        let block_number = block.get_number();
        let block_hash = block.get_hash();

        if self.number_idx.contains_key(&block_number) {
            // Typically this will never happen! But in case some chain really fucks up...
            return Err(ChainPieceError::DuplicateBlockNumber(block_number));
        }

        self.number_idx.insert(block_number, block_hash);
        self.queue.push_back(block_number);
        Ok(())
    }

    fn remove_head_block(&mut self, remove_head: u64) -> bool {
        // Remove the block at head up to remove_head
        if self.queue.back().unwrap() >= &remove_head {
            let number = self.queue.pop_back().unwrap();
            self.number_idx.remove(&number).unwrap();
            return true;
        }
        false
    }

    fn remove_tail_block(&mut self) -> bool {
        if self.queue.len() > (self.threshold as usize) {
            let number = self.queue.pop_front().unwrap();
            self.number_idx.remove(&number).unwrap();
            return true;
        }
        false
    }

    pub fn check_blocks_continuity(&self, blocks: &[B]) -> bool {
        for (idx, block) in blocks.iter().enumerate() {
            if idx == 0 {
                continue;
            }

            let prev_block = &blocks[idx - 1];
            let is_continuous = block.get_number() - 1 == prev_block.get_number()
                && (block.get_parent_hash() == prev_block.get_hash());

            if !is_continuous {
                return false;
            }
        }

        true
    }

    pub fn merge(&mut self, blocks: &Vec<B>) -> Result<MergeStatus, ChainPieceError> {
        if blocks.is_empty() {
            return Ok(MergeStatus::Nothing);
        }

        if !self.check_blocks_continuity(blocks) {
            return Err(ChainPieceError::Interrupted(self.queue.back().cloned()));
        }

        if self.queue.is_empty() {
            blocks
                .iter()
                .for_each(|b| self.append_new_block(b).unwrap());
            while self.remove_tail_block() {}
            let head = *self.queue.back().unwrap();
            return Ok(MergeStatus::Ok(head));
        }

        let (head, tail) = self.get_head_and_tail();

        if blocks[0].get_number() > head + 1 {
            // Block-gap! We cannot accept this!
            // This is an extra logic guard
            // It mostly serves testing, as in reality this case
            // should not happen
            return Err(ChainPieceError::Interrupted(Some(head)));
        }

        if blocks.last().unwrap().get_number() < tail {
            return Err(ChainPieceError::ReorgTooDeep);
        }

        for block in blocks.iter() {
            // This is a sequence of properly linked blocks
            // What we are looking for is where our chain and the blocks diverge
            let (head, _) = self.get_head_and_tail();
            let block_number = block.get_number();
            let parent_block_number = block_number - 1;
            let is_on_chain = self.is_on_chain(block);
            let is_parent_on_chain = self.is_parent_on_chain(block);

            match (is_on_chain, is_parent_on_chain) {
                (true, true) => {
                    // nothing to do here
                    continue;
                }
                (true, false) => {
                    // This block is probably the tail block!
                    assert_eq!(block_number, tail);
                    continue;
                }
                (false, true) => {
                    if block_number == head + 1 {
                        // New block, add it to chain
                        self.append_new_block(block)?;
                        while self.remove_tail_block() {}
                        continue;
                    }
                    // Reorg block, return it
                    while self.remove_head_block(block_number) {}
                    return Ok(MergeStatus::Reorg(block_number));
                }
                (false, false) => {
                    if block_number < tail {
                        continue;
                    }
                    if block_number == tail || parent_block_number == tail {
                        return Err(ChainPieceError::ReorgTooDeep);
                    }
                    // Block belongs to a fork, immediately step back!
                    return Err(ChainPieceError::StepBack(block_number - 1));
                }
            }
        }

        let head = *self.queue.back().unwrap();
        Ok(MergeStatus::Ok(head))
    }

    pub fn extract_blocks(&self) -> Vec<B> {
        let mut blocks = vec![];

        let mut block_numbers = self.number_idx.keys().collect::<Vec<&u64>>();
        block_numbers.sort();

        let default_parent_hash = String::from("");
        for block_number in block_numbers.iter() {
            let hash = self.number_idx.get(block_number).cloned().unwrap();

            let parent_number = if **block_number > 0 {
                Some(**block_number - 1)
            } else {
                None
            };

            let parent_hash = match parent_number {
                Some(value) => self
                    .number_idx
                    .get(&value)
                    .cloned()
                    .unwrap_or(default_parent_hash.clone()),
                None => default_parent_hash.clone(),
            };

            let block = B::from((**block_number, hash, parent_hash));
            blocks.push(block);
        }

        blocks
    }
}
