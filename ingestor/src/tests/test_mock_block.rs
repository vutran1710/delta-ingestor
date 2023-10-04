use crate::proto::ethereum::Block;
use crate::proto::BlockTrait;

#[test]
fn test_mock_block() {
    let block = Block::mock_new(0, "".to_string(), None);
    println!("------- block: {:?}", block);
    println!("------- hash: {:?}", block.get_hash());
    println!("------- number: {:?}", block.get_number());
    println!("------- parent-hash: {:?}", block.get_parent_hash());
    let block = Block::mock_new(1, "hello".to_string(), Some(block.get_hash()));
    println!("------- block: {:?}", block);
    println!("------- hash: {:?}", block.get_hash());
    println!("------- number: {:?}", block.get_number());
    println!("------- parent-hash: {:?}", block.get_parent_hash());
    let block = Block::mock_new(2, "world".to_string(), Some(block.get_hash()));
    println!("------- block: {:?}", block);
    println!("------- hash: {:?}", block.get_hash());
    println!("------- number: {:?}", block.get_number());
    println!("------- parent-hash: {:?}", block.get_parent_hash());
}
