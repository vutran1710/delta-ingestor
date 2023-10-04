use serde::Deserialize;
use serde::Serialize;

// @generated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
pub struct Block {
    #[prost(string, tag = "1")]
    pub block_hash: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub parent_hash: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub block_number: u64,
    #[prost(message, repeated, tag = "4")]
    pub transactions: ::prost::alloc::vec::Vec<Transaction>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message, Serialize, Deserialize)]
pub struct Transaction {
    #[prost(string, tag = "1")]
    pub hash: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub from_addr: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub to_addr: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub amount: u64,
}
// @@protoc_insertion_point(module)
