use std::fmt::Debug;
use web3::types::Bytes;

mod ethereum;
mod mockchain;

use common_libs::hex::encode as hex_encode;
use common_libs::hex::ToHex;
use common_libs::sha2::Digest;
use common_libs::sha2::Sha256;

fn hex_address<T: Debug>(value: T) -> String {
    format!("{:?}", value)
}

fn bytes_to_hex(value: Bytes) -> String {
    format!("0x{}", value.0.encode_hex::<String>())
}

fn sha256_hasing(value: String) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.into_bytes());
    let result = hasher.finalize();
    format!("0x{}", hex_encode(result))
}
