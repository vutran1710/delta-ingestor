#![allow(dead_code)]

///### Get block hash with tag in protobuf message = 2
///read more (https://protobuf.dev/programming-guides/encoding/#order)
pub fn get_hash_from_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut i = 0;
    while i < bytes.len() {
        let tag = bytes[i] >> 3;
        let wire_type = bytes[i] & 0x7;
        i += 1;
        if let (2, 2) = (tag, wire_type) {
            let len = read_variant(bytes, &mut i) as usize;
            return Some(bytes[i..i + len].to_vec());
        } else {
            skip_field(bytes, &mut i, wire_type)?;
        }
    }
    None
}

/// In Protobuf encoding, the last three bits of the first byte of a field key
/// are used to encode the wire type, which defines the type of the value that
/// follows. For example, wire type 0 means varint, wire type 2 means
/// length-delimited, and so on.  In the given code, the expression b & 0x7 is
/// used to extract the last three bits of the first byte of the field key,
/// which represents the wire type. The expression b >> 3 is used to extract the
/// first 5 bits of the first byte, which represent the tag number.
pub fn get_block_number(bytes: &[u8]) -> Option<u64> {
    let mut i = 0;
    while let Some(b) = bytes.get(i) {
        i += 1;
        if let (3, 0) = (b >> 3, b & 0x7) {
            return Some(read_variant(bytes, &mut i));
        }
    }
    None
}

///#### Read Variant Protobuf
/// In Protobuf encoding, values are encoded in 7-bit chunks, where the most
/// significant bit (MSB) of each chunk is used as a continuation flag.
/// Therefore, to extract the value of each chunk, we need to mask out the MSB
/// using the bitwise AND operator with 0x7f (binary 0111 1111 in two's
/// complement).
fn read_variant(bytes: &[u8], i: &mut usize) -> u64 {
    let mut result = 0;
    let mut shift = 0;
    loop {
        let b = bytes[*i];
        *i += 1;
        result |= (b as u64 & 0x7f) << shift;
        if b & 0x80 == 0 {
            return result;
        }
        shift += 7;
    }
}

fn skip_field(bytes: &[u8], i: &mut usize, wire_type: u8) -> Option<()> {
    match wire_type {
        0 => {
            // Variant
            read_variant(bytes, i);
        }
        1 => {
            // 64-bit
            *i += 8;
        }
        2 => {
            // Length-delimited
            let len = read_variant(bytes, i) as usize;
            *i += len;
        }
        3 => {
            // Start group
            loop {
                let tag = bytes[*i] >> 3;
                let wire_type = bytes[*i] & 0x7;
                *i += 1;
                if let (4, 4) = (tag, wire_type) {
                    break;
                } else {
                    skip_field(bytes, i, wire_type)?;
                }
            }
        }
        4 => {
            // End group
            return None;
        }
        5 => {
            // 32-bit
            *i += 4;
        }
        _ => {
            // Unknown wire type
            return None;
        }
    }
    Some(())
}
