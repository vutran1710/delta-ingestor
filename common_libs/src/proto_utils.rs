use prost_build::Config;
use prost_reflect::DescriptorPool;
use std::env::temp_dir;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::PathBuf;
use uuid::Uuid;

pub fn save_temp_proto(proto_content: String) -> Result<PathBuf, String> {
    let dir = temp_dir();
    let temp_proto = dir.join(format!("{}.proto", Uuid::new_v4()));
    fs::write(temp_proto.clone(), proto_content).map_err(|e| e.to_string())?;
    Ok(temp_proto)
}

pub fn compile_temp_descriptor_set_for_protobuf(
    proto_file_path: PathBuf,
) -> Result<PathBuf, String> {
    let dir = temp_dir();
    let temp_descriptor = dir.join(format!("{}.bin", Uuid::new_v4()));
    let mut builder = Config::new();
    builder
        .file_descriptor_set_path(temp_descriptor.clone())
        .compile_protos(&[proto_file_path], &[dir])
        .map_err(|e| e.to_string())?;
    Ok(temp_descriptor)
}

pub fn create_descriptor_pool_from_descriptor_set(
    descriptor_path: Option<PathBuf>,
    descriptor_file: Option<File>,
) -> Result<DescriptorPool, String> {
    let file = match (descriptor_path, descriptor_file) {
        (_, Some(file)) => file,
        (Some(path), None) => File::open(path).unwrap(),
        (None, None) => return Err("Descriptor file is required!".to_string()),
    };
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).map_err(|e| e.to_string())?;
    DescriptorPool::decode(buffer.as_slice())
        .map(Ok)
        .map_err(|e| e.to_string())?
}
