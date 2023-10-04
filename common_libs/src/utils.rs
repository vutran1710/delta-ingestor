use relative_path::RelativePath;
use std::env::current_dir;
use std::fs::File;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub fn load_file(path: &str) -> File {
    let dir = current_dir().unwrap();
    let path_rf = RelativePath::new(path).to_path(dir);
    let os_string = path_rf.clone().into_os_string();
    let full_path = os_string.to_str().unwrap();
    File::open(path_rf).unwrap_or_else(|_| panic!("File not found: {}", full_path))
}

pub fn get_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
