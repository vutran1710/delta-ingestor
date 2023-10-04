use crate::proto_utils::*;
use log::info;
use std::env::set_var;

#[test]
fn test_compile_proto() {
    env_logger::try_init().unwrap_or_default();
    set_var("OUT_DIR", "/tmp");
    let proto = r#"
syntax = "proto3";
package tutorial;

message Person {
  string name = 1;
  int32 id = 2;  // Unique ID number for this person.
  string email = 3;

  enum PhoneType {
    MOBILE = 0;
    HOME = 1;
    WORK = 2;
  }

  message PhoneNumber {
    string number = 1;
    PhoneType type = 2;
  }

  repeated PhoneNumber phones = 4;
}
"#;
    let temp_proto = save_temp_proto(proto.to_string()).unwrap();
    info!("Temp proto: {:?}", temp_proto);

    let descriptor_path = compile_temp_descriptor_set_for_protobuf(temp_proto).unwrap();
    info!("Descriptor: {:?}", descriptor_path);

    let descriptor_pool =
        create_descriptor_pool_from_descriptor_set(Some(descriptor_path), None).unwrap();
    info!("Descriptor: {:?}", descriptor_pool);

    for message in descriptor_pool.all_messages() {
        let fields = message.fields().collect::<Vec<_>>();
        let name = message.name();
        info!("name={:?}, Fields: {:?}", name, fields);

        if name == "Person" {
            assert_eq!(fields.len(), 4);
        }

        if name == "PhoneNumber" {
            assert_eq!(fields.len(), 2);
        }
    }
}
