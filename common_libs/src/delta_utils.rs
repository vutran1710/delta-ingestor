use super::proto_utils::create_descriptor_pool_from_descriptor_set;

use deltalake::schema::SchemaDataType;
use deltalake::schema::SchemaField;
use deltalake::SchemaTypeArray;
use deltalake::SchemaTypeStruct;
use prost_reflect::FieldDescriptor;
use prost_reflect::Kind;
use prost_reflect::MessageDescriptor;
use std::collections::HashMap;
use std::fs::File;

fn field_type_to_schema_type(kind: Kind) -> SchemaDataType {
    match kind {
        Kind::Bool => SchemaDataType::primitive("boolean".to_string()),
        Kind::Float => SchemaDataType::primitive("float".to_string()),
        Kind::String => SchemaDataType::primitive("string".to_string()),
        Kind::Bytes => SchemaDataType::primitive("binary".to_string()),
        Kind::Double => SchemaDataType::primitive("double".to_string()),
        Kind::Int32 => SchemaDataType::primitive("integer".to_string()),
        Kind::Int64 => SchemaDataType::primitive("long".to_string()),
        Kind::Uint32 => SchemaDataType::primitive("integer".to_string()),
        Kind::Uint64 => SchemaDataType::primitive("long".to_string()),
        Kind::Fixed32 => SchemaDataType::primitive("decimal".to_string()),
        Kind::Sfixed32 => SchemaDataType::primitive("decimal".to_string()),
        Kind::Fixed64 => SchemaDataType::primitive("decimal".to_string()),
        Kind::Sfixed64 => SchemaDataType::primitive("decimal".to_string()),
        Kind::Sint32 => SchemaDataType::primitive("integer".to_string()),
        Kind::Sint64 => SchemaDataType::primitive("long".to_string()),
        Kind::Message(inner_msg) => {
            let schema_struct = SchemaTypeStruct::new(proto_descriptor_to_delta_schema(inner_msg));
            SchemaDataType::r#struct(schema_struct)
        }
        Kind::Enum(_) => SchemaDataType::primitive("integer".to_string()),
    }
}

fn message_field_to_schema_field(field: FieldDescriptor) -> SchemaField {
    let field_name = field.name().to_owned();
    let field_type = if field.is_list() {
        let schema_array =
            SchemaTypeArray::new(Box::new(field_type_to_schema_type(field.kind())), false);
        SchemaDataType::array(schema_array)
    } else {
        field_type_to_schema_type(field.kind())
    };
    SchemaField::new(
        field_name,
        field_type,
        field.containing_oneof().is_some(),
        HashMap::new(),
    )
}

pub fn proto_descriptor_to_delta_schema(msg: MessageDescriptor) -> Vec<SchemaField> {
    msg.fields().map(message_field_to_schema_field).collect()
}

pub fn analyze_proto_descriptor(descriptor_file: File) -> (String, Vec<SchemaField>) {
    let descriptor_pool = create_descriptor_pool_from_descriptor_set(None, Some(descriptor_file))
        .expect("Failed to create descriptor pool! Descriptor file probably malformed");
    let file_descriptor = descriptor_pool.files().next().expect("No file descriptor");
    let package = file_descriptor.package_name().to_owned();
    let block_message_field = descriptor_pool
        .get_message_by_name(&format!("{package}.Block"))
        .expect("No Block message");
    (
        package,
        proto_descriptor_to_delta_schema(block_message_field),
    )
}
