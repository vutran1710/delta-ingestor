use std::collections::HashMap;
use std::io::BufReader;
use std::sync::Arc;

use common_libs::async_trait::async_trait;
use common_libs::delta_utils;
use common_libs::deltalake::arrow::datatypes::Schema as ArrowSchema;
use common_libs::deltalake::arrow::json::ReaderBuilder;
use common_libs::deltalake::operations::create::CreateBuilder;
use common_libs::deltalake::schema::Schema;
use common_libs::deltalake::schema::SchemaDataType;
use common_libs::deltalake::writer::DeltaWriter;
use common_libs::deltalake::writer::RecordBatchWriter;
use common_libs::deltalake::DeltaConfigKey;
use common_libs::deltalake::DeltaTable;
use common_libs::deltalake::DeltaTableBuilder;
use common_libs::deltalake::DeltaTableError;
use common_libs::deltalake::SchemaField;
use common_libs::envy;
use common_libs::log::info;
use common_libs::tokio::sync::Mutex;
use common_libs::utils;

use serde::Deserialize;
use serde::Serialize;
use serde_json::json;

use crate::config::CommandConfig;
use crate::core::ProducerTrait;
use crate::errors::ProducerError;
use crate::proto::BlockTrait;

impl From<DeltaTableError> for ProducerError {
    fn from(value: DeltaTableError) -> Self {
        ProducerError::Publish(format!("Publishing failed: {:?}", value))
    }
}

#[derive(Serialize, Deserialize)]
pub struct DeltaLakeConfig {
    pub table_path: String,
    pub aws_endpoint: String,
    pub aws_region: String,
    pub aws_s3_allow_unsafe_rename: bool,
    pub aws_secret_access_key: String,
    pub aws_access_key_id: String,
    pub aws_allow_http: bool,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct DeltaLakeProducer {
    table: Arc<Mutex<DeltaTable>>,
    writer: Arc<Mutex<RecordBatchWriter>>,
    schema_ref: Arc<ArrowSchema>,
    chain_name: String,
    table_path: String,
}

impl DeltaLakeProducer {
    pub async fn new(cfg: &CommandConfig) -> Result<Self, ProducerError> {
        assert!(
            cfg.block_descriptor.is_some(),
            "Block descriptor is required"
        );
        let block_descriptor_file =
            utils::load_file(cfg.block_descriptor.clone().unwrap().as_str());
        let (chain_name, table_schemas) =
            delta_utils::analyze_proto_descriptor(block_descriptor_file);
        let deltalake_cfg = envy::from_env::<DeltaLakeConfig>().unwrap();

        // NOTE: At this point, table-path always exists! Safe to call unwrap()
        let (table, is_create_new) = Self::open_table(
            &deltalake_cfg.table_path,
            table_schemas,
            DeltaLakeProducer::get_table_config(),
        )
        .await?;

        if !is_create_new {
            info!("Opened existing table");
        } else {
            info!("Created new table");
        }

        let metadata = table
            .get_metadata()
            .map_err(|e| ProducerError::Initialization(format!("{:?}", e)))?;
        let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema.clone())
            .map_err(|e| ProducerError::Initialization(format!("{:?}", e)))?;
        let schema_ref = Arc::new(arrow_schema);

        let writer = RecordBatchWriter::for_table(&table)?;
        let delta_lake_client = Self {
            table: Arc::new(Mutex::new(table)),
            writer: Arc::new(Mutex::new(writer)),
            schema_ref,
            chain_name,
            table_path: deltalake_cfg.table_path,
        };
        Ok(delta_lake_client)
    }

    fn get_table_config() -> HashMap<DeltaConfigKey, Option<String>> {
        let mut table_config = HashMap::new();
        table_config.insert(DeltaConfigKey::AppendOnly, Some("true".to_string()));
        table_config.insert(
            DeltaConfigKey::AutoOptimizeAutoCompact,
            Some("true".to_string()),
        );
        // TODO: we should determine the exact value here
        // table_config.insert(
        //     DeltaConfigKey::LogRetentionDuration,
        //     Some("100".to_string()),
        // );
        // table_config.insert(
        //     DeltaConfigKey::DeletedFileRetentionDuration,
        //     Some("20".to_string()),
        // );
        // table_config.insert(DeltaConfigKey::CheckpointInterval, Some("10".to_string()));
        return table_config;
    }

    pub async fn open_table(
        table_path: &str,
        columns: Vec<SchemaField>,
        table_config: HashMap<DeltaConfigKey, Option<String>>,
    ) -> Result<(DeltaTable, bool), ProducerError> {
        info!("Opening table at: {table_path}");
        let mut table = DeltaTableBuilder::from_uri(table_path)
            .with_allow_http(true)
            .build()
            .unwrap();

        match table.load().await {
            Ok(()) => Ok((table, false)),
            Err(DeltaTableError::NotATable(_)) => {
                let table_config = table_config
                    .into_iter()
                    .map(|(key, val)| (Into::<String>::into(key.as_ref()), val));

                let table = CreateBuilder::default()
                    .with_object_store(table.object_store())
                    .with_columns(columns)
                    .with_column(
                        "created_at",
                        SchemaDataType::primitive("long".to_string()),
                        false,
                        None,
                    )
                    .with_configuration(table_config)
                    .await
                    .unwrap();

                Ok((table, true))
            }
            Err(e) => Err(ProducerError::Initialization(format!(
                "Create/Load table failed: {:?}",
                e
            ))),
        }
    }
}

#[async_trait]
impl<B: BlockTrait> ProducerTrait<B> for DeltaLakeProducer {
    async fn publish_blocks(&self, blocks: Vec<B>) -> Result<(), ProducerError> {
        let content = blocks
            .iter()
            .map(|b| {
                let json_val = serde_json::to_value(&b).unwrap();
                let timestamp = b.get_writer_timestamp();
                let mut current_value = json_val.as_object().unwrap().to_owned();
                current_value.insert("created_at".to_string(), json!(timestamp));
                serde_json::to_string(&current_value).unwrap()
            })
            .collect::<Vec<String>>()
            .join("\n");

        info!("Blocks serialized as json & joined as line-delimited");

        let mut table = self.table.lock().await;
        let mut writer = self.writer.lock().await;

        let buf_reader = BufReader::new(content.as_bytes());
        let reader = ReaderBuilder::new(self.schema_ref.clone()).with_batch_size(blocks.len());
        let mut reader = reader.build(buf_reader).unwrap();
        let batch = reader.next().unwrap().unwrap();
        info!("RecordBatch -> rows = {}", batch.num_rows());
        writer.write(batch).await?;

        info!("Committing data to delta lake");
        let adds = writer.flush_and_commit(&mut table).await?;
        info!("{} adds written", adds);

        Ok(())
    }
}
