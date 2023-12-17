use common_libs::deltalake::parquet::file::properties::WriterProperties;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_libs::async_trait::async_trait;
use common_libs::deltalake::arrow::array::*;
use common_libs::deltalake::arrow::datatypes::Schema as ArrowSchema;
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
    block_partition: u32,
}

impl DeltaLakeProducer {
    pub async fn new(cfg: &CommandConfig) -> Result<Self, ProducerError> {
        let deltalake_cfg = envy::from_env::<DeltaLakeConfig>().unwrap();

        // NOTE: At this point, table-path always exists! Safe to call unwrap()
        let (table, is_create_new) = Self::open_table(
            &deltalake_cfg.table_path,
            vec![
                SchemaField::new(
                    "block_number".to_string(),
                    SchemaDataType::primitive("long".to_string()),
                    false,
                    HashMap::default(),
                ),
                SchemaField::new(
                    "block_partition".to_string(),
                    SchemaDataType::primitive("long".to_string()),
                    false,
                    HashMap::default(),
                ),
                SchemaField::new(
                    "hash".to_string(),
                    SchemaDataType::primitive("string".to_string()),
                    false,
                    HashMap::default(),
                ),
                SchemaField::new(
                    "parent_hash".to_string(),
                    SchemaDataType::primitive("string".to_string()),
                    false,
                    HashMap::default(),
                ),
                SchemaField::new(
                    "block_data".to_string(),
                    SchemaDataType::primitive("binary".to_string()),
                    false,
                    HashMap::default(),
                ),
                SchemaField::new(
                    "created_at".to_string(),
                    SchemaDataType::primitive("long".to_string()),
                    false,
                    HashMap::default(),
                ),
            ],
            DeltaLakeProducer::get_table_config(),
        )
        .await?;

        if !is_create_new {
            info!("Opened existing table");
        } else {
            info!("Created new table");
        }

        info!("block-parition = {}", cfg.block_partition);

        let metadata = table
            .get_metadata()
            .map_err(|e| ProducerError::Initialization(format!("{:?}", e)))?;
        let arrow_schema = <ArrowSchema as TryFrom<&Schema>>::try_from(&metadata.schema.clone())
            .map_err(|e| ProducerError::Initialization(format!("{:?}", e)))?;

        let schema_ref = Arc::new(arrow_schema);

        let writer = RecordBatchWriter::for_table(&table)?.with_writer_properties(
            WriterProperties::builder()
                .set_compression(common_libs::deltalake::parquet::basic::Compression::LZ4)
                .build(),
        );
        let delta_lake_client = Self {
            table: Arc::new(Mutex::new(table)),
            writer: Arc::new(Mutex::new(writer)),
            schema_ref,
            chain_name: cfg.chain.to_string(),
            table_path: deltalake_cfg.table_path,
            block_partition: cfg.block_partition,
        };
        Ok(delta_lake_client)
    }

    fn get_table_config() -> HashMap<DeltaConfigKey, Option<String>> {
        let mut table_config = HashMap::new();
        table_config.insert(DeltaConfigKey::AppendOnly, Some("true".to_string()));
        table_config.insert(
            DeltaConfigKey::AutoOptimizeAutoCompact,
            Some("auto".to_string()),
        );
        table_config.insert(
            DeltaConfigKey::AutoOptimizeOptimizeWrite,
            Some("true".to_string()),
        );
        table_config.insert(
            DeltaConfigKey::DataSkippingNumIndexedCols,
            Some("2".to_string()),
        );
        table_config.insert(
            DeltaConfigKey::LogRetentionDuration,
            Some("interval 7 days".to_string()),
        );
        table_config.insert(
            DeltaConfigKey::DeletedFileRetentionDuration,
            Some("interval 2 days".to_string()),
        );
        table_config.insert(
            DeltaConfigKey::CheckpointInterval,
            Some("interval 1 hour".to_string()),
        );
        return table_config;
    }

    pub async fn open_table(
        table_path: &str,
        columns: Vec<SchemaField>,
        table_config: HashMap<DeltaConfigKey, Option<String>>,
    ) -> Result<(DeltaTable, bool), ProducerError> {
        info!(
            "Opening table at: {table_path}, config={} keys",
            table_config.len()
        );
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
                    .with_configuration(table_config)
                    .with_partition_columns(vec!["block_partition".to_string()])
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
        let mut block_numbers = vec![];
        let mut block_partitions = vec![];
        let mut block_hashes = vec![];
        let mut block_parent_hashes = vec![];
        let mut block_data = vec![];
        let mut created_ats = vec![];

        let mut partition_set = HashSet::new();

        for block in blocks {
            block_numbers.push(block.get_number() as i64);
            let partition_number = block.get_number() / self.block_partition as u64;
            partition_set.insert(partition_number);
            block_partitions.push(partition_number as i64);
            block_hashes.push(block.get_hash());
            block_parent_hashes.push(block.get_parent_hash());
            let block_bytes = block.encode_to_vec();
            block_data.push(block_bytes);
            created_ats.push(block.get_writer_timestamp() as i64);
        }

        info!("block batch partition set = {:?}", partition_set);

        let arrow_array: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int64Array::from(block_numbers)),
            Arc::new(Int64Array::from(block_partitions)),
            Arc::new(StringArray::from(block_hashes)),
            Arc::new(StringArray::from(block_parent_hashes)),
            Arc::new(BinaryArray::from_iter_values(block_data)),
            Arc::new(Int64Array::from(created_ats)),
        ];

        let batch = RecordBatch::try_new(self.schema_ref.clone(), arrow_array).unwrap();
        let mut table = self.table.lock().await;
        let mut writer = self.writer.lock().await;

        info!("RecordBatch -> rows = {}", batch.num_rows());
        writer.write(batch).await?;

        info!("Committing data to delta lake");
        let adds = writer.flush_and_commit(&mut table).await?;
        info!("{} adds written", adds);

        Ok(())
    }
}
