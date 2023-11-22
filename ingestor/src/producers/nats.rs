use std::fs::File;
use std::io::Write;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use prost::Message;
use common_libs::async_trait::async_trait;
use common_libs::log;
use crate::core::ProducerTrait;
use crate::errors::ProducerError;
use crate::proto::BlockTrait;
use crate::proto::ethereum::Blocks;

#[derive(Clone)]
enum PublishMode {
    Bytes,
    String,
}

pub struct NatsProducer {
    conn: nats::Connection,
    subject: String,
    mode: PublishMode
}

impl NatsProducer {
    pub fn new() -> Result<Self, ProducerError> {
        let uri = std::env::var("NATS_URI").unwrap_or("nats://localhost:4222".to_string());
        let subject = std::env::var("NATS_NATS_SUBJECT").unwrap();
        let mode_env = std::env::var("NATS_MODE").unwrap_or("string".to_string());
       let mode = match mode_env.as_str() {
            "string" => PublishMode::String,
            "bytes" => PublishMode::Bytes,
           _ => unimplemented!("Unknown NATS_MODE: {}", mode_env)
        };
        let conn = nats::connect(uri).map_err(|e| {
            ProducerError::Initialization(format!("Failed to connect to Nats: {}", e))
        })?;
        Ok(NatsProducer {
            conn,
            subject,
            mode
        })
    }

    pub fn publish(&self, msg: &[u8]) -> Result<(), ProducerError> {
        self.conn.publish(&self.subject, msg).map_err(
            |e| ProducerError::Publish(format!("Failed to publish message: {}", e)),
        )?;
        Ok(())
    }
}

#[async_trait]
impl<B: BlockTrait> ProducerTrait<B> for NatsProducer {
    async fn publish_blocks(&self, blocks: Vec<B>) -> Result<(), ProducerError> {
        match self.mode {
            PublishMode::Bytes => {
                let all_blocks = blocks.into_iter().map(|b| b.encode_to_vec()).collect::<Vec<Vec<u8>>>();
                let full_block = Blocks{
                    ethereum_blocks: all_blocks
                };
                self.publish(full_block.encode_to_vec().as_slice())?;
            }
            PublishMode::String => {
                for block in blocks {
                    let block_json = serde_json::to_string(&block).unwrap();
                    self.publish(&block_json.as_bytes())?;

                }
            }
        }

        Ok(())
    }
}