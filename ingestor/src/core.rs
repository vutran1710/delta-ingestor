use super::chain_piece::ChainPiece;
use super::chain_piece::MergeStatus;
use super::errors::ClientError;
use super::errors::IngestorError;
use super::errors::ProducerError;
use super::errors::ResumerError;
use crate::errors::ChainPieceError;
use crate::proto::BlockTrait;
use common_libs::async_trait::async_trait;
use common_libs::flume::bounded as channel;
use common_libs::flume::Receiver;
use common_libs::flume::Sender;
use common_libs::futures::future::try_join_all;
use common_libs::log;
use common_libs::tokio::time::sleep;
use common_libs::tokio::time::Duration;
use common_libs::tokio::try_join;
use common_libs::tokio_retry::strategy::jitter;
use common_libs::tokio_retry::strategy::ExponentialBackoff;
use common_libs::tokio_retry::Retry;
use metrics::ingestor::IngestorMetrics;
use metrics::Registry;
use std::sync::Arc;

pub fn get_block_numbers<B: BlockTrait>(blocks: &[B]) -> Vec<u64> {
    blocks.iter().map(|b| b.get_number()).collect()
}

pub fn create_ranges(
    start_block: u64,
    stop_block: u64,
    batch_size: u8,
    limit_number_of_range: u8,
) -> Vec<(u64, u64)> {
    let mut ranges = vec![];
    let batch_size = u64::from(batch_size);

    for i in 0..limit_number_of_range {
        let pad = u64::from(i) * batch_size;
        let start = start_block + pad;
        let expected_stop = start + batch_size;

        if start > stop_block {
            break;
        }

        let mut actual_stop = start;

        while actual_stop < (stop_block + 1).min(expected_stop) {
            actual_stop += 1;
        }

        ranges.push((start, actual_stop));
    }
    ranges
}

#[async_trait]
pub trait ClientTrait<B: BlockTrait>: Send + Sync {
    async fn get_latest_block_number(&self) -> Result<u64, ClientError>;
    async fn get_full_blocks(&self, sorted_block_numbers: Vec<u64>) -> Result<Vec<B>, ClientError>;
    async fn get_light_blocks(&self, sorted_block_numbers: Vec<u64>)
        -> Result<Vec<B>, ClientError>;
    fn set_blocking_round(&self, block_round: u8);
}

#[async_trait]
pub trait ProducerTrait<B: BlockTrait>: Send + Sync {
    async fn publish_blocks(&self, blocks: Vec<B>) -> Result<(), ProducerError>;
}

#[async_trait]
pub trait ResumerTrait<B: BlockTrait>: Send + Sync {
    async fn get_latest_blocks(&self) -> Result<Vec<B>, ResumerError>;
    async fn save_latest_blocks(&self, blocks: &[B]) -> Result<(), ResumerError>;
}

pub struct RemoteConfigValue {
    pub task_limit: u8,
    pub batch_size: u8,
    pub load_balancer_blocking: u8,
}

#[async_trait]
pub trait ConfigTrait: Send + Sync {
    // NOTE: not-adjustable at runtime
    fn get_start_block(&self) -> u64;
    fn get_stop_block(&self) -> Option<u64>;
    fn get_channel_size(&self) -> u16;
    fn get_reorg_threshold(&self) -> u16;
    fn get_block_time(&self) -> u16;
    // NOTE: adjustable
    async fn get_remote_config(&self) -> RemoteConfigValue;
}

#[async_trait]
pub trait IngestorTrait<B: BlockTrait>: Send + Sync {
    fn get_resumer(&self) -> Arc<dyn ResumerTrait<B>>;
    fn get_config(&self) -> Arc<dyn ConfigTrait>;
    fn get_client(&self) -> Arc<dyn ClientTrait<B>>;
    fn get_producer(&self) -> Arc<dyn ProducerTrait<B>>;
    fn get_metrics(&self) -> IngestorMetrics;

    fn get_start_block(&self, last_saved_block: Option<u64>) -> u64 {
        let config = self.get_config();
        let cfg_start_block = config.get_start_block();

        match last_saved_block {
            Some(block) => block + 1,
            None => cfg_start_block,
        }
    }

    async fn task_download_blocks_in_range(
        &self,
        range: (u64, u64),
    ) -> Result<Vec<B>, ClientError> {
        assert!(range.0 < range.1, "Bad block range: {range:?}");
        let metrics = self.get_metrics();
        let client = self.get_client();

        let timer = metrics
            .blocks_download_duration
            .with_label_values(&["single-task"])
            .start_timer();
        let blocks = client.get_full_blocks((range.0..range.1).collect()).await?;
        timer.stop_and_record();

        assert!(!blocks.is_empty());

        let count = blocks.len();
        let first = blocks.first().unwrap().get_number();
        let last = blocks.last().unwrap().get_number();
        metrics.downloaded_blocks_counter.inc_by(count as u64);
        log::info!("fetched {count} blocks: ({first} - {last})",);

        Ok(blocks)
    }

    async fn validate_blocks(
        &self,
        batch: u8,
        chain: &mut ChainPiece<B>,
        blocks: Vec<B>,
    ) -> Result<(Option<u64>, Vec<B>), IngestorError> {
        let client = self.get_client();
        match chain.merge(&blocks) {
            Ok(MergeStatus::Ok(head)) => {
                log::info!("Merge OK (head={head}), continue!");
                Ok((Some(head), blocks))
            }
            Ok(MergeStatus::Reorg(reorg_block)) => {
                let last_downloaded = reorg_block - 1;
                log::info!("Reorg found, reorg_block={reorg_block}");
                log::info!("Set last_downloaded={last_downloaded}");
                Ok((Some(last_downloaded), vec![]))
            }
            Ok(MergeStatus::Nothing) => {
                panic!("No blocks to process!")
            }
            Err(ChainPieceError::StepBack(next_head)) => {
                let (from, to) = (next_head - (batch as u64), next_head);
                log::warn!("Tracing chain divergen, blocks ({to}->{from})");
                let trace_blocks = client.get_light_blocks((from..=to).collect()).await?;
                self.validate_blocks(batch, chain, trace_blocks).await
            }
            Err(ChainPieceError::Interrupted(head)) => {
                log::error!("blocks disconnected, retrying last-downloaded={:?}", head);
                Ok((head, vec![]))
            }
            Err(e) => panic!("Cannot hande this: {:?}", e),
        }
    }

    async fn task_download_blocks(&self, sender: Sender<Vec<B>>) -> Result<(), IngestorError> {
        let metrics = self.get_metrics();
        let resumer = self.get_resumer();
        let config = self.get_config();
        let client = self.get_client();

        let block_time = config.get_block_time();
        let cfg_start_block = config.get_start_block();
        let reorg_threshold = config.get_reorg_threshold();
        log::info!("REORG_THRESHOLD={reorg_threshold}");

        let latest_blocks = resumer.get_latest_blocks().await?;
        log::info!("latest_blocks: {:?}", get_block_numbers(&latest_blocks));
        let mut chain = ChainPiece::new(reorg_threshold);
        chain.merge(&latest_blocks).unwrap();

        let stop_block = config.get_stop_block();
        let mut last_downloaded_block = latest_blocks.last().map(|b| b.get_number());

        if let Some(block_number) = last_downloaded_block {
            metrics.last_published_block.set(block_number as i64);
        }

        if let Some(target_block) = stop_block {
            metrics.ingestor_target_block.inc_by(target_block);
        }

        if let Some(block) = last_downloaded_block {
            log::info!("Resume from last-downloaded-block={block}");
            assert!(cfg_start_block <= block, "config' start_block > block");
            metrics.last_downloaded_block.set(block as i64);
        }

        let mut chain_latest_block_number = client.get_latest_block_number().await?;
        log::info!("Chain's latest block = {chain_latest_block_number}");
        metrics
            .chain_latest_block
            .set(chain_latest_block_number as i64);

        let start_block_at_run = self.get_start_block(last_downloaded_block);
        metrics.ingestor_start_block.inc_by(start_block_at_run);

        loop {
            let actual_start_block = self.get_start_block(last_downloaded_block);
            let actual_stop_block = stop_block
                .unwrap_or(chain_latest_block_number)
                .min(chain_latest_block_number);
            log::info!("New download round: start-block={actual_start_block} stop-block={actual_stop_block}");

            if actual_start_block > actual_stop_block {
                if stop_block.is_none() || (stop_block.unwrap() > chain_latest_block_number) {
                    log::info!("Stop download to wait({block_time} secs) for chain update!");
                    sleep(Duration::from_secs(block_time as u64)).await;
                    chain_latest_block_number = client.get_latest_block_number().await?;
                    metrics
                        .chain_latest_block
                        .set(chain_latest_block_number as i64);
                    log::info!("Updated chain's latest block = {chain_latest_block_number}");
                    continue;
                }
                log::info!("Nothing more to download");
                break;
            }

            let RemoteConfigValue {
                task_limit,
                batch_size,
                load_balancer_blocking,
            } = config.get_remote_config().await;
            log::info!(
                "Download config: batch_size={}, task_limit={}, blocking_round={}",
                batch_size,
                task_limit,
                load_balancer_blocking,
            );
            metrics
                .clone()
                .config_gauge
                .with_label_values(&["batch_size"])
                .set(batch_size as i64);
            metrics
                .clone()
                .config_gauge
                .with_label_values(&["task_limit"])
                .set(task_limit as i64);
            metrics
                .clone()
                .config_gauge
                .with_label_values(&["blocking_round"])
                .set(load_balancer_blocking as i64);
            client.set_blocking_round(load_balancer_blocking);

            let ranges = create_ranges(
                actual_start_block,
                actual_stop_block,
                batch_size,
                task_limit,
            );

            log::info!("ranges to download: {:?}", ranges);
            let download_tasks = ranges
                .into_iter()
                .map(|range| self.task_download_blocks_in_range(range));

            let timer = metrics
                .clone()
                .blocks_download_duration
                .with_label_values(&["group-task"])
                .start_timer();
            let downloaded_blocks = try_join_all(download_tasks)
                .await?
                .into_iter()
                .flatten()
                .collect::<Vec<B>>();
            timer.stop_and_record();

            let (block_number, validated_blocks) = self
                .validate_blocks(batch_size, &mut chain, downloaded_blocks)
                .await?;

            last_downloaded_block = block_number;

            if !validated_blocks.is_empty() {
                sender
                    .send_async(validated_blocks)
                    .await
                    .expect("channel failing");
            }

            if let Some(block_number) = last_downloaded_block {
                metrics.last_downloaded_block.set(block_number as i64);
            }
        }

        log::info!("---------------------------- DOWNLOAD FINISHED ----------------------------");

        Ok(())
    }

    async fn task_publish_blocks(&self, receiver: Receiver<Vec<B>>) -> Result<(), IngestorError> {
        let metrics = self.get_metrics();
        let producer = self.get_producer();
        let resumer = self.get_resumer();
        let config = self.get_config();
        let mut counter = 0;
        let retry_strategy = ExponentialBackoff::from_millis(10).map(jitter);

        let latest_blocks = resumer.get_latest_blocks().await?;
        let mut chain = ChainPiece::new(config.get_reorg_threshold());
        chain.merge(&latest_blocks).unwrap();

        while let Ok(blocks) = receiver.recv_async().await {
            let batch_size = blocks.len() as u64;
            let first = blocks[0].get_number();
            let last = blocks.last().unwrap().get_number();
            counter += batch_size;

            log::info!("Publishing {batch_size} blocks \n {first}->{last}",);
            let timer = metrics.publish_request_duration.start_timer();
            // --- START: observe publishing request
            producer.publish_blocks(blocks.clone()).await?;
            timer.stop_and_record();
            // --- END: observe publishing request
            metrics.published_blocks_counter.inc_by(batch_size);

            if matches!(chain.merge(&blocks.clone()).unwrap(), MergeStatus::Reorg(_)) {
                log::info!("Reorg resolved!");
                chain.merge(&blocks.clone()).unwrap();
            }

            let latest_blocks = chain.extract_blocks();
            let check_point_block_numbers = get_block_numbers(&latest_blocks);

            Retry::spawn(retry_strategy.clone(), || {
                log::info!("Checkpoint saving blocks={:?}", check_point_block_numbers);
                resumer.save_latest_blocks(&latest_blocks)
            })
            .await?;
            metrics
                .last_published_block
                .set(check_point_block_numbers.last().cloned().unwrap() as i64);

            log::info!(
                "published: {batch_size} blocks; range=({first} - {last}); from_start={counter} blocks",
            );
        }

        log::info!("---------------------------- PUBLISH FINISHED ----------------------------");

        Ok(())
    }

    async fn run(&self) -> Result<(), IngestorError> {
        let config = self.get_config();
        log::info!(
            "Start Ingestor with config_start_block={}, stop_block={:?}, reorg_threshold={:?}",
            config.get_start_block(),
            config.get_stop_block(),
            config.get_reorg_threshold(),
        );

        let channel_size = config.get_channel_size() as usize;

        log::info!("Init a mspc bounded-channel of size {channel_size}");
        let (sender, receiver) = channel::<Vec<B>>(channel_size);

        try_join! {
            self.task_download_blocks(sender),
            self.task_publish_blocks(receiver),
        }?;

        Ok(())
    }
}

pub struct Ingestor<B: BlockTrait> {
    client: Arc<dyn ClientTrait<B>>,
    producer: Arc<dyn ProducerTrait<B>>,
    resumer: Arc<dyn ResumerTrait<B>>,
    config: Arc<dyn ConfigTrait>,
    metrics: IngestorMetrics,
}

impl<B: BlockTrait> Ingestor<B> {
    pub(crate) fn new<C, P, R, F>(
        client: Arc<C>,
        producer: Arc<P>,
        resumer: Arc<R>,
        config: Arc<F>,
        metrics_registry: &Registry,
    ) -> Self
    where
        C: ClientTrait<B> + 'static,
        P: ProducerTrait<B> + 'static,
        R: ResumerTrait<B> + 'static,
        F: ConfigTrait + 'static,
    {
        Self {
            client,
            producer,
            resumer,
            config,
            metrics: IngestorMetrics::new(metrics_registry),
        }
    }
}

#[async_trait]
impl<B: BlockTrait> IngestorTrait<B> for Ingestor<B>
where
    B: BlockTrait,
{
    fn get_resumer(&self) -> Arc<dyn ResumerTrait<B>> {
        self.resumer.clone()
    }

    fn get_config(&self) -> Arc<dyn ConfigTrait> {
        self.config.clone()
    }

    fn get_client(&self) -> Arc<dyn ClientTrait<B>> {
        self.client.clone()
    }

    fn get_producer(&self) -> Arc<dyn ProducerTrait<B>> {
        self.producer.clone()
    }

    fn get_metrics(&self) -> IngestorMetrics {
        self.metrics.clone()
    }
}
