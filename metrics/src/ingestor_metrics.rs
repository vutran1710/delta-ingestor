use prometheus::Histogram;
use prometheus::HistogramOpts;
use prometheus::HistogramVec;
use prometheus::IntCounter;
use prometheus::IntCounterVec;
use prometheus::IntGauge;
use prometheus::IntGaugeVec;
use prometheus::Opts;
use prometheus::Registry;

#[derive(Clone)]
pub struct IngestorMetrics {
    pub ingestor_start_block: IntCounter,
    pub ingestor_target_block: IntCounter,
    pub chain_latest_block: IntGauge,
    pub last_published_block: IntGauge,
    pub last_downloaded_block: IntGauge,
    pub downloaded_blocks_counter: IntCounter,
    pub published_blocks_counter: IntCounter,
    pub publish_request_duration: Histogram,
    pub blocks_download_duration: HistogramVec,
    pub config_gauge: IntGaugeVec,
}

impl IngestorMetrics {
    pub fn new(registry: &Registry) -> Self {
        let ingestor_start_block =
            IntCounter::new("ingestor_start_block", "start block to ingest").unwrap();
        registry
            .register(Box::new(ingestor_start_block.clone()))
            .unwrap();

        let ingestor_target_block =
            IntCounter::new("ingestor_target_block", "latest block from chain").unwrap();
        registry
            .register(Box::new(ingestor_target_block.clone()))
            .unwrap();

        let last_published_block =
            IntGauge::new("last_published_block", "latest published block").unwrap();
        registry
            .register(Box::new(last_published_block.clone()))
            .unwrap();

        let last_downloaded_block =
            IntGauge::new("last_downloaded_block", "latest downloaded block").unwrap();
        registry
            .register(Box::new(last_downloaded_block.clone()))
            .unwrap();

        let chain_latest_block = IntGauge::new("chain_latest_block", "chain latest block").unwrap();
        registry
            .register(Box::new(chain_latest_block.clone()))
            .unwrap();

        let downloaded_blocks_counter =
            IntCounter::new("downloaded_blocks_counter", "number of blocks downloaded").unwrap();
        registry
            .register(Box::new(downloaded_blocks_counter.clone()))
            .unwrap();

        let published_blocks_counter =
            IntCounter::new("published_blocks_counter", "number of blocks published").unwrap();
        registry
            .register(Box::new(published_blocks_counter.clone()))
            .unwrap();

        let opts = HistogramOpts::new("publish_request_duration", "duration of publish request");
        let publish_request_duration = Histogram::with_opts(opts).unwrap();
        registry
            .register(Box::new(publish_request_duration.clone()))
            .unwrap();

        let opts = HistogramOpts::new("blocks_download_duration", "duration of rpc request");
        let blocks_download_duration = HistogramVec::new(opts, &["task_type"]).unwrap();
        registry
            .register(Box::new(blocks_download_duration.clone()))
            .unwrap();

        let opts = Opts::new("configurations", "dynamic ingestor configurations");
        let config_gauge = IntGaugeVec::new(opts, &["config_name"]).unwrap();
        registry.register(Box::new(config_gauge.clone())).unwrap();

        Self {
            ingestor_start_block,
            ingestor_target_block,
            chain_latest_block,
            last_published_block,
            last_downloaded_block,
            downloaded_blocks_counter,
            published_blocks_counter,
            publish_request_duration,
            blocks_download_duration,
            config_gauge,
        }
    }
}

#[derive(Clone)]
pub struct EthClientMetrics {
    pub request_count: IntCounterVec,
}

impl EthClientMetrics {
    pub fn new(registry: &Registry) -> EthClientMetrics {
        let opts = Opts::new("eth_request_count", "count number of web3 requests");
        let request_count =
            IntCounterVec::new(opts, &["endpoint", "method", "node", "status"]).unwrap();
        registry.register(Box::new(request_count.clone())).unwrap();

        Self { request_count }
    }
}
