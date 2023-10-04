mod ingestor_metrics;

use common_libs::log;
use common_libs::warp;
use common_libs::warp::Filter;
use common_libs::warp::Rejection;
use common_libs::warp::Reply;

pub use prometheus::default_registry;
pub use prometheus::Registry;

use prometheus::TextEncoder;

pub mod ingestor {
    pub use super::ingestor_metrics::EthClientMetrics;
    pub use super::ingestor_metrics::IngestorMetrics;
}

async fn metrics_handler() -> Result<impl Reply, Rejection> {
    let encoder = TextEncoder::new();
    let mut buffer = String::from("");

    encoder
        .encode_utf8(&prometheus::gather(), &mut buffer)
        .expect("Failed to encode metrics");

    let response = buffer.clone();
    buffer.clear();

    Ok(response)
}

pub async fn run_metric_server(port: u16) {
    log::info!("Start Metrics Server");
    let metrics_route = warp::path!("metrics").and_then(metrics_handler);
    warp::serve(metrics_route).run(([0, 0, 0, 0], port)).await;
}
