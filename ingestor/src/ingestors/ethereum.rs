use crate::clients::EthereumClient;
use crate::clients::NodeType;
use crate::clients::RREthClient;
use crate::config::CommandConfig;
use crate::config::Config;
use crate::config::ConfigTOML;
use crate::config::Ethereum;
use crate::core::Ingestor;
use crate::errors::ClientError;
use crate::errors::IngestorError;
use crate::name_svc::NameService;
use crate::producers::create_producer;
use crate::proto::ethereum::Block;
use crate::proto::BlockChain;
use crate::resumers::create_resumer;
use common_libs::futures_util::future::try_join_all;
use common_libs::log::info;
use common_libs::log::warn;
use common_libs::tokio::time::timeout;
use common_libs::tokio::time::Duration;
use metrics::Registry;
use std::collections::HashSet;
use std::sync::Arc;

pub(crate) async fn check_rpc_for_data_integrity(endpoints: Vec<String>) -> HashSet<String> {
    let mut result = HashSet::new();

    'loop_check_endpoint: for endpoint in endpoints {
        info!("Validating RPC: {endpoint}");
        let client = EthereumClient::new(endpoint.clone(), NodeType::Archive).unwrap();
        let arc_client = Arc::new(client);
        let arc_client2 = arc_client.clone();
        let arc_client3 = arc_client.clone();

        let range1 = (10_000_000..10_000_003).collect::<Vec<u64>>();
        let range2 = (10_000_003..10_000_006).collect::<Vec<u64>>();
        let range3 = (10_000_006..10_000_010).collect::<Vec<u64>>();
        let requests = vec![
            arc_client.get_logs(&range1),
            arc_client2.get_logs(&range2),
            arc_client3.get_logs(&range3),
        ];
        let requests = try_join_all(requests).await;

        if requests.is_err() {
            continue;
        }

        let requests = requests.unwrap();

        for logs in requests {
            let tx_hashes = logs.keys().collect::<Vec<_>>();
            for tx_hash in tx_hashes {
                let log_count = logs
                    .get(tx_hash)
                    .map(|vec_logs| vec_logs.len())
                    .unwrap_or(0);

                let inner_check = match format!("{:?}", tx_hash).as_str() {
                    "0x4b4b366c4e76221293449ad9480cd8e60b42d950bfecbc4c576f11e69bb9ac08" => {
                        log_count == 25
                    }
                    "0xa2bb7181b3e745b2462b409eba0ec6c13ae0b8f344bddf36c917884321456676" => {
                        log_count == 10
                    }
                    "0x90344c3d7e1e047d61d5818e6dfc8111d90d67857858311b824149fdda9de80f" => {
                        log_count == 9
                    }
                    _ => true,
                };
                if !inner_check {
                    warn!("IGNORE THIS ENDPOINT: {endpoint}");
                    continue 'loop_check_endpoint;
                }
            }
        }

        info!("-----> {endpoint} is OK!");
        result.insert(endpoint.clone());
    }

    result
}

pub(crate) async fn create_client(
    cfg: &CommandConfig,
    registry: &Registry,
    ethereum_config: Ethereum,
) -> Result<RREthClient, ClientError> {
    let mut usable_clients = vec![];
    let mut chain_id_check = HashSet::new();
    let mut chain_id: u64 = 0;

    // WARN: THIS ONLY WORKS WITH ETHEREUM!!
    let endpoints = ethereum_config
        .rpc
        .iter()
        .map(|rpc| rpc.endpoint.clone())
        .collect::<Vec<_>>();
    let check_endpoints = check_rpc_for_data_integrity(endpoints).await;
    info!("Valid RPC:{:?}", check_endpoints);
    // ---------------------------- DO NOT USE WITH OTHER RPC ---------------------

    for rpc in ethereum_config.rpc {
        if !check_endpoints.contains(&rpc.endpoint) {
            continue;
        }

        info!(
            "Testing RPC: {}, timeout={} batch={}",
            rpc.endpoint, cfg.request_timeout, cfg.batch
        );
        if let Ok((rpc_chain_id, clients)) = rpc_check(rpc.endpoint.clone(), cfg).await {
            chain_id_check.insert(rpc_chain_id);

            if chain_id_check.len() > 1 {
                let msg = format!(
                    "RPC does not return the same chain-id: rpc={}, chain_id={}",
                    rpc.endpoint, rpc_chain_id,
                );
                return Err(ClientError::BadConfig(msg));
            }

            chain_id = rpc_chain_id;
            let node_types = clients
                .iter()
                .map(|c| c.node_type.clone())
                .collect::<Vec<NodeType>>();
            info!(
                "- chain_id: #{}, supported node types: {:?}",
                rpc_chain_id, node_types
            );
            usable_clients.extend(clients);
        }
    }

    if usable_clients.is_empty() {
        return Err(ClientError::ServerUnavailable(String::from(
            "No usable clients",
        )));
    }

    info!(
        "Init RoundRobinClient with {} client(s)",
        usable_clients.len(),
    );

    let client = RREthClient::new(
        usable_clients,
        registry,
        cfg.request_timeout,
        cfg.lb_blocking,
        chain_id,
    );
    Ok(client)
}

pub async fn rpc_check(
    endpoint: String,
    cfg: &CommandConfig,
) -> Result<(u64, Vec<EthereumClient>), ClientError> {
    let mut clients = vec![];
    let test_client = EthereumClient::new(endpoint.clone(), NodeType::Light)?;
    let block_range =
        (cfg.start_block..cfg.start_block + u64::from(cfg.batch)).collect::<Vec<u64>>();
    let time_limit = Duration::from_secs(u64::from(cfg.request_timeout));

    let chain_id = timeout(time_limit, test_client.get_chain_id()).await??;
    let test_get_blocks = timeout(time_limit, test_client.get_blocks(&block_range));
    let test_get_logs = timeout(time_limit, test_client.get_logs(&block_range));

    if let Ok(Ok(_)) = test_get_blocks.await {
        if let Ok(client) = EthereumClient::new(endpoint.clone(), NodeType::Light) {
            clients.push(client)
        }
    }

    if let Ok(Ok(_)) = test_get_logs.await {
        if let Ok(client) = EthereumClient::new(endpoint.clone(), NodeType::Archive) {
            clients.push(client)
        }
    }

    if clients.is_empty() {
        return Err(ClientError::ServerUnavailable("Non-usable".to_string()));
    }

    Ok((chain_id, clients))
}

pub async fn create_ingestor(
    cfg: &CommandConfig,
    registry: &Registry,
) -> Result<Ingestor<Block>, IngestorError> {
    let ethereum_config = ConfigTOML::from_file(&cfg.config_file).ethereum.unwrap();
    let client = Arc::new(create_client(cfg, registry, ethereum_config).await?);
    let chain_type = BlockChain::Ethereum;
    let name_service = NameService::from((cfg, chain_type));
    let producer = Arc::new(create_producer(cfg, &name_service).await?);
    let resumer = Arc::new(create_resumer(cfg, &name_service).await?);
    let config = Arc::new(Config::new(cfg, name_service.config_key.clone()).await?);
    let ingestor = Ingestor::new(client, producer, resumer, config, registry);
    Ok(ingestor)
}
