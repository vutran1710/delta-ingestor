use super::private::*;
use super::round_robin::RoundRobin;

pub type RRBLoadBalancer<T> = LoadBalancer<T, RoundRobin>;

impl<T: Send + Sync + Clone> RRBLoadBalancer<T> {
    pub fn new(clients: Vec<(Key, T, u8)>, interleave: bool) -> Self {
        let keys = clients.iter().map(|(k, ..)| k.to_owned());
        let weights_iter = clients.iter().map(|(.., w)| w.to_owned());
        let clients_iter = clients.iter().map(|(_, c, _)| c.clone());

        let items = keys.clone().zip(clients_iter).collect();
        let weights = keys.clone().zip(weights_iter).collect();

        let algo = RoundRobin::new(weights, interleave);
        LoadBalancer::init(items, algo)
    }
}
