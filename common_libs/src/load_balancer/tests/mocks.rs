#![allow(dead_code)]

use crate::load_balancer::private::Algorithm;
use crate::load_balancer::private::LoadBalancer;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Clone)]
pub struct FakeConnection {
    pub name: String,
    hit: Arc<AtomicU32>,
}

impl Display for FakeConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.name.clone();
        let hit = self.hit.load(Ordering::SeqCst);
        write!(f, "Client: {} -- Hit count: {}", name, hit)
    }
}

impl FakeConnection {
    pub fn new(name: &str) -> Self {
        Self {
            hit: Arc::new(AtomicU32::default()),
            name: name.to_owned(),
        }
    }

    pub fn exec(&self) -> (u32, String) {
        let current = self.hit.load(Ordering::SeqCst);
        let hit_increase = current + 1;
        self.hit.store(hit_increase, Ordering::SeqCst);
        (hit_increase, self.name.clone())
    }
}

#[derive(Default)]
pub struct DummyAlgo {
    keys: Vec<String>,
    idx_rotation: AtomicUsize,
}

impl DummyAlgo {
    pub fn new(keys: Vec<String>) -> Self {
        Self {
            keys,
            idx_rotation: AtomicUsize::default(),
        }
    }
}

impl Algorithm for DummyAlgo {
    fn next(&self) -> String {
        let idx = self.idx_rotation.load(Ordering::SeqCst);
        if idx + 1 == self.keys.len() {
            self.idx_rotation.store(0, Ordering::SeqCst);
            return self.keys[0].to_owned();
        }
        self.idx_rotation.fetch_add(1, Ordering::SeqCst);
        self.keys[idx + 1].to_owned()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Server {
    Nah,
    A,
    B,
    C,
}

pub fn create_dumb_balancer() -> LoadBalancer<Server, DummyAlgo> {
    let items = HashMap::from([
        ("a".to_owned(), Server::A),
        ("b".to_owned(), Server::B),
        ("c".to_owned(), Server::C),
    ]);

    let keys = items.clone().into_keys().collect();
    let algo = DummyAlgo::new(keys);
    LoadBalancer::init(items, algo)
}
