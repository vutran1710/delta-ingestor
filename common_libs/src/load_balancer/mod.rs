use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
mod load_balancers;
mod round_robin;
mod tests;

mod blacklist {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct Blacklist {
        inner: Arc<HashMap<String, AtomicU32>>,
    }

    impl Blacklist {
        pub fn new(keys: Vec<String>) -> Self {
            let mut items = HashMap::new();
            keys.into_iter().for_each(|key| {
                items.insert(key, AtomicU32::default());
            });
            let inner = Arc::new(items);
            Self { inner }
        }

        pub fn is_blocked(&self, key: String) -> bool {
            if let Some(counter) = self.inner.get(&key) {
                let current = counter.load(Ordering::SeqCst);
                return current > 0;
            }

            false
        }

        pub fn update(&self, key: String, count: i32) -> Result<(), String> {
            if let Some(counter) = self.inner.get(&key) {
                let abs_count = count.unsigned_abs();
                match count.is_positive() {
                    true => counter.fetch_add(abs_count, Ordering::SeqCst),
                    false => counter.fetch_sub(abs_count, Ordering::SeqCst),
                };
                return Ok(());
            }

            Err(format!("Key {} doesnt exist", key))
        }

        #[cfg(test)]
        pub(crate) fn inspect(&self, key: String) -> Result<u32, String> {
            let val = self.inner.get(&key).ok_or("Bad key".to_string())?;
            Ok(val.load(Ordering::SeqCst))
        }
    }
}

mod private {
    use super::*;
    use blacklist::*;
    pub type Key = String;
    pub type ItemMap<T> = HashMap<Key, T>;
    pub type WeightMap = HashMap<Key, u8>;

    pub trait Algorithm: Send + Sync {
        fn next(&self) -> String;
    }

    #[derive(Debug, Clone)]
    pub struct LoadBalancer<T, A>
    where
        T: Send + Sync,
        A: Algorithm,
    {
        items: ItemMap<T>,
        algo: A,
        blacklist: Blacklist,
    }

    impl<T: Send + Sync, A: Algorithm> LoadBalancer<T, A> {
        pub fn init(items: ItemMap<T>, algo: A) -> Self {
            let keys = items.keys().cloned().collect();
            Self {
                items,
                algo,
                blacklist: Blacklist::new(keys),
            }
        }

        pub fn get(&self) -> &T {
            loop {
                let key = self.algo.next();

                if self.blacklist.is_blocked(key.clone()) {
                    self.blacklist.update(key.clone(), -1).unwrap();
                    continue;
                }

                return self.items.get(&key).unwrap();
            }
        }

        pub fn block(&self, key: String, times: u32) {
            self.blacklist.update(key, times as i32).unwrap();
        }

        #[cfg(test)]
        pub(crate) fn inspect_block_counter(&self, key: String) -> u32 {
            self.blacklist.inspect(key).unwrap()
        }
    }
}

pub use load_balancers::RRBLoadBalancer;
