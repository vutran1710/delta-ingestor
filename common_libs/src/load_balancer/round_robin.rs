use super::private::Algorithm;
use super::private::WeightMap;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

/// Round Robin algorithm
/// - support `weighted` mode
/// - include `classic-wrr` & `interleave-wrr`
/// - Weight=Zero means that items will be ignored from the selection
/// - assume all inputs are valid, panic otherwise, does not support resilency
type SafeQueue = Mutex<ArrayQueue<u8>>;
type SafeQueueLocked<'a> = MutexGuard<'a, ArrayQueue<u8>>;

#[derive(Debug, Clone)]
pub struct RoundRobin {
    interleave: bool,
    queue: Arc<SafeQueue>,
    items: Vec<(String, u8)>,
}

impl RoundRobin {
    pub fn new(items: WeightMap, interleave: bool) -> Self {
        let queue_size = items.values().map(|weight| weight.to_owned()).sum::<u8>();
        assert!(queue_size > 0);
        let queue = ArrayQueue::new(queue_size as usize);
        let queue = Arc::new(Mutex::new(queue));
        let mut items = items
            .into_iter()
            .map(|(key, weight)| (key, weight))
            .collect::<Vec<(String, u8)>>();

        items.sort_by(|a, b| a.1.cmp(&b.1));

        Self {
            queue,
            items,
            interleave,
        }
    }

    fn create_weight_counters(&self) -> Vec<u8> {
        self.items
            .iter()
            .map(|(_, weight)| *weight)
            .collect::<Vec<u8>>()
    }

    fn fill_interleave_queue(&self, queue: &SafeQueueLocked) {
        let mut counters = self.create_weight_counters();

        while !queue.is_full() {
            for (idx, counter) in counters.iter_mut().enumerate() {
                if *counter > 0 {
                    queue.push(idx as u8).unwrap();
                    *counter -= 1;
                }
            }
        }
    }

    fn fill_classical_queue(&self, queue: &SafeQueueLocked) {
        let mut counters = self.create_weight_counters();

        while !queue.is_full() {
            for (idx, counter) in counters.iter_mut().enumerate() {
                while *counter > 0 {
                    queue.push(idx as u8).unwrap();
                    *counter -= 1;
                }
            }
        }
    }

    fn fill_queue(&self, queue: &SafeQueueLocked) {
        if !queue.is_empty() {
            return;
        }

        if self.interleave {
            self.fill_interleave_queue(queue);
        } else {
            self.fill_classical_queue(queue);
        }
    }
}

impl Algorithm for RoundRobin {
    fn next(&self) -> String {
        if self.items.len() == 1 {
            return self.items[0].0.to_owned();
        }

        let queue = self.queue.lock().unwrap();
        self.fill_queue(&queue);
        let idx = queue.pop().unwrap();
        self.items[idx as usize].0.to_owned()
    }
}
