mod mocks;

#[cfg(test)]
mod round_robin;

#[cfg(test)]
mod core {
    use std::sync::atomic::AtomicU8;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use super::mocks::*;
    use crate::env_logger;
    use crate::futures_util::future::join_all;
    use crate::log;
    use crate::tokio;
    use crate::tokio::time;

    #[test]
    fn test_dumb_balancer() {
        env_logger::try_init().unwrap_or_default();
        let lb = create_dumb_balancer();
        let mut prev_value = Server::Nah;

        for _ in 0..5 {
            let server = lb.get();
            log::info!("Got: Server: {:?}", server);
            assert_ne!(format!("{:?}", server), format!("{:?}", prev_value));
            prev_value = server.clone();
            if server == &Server::Nah {
                panic!()
            }
        }
    }

    #[test]
    fn test_balancer_with_blacklist() {
        env_logger::try_init().unwrap_or_default();
        let lb = create_dumb_balancer();
        let key = "a".to_string();

        let collect_servers = |round: u8| {
            (0..round)
                .map(|_| lb.get().clone())
                .collect::<Vec<Server>>()
        };

        for nth in 0..50 {
            let initial_servers_before_test = collect_servers(3);
            log::debug!("Blacklist blocking test, round #{nth}");

            let expected = match initial_servers_before_test[..] {
                // NOTE: because the order of the servers is not deterministic,
                // we have to manually include all the possible permutations
                [Server::A, Server::B, Server::C] => [6, 11],
                [Server::A, Server::C, Server::B] => [6, 11],
                [Server::C, Server::A, Server::B] => [6, 12],
                [Server::C, Server::B, Server::A] => [7, 13],
                [Server::B, Server::C, Server::A] => [7, 13],
                [Server::B, Server::A, Server::C] => [6, 12],
                _ => panic!(),
            };

            lb.block(key.clone(), 12);
            collect_servers(12);
            assert_eq!(lb.inspect_block_counter(key.clone()), expected[0]);

            let mut get_til_unblock = 0;
            while lb.inspect_block_counter(key.clone()) > 0 {
                lb.get();
                get_til_unblock += 1;
            }

            assert_eq!(get_til_unblock, expected[1]);
            assert_eq!(lb.inspect_block_counter(key.clone()), 0);
        }
    }

    #[tokio::test]
    async fn test_blacklist_multi_threaded() {
        env_logger::try_init().unwrap_or_default();
        let lb = create_dumb_balancer();
        let key = "a".to_string();

        let arc_lb = Arc::new(lb);

        let counter = Arc::new(AtomicU8::default());

        let initial_servers_before_test = arc_lb.get();

        for _ in 0..5 {
            assert_eq!(arc_lb.inspect_block_counter(key.clone()), 0);
            arc_lb.block(key.clone(), 3);
            let counter1 = counter.clone();
            let lb1 = arc_lb.clone();
            let task1 = tokio::spawn(async move {
                for _ in 0..10 {
                    if lb1.get() == &Server::A {
                        counter1.fetch_add(1, Ordering::SeqCst);
                    }
                    time::sleep(time::Duration::from_millis(2)).await;
                }
            });

            let counter2 = counter.clone();
            let lb2 = arc_lb.clone();
            let task2 = tokio::spawn(async move {
                for _ in 0..5 {
                    if lb2.get() == &Server::A {
                        counter2.fetch_add(1, Ordering::SeqCst);
                    }
                    time::sleep(time::Duration::from_millis(3)).await;
                }
            });

            join_all([task1, task2]).await;

            let presence_counter = counter.load(Ordering::SeqCst);

            assert_eq!(presence_counter, 3);
            while arc_lb.inspect_block_counter(key.clone()) != 0
                && arc_lb.get() != initial_servers_before_test
            {}
            counter.swap(0, Ordering::SeqCst);
        }
    }
}
