use crate::env_logger;
use crate::load_balancer::private::Algorithm;
use crate::load_balancer::round_robin::RoundRobin;
use crate::log::debug;
use std::collections::HashMap;

#[test]
fn test_plain_round_robin() {
    env_logger::try_init().unwrap_or_default();

    let items = HashMap::from([
        ("a".to_string(), 1),
        ("b".to_string(), 1),
        ("c".to_string(), 1),
    ]);

    let rrb = RoundRobin::new(items, false);
    debug!("{:?}", rrb);

    // NOTE: since all weights are 1,
    // the order of keys is not deterministic nor important
    let first_round = Vec::from_iter(0..3)
        .iter()
        .map(|_| rrb.next())
        .collect::<Vec<String>>();

    let expected = vec![first_round.clone(), first_round.clone(), first_round]
        .into_iter()
        .flatten()
        .collect::<Vec<String>>();

    let actual = Vec::from_iter(0..expected.len())
        .iter()
        .map(|_| {
            debug!("{:?}", rrb);
            rrb.next()
        })
        .collect::<Vec<String>>();

    assert_eq!(expected, actual);

    for _ in 0..256 {
        // Should not blow up when using u8!
        rrb.next();
    }
}

#[test]
fn test_classical_weighted_round_robin() {
    env_logger::try_init().unwrap_or_default();

    let items = HashMap::from([
        ("a".to_string(), 2),
        ("b".to_string(), 1),
        ("c".to_string(), 3),
        ("d".to_string(), 0),
    ]);

    let rrb = RoundRobin::new(items, false);
    debug!("{:?}", rrb);

    // NOTE: Items shall be sorted & returned by weighted from least to most
    let expected = vec![
        "b", "a", "a", "c", "c", "c", //
        "b", "a", "a", "c", "c", "c", //
        "b", "a", "a", "c", "c", "c", //
    ];

    let actual = Vec::from_iter(0..expected.len())
        .iter()
        .map(|_| {
            debug!("{:?}", rrb);
            rrb.next()
        })
        .collect::<Vec<String>>();

    assert_eq!(expected, actual);

    for _ in 0..256 {
        // Should not blow up!
        rrb.next();
    }
}

#[test]
fn test_interleave_weighted_round_robin() {
    env_logger::try_init().unwrap_or_default();

    let items = HashMap::from([
        ("a".to_string(), 2),
        ("b".to_string(), 1),
        ("c".to_string(), 3),
        ("d".to_string(), 0),
    ]);

    let rrb = RoundRobin::new(items, true);

    //
    let expected = vec![
        "b", "a", "c", "a", "c", "c", //
        "b", "a", "c", "a", "c", "c", //
        "b", "a", "c", "a", "c", "c", //
    ];

    let actual = Vec::from_iter(0..expected.len())
        .iter()
        .map(|_| {
            debug!("{:?}", rrb);
            rrb.next()
        })
        .collect::<Vec<String>>();

    assert_eq!(expected, actual);

    for _ in 0..256 {
        // Should not blow up!
        rrb.next();
    }
}
