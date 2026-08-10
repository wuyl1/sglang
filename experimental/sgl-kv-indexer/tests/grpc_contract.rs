// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! gRPC contract tests: exercise all four RPCs of the `KVIndexer` service
//! over the wire (real tonic server + client), not just the backend trait.

#[path = "common/id.rs"]
mod test_id;
#[allow(dead_code)]
#[path = "common/kv.rs"]
mod test_kv;
#[path = "common/net.rs"]
mod test_net;

use std::time::Duration;

use tonic::transport::Server;
use tonic::Code;

use sgl_kv_indexer::pb::kv_indexer_client::KvIndexerClient;
use sgl_kv_indexer::pb::kv_indexer_server::KvIndexerServer;
use sgl_kv_indexer::pb::{
    ApplyExternalKvBatchRequest, ExternalKvAction, ExternalKvActionType,
    GetExternalKvHitCountsRequest, MatchExternalKvPrefixRequest, MatchExternalKvRequest,
};
use sgl_kv_indexer::{InMemoryKvIndexerBackend, KvIndexerService};
use test_id::nanos;
use test_kv::{action, apply_request, hbm};
use test_net::free_addr;

async fn start_backend(
    backend: InMemoryKvIndexerBackend,
) -> KvIndexerClient<tonic::transport::Channel> {
    let svc = KvIndexerServer::new(KvIndexerService::new(backend));
    let addr = free_addr();
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .expect("server serve");
    });

    let endpoint = format!("http://{addr}");
    for _ in 0..50 {
        if let Ok(c) = KvIndexerClient::connect(endpoint.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("client failed to connect to {endpoint}");
}

/// Starts a real gRPC server with isolated process-local state.
async fn start() -> KvIndexerClient<tonic::transport::Channel> {
    start_backend(InMemoryKvIndexerBackend::new()).await
}

fn apply(
    worker: &str,
    addr: &str,
    seq: u64,
    action_type: ExternalKvActionType,
    tier: i32,
    hashes: &[&str],
) -> ApplyExternalKvBatchRequest {
    apply_request(worker, addr, seq, vec![action(action_type, tier, hashes)])
}

fn apply_report(
    worker: &str,
    addr: &str,
    seq: u64,
    tier: i32,
    hashes: &[&str],
) -> ApplyExternalKvBatchRequest {
    apply(
        worker,
        addr,
        seq,
        ExternalKvActionType::ActionReport,
        tier,
        hashes,
    )
}

#[tokio::test]
async fn multiple_workers_share_one_indexer_server() {
    let mut indexer = start().await;
    let suffix = nanos();
    let worker_0 = format!("worker-0-{suffix}");
    let worker_1 = format!("worker-1-{suffix}");
    let hash_0 = format!("horizontal-h0-{suffix}");
    let hash_1 = format!("horizontal-h1-{suffix}");
    let shared_hash = format!("horizontal-shared-{suffix}");

    indexer
        .apply_external_kv_batch(apply_report(
            &worker_0,
            "10.0.0.1:9000",
            1,
            hbm(),
            &[&hash_0, &shared_hash],
        ))
        .await
        .expect("apply worker-0");
    indexer
        .apply_external_kv_batch(apply_report(
            &worker_1,
            "10.0.0.2:9000",
            1,
            hbm(),
            &[&hash_1, &shared_hash],
        ))
        .await
        .expect("apply worker-1");

    let response = indexer
        .match_external_kv(MatchExternalKvRequest {
            hashes: vec![hash_0.clone(), hash_1.clone(), shared_hash.clone()],
            count_as_hit: false,
        })
        .await
        .expect("query indexer")
        .into_inner();
    assert!(response
        .matches
        .iter()
        .any(|entry| entry.worker_id == worker_0));
    assert!(response
        .matches
        .iter()
        .any(|entry| entry.worker_id == worker_1));

    // Keep one wire-level smoke check for hit counting; detailed counter
    // semantics live in memory_integration.rs.
    indexer
        .match_external_kv(MatchExternalKvRequest {
            hashes: vec![hash_0.clone()],
            count_as_hit: true,
        })
        .await
        .expect("counting match over gRPC");
    let miss = format!("horizontal-miss-{suffix}");
    let counts = indexer
        .get_external_kv_hit_counts(GetExternalKvHitCountsRequest {
            hashes: vec![hash_0.clone(), miss.clone()],
        })
        .await
        .expect("hit counts over gRPC")
        .into_inner();
    let count = |hash: &str| {
        counts
            .entries
            .iter()
            .find(|entry| entry.hash == hash)
            .map(|entry| entry.hit_count_total)
            .unwrap_or(0)
    };
    assert!(count(&hash_0) >= 1, "matched hash should have a hit");
    assert_eq!(count(&miss), 0, "unmatched hash must not be counted");
}

#[tokio::test]
async fn validation_errors_map_to_invalid_argument_over_grpc() {
    let mut c = start().await;

    // empty worker_id
    let err = c
        .apply_external_kv_batch(apply_report("", "addr", 1, hbm(), &["h"]))
        .await
        .expect_err("empty worker_id must be rejected");
    assert_eq!(
        err.code(),
        Code::InvalidArgument,
        "empty worker_id -> InvalidArgument"
    );

    // REPORT action with no hashes
    let err = c
        .apply_external_kv_batch(apply(
            "w",
            "addr",
            1,
            ExternalKvActionType::ActionReport,
            hbm(),
            &[],
        ))
        .await
        .expect_err("empty hashes must be rejected");
    assert_eq!(
        err.code(),
        Code::InvalidArgument,
        "empty hashes -> InvalidArgument"
    );

    // unknown action type
    let bad = ApplyExternalKvBatchRequest {
        worker_id: "w".into(),
        seq: 1,
        worker_address: String::new(),
        cache_spec: None,
        actions: vec![ExternalKvAction {
            r#type: 999,
            tier: hbm(),
            hashes: vec!["h".into()],
            component_masks: Vec::new(),
            block_sizes: Vec::new(),
        }],
    };
    let err = c
        .apply_external_kv_batch(bad)
        .await
        .expect_err("unknown action type rejected");
    assert_eq!(
        err.code(),
        Code::InvalidArgument,
        "unknown action type -> InvalidArgument"
    );

    // invalid tier in an ApplyBatch action
    let err = c
        .apply_external_kv_batch(apply(
            "w",
            "addr",
            1,
            ExternalKvActionType::ActionReport,
            999,
            &["h"],
        ))
        .await
        .expect_err("bad tier rejected");
    assert_eq!(
        err.code(),
        Code::InvalidArgument,
        "bad tier -> InvalidArgument"
    );
}

#[tokio::test]
async fn match_prefix_over_grpc() {
    let mut c = start().await;
    let (w_long, w_short) = (format!("long-{}", nanos()), format!("short-{}", nanos()));
    let (a, b, d) = ("mp-a", "mp-b", "mp-c");

    c.apply_external_kv_batch(apply_report(&w_long, "10.0.0.1:9000", 1, hbm(), &[a, b, d]))
        .await
        .expect("apply long");
    c.apply_external_kv_batch(apply_report(&w_short, "10.0.0.2:9000", 1, hbm(), &[a]))
        .await
        .expect("apply short");

    let resp = c
        .match_external_kv_prefix(MatchExternalKvPrefixRequest {
            hashes: vec![a.into(), b.into(), d.into()],
            max_blocks: 0,
        })
        .await
        .expect("prefix ok")
        .into_inner();

    assert_eq!(resp.best_prefix_blocks, 3);
    assert_eq!(resp.blocks_read, 3);
    // Descending by prefix length: long (3) before short (1).
    assert_eq!(resp.matches.len(), 2);
    assert_eq!(resp.matches[0].worker_id, w_long);
    assert_eq!(resp.matches[0].matched_prefix_blocks, 3);
    assert_eq!(resp.matches[0].worker_address, "10.0.0.1:9000");
    assert_eq!(resp.matches[1].worker_id, w_short);
    assert_eq!(resp.matches[1].matched_prefix_blocks, 1);
}
