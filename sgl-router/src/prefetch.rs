// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Reserve UMBP prefetch keys before forwarding to the selected worker. Only
//! submission is awaited; transfer and status polling overlap the engine queue.

use crate::server::metrics::MetricsRegistry;
use crate::state::kv_events::{compute_block_hashes_hex, compute_block_hashes_hex_bigram};
use anyhow::{ensure, Context, Result};
use futures::future::join_all;
use parking_lot::Mutex;
use prost::Message;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

const SUBMIT_PATH: &str = "/umbp.UMBPStandalone/SubmitPrefetch";
const STATUS_PATH: &str = "/umbp.UMBPStandalone/GetPrefetchStatus";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrefetchConfig {
    #[serde(default = "submit_enabled")]
    submit_enabled: bool,
    worker_targets: HashMap<String, Vec<RankConfig>>,
    block_size: u32,
    #[serde(default)]
    is_bigram: bool,
    key_templates: Vec<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_timeout_ms")]
    lease_ttl_ms: u64,
    #[serde(default = "default_poll_interval_ms")]
    poll_interval_ms: u64,
    #[serde(default = "default_max_keys")]
    max_keys_per_request: usize,
    #[serde(default = "default_min_blocks")]
    min_prefix_blocks: usize,
    #[serde(default = "default_max_keys")]
    max_blocks_per_request: usize,
    #[serde(default = "default_max_inflight")]
    max_inflight_requests: usize,
    #[serde(default = "default_inflight_keys")]
    max_inflight_keys: usize,
    // Optional static estimate; this is not a live DRAM capacity measurement.
    #[serde(default)]
    bytes_per_key: u64,
    #[serde(default)]
    max_inflight_bytes: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RankConfig {
    rank: u32,
    endpoint: String,
}

fn submit_enabled() -> bool {
    true
}
fn default_timeout_ms() -> u64 {
    5_000
}
fn default_poll_interval_ms() -> u64 {
    2
}
fn default_max_keys() -> usize {
    4_096
}

fn default_min_blocks() -> usize {
    1
}
fn default_max_inflight() -> usize {
    16
}
fn default_inflight_keys() -> usize {
    65_536
}

struct RankTarget {
    rank: u32,
    pool: Arc<Pool>,
}

struct Pool {
    endpoint: String,
    channel: Channel,
    active: Mutex<Active>,
}

#[derive(Default)]
struct Active {
    keys: HashMap<String, Arc<Batch>>,
    requests: usize,
}

struct Batch {
    request_id: String,
    submitted: watch::Receiver<bool>,
    // READY history may predate this submission; only share in-flight tasks.
    reusable: AtomicBool,
}

// Owned by the background task, never by an individual request waiting on it.
struct Reservation {
    pool: Arc<Pool>,
    batch: Arc<Batch>,
    keys: Vec<String>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut active = self.pool.active.lock();
        for key in &self.keys {
            active.keys.remove(key);
        }
        active.requests -= 1;
    }
}

pub struct UmbpPrefetchCoordinator {
    config: PrefetchConfig,
    targets: HashMap<String, Vec<RankTarget>>,
    shutdown: CancellationToken,
}

impl UmbpPrefetchCoordinator {
    pub fn from_file(path: &str) -> Result<Self> {
        let file = std::fs::File::open(path)
            .with_context(|| format!("read UMBP prefetch config {path:?}"))?;
        Self::new(serde_json::from_reader(file).context("parse UMBP prefetch config")?)
    }

    fn new(config: PrefetchConfig) -> Result<Self> {
        ensure!(
            !config.worker_targets.is_empty(),
            "UMBP prefetch worker_targets must be non-empty"
        );
        ensure!(
            config.block_size > 0,
            "UMBP prefetch block_size must be positive"
        );
        ensure!(
            !config.key_templates.is_empty()
                && config
                    .key_templates
                    .iter()
                    .all(|key| key.contains("{hash}")),
            "UMBP prefetch key_templates must each contain {{hash}}"
        );
        ensure!(
            (1..=300_000).contains(&config.timeout_ms) && config.lease_ttl_ms <= 300_000,
            "UMBP prefetch timeout_ms must be in 1..=300000 and lease_ttl_ms <= 300000"
        );
        ensure!(
            config.poll_interval_ms > 0 && (1..=4096).contains(&config.max_keys_per_request),
            "UMBP prefetch poll_interval_ms must be positive and max_keys_per_request in 1..=4096"
        );
        ensure!(
            config.min_prefix_blocks > 0
                && config.min_prefix_blocks <= config.max_blocks_per_request
                && config.max_blocks_per_request <= 4096
                && (1..=1024).contains(&config.max_inflight_requests)
                && (1..=65_536).contains(&config.max_inflight_keys),
            "UMBP prefetch requires positive, bounded prefix and inflight limits"
        );
        ensure!(
            config.max_inflight_bytes == 0 || config.bytes_per_key > 0,
            "UMBP prefetch max_inflight_bytes requires bytes_per_key"
        );
        let mut pools = HashMap::new();
        let mut targets = HashMap::new();
        for (worker, ranks) in &config.worker_targets {
            ensure!(
                !ranks.is_empty(),
                "UMBP prefetch worker {worker:?} has no targets"
            );
            let mut seen = HashSet::new();
            let mut clients = Vec::new();
            for target in ranks {
                ensure!(
                    seen.insert(target.rank),
                    "UMBP prefetch worker {worker:?} repeats rank {}",
                    target.rank
                );
                let endpoint = Endpoint::from_shared(target.endpoint.clone())?;
                ensure!(
                    endpoint.uri().scheme_str() == Some("http"),
                    "UMBP prefetch endpoints must use http"
                );
                let address = endpoint.uri().to_string().trim_end_matches('/').to_owned();
                let pool = pools.entry(address.clone()).or_insert_with(|| {
                    Arc::new(Pool {
                        endpoint: address,
                        channel: endpoint.connect_lazy(),
                        active: Mutex::new(Active::default()),
                    })
                });
                clients.push(RankTarget {
                    rank: target.rank,
                    pool: pool.clone(),
                });
            }
            ensure!(
                targets
                    .insert(normalize_worker_url(worker).to_owned(), clients)
                    .is_none(),
                "UMBP prefetch repeats normalized worker URL {worker:?}"
            );
        }
        Ok(Self {
            config,
            targets,
            shutdown: CancellationToken::new(),
        })
    }

    pub fn block_size(&self) -> u32 {
        self.config.block_size
    }

    /// Configured fallback when workers do not publish KV-event hash metadata.
    pub fn hash_config(&self) -> (u32, bool) {
        (self.config.block_size, self.config.is_bigram)
    }

    /// Hash only the bounded complete prefix, using the engine's hash mode.
    pub fn hashes(&self, ids: &[u32], (block_size, bigram): (u32, bool)) -> Vec<String> {
        let block_size = block_size as usize;
        let blocks = (ids.len().saturating_sub(usize::from(bigram)) / block_size)
            .min(self.config.max_blocks_per_request);
        if blocks < self.config.min_prefix_blocks {
            return Vec::new();
        }
        let ids = &ids[..blocks * block_size + usize::from(bigram)];
        if bigram {
            compute_block_hashes_hex_bigram(ids, block_size)
        } else {
            compute_block_hashes_hex(ids, block_size)
        }
    }

    /// Decide only for the selected worker. Wait for submission, never transfer.
    /// Shared tasks survive cancellation of any individual HTTP request.
    pub async fn submit(
        &self,
        worker_url: &str,
        hashes: &[String],
        action_id: &str,
        metrics: &Arc<MetricsRegistry>,
    ) {
        metrics.record_prefetch("evaluated");
        let skip = if !self.config.submit_enabled {
            Some("disabled")
        } else if hashes.len() < self.config.min_prefix_blocks {
            Some("short_prefix")
        } else {
            None
        };
        if let Some(reason) = skip {
            metrics.record_prefetch(reason);
            return;
        }
        let Some(targets) = self.targets.get(normalize_worker_url(worker_url)) else {
            metrics.record_prefetch("unmapped");
            return;
        };
        let hashes = &hashes[..hashes.len().min(self.config.max_blocks_per_request)];
        // TP ranks sharing a physical endpoint also share the same keyspace.
        let mut plans = BTreeMap::new();
        for target in targets {
            let (_, keys) = plans
                .entry(&target.pool.endpoint)
                .or_insert_with(|| (target.pool.clone(), Vec::new()));
            keys.extend(expand_keys(&self.config.key_templates, target.rank, hashes));
        }
        let mut waiters = Vec::new();
        for (_, (pool, mut keys)) in plans {
            keys.sort_unstable();
            keys.dedup();
            let mut reused = HashSet::new();
            let mut active = pool.active.lock();
            let mut missing = Vec::new();
            for key in keys {
                if let Some(batch) = active.keys.get(&key) {
                    if reused.insert(Arc::as_ptr(batch)) {
                        if batch.reusable.load(Ordering::Relaxed) {
                            metrics.record_prefetch("reused");
                            tracing::debug!(%action_id, request_id = %batch.request_id, endpoint = %pool.endpoint,
                                "UMBP prefetch reused");
                            waiters.push(batch.submitted.clone());
                        } else {
                            metrics.record_prefetch("cooldown");
                        }
                    }
                } else {
                    missing.push(key);
                }
            }
            for keys in missing.chunks(self.config.max_keys_per_request) {
                let key_count = active.keys.len() + keys.len();
                if active.requests >= self.config.max_inflight_requests
                    || key_count > self.config.max_inflight_keys
                    || (self.config.max_inflight_bytes > 0
                        && key_count as u64
                            > self.config.max_inflight_bytes / self.config.bytes_per_key)
                {
                    metrics.record_prefetch("budget");
                    continue;
                }
                let (submitted, receiver) = watch::channel(false);
                let batch = Arc::new(Batch {
                    request_id: operation_id(action_id, &pool.endpoint, keys, &self.config),
                    submitted: receiver.clone(),
                    reusable: AtomicBool::new(true),
                });
                active.requests += 1;
                for key in keys {
                    active.keys.insert(key.clone(), batch.clone());
                }
                let reservation = Reservation {
                    pool: pool.clone(),
                    batch: batch.clone(),
                    keys: keys.to_vec(),
                };
                let config = &self.config;
                let request = PrefetchRequest {
                    keys: keys.to_vec(),
                    lease_ttl_ms: config.lease_ttl_ms,
                    timeout_ms: config.timeout_ms,
                    request_id: batch.request_id.clone(),
                };
                let shutdown = self.shutdown.clone();
                let action_id = action_id.to_owned();
                let metrics = metrics.clone();
                let interval = Duration::from_millis(config.poll_interval_ms);
                metrics.record_prefetch("issued");
                tokio::spawn(async move {
                    tokio::select! {
                        () = shutdown.cancelled() => {},
                        () = execute(reservation, request, submitted, action_id, interval, metrics) => {},
                    }
                });
                waiters.push(receiver);
            }
        }
        join_all(waiters.into_iter().map(|mut waiter| async move {
            let _ = waiter.wait_for(|submitted| *submitted).await;
        }))
        .await;
    }
}

impl Drop for UmbpPrefetchCoordinator {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn normalize_worker_url(url: &str) -> &str {
    url.trim().trim_end_matches('/')
}

fn expand_keys(templates: &[String], rank: u32, hashes: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    let rank = rank.to_string();
    for hash in hashes {
        for template in templates {
            let key = template.replace("{hash}", hash).replace("{rank}", &rank);
            if seen.insert(key.clone()) {
                keys.push(key);
            }
        }
    }
    keys
}

// The ID binds the immutable wire payload: UMBP rejects reuse with new keys/options.
fn operation_id(
    action_id: &str,
    endpoint: &str,
    keys: &[String],
    config: &PrefetchConfig,
) -> String {
    let mut digest = Sha256::new();
    for part in std::iter::once(action_id)
        .chain(std::iter::once(endpoint))
        .chain(keys.iter().map(String::as_str))
    {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part.as_bytes());
    }
    digest.update(config.lease_ttl_ms.to_le_bytes());
    digest.update(config.timeout_ms.to_le_bytes());
    format!("sgl-router-{:x}", digest.finalize())
}

async fn execute(
    reservation: Reservation,
    request: PrefetchRequest,
    submitted: watch::Sender<bool>,
    action_id: String,
    interval: Duration,
    metrics: Arc<MetricsRegistry>,
) {
    let started = tokio::time::Instant::now();
    let timeout = Duration::from_millis(request.timeout_ms);
    // Keep speculative budget through the execution/lease window even when an
    // RPC outcome is ambiguous. This is a static bound, not proof I/O has stopped.
    let lease = Duration::from_millis(if request.lease_ttl_ms == 0 {
        500
    } else {
        request.lease_ttl_ms
    });
    let mut retire_at = started + timeout * 2 + lease;
    let request_id = request.request_id.clone();
    let pool = &reservation.pool;
    let response = unary::<PrefetchRequest, PrefetchSubmitResponse>(
        pool.channel.clone(),
        SUBMIT_PATH,
        request,
        timeout,
    )
    .await;
    match response {
        Ok(response) if !response.accepted => {
            metrics.record_prefetch("rejected");
            submitted.send_replace(true);
            tracing::warn!(%action_id, %request_id, error = %response.error, "UMBP prefetch rejected");
            return;
        }
        Ok(response) if response.request_id == request_id => {
            metrics.record_prefetch("accepted");
            metrics.observe_prefetch_acceptance(started.elapsed().as_secs_f64());
            tracing::debug!(%action_id, %request_id, key_count = reservation.keys.len(),
                accepted_latency_us = started.elapsed().as_micros() as u64, "UMBP prefetch accepted");
        }
        response => {
            metrics.record_prefetch("submit_error");
            tracing::warn!(%action_id, %request_id, error = ?response,
                "UMBP prefetch submission uncertain; forwarding with normal fallback");
        }
    }
    submitted.send_replace(true);
    let polling = async {
        loop {
            let status = unary::<PrefetchStatusRequest, PrefetchStatusResponse>(
                pool.channel.clone(),
                STATUS_PATH,
                PrefetchStatusRequest {
                    request_id: request_id.clone(),
                },
                timeout,
            )
            .await?;
            let outcome = match status.state {
                1 | 2 => {
                    tokio::time::sleep(interval).await;
                    continue;
                }
                3 => "ready",
                4 => "partial",
                5 => "failed",
                6 => "expired",
                _ => return Err(Status::not_found("UMBP prefetch status is unknown")),
            };
            metrics.record_prefetch(outcome);
            metrics.observe_prefetch_completion(started.elapsed().as_secs_f64());
            tracing::info!(%action_id, %request_id, state = status.state,
                completed_keys = status.completed_keys, total_keys = status.total_keys,
                queue_latency_us = status.queue_latency_us, execution_latency_us = status.execution_latency_us,
                total_latency_us = status.total_latency_us, error = %status.error, "UMBP prefetch completed");
            return Ok::<PrefetchStatusResponse, Status>(status);
        }
    };
    match tokio::time::timeout(timeout, polling).await {
        Ok(Ok(status)) => {
            if status.state == 3 {
                retire_at = retire_at.min(tokio::time::Instant::now() + lease);
            }
        }
        Ok(Err(error)) => {
            metrics.record_prefetch("status_error");
            tracing::warn!(%action_id, %request_id, %error, "UMBP prefetch status failed");
        }
        Err(_) => {
            metrics.record_prefetch("observation_timeout");
            tracing::warn!(%action_id, %request_id, "UMBP prefetch status polling timed out");
        }
    }
    reservation.batch.reusable.store(false, Ordering::Relaxed);
    tokio::time::sleep_until(retire_at).await;
    // Reservation's Drop reclaims all key metadata and budget together.
}

async fn unary<Req, Resp>(
    channel: Channel,
    path: &'static str,
    message: Req,
    timeout: Duration,
) -> Result<Resp, Status>
where
    Req: Message + Default + Send + Sync + 'static,
    Resp: Message + Default + Send + Sync + 'static,
{
    // The connection/readiness wait also needs a local deadline. A gRPC request
    // deadline alone does not bound connect_lazy() to an unreachable endpoint.
    tokio::time::timeout(timeout, async {
        let mut grpc = tonic::client::Grpc::new(channel);
        grpc.ready()
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut request = Request::new(message);
        request.set_timeout(timeout);
        grpc.unary(
            request,
            PathAndQuery::from_static(path),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .map(tonic::Response::into_inner)
    })
    .await
    .map_err(|_| Status::deadline_exceeded("UMBP prefetch RPC timed out"))?
}

// Wire fields match MoRI's distributed/proto/umbp_standalone.proto.
#[derive(Clone, PartialEq, Message)]
struct PrefetchRequest {
    #[prost(string, repeated, tag = "1")]
    keys: Vec<String>,
    #[prost(uint64, tag = "2")]
    lease_ttl_ms: u64,
    #[prost(uint64, tag = "3")]
    timeout_ms: u64,
    #[prost(string, tag = "4")]
    request_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct PrefetchSubmitResponse {
    #[prost(bool, tag = "1")]
    accepted: bool,
    #[prost(string, tag = "2")]
    request_id: String,
    #[prost(string, tag = "3")]
    error: String,
}

#[derive(Clone, PartialEq, Message)]
struct PrefetchStatusRequest {
    #[prost(string, tag = "1")]
    request_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct PrefetchStatusResponse {
    #[prost(int32, tag = "1")]
    state: i32,
    #[prost(bool, repeated, tag = "2")]
    ok: Vec<bool>,
    #[prost(uint64, tag = "3")]
    completed_keys: u64,
    #[prost(uint64, tag = "4")]
    total_keys: u64,
    #[prost(string, tag = "5")]
    error: String,
    #[prost(uint64, tag = "6")]
    queue_latency_us: u64,
    #[prost(uint64, tag = "7")]
    execution_latency_us: u64,
    #[prost(uint64, tag = "8")]
    total_latency_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_and_deduplicates_rank_keys() {
        let templates = vec![
            "m_{hash}_tp{rank}_kv".into(),
            "m_{hash}_tp{rank}_kv".into(),
            "m_{hash}_swa".into(),
        ];
        assert_eq!(
            expand_keys(&templates, 3, &["abc".into(), "abc".into()]),
            ["m_abc_tp3_kv", "m_abc_swa"]
        );
        assert_eq!(
            normalize_worker_url(" http://worker:30000/ "),
            "http://worker:30000"
        );
    }

    #[tokio::test]
    async fn validates_config_before_creating_clients() {
        let value = serde_json::json!({
            "worker_targets": {"http://worker:30000": [{"rank": 0, "endpoint": "http://worker:18100"}]},
            "block_size": 256, "key_templates": ["m_{hash}_tp{rank}"]
        });
        let coordinator =
            UmbpPrefetchCoordinator::new(serde_json::from_value(value.clone()).unwrap()).unwrap();
        assert_eq!(coordinator.hash_config(), (256, false));
        assert!(coordinator.hashes(&[1, 2, 3], (4, false)).is_empty());
        assert_eq!(coordinator.hashes(&[1, 2, 3, 4, 5, 6], (4, false)).len(), 1);
        assert_eq!(coordinator.hashes(&[1, 2, 3, 4, 5, 6], (4, true)).len(), 1);
        let keys = vec!["abc".to_owned()];
        let id = operation_id("action", "http://pool:1", &keys, &coordinator.config);
        assert_eq!(
            id,
            operation_id("action", "http://pool:1", &keys, &coordinator.config)
        );
        assert_ne!(
            id,
            operation_id("action", "http://pool:2", &keys, &coordinator.config)
        );
        assert_ne!(
            id,
            operation_id(
                "action",
                "http://pool:1",
                &["def".into()],
                &coordinator.config
            )
        );
        for (field, invalid) in [
            ("block_size", serde_json::json!(0)),
            ("timeout_ms", serde_json::json!(0)),
            ("max_keys_per_request", serde_json::json!(0)),
            ("min_prefix_blocks", serde_json::json!(0)),
            ("max_blocks_per_request", serde_json::json!(0)),
            ("max_inflight_requests", serde_json::json!(0)),
            ("max_inflight_keys", serde_json::json!(0)),
            ("max_inflight_bytes", serde_json::json!(1024)),
            ("key_templates", serde_json::json!(["m_{rank}"])),
            ("worker_targets", serde_json::json!({})),
        ] {
            let mut bad = value.clone();
            bad[field] = invalid;
            assert!(
                UmbpPrefetchCoordinator::new(serde_json::from_value(bad).unwrap()).is_err(),
                "{field}"
            );
        }
    }

    #[tokio::test]
    async fn unreachable_control_endpoint_does_not_block_forwarding() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        // Keep TCP open without an HTTP/2 handshake, so readiness cannot finish.
        let channel = Endpoint::from_shared(format!("http://{}", listener.local_addr().unwrap()))
            .unwrap()
            .connect_lazy();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            unary::<PrefetchStatusRequest, PrefetchStatusResponse>(
                channel,
                STATUS_PATH,
                PrefetchStatusRequest {
                    request_id: "test".into(),
                },
                Duration::from_millis(20),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            result.unwrap_err().code(),
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
        ));
    }
}
