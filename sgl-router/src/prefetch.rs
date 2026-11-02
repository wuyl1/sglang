// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Reserve UMBP prefetch keys before forwarding to the selected worker. Only
//! submission is awaited; transfer and status polling overlap the engine queue.

use anyhow::{ensure, Context, Result};
use futures::future::join_all;
use prost::Message;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use uuid::Uuid;

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

struct RankTarget {
    rank: u32,
    channel: Channel,
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
                clients.push(RankTarget {
                    rank: target.rank,
                    channel: endpoint.connect_lazy(),
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

    /// Wait for acceptance on each rank, so UMBP's arriving GET can find the
    /// reservation. Failure leaves normal engine GET fallback available.
    pub async fn submit(&self, worker_url: &str, hashes: &[String], correlation_id: &str) {
        if !self.config.submit_enabled || hashes.is_empty() {
            return;
        }
        let Some(targets) = self.targets.get(normalize_worker_url(worker_url)) else {
            return;
        };
        let submissions = targets.iter().flat_map(|target| {
            let keys = expand_keys(&self.config.key_templates, target.rank, hashes);
            keys.chunks(self.config.max_keys_per_request)
                .map(|chunk| self.submit_batch(target, chunk.to_vec(), correlation_id.to_owned()))
                .collect::<Vec<_>>()
        });
        join_all(submissions).await;
    }

    async fn submit_batch(&self, target: &RankTarget, keys: Vec<String>, correlation_id: String) {
        let request_id = format!("sgl-router-{}-r{}", Uuid::new_v4().simple(), target.rank);
        let rank = target.rank;
        let key_count = keys.len();
        let timeout = Duration::from_millis(self.config.timeout_ms);
        let started = std::time::Instant::now();
        let response = unary::<PrefetchRequest, PrefetchSubmitResponse>(
            target.channel.clone(),
            SUBMIT_PATH,
            PrefetchRequest {
                keys,
                lease_ttl_ms: self.config.lease_ttl_ms,
                timeout_ms: self.config.timeout_ms,
                request_id: request_id.clone(),
            },
            timeout,
        )
        .await;
        match response {
            Ok(response) if response.accepted && !response.request_id.is_empty() => {
                tracing::debug!(request_id = %response.request_id, rank, correlation_id, key_count,
                    accepted_latency_us = started.elapsed().as_micros() as u64, "UMBP prefetch accepted");
                let channel = target.channel.clone();
                let poll_interval = Duration::from_millis(self.config.poll_interval_ms);
                let shutdown = self.shutdown.clone();
                tokio::spawn(poll_status(
                    channel,
                    response.request_id,
                    rank,
                    correlation_id,
                    poll_interval,
                    timeout,
                    shutdown,
                ));
            }
            Ok(response) => {
                tracing::warn!(request_id, rank, correlation_id, error = %response.error,
                "UMBP prefetch rejected; forwarding with normal fallback")
            }
            Err(error) => tracing::warn!(request_id, rank, correlation_id, %error,
                "UMBP prefetch submit failed; forwarding with normal fallback"),
        }
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

async fn poll_status(
    channel: Channel,
    request_id: String,
    rank: u32,
    correlation_id: String,
    poll_interval: Duration,
    timeout: Duration,
    shutdown: CancellationToken,
) {
    let polling = async {
        loop {
            let status = unary::<PrefetchStatusRequest, PrefetchStatusResponse>(
                channel.clone(),
                STATUS_PATH,
                PrefetchStatusRequest {
                    request_id: request_id.clone(),
                },
                timeout,
            )
            .await?;
            match status.state {
                1 | 2 => tokio::time::sleep(poll_interval).await,
                3..=6 => {
                    tracing::info!(%request_id, rank, correlation_id, state = status.state,
                        completed_keys = status.completed_keys, total_keys = status.total_keys,
                        queue_latency_us = status.queue_latency_us, execution_latency_us = status.execution_latency_us,
                        total_latency_us = status.total_latency_us, error = %status.error, "UMBP prefetch completed");
                    return Ok::<(), Status>(());
                }
                _ => return Err(Status::not_found("UMBP prefetch status is unknown")),
            }
        }
    };
    tokio::select! {
        () = shutdown.cancelled() => {},
        result = tokio::time::timeout(timeout, polling) => match result {
            Ok(Ok(())) => {},
            Ok(Err(error)) => tracing::warn!(%request_id, rank, correlation_id, %error, "UMBP prefetch status failed"),
            Err(_) => tracing::warn!(%request_id, rank, correlation_id, "UMBP prefetch status polling timed out"),
        }
    }
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
        for (field, invalid) in [
            ("block_size", serde_json::json!(0)),
            ("timeout_ms", serde_json::json!(0)),
            ("max_keys_per_request", serde_json::json!(0)),
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
