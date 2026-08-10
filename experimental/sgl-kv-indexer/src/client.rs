// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Router-facing client for the prefix-match query.
//!
//! A successful query distinguishes a real match from an empty result. Transport
//! failures, deadlines, and server rejections remain errors so callers can fail
//! the routing request instead of silently using a different signal.
//!
//! The surface is deliberately tiny (one trait, one method, one outcome type):
//! the router intersects [`PrefixMatch::address`] with its own registered worker
//! URLs and picks by its own load metric, so anything beyond address and prefix
//! length would just be unused weight.

use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use crate::pb::kv_indexer_client::KvIndexerClient;
use crate::pb::MatchExternalKvPrefixRequest;

/// Default per-query deadline. Indexer failures are request failures, so this
/// allows normal cross-host scheduling jitter without stalling requests for an
/// unbounded duration.
pub const DEFAULT_QUERY_DEADLINE: Duration = Duration::from_millis(100);

/// One worker's contiguous prefix hit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixMatch {
    /// Router-facing routing identity; intersect byte-for-byte with registered
    /// worker URLs. Never empty (the indexer drops unroutable workers).
    pub address: String,
    /// Length of the contiguous request prefix this worker holds.
    pub matched_prefix_blocks: u32,
    /// Opaque worker id, for the caller's logs only.
    pub worker_id: String,
}

/// A failed prefix query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefixIndexError {
    /// The endpoint could not be reached.
    Unreachable,
    /// The query exceeded its deadline.
    Timeout,
    /// The server rejected the request.
    Rejected(tonic::Code),
}

impl std::fmt::Display for PrefixIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable => f.write_str("KV Indexer is unreachable"),
            Self::Timeout => f.write_str("KV Indexer query timed out"),
            Self::Rejected(code) => write!(f, "KV Indexer rejected the query with {code}"),
        }
    }
}

impl std::error::Error for PrefixIndexError {}

/// Result of a successful prefix query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefixOutcome {
    Matched {
        /// Sorted by `matched_prefix_blocks`, descending.
        matches: Vec<PrefixMatch>,
        /// Longest contiguous prefix held by any single worker.
        best_prefix_blocks: u32,
    },
    /// No worker holds a prefix (or the request had no hashes).
    Empty,
}

/// Client configuration.
#[derive(Debug, Clone)]
pub struct PrefixIndexConfig {
    /// gRPC endpoint of the indexer, e.g. `http://10.0.0.1:50051`.
    pub endpoint: String,
    /// Per-query deadline.
    pub query_deadline: Duration,
}

impl PrefixIndexConfig {
    /// Config with the default query deadline ([`DEFAULT_QUERY_DEADLINE`]).
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            query_deadline: DEFAULT_QUERY_DEADLINE,
        }
    }
}

/// The prefix-match query the router links against.
#[tonic::async_trait]
pub trait PrefixIndex: Send + Sync {
    /// Queries the longest contiguous prefix each worker holds for `hashes`
    /// (prompt order, `hashes[0]` first).
    async fn match_prefix(&self, hashes: Vec<i64>) -> Result<PrefixOutcome, PrefixIndexError>;
}

/// tonic-backed [`PrefixIndex`] with a lazily-established connection.
pub struct GrpcPrefixIndex {
    /// `None` when the endpoint URI could not be parsed; queries then fail as
    /// [`PrefixIndexError::Unreachable`].
    channel: Option<Channel>,
    deadline: Duration,
}

impl GrpcPrefixIndex {
    pub fn new(config: PrefixIndexConfig) -> Self {
        let channel = Endpoint::from_shared(config.endpoint)
            .ok()
            .map(|endpoint| endpoint.connect_lazy());
        Self {
            channel,
            deadline: config.query_deadline,
        }
    }
}

#[tonic::async_trait]
impl PrefixIndex for GrpcPrefixIndex {
    async fn match_prefix(&self, hashes: Vec<i64>) -> Result<PrefixOutcome, PrefixIndexError> {
        let Some(channel) = self.channel.clone() else {
            return Err(PrefixIndexError::Unreachable);
        };
        if hashes.is_empty() {
            return Ok(PrefixOutcome::Empty);
        }

        let mut client = KvIndexerClient::new(channel);
        let request = MatchExternalKvPrefixRequest {
            // The bridge encodes block hashes as decimal strings; mirror it.
            hashes: hashes.iter().map(|hash| hash.to_string()).collect(),
            max_blocks: 0,
        };

        match tokio::time::timeout(self.deadline, client.match_external_kv_prefix(request)).await {
            Err(_) => Err(PrefixIndexError::Timeout),
            Ok(Err(status)) => Err(classify(status.code())),
            Ok(Ok(response)) => {
                let response = response.into_inner();
                if response.matches.is_empty() {
                    return Ok(PrefixOutcome::Empty);
                }
                let matches = response
                    .matches
                    .into_iter()
                    .map(|m| PrefixMatch {
                        address: m.worker_address,
                        matched_prefix_blocks: m.matched_prefix_blocks,
                        worker_id: m.worker_id,
                    })
                    .collect();
                Ok(PrefixOutcome::Matched {
                    matches,
                    best_prefix_blocks: response.best_prefix_blocks,
                })
            }
        }
    }
}

fn classify(code: tonic::Code) -> PrefixIndexError {
    match code {
        tonic::Code::Unavailable => PrefixIndexError::Unreachable,
        tonic::Code::DeadlineExceeded => PrefixIndexError::Timeout,
        _ => PrefixIndexError::Rejected(code),
    }
}
