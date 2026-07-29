// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! Contract tests against sgl-router's in-process `HashTree::match_prefix`.
//!
//! The indexer is meant to replace the router's process-local tree, so its
//! prefix answers must relate to the tree's in a known, safe way. sgl-router is a
//! separate Cargo workspace we cannot depend on, so its `match_prefix` is ported
//! here as a standalone reference oracle and we assert two properties:
//!
//!   * **Equivalence** — with hole-free placement (the SGLang norm, since the
//!     radix cache evicts leaves before ancestors) the two agree on the longest
//!     prefix and on the set of workers achieving it.
//!   * **Safety direction** — where they diverge, this implementation is the
//!     conservative one. A worker that reports a full chain then drops a middle
//!     block makes the tree walk *through* the hole and still credit a hit it
//!     cannot serve, while this implementation stops at the hole.
//!
//! Block-hash fixtures are the engine-pinned cross-language goldens from
//! sgl-router's `kv_events::hash` tests, so the decimal wire encoding the bridge
//! uses (`i64::to_string`) is exercised here rather than assumed.

use std::collections::{HashMap, HashSet};

use tonic::Status;

use sgl_kv_indexer::pb::{
    ApplyExternalKvBatchRequest, ApplyExternalKvBatchResponse, ExternalKvNodeMatch,
    GetExternalKvHitCountsRequest, GetExternalKvHitCountsResponse, MatchExternalKvPrefixRequest,
    MatchExternalKvRequest, MatchExternalKvResponse, TierHashes, TierType,
};
use sgl_kv_indexer::KvIndexerBackend;

// Engine-pinned goldens (sgl-router kv_events::hash cross-language tests):
// Python chain([10,20,30,40,50,60,70,80], 2).
const CHAIN: [i64; 4] = [
    978178666101069530,
    -895308556211281782,
    -8033692805846017938,
    835415944263129316,
];

fn key(hash: i64) -> String {
    hash.to_string()
}

// --- in-memory backend: match_external_kv over a placement map, prefix via the
// trait's default (semantic) implementation ---------------------------------

#[derive(Default)]
struct MemBackend {
    /// hash -> workers holding it.
    holders: HashMap<String, HashSet<String>>,
    /// worker -> routing address.
    address: HashMap<String, String>,
}

impl MemBackend {
    fn report(&mut self, worker: &str, address: &str, chain: &[i64]) {
        self.address.insert(worker.to_string(), address.to_string());
        for &h in chain {
            self.holders
                .entry(key(h))
                .or_default()
                .insert(worker.to_string());
        }
    }

    fn revoke(&mut self, worker: &str, hashes: &[i64]) {
        for &h in hashes {
            if let Some(set) = self.holders.get_mut(&key(h)) {
                set.remove(worker);
            }
        }
    }
}

#[tonic::async_trait]
impl KvIndexerBackend for MemBackend {
    async fn apply_external_kv_batch(
        &self,
        _request: ApplyExternalKvBatchRequest,
    ) -> Result<ApplyExternalKvBatchResponse, Status> {
        unimplemented!("contract test drives placement directly")
    }

    async fn match_external_kv(
        &self,
        request: MatchExternalKvRequest,
    ) -> Result<MatchExternalKvResponse, Status> {
        let mut by_worker: HashMap<String, Vec<String>> = HashMap::new();
        for hash in &request.hashes {
            if let Some(workers) = self.holders.get(hash) {
                for worker in workers {
                    by_worker
                        .entry(worker.clone())
                        .or_default()
                        .push(hash.clone());
                }
            }
        }
        let matches = by_worker
            .into_iter()
            .map(|(worker, hashes)| ExternalKvNodeMatch {
                address: self.address.get(&worker).cloned().unwrap_or_default(),
                worker_id: worker,
                hashes_by_tier: vec![TierHashes {
                    tier: TierType::TierHbm as i32,
                    hashes,
                }],
            })
            .collect();
        Ok(MatchExternalKvResponse { matches })
    }

    async fn get_external_kv_hit_counts(
        &self,
        _request: GetExternalKvHitCountsRequest,
    ) -> Result<GetExternalKvHitCountsResponse, Status> {
        unimplemented!("contract test does not exercise hit counts")
    }
}

/// Runs the implementation under test: its longest prefix and the set of worker
/// ids achieving it.
async fn subject(backend: &MemBackend, query: &[i64]) -> (u32, HashSet<String>) {
    let resp = backend
        .match_external_kv_prefix(MatchExternalKvPrefixRequest {
            hashes: query.iter().map(|&h| key(h)).collect(),
            max_blocks: 0,
        })
        .await
        .expect("prefix query");
    let winners = resp
        .matches
        .iter()
        .filter(|m| m.matched_prefix_blocks == resp.best_prefix_blocks)
        .map(|m| m.worker_id.clone())
        .collect();
    (resp.best_prefix_blocks, winners)
}

// --- reference oracle: sgl-router HashTree::match_prefix (ported) ------------
//
// Chains attach at the root (parent_hash = None), which is the SGLang emission
// shape the equivalence claim is about. Only the surface the contract needs is
// ported: insert, remove (with upward prune), and match_prefix.

mod oracle {
    use std::collections::{HashMap, HashSet};

    type NodeId = u64;
    const ROOT: NodeId = 0;

    struct Node {
        block_hash: i64,
        parent: Option<NodeId>,
        workers: HashSet<String>,
        children: HashMap<i64, NodeId>,
    }

    pub struct HashTree {
        nodes: HashMap<NodeId, Node>,
        by_hash: HashMap<i64, HashSet<NodeId>>,
        next_id: NodeId,
    }

    impl HashTree {
        pub fn new() -> Self {
            let mut nodes = HashMap::new();
            nodes.insert(
                ROOT,
                Node {
                    block_hash: i64::MIN,
                    parent: None,
                    workers: HashSet::new(),
                    children: HashMap::new(),
                },
            );
            Self {
                nodes,
                by_hash: HashMap::new(),
                next_id: 1,
            }
        }

        pub fn insert(&mut self, worker: &str, chain: &[i64]) {
            let mut current = ROOT;
            for &h in chain {
                let child = self.nodes[&current].children.get(&h).copied();
                let id = match child {
                    Some(id) => id,
                    None => {
                        let id = self.next_id;
                        self.next_id += 1;
                        self.nodes.insert(
                            id,
                            Node {
                                block_hash: h,
                                parent: Some(current),
                                workers: HashSet::new(),
                                children: HashMap::new(),
                            },
                        );
                        self.nodes.get_mut(&current).unwrap().children.insert(h, id);
                        self.by_hash.entry(h).or_default().insert(id);
                        id
                    }
                };
                self.nodes
                    .get_mut(&id)
                    .unwrap()
                    .workers
                    .insert(worker.to_string());
                current = id;
            }
        }

        pub fn remove(&mut self, worker: &str, hashes: &[i64]) {
            let mut targets: Vec<NodeId> = Vec::new();
            for h in hashes {
                if let Some(set) = self.by_hash.get(h) {
                    targets.extend(set.iter().copied());
                }
            }
            for id in targets {
                let prune = match self.nodes.get_mut(&id) {
                    Some(node) => {
                        node.workers.remove(worker);
                        node.workers.is_empty() && node.children.is_empty()
                    }
                    None => false,
                };
                if prune {
                    self.prune_cascade(id);
                }
            }
        }

        /// Longest contiguous prefix from the root and the workers at its
        /// deepest node — note the workers come from that node alone, not from
        /// every ancestor, which is the source of the divergence under holes.
        pub fn match_prefix(&self, hashes: &[i64]) -> (usize, HashSet<String>) {
            let mut current = ROOT;
            let mut matched = 0usize;
            let mut last: Option<NodeId> = None;
            for &h in hashes {
                match self.nodes[&current].children.get(&h).copied() {
                    Some(child) => {
                        current = child;
                        matched += 1;
                        last = Some(child);
                    }
                    None => break,
                }
            }
            let workers = last
                .map(|id| self.nodes[&id].workers.clone())
                .unwrap_or_default();
            (matched, workers)
        }

        fn prune_cascade(&mut self, start: NodeId) {
            let mut cursor = start;
            loop {
                if cursor == ROOT {
                    return;
                }
                let (parent, block_hash) = match self.nodes.get(&cursor) {
                    Some(n) => match n.parent {
                        Some(p) => (p, n.block_hash),
                        None => return,
                    },
                    None => return,
                };
                let prunable = self
                    .nodes
                    .get(&cursor)
                    .map(|n| n.workers.is_empty() && n.children.is_empty())
                    .unwrap_or(false);
                if !prunable {
                    return;
                }
                if let Some(p) = self.nodes.get_mut(&parent) {
                    p.children.remove(&block_hash);
                }
                if let Some(set) = self.by_hash.get_mut(&block_hash) {
                    set.remove(&cursor);
                    if set.is_empty() {
                        self.by_hash.remove(&block_hash);
                    }
                }
                self.nodes.remove(&cursor);
                cursor = parent;
                let parent_prunable = self
                    .nodes
                    .get(&cursor)
                    .map(|n| cursor != ROOT && n.workers.is_empty() && n.children.is_empty())
                    .unwrap_or(false);
                if !parent_prunable {
                    return;
                }
            }
        }
    }
}

// --- tests ------------------------------------------------------------------

#[tokio::test]
async fn equivalent_on_hole_free_placement() {
    // Two workers with nested prefixes of the same chain — leaves-first eviction
    // means every worker holds a contiguous prefix, so there are no holes.
    let mut tree = oracle::HashTree::new();
    let mut mem = MemBackend::default();

    // w-long holds all 4 blocks; w-short holds the first 2.
    tree.insert("w-long", &CHAIN);
    tree.insert("w-short", &CHAIN[..2]);
    mem.report("w-long", "10.0.0.1:9000", &CHAIN);
    mem.report("w-short", "10.0.0.2:9000", &CHAIN[..2]);

    // Query the full chain: deepest prefix is 4, held only by w-long.
    let (oracle_len, oracle_workers) = tree.match_prefix(&CHAIN);
    let (len, workers) = subject(&mem, &CHAIN).await;
    assert_eq!(len as usize, oracle_len);
    assert_eq!(len, 4);
    assert_eq!(workers, oracle_workers);
    assert_eq!(workers, HashSet::from(["w-long".to_string()]));

    // Query only the first 2 blocks: both workers tie at prefix 2.
    let (oracle_len, oracle_workers) = tree.match_prefix(&CHAIN[..2]);
    let (len, workers) = subject(&mem, &CHAIN[..2]).await;
    assert_eq!(len as usize, oracle_len);
    assert_eq!(workers, oracle_workers);
    assert_eq!(
        workers,
        HashSet::from(["w-long".to_string(), "w-short".to_string()])
    );
}

#[tokio::test]
async fn conservative_when_worker_drops_a_middle_block() {
    // A worker reports the whole chain, then loses one interior block. The
    // oracle keeps crediting the full prefix (its deepest node still lists the
    // worker); this implementation stops at the hole. They MUST diverge, and
    // this side MUST be the shorter (safe) one — which also proves the oracle is
    // an independent reference, not a copy of the code under test.
    let hole = CHAIN[1];

    let mut tree = oracle::HashTree::new();
    tree.insert("w1", &CHAIN);
    tree.remove("w1", &[hole]);

    let mut mem = MemBackend::default();
    mem.report("w1", "10.0.0.1:9000", &CHAIN);
    mem.revoke("w1", &[hole]);

    let (oracle_len, oracle_workers) = tree.match_prefix(&CHAIN);
    let (len, _workers) = subject(&mem, &CHAIN).await;

    assert_eq!(oracle_len, 4, "oracle walks through the hole");
    assert_eq!(oracle_workers, HashSet::from(["w1".to_string()]));
    assert_eq!(len, 1, "implementation stops at the hole");
    assert!(
        (len as usize) < oracle_len,
        "implementation must be the conservative side"
    );
}
