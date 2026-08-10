// SPDX-FileCopyrightText: Copyright (c) 2026 The SGLang Authors
// SPDX-License-Identifier: Apache-2.0

//! SGLang KV Indexer: a gRPC service that tracks externally-managed KV cache
//! block placements (as reported by inference engines such as SGLang HiCache)
//! and answers placement-match queries for KV-aware routing.

pub mod bridge;
pub mod client;

pub mod pb {
    tonic::include_proto!("kv_indexer.v1");
}

mod memory_backend;
mod service;
mod shutdown;

pub use client::{
    GrpcPrefixIndex, PrefixIndex, PrefixIndexConfig, PrefixIndexError, PrefixMatch, PrefixOutcome,
};
pub use memory_backend::InMemoryKvIndexerBackend;
pub use service::{
    component_bit, BlockComponents, KvIndexerBackend, KvIndexerService, WorkerPrefixInput,
    COMPONENT_FULL, COMPONENT_MAMBA, COMPONENT_SWA,
};
pub use shutdown::shutdown_signal;
