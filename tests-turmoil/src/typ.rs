//! Type configuration for turmoil tests.

use std::io::Cursor;

use serde::Deserialize;
use serde::Serialize;

pub type NodeId = u64;

/// Node information including network address.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Node {
    pub addr: String,
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.addr)
    }
}

/// Client request - a simple key-value write operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub client_id: String,
    pub serial: u64,
    pub key: String,
    pub value: String,
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Request{{client:{}, serial:{}, key:{}, value:{}}}",
            self.client_id, self.serial, self.key, self.value
        )
    }
}

/// Client response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Response {
    pub value: Option<String>,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        Node = Node,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type Vote = openraft::Vote<TypeConfig>;
pub type LogId = openraft::LogId<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;
pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
pub type Snapshot = openraft::storage::Snapshot<TypeConfig>;
pub type StoredMembership = openraft::StoredMembership<TypeConfig>;

pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;
pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;
pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;
pub type SnapshotResponse = openraft::raft::SnapshotResponse<TypeConfig>;
pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;

pub type SnapshotData = Cursor<Vec<u8>>;
pub type RaftMetrics = openraft::RaftMetrics<TypeConfig>;
pub type RaftStateSnapshot = openraft::RaftStateSnapshot<TypeConfig>;
