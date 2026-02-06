//! Random client for chaos testing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::time::sleep;
use turmoil::net::TcpStream;

use crate::cluster::ClusterInfo;
use crate::network::RpcType;
use crate::typ::*;

/// Client request to be sent to a Raft node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientWriteRequest {
    pub request: Request,
}

/// Client response from a Raft node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientWriteResponse {
    Success(Response),
    NotLeader { leader_id: Option<NodeId> },
    Error(String),
}

/// A random client that sends requests to the cluster.
pub struct RandomClient {
    rng: StdRng,
    client_id: String,
    serial: u64,
    cluster: ClusterInfo,
    /// Track which node we think is leader.
    leader_hint: Option<NodeId>,
}

impl RandomClient {
    pub fn new(seed: u64, cluster: ClusterInfo) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            client_id: format!("client-{}", seed),
            serial: 0,
            cluster,
            leader_hint: Some(1), // Start with node 1 as initial guess
        }
    }

    /// Generate a random write request.
    pub fn gen_request(&mut self) -> Request {
        self.serial += 1;
        Request {
            client_id: self.client_id.clone(),
            serial: self.serial,
            key: format!("key-{}", self.rng.r#gen::<u32>() % 100),
            value: format!("value-{}", self.rng.r#gen::<u64>()),
        }
    }

    /// Try to send a request to the cluster.
    /// Will follow leader redirects.
    pub async fn send_request(&mut self, req: Request) -> Result<Response, ClientError> {
        let mut attempts = 0;
        let max_attempts = 5;

        loop {
            attempts += 1;
            if attempts > max_attempts {
                return Err(ClientError::TooManyRedirects);
            }

            // Pick a node to try
            let node_id = self.leader_hint.unwrap_or_else(|| {
                let node_ids: Vec<_> = self.cluster.nodes.keys().cloned().collect();
                let idx = self.rng.r#gen::<usize>() % node_ids.len();
                node_ids[idx]
            });

            let addr = self.cluster.addr(node_id);

            match self.send_to_node(addr, &req).await {
                Ok(ClientWriteResponse::Success(resp)) => {
                    return Ok(resp);
                }
                Ok(ClientWriteResponse::NotLeader { leader_id }) => {
                    self.leader_hint = leader_id;
                    sleep(Duration::from_millis(10)).await;
                }
                Ok(ClientWriteResponse::Error(e)) => {
                    return Err(ClientError::RaftError(e));
                }
                Err(e) => {
                    tracing::warn!("Failed to reach node {}: {}", node_id, e);
                    self.leader_hint = None; 
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    async fn send_to_node(&self, addr: &str, req: &Request) -> Result<ClientWriteResponse, ClientError> {
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?;

        let write_req = ClientWriteRequest { request: req.clone() };
        let payload = bincode::serialize(&write_req)
            .map_err(|e| ClientError::SerializationError(e.to_string()))?;

        stream.write_u8(RpcType::ClientWrite as u8).await?;
        stream.write_u32(payload.len() as u32).await?;
        stream.write_all(&payload).await?;

        let resp_len = stream.read_u32().await?;
        let mut resp_buf = vec![0u8; resp_len as usize];
        stream.read_exact(&mut resp_buf).await?;

        bincode::deserialize(&resp_buf).map_err(|e| ClientError::SerializationError(e.to_string()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Too many redirects")]
    TooManyRedirects,
    #[error("Raft error: {0}")]
    RaftError(String),
}

/// Record of a client operation for linearizability checking.
#[derive(Debug, Clone)]
pub struct Operation {
    pub client_id: String,
    pub serial: u64,
    pub request: Request,
    pub start_time: u64,
    pub end_time: Option<u64>,
    pub result: OperationResult,
}

#[derive(Debug, Clone)]
pub enum OperationResult {
    Pending,
    Success(Response),
    Failed(String),
}

/// History of all operations for invariant checking.
#[derive(Default)]
pub struct OperationHistory {
    pub operations: Vec<Operation>,
    pub committed_values: HashMap<String, Vec<(u64, String)>>, // key -> [(serial, value)]
}

impl OperationHistory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_start(&mut self, req: &Request, time: u64) -> usize {
        let idx = self.operations.len();
        self.operations.push(Operation {
            client_id: req.client_id.clone(),
            serial: req.serial,
            request: req.clone(),
            start_time: time,
            end_time: None,
            result: OperationResult::Pending,
        });
        idx
    }

    pub fn record_success(&mut self, idx: usize, resp: Response, time: u64) {
        if let Some(op) = self.operations.get_mut(idx) {
            op.end_time = Some(time);
            op.result = OperationResult::Success(resp);

            self.committed_values
                .entry(op.request.key.clone())
                .or_default()
                .push((op.serial, op.request.value.clone()));
        }
    }

    pub fn record_failure(&mut self, idx: usize, error: String, time: u64) {
        if let Some(op) = self.operations.get_mut(idx) {
            op.end_time = Some(time);
            op.result = OperationResult::Failed(error);
        }
    }
}

/// Run a chaos client that sends random requests.
pub async fn run_chaos_client(
    seed: u64,
    cluster: ClusterInfo,
    num_requests: usize,
    history: Arc<Mutex<OperationHistory>>,
) {
    let mut client = RandomClient::new(seed, cluster);
    let mut time = 0u64;

    for _ in 0..num_requests {
        let req = client.gen_request();
        time += 1;

        let idx = {
            let mut hist = history.lock().await;
            hist.record_start(&req, time)
        };

        match client.send_request(req).await {
            Ok(resp) => {
                time += 1;
                let mut hist = history.lock().await;
                hist.record_success(idx, resp, time);
            }
            Err(e) => {
                time += 1;
                let mut hist = history.lock().await;
                hist.record_failure(idx, e.to_string(), time);
            }
        }

        let delay = client.rng.gen_range(10..100);
        sleep(Duration::from_millis(delay)).await;
    }
}
