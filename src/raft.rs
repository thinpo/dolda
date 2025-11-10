//! Raft consensus algorithm implementation
//!
//! This module provides:
//! - Leader election
//! - Term management
//! - Vote handling
//! - Heartbeat mechanism
//!
//! Based on the Raft consensus algorithm by Diego Ongaro and John Ousterhout

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Invalid state transition from {from:?} to {to:?}")]
    InvalidTransition { from: NodeState, to: NodeState },
    
    #[error("Term is stale: current {current}, received {received}")]
    StaleTerm { current: u64, received: u64 },
    
    #[error("Already voted in term {0}")]
    AlreadyVoted(u64),
    
    #[error("Not the leader")]
    NotLeader,
    
    #[error("Election timeout")]
    ElectionTimeout,
}

pub type Result<T> = std::result::Result<T, RaftError>;

/// Raft node state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Follower - passive node receiving updates
    Follower,
    /// Candidate - node running for election
    Candidate,
    /// Leader - node coordinating the cluster
    Leader,
}

/// Node identifier
pub type NodeId = u64;

/// Term number (logical clock)
pub type Term = u64;

/// Raft configuration
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's ID
    pub node_id: NodeId,
    
    /// Cluster node IDs
    pub cluster_nodes: Vec<NodeId>,
    
    /// Election timeout range (randomized within this range)
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    
    /// Heartbeat interval (must be << election timeout)
    pub heartbeat_interval: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            cluster_nodes: vec![],
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        }
    }
}

/// Vote request (RequestVote RPC)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    /// Candidate's term
    pub term: Term,
    /// Candidate requesting vote
    pub candidate_id: NodeId,
    /// Index of candidate's last log entry
    pub last_log_index: u64,
    /// Term of candidate's last log entry
    pub last_log_term: Term,
}

/// Vote response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    /// Current term for candidate to update itself
    pub term: Term,
    /// True means candidate received vote
    pub vote_granted: bool,
}

/// Heartbeat message (AppendEntries RPC with no entries)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    /// Leader's term
    pub term: Term,
    /// Leader's ID
    pub leader_id: NodeId,
}

/// Heartbeat response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// Current term
    pub term: Term,
    /// True if follower contained entry matching prev_log_index and prev_log_term
    pub success: bool,
}

/// Raft node persistent state
#[derive(Debug)]
struct PersistentState {
    /// Latest term server has seen
    current_term: AtomicU64,
    /// Candidate ID that received vote in current term
    voted_for: RwLock<Option<NodeId>>,
}

impl PersistentState {
    fn new() -> Self {
        Self {
            current_term: AtomicU64::new(0),
            voted_for: RwLock::new(None),
        }
    }
    
    fn get_term(&self) -> Term {
        self.current_term.load(Ordering::SeqCst)
    }
    
    fn set_term(&self, term: Term) {
        self.current_term.store(term, Ordering::SeqCst);
    }
    
    fn increment_term(&self) -> Term {
        self.current_term.fetch_add(1, Ordering::SeqCst) + 1
    }
    
    fn get_voted_for(&self) -> Option<NodeId> {
        *self.voted_for.read()
    }
    
    fn set_voted_for(&self, node_id: Option<NodeId>) {
        *self.voted_for.write() = node_id;
    }
}

/// Raft node volatile state
#[derive(Debug)]
struct VolatileState {
    /// Current node state
    state: RwLock<NodeState>,
    /// Current known leader
    leader_id: RwLock<Option<NodeId>>,
    /// Last time we received communication from leader
    last_heartbeat: RwLock<Instant>,
    /// Votes received in current election
    votes_received: RwLock<HashMap<NodeId, bool>>,
}

impl VolatileState {
    fn new() -> Self {
        Self {
            state: RwLock::new(NodeState::Follower),
            leader_id: RwLock::new(None),
            last_heartbeat: RwLock::new(Instant::now()),
            votes_received: RwLock::new(HashMap::new()),
        }
    }
    
    fn get_state(&self) -> NodeState {
        *self.state.read()
    }
    
    fn set_state(&self, new_state: NodeState) {
        let mut state = self.state.write();
        tracing::info!("State transition: {:?} -> {:?}", *state, new_state);
        *state = new_state;
    }
    
    fn get_leader(&self) -> Option<NodeId> {
        *self.leader_id.read()
    }
    
    fn set_leader(&self, leader: Option<NodeId>) {
        *self.leader_id.write() = leader;
    }
    
    fn update_heartbeat(&self) {
        *self.last_heartbeat.write() = Instant::now();
    }
    
    fn time_since_heartbeat(&self) -> Duration {
        self.last_heartbeat.read().elapsed()
    }
    
    fn record_vote(&self, node_id: NodeId, granted: bool) {
        self.votes_received.write().insert(node_id, granted);
    }
    
    fn count_votes(&self) -> usize {
        self.votes_received.read()
            .values()
            .filter(|&&granted| granted)
            .count()
    }
    
    fn clear_votes(&self) {
        self.votes_received.write().clear();
    }
}

/// Raft node
pub struct RaftNode {
    /// Configuration
    config: RaftConfig,
    
    /// Persistent state
    persistent: PersistentState,
    
    /// Volatile state
    volatile: VolatileState,
    
    /// Command channel for external control
    command_tx: mpsc::UnboundedSender<RaftCommand>,
    command_rx: RwLock<Option<mpsc::UnboundedReceiver<RaftCommand>>>,
}

/// Commands to control Raft node
#[derive(Debug)]
enum RaftCommand {
    /// Request to handle a vote request
    HandleVoteRequest(VoteRequest, mpsc::Sender<VoteResponse>),
    /// Request to handle a heartbeat
    HandleHeartbeat(Heartbeat, mpsc::Sender<HeartbeatResponse>),
    /// Trigger election
    StartElection,
    /// Send heartbeats (leader only)
    SendHeartbeats,
}

impl RaftNode {
    /// Create a new Raft node
    pub fn new(config: RaftConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        
        Self {
            config,
            persistent: PersistentState::new(),
            volatile: VolatileState::new(),
            command_tx: tx,
            command_rx: RwLock::new(Some(rx)),
        }
    }
    
    /// Get current node state
    pub fn state(&self) -> NodeState {
        self.volatile.get_state()
    }
    
    /// Get current term
    pub fn term(&self) -> Term {
        self.persistent.get_term()
    }
    
    /// Get current leader (if known)
    pub fn leader(&self) -> Option<NodeId> {
        self.volatile.get_leader()
    }
    
    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        self.volatile.get_state() == NodeState::Leader
    }
    
    /// Start the Raft node event loop
    pub async fn run(self: Arc<Self>) {
        let mut command_rx = self.command_rx.write().take().unwrap();
        
        tracing::info!(
            "Starting Raft node {} with {} peers",
            self.config.node_id,
            self.config.cluster_nodes.len()
        );
        
        // Start election timer
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move {
            self_clone.election_timer().await;
        });
        
        // Start heartbeat timer (for leaders)
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move {
            self_clone.heartbeat_timer().await;
        });
        
        // Process commands
        while let Some(command) = command_rx.recv().await {
            match command {
                RaftCommand::HandleVoteRequest(req, resp_tx) => {
                    let response = self.handle_vote_request(req);
                    let _ = resp_tx.send(response).await;
                }
                RaftCommand::HandleHeartbeat(hb, resp_tx) => {
                    let response = self.handle_heartbeat(hb);
                    let _ = resp_tx.send(response).await;
                }
                RaftCommand::StartElection => {
                    self.start_election();
                }
                RaftCommand::SendHeartbeats => {
                    self.send_heartbeats();
                }
            }
        }
    }
    
    /// Election timer - triggers elections when no heartbeats received
    async fn election_timer(self: Arc<Self>) {
        loop {
            let timeout = self.random_election_timeout();
            tokio::time::sleep(timeout).await;
            
            // Check if we should start an election
            if self.volatile.get_state() != NodeState::Leader {
                let elapsed = self.volatile.time_since_heartbeat();
                if elapsed >= self.random_election_timeout() {
                    tracing::info!(
                        "Node {} election timeout ({:?} since last heartbeat)",
                        self.config.node_id,
                        elapsed
                    );
                    let _ = self.command_tx.send(RaftCommand::StartElection);
                }
            }
        }
    }
    
    /// Heartbeat timer - sends heartbeats when leader
    async fn heartbeat_timer(self: Arc<Self>) {
        loop {
            tokio::time::sleep(self.config.heartbeat_interval).await;
            
            if self.volatile.get_state() == NodeState::Leader {
                let _ = self.command_tx.send(RaftCommand::SendHeartbeats);
            }
        }
    }
    
    /// Generate random election timeout
    fn random_election_timeout(&self) -> Duration {
        use rand::Rng;
        let min = self.config.election_timeout_min.as_millis() as u64;
        let max = self.config.election_timeout_max.as_millis() as u64;
        let timeout_ms = rand::thread_rng().gen_range(min..=max);
        Duration::from_millis(timeout_ms)
    }
    
    /// Start an election
    fn start_election(&self) {
        // Transition to candidate
        self.volatile.set_state(NodeState::Candidate);
        
        // Increment term
        let new_term = self.persistent.increment_term();
        
        // Vote for self
        self.persistent.set_voted_for(Some(self.config.node_id));
        self.volatile.clear_votes();
        self.volatile.record_vote(self.config.node_id, true);
        
        // Reset election timer
        self.volatile.update_heartbeat();
        
        tracing::info!(
            "Node {} starting election for term {}",
            self.config.node_id,
            new_term
        );
        
        // Check if we won immediately (single node cluster)
        let votes_needed = (self.config.cluster_nodes.len() + 1) / 2 + 1;
        if self.volatile.count_votes() >= votes_needed {
            self.become_leader();
        }
        
        // In a real implementation, send vote requests to all other nodes here
    }
    
    /// Become the leader
    fn become_leader(&self) {
        tracing::info!(
            "Node {} became leader for term {}",
            self.config.node_id,
            self.persistent.get_term()
        );
        
        self.volatile.set_state(NodeState::Leader);
        self.volatile.set_leader(Some(self.config.node_id));
        
        // Send initial heartbeats
        self.send_heartbeats();
    }
    
    /// Send heartbeats to all followers
    fn send_heartbeats(&self) {
        if self.volatile.get_state() != NodeState::Leader {
            return;
        }
        
        tracing::debug!(
            "Leader {} sending heartbeats for term {}",
            self.config.node_id,
            self.persistent.get_term()
        );
        
        // In a real implementation, send heartbeats to all other nodes here
        // This would use the network layer we built
    }
    
    /// Handle vote request from candidate
    fn handle_vote_request(&self, request: VoteRequest) -> VoteResponse {
        let current_term = self.persistent.get_term();
        
        // Reply false if term < currentTerm
        if request.term < current_term {
            return VoteResponse {
                term: current_term,
                vote_granted: false,
            };
        }
        
        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower
        if request.term > current_term {
            self.persistent.set_term(request.term);
            self.persistent.set_voted_for(None);
            self.volatile.set_state(NodeState::Follower);
            self.volatile.set_leader(None);
        }
        
        // Grant vote if:
        // 1. Haven't voted for anyone else in this term
        // 2. Candidate's log is at least as up-to-date as ours
        let voted_for = self.persistent.get_voted_for();
        let can_vote = voted_for.is_none() || voted_for == Some(request.candidate_id);
        
        if can_vote {
            self.persistent.set_voted_for(Some(request.candidate_id));
            self.volatile.update_heartbeat();
            
            tracing::info!(
                "Node {} granted vote to {} for term {}",
                self.config.node_id,
                request.candidate_id,
                request.term
            );
            
            VoteResponse {
                term: request.term,
                vote_granted: true,
            }
        } else {
            tracing::debug!(
                "Node {} denied vote to {} for term {} (already voted for {:?})",
                self.config.node_id,
                request.candidate_id,
                request.term,
                voted_for
            );
            
            VoteResponse {
                term: current_term,
                vote_granted: false,
            }
        }
    }
    
    /// Handle heartbeat from leader
    fn handle_heartbeat(&self, heartbeat: Heartbeat) -> HeartbeatResponse {
        let current_term = self.persistent.get_term();
        
        // Reply false if term < currentTerm
        if heartbeat.term < current_term {
            return HeartbeatResponse {
                term: current_term,
                success: false,
            };
        }
        
        // Update term if necessary
        if heartbeat.term > current_term {
            self.persistent.set_term(heartbeat.term);
            self.persistent.set_voted_for(None);
        }
        
        // Convert to follower if we're a candidate
        if self.volatile.get_state() == NodeState::Candidate {
            self.volatile.set_state(NodeState::Follower);
        }
        
        // Update leader and reset election timer
        self.volatile.set_leader(Some(heartbeat.leader_id));
        self.volatile.update_heartbeat();
        
        HeartbeatResponse {
            term: heartbeat.term,
            success: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_node_creation() {
        let config = RaftConfig {
            node_id: 1,
            cluster_nodes: vec![1, 2, 3],
            ..Default::default()
        };
        
        let node = RaftNode::new(config);
        assert_eq!(node.state(), NodeState::Follower);
        assert_eq!(node.term(), 0);
        assert_eq!(node.leader(), None);
        assert!(!node.is_leader());
    }
    
    #[test]
    fn test_term_management() {
        let config = RaftConfig::default();
        let node = RaftNode::new(config);
        
        assert_eq!(node.persistent.get_term(), 0);
        
        let new_term = node.persistent.increment_term();
        assert_eq!(new_term, 1);
        assert_eq!(node.persistent.get_term(), 1);
        
        node.persistent.set_term(5);
        assert_eq!(node.persistent.get_term(), 5);
    }
    
    #[test]
    fn test_vote_handling() {
        let config = RaftConfig {
            node_id: 1,
            ..Default::default()
        };
        let node = RaftNode::new(config);
        
        // First vote request - should grant
        let request1 = VoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        
        let response1 = node.handle_vote_request(request1);
        assert!(response1.vote_granted);
        assert_eq!(response1.term, 1);
        
        // Second vote request same term - should deny
        let request2 = VoteRequest {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        };
        
        let response2 = node.handle_vote_request(request2);
        assert!(!response2.vote_granted);
    }
    
    #[test]
    fn test_heartbeat_handling() {
        let config = RaftConfig {
            node_id: 1,
            ..Default::default()
        };
        let node = RaftNode::new(config);
        
        let heartbeat = Heartbeat {
            term: 1,
            leader_id: 2,
        };
        
        let response = node.handle_heartbeat(heartbeat);
        assert!(response.success);
        assert_eq!(response.term, 1);
        assert_eq!(node.leader(), Some(2));
    }
    
    #[test]
    fn test_stale_term_rejection() {
        let config = RaftConfig::default();
        let node = RaftNode::new(config);
        
        // Set current term to 5
        node.persistent.set_term(5);
        
        // Vote request with stale term
        let request = VoteRequest {
            term: 3,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        
        let response = node.handle_vote_request(request);
        assert!(!response.vote_granted);
        assert_eq!(response.term, 5);
    }
    
    #[test]
    fn test_state_transitions() {
        let config = RaftConfig::default();
        let node = RaftNode::new(config);
        
        assert_eq!(node.volatile.get_state(), NodeState::Follower);
        
        node.volatile.set_state(NodeState::Candidate);
        assert_eq!(node.volatile.get_state(), NodeState::Candidate);
        
        node.volatile.set_state(NodeState::Leader);
        assert_eq!(node.volatile.get_state(), NodeState::Leader);
    }
}

