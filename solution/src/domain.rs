use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::time::{Duration, SystemTime};

use async_channel::Sender;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// You do not have to provide any implementation of this trait.
#[async_trait::async_trait]
pub trait StableStorage: Send + Sync {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;
    async fn get(&self, key: &str) -> Option<Vec<u8>>;
}

// You do not have to provide any implementation of this trait.
#[async_trait::async_trait]
pub trait RaftSender: Send + Sync {
    async fn send(&self, target: &Uuid, msg: RaftMessage);
}

// You do not have to provide any implementation of this trait.
#[async_trait::async_trait]
pub trait StateMachine: Send + Sync {
    /// Initializes the state machine from a serialized state.
    async fn initialize(&mut self, state: &[u8]);
    /// Applies a command to the state machine.
    async fn apply(&mut self, command: &[u8]) -> Vec<u8>;
    /// Serializes the state machine so that it can be snapshotted.
    async fn serialize(&self) -> Vec<u8>;
}

pub struct ServerConfig {
    pub self_id: Uuid,
    /// The range from which election timeout should be randomly chosen.
    pub election_timeout_range: RangeInclusive<Duration>,
    /// Periodic heartbeat interval.
    pub heartbeat_timeout: Duration,
    /// Initial cluster configuration.
    pub servers: HashSet<Uuid>,
    /// Maximum number of log entries that can be sent in one AppendEntries message.
    pub append_entries_batch_size: usize,
    /// Maximum number of snapshot bytes that can be sent in one InstallSnapshot message.
    pub snapshot_chunk_size: usize,
    /// Number of catch up round when adding a server to the cluster.
    pub catch_up_rounds: u64,
    /// The duration since last activity after which a client session should be expired.
    pub session_expiration: Duration,
}

pub struct ClientRequest {
    pub reply_to: Sender<ClientRequestResponse>,
    pub content: ClientRequestContent,
}

pub enum ClientRequestContent {
    /// Apply a command to the state machine.
    Command {
        command: Vec<u8>,
        client_id: Uuid,
        sequence_num: u64,
        lowest_sequence_num_without_response: u64,
    },
    /// Create a snapshot of the current state of the state machine.
    Snapshot,
    /// Add a server to the cluster.
    AddServer { new_server: Uuid },
    /// Remove a server from the cluster.
    RemoveServer { old_server: Uuid },
    /// Open a new client session.
    RegisterClient,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientRequestResponse {
    CommandResponse(CommandResponseArgs),
    SnapshotResponse(SnapshotResponseArgs),
    AddServerResponse(AddServerResponseArgs),
    RemoveServerResponse(RemoveServerResponseArgs),
    RegisterClientResponse(RegisterClientResponseArgs),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandResponseArgs {
    pub client_id: Uuid,
    pub sequence_num: u64,
    pub content: CommandResponseContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandResponseContent {
    CommandApplied { output: Vec<u8> },
    NotLeader { leader_hint: Option<Uuid> },
    SessionExpired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotResponseArgs {
    pub content: SnapshotResponseContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotResponseContent {
    SnapshotCreated { last_included_index: usize },
    NothingToSnapshot { last_included_index: usize },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddServerResponseArgs {
    pub new_server: Uuid,
    pub content: AddServerResponseContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AddServerResponseContent {
    ServerAdded,
    NotLeader { leader_hint: Option<Uuid> },
    Timeout,
    AddInProgress,
    AlreadyPresent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoveServerResponseArgs {
    pub old_server: Uuid,
    pub content: RemoveServerResponseContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoveServerResponseContent {
    ServerRemoved,
    NotLeader { leader_hint: Option<Uuid> },
    RemoveInProgress,
    NotPresent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterClientResponseArgs {
    pub content: RegisterClientResponseContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterClientResponseContent {
    ClientRegistered { client_id: Uuid },
    NotLeader { leader_hint: Option<Uuid> },
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RaftMessage {
    pub header: RaftMessageHeader,
    pub content: RaftMessageContent,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RaftMessageHeader {
    pub source: Uuid,
    pub term: u64,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum RaftMessageContent {
    AppendEntries(AppendEntriesArgs),
    AppendEntriesResponse(AppendEntriesResponseArgs),
    RequestVote(RequestVoteArgs),
    RequestVoteResponse(RequestVoteResponseArgs),
    InstallSnapshot(InstallSnapshotArgs),
    InstallSnapshotResponse(InstallSnapshotResponseArgs),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct AppendEntriesArgs {
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: usize,
}

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub content: LogEntryContent,
    pub term: u64,
    pub timestamp: SystemTime,
}

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum LogEntryContent {
    Command {
        data: Vec<u8>,
        client_id: Uuid,
        sequence_num: u64,
        lowest_sequence_num_without_response: u64,
    },
    Configuration {
        servers: HashSet<Uuid>,
    },
    RegisterClient,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct AppendEntriesResponseArgs {
    pub success: bool,
    pub last_log_index: usize,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RequestVoteArgs {
    pub last_log_index: usize,
    pub last_log_term: u64,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RequestVoteResponseArgs {
    pub vote_granted: bool,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct InstallSnapshotArgs {
    pub last_included_index: usize,
    pub last_included_term: u64,
    pub last_config: Option<HashSet<Uuid>>,
    pub client_sessions: Option<HashMap<Uuid, ClientSession>>,
    pub offset: usize,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientSession {
    pub last_activity: SystemTime,
    pub responses: HashMap<u64, Vec<u8>>,
    pub lowest_sequence_num_without_response: u64,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct InstallSnapshotResponseArgs {
    pub last_included_index: usize,
    pub offset: usize,
}
