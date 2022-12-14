use std::ops::RangeInclusive;
use std::time::Duration;
use std::collections::{HashSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use async_channel::Sender;
use rand::Rng;

use executor::ModuleRef;

use crate::domain::*;
use crate::Raft;


/// State of a Raft process.
/// It shall be kept in stable storage, and updated before replying to messages.
#[derive(Clone, Deserialize, Serialize)]
pub struct PersistentState {

    /// Number of the current term. `0` at boot.
    pub(crate) current_term: u64,

    pub(crate) vote: Vote,
    
    pub(crate) log: Vec<LogEntry>,
}

pub struct VolatileState {
    pub(crate) commit_index: usize,
    pub(crate) last_applied: usize,
    
    pub(crate) process_type: ProcessType,
    pub(crate) leader_data: Option<LeaderData>
}

#[derive(Clone, Copy, Deserialize, Serialize)]
pub enum Vote {
    NoVote,
    VotedFor(Uuid),
    Leader(Uuid),
}

/// State of a Raft process with a corresponding (volatile) information.
#[derive(Debug)]
pub enum ProcessType {
    Follower,
    Candidate { votes_received: HashSet<Uuid> },
    Leader,
}

#[derive(Debug)]
pub struct LeaderData {
    pub(crate) next_index:  HashMap<Uuid, usize>,
    pub(crate) match_index: HashMap<Uuid, usize>,

    // Number of followers that have `match_index` higher than `commit_index` of the leader
    pub(crate) num_updated_servers: usize,

    pub(crate) reply_to: HashMap<usize, Sender<ClientRequestResponse>>,
    pub(crate) successes: HashSet<Uuid>,
}

pub struct Heartbeat;

pub struct Timeout;

pub struct Init {
    pub(crate) self_ref: ModuleRef<Raft>,
}



pub(crate) async fn run_timer(
    raft_ref: ModuleRef<Raft>,
    interval_range: RangeInclusive<Duration>,
    abort: Arc<AtomicBool>,
) {
    let duration = rand::thread_rng().gen_range(interval_range.clone());
    {tokio::time::sleep(duration).await;}

    while !abort.load(Ordering::Relaxed) {
        let duration = rand::thread_rng().gen_range(interval_range.clone());
        
        raft_ref.send(Timeout).await;
        tokio::time::sleep(duration).await;
    }
}

/// Periodically send `Heartbeat` messages to a module
pub(crate) async fn run_heartbeat(
    raft_ref: ModuleRef<Raft>,
    interval: Duration,
    abort: Arc<AtomicBool>
) {
    let mut interval = tokio::time::interval(interval);
    while !abort.load(Ordering::Relaxed) {
        raft_ref.send(Heartbeat).await;
        interval.tick().await;
    }
}

pub(crate) fn from_option_deserialize<'a, T>(if_none: T, some: &'a Option<Vec<u8>>) -> T
where
    T: serde::de::Deserialize<'a>
{
    match some {
        None        => if_none,
        Some(data)  => bincode::deserialize(&data[..]).unwrap(),
    }
}