use std::time::SystemTime;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use executor::{Handler, ModuleRef, System};

pub use domain::*;

mod domain;

const STATE_KEY: &str = "STATE";
const DEFAULT_DURATION: u64 = 10000;

pub struct Raft {
    // TODO you can add fields to this struct.
    state:          ProcessState,
    config:         ServerConfig,
    storage:        Box<dyn StableStorage>,
    sender:         Box<dyn RaftSender>,
    process_type:   ProcessType,
    timer_abort:    Arc<AtomicBool>,
    self_ref:       Option<ModuleRef<Self>>,

    election_timeout: Duration,

}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system:                     &mut System,
        config:                     ServerConfig,
        first_log_entry_timestamp:  SystemTime,
        state_machine:              Box<dyn StateMachine>,
        stable_storage:             Box<dyn StableStorage>,
        message_sender:             Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {

        let state = match stable_storage.get(STATE_KEY).await {
            Some(data) => bincode::deserialize(&data[..]).unwrap(),
            None => ProcessState {
                current_term: 0,
                voted_for: None,
                leader_id: None,
            }
        };

        let self_ref = system
            .register_module(Self {
                state:          state,
                config:         config,
                storage:        stable_storage,
                sender:         message_sender,
                process_type:   ProcessType::Follower,
                timer_abort:    Arc::new(AtomicBool::new(false)),
                self_ref:       None,

                election_timeout: Duration::from_millis(DEFAULT_DURATION),
            })
            .await;
        self_ref
            .send(Init {
                self_ref: self_ref.clone(),
            })
            .await;
        self_ref
    }

    fn reset_timer(&mut self, interval: Duration) {
        self.timer_abort.store(true, Ordering::Relaxed);
        self.timer_abort = Arc::new(AtomicBool::new(false));
        tokio::spawn(run_timer(
            self.self_ref.as_ref().unwrap().clone(),
            interval,
            self.timer_abort.clone(),
        ));
    }

    /// Set the process's term to the higher number.
    fn update_term(&mut self, new_term: u64) {
        assert!(self.state.current_term < new_term);
        self.state.current_term = new_term;
        self.state.voted_for = None;
        self.state.leader_id = None;
        // No reliable state update called here, must be called separately.
    }

    /// Reliably save the state.
    async fn update_state(&mut self) {
        self.storage.put(STATE_KEY, &bincode::serialize(&self.state).unwrap()[..]).await.unwrap();
    }

    /// Broadcast a message
    async fn broadcast(&mut self, content: RaftMessageContent) {
        for id in &self.config.servers {
            self.sender
            .send(
                &id,
                RaftMessage {
                    header: RaftMessageHeader {
                        source: self.config.self_id,
                        term:   self.state.current_term,
                    },
                    content: content.clone()
                },
            )
            .await;
        }
        
        
    }

    /// Send a message
    async fn send(&self, target: &Uuid, content: RaftMessageContent) {
        self.sender
            .send(
                target, 
                RaftMessage {
                    header: RaftMessageHeader {
                        source: self.config.self_id,
                        term:   self.state.current_term,
                    },
                    content: content
                })
            .await;
        
    }

    /// Initialize the vote
    async fn init_vote(&mut self) {

        self.update_term(self.state.current_term + 1);
        {self.update_state().await;}
        self.reset_timer(self.election_timeout);

        let mut votes = HashSet::new();
        votes.insert(self.config.self_id);
        self.process_type = ProcessType::Candidate { votes_received: votes };
        
        /*
        if self.config.processes_count == 1 {
            self.process_type = ProcessType::Leader;
            //self.self_ref.as_ref().unwrap().send(RunHeartbeat).await;
            
        } else */
        {
            self.broadcast(
                RaftMessageContent::RequestVote (
                    RequestVoteArgs {
                        last_log_index: todo!(),
                        last_log_term:  todo!(),
                    }
                ),
            )
            .await;
        }
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) {
        match msg.content {
            RaftMessageContent::AppendEntries(_)            => todo!(),
            RaftMessageContent::AppendEntriesResponse(_)    => todo!(),
            RaftMessageContent::RequestVote(_)              => todo!(),
            RaftMessageContent::RequestVoteResponse(_)      => todo!(),

            RaftMessageContent::InstallSnapshot(_)
                => unimplemented!("Snapshots omitted"),

            RaftMessageContent::InstallSnapshotResponse(_)
                => unimplemented!("Snapshots omitted"),
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, msg: ClientRequest) {
        match msg.content {
            ClientRequestContent::Command { .. }      => todo!(),
            ClientRequestContent::Snapshot            => unimplemented!("Snapshots omitted"),
            ClientRequestContent::AddServer { .. }    => unimplemented!("Cluster membership changes omitted"),
            ClientRequestContent::RemoveServer { .. } => unimplemented!("Cluster membership changes omitted"),
            ClientRequestContent::RegisterClient      => todo!(),
        }
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.

/// State of a Raft process.
/// It shall be kept in stable storage, and updated before replying to messages.
#[derive(Default, Clone, Copy, Deserialize, Serialize)]
pub(crate) struct ProcessState {

    /// Number of the current term. `0` at boot.
    pub(crate) current_term: u64,

    /// Identifier of a process which has received this process' vote.
    /// `None if this process has not voted in this term.
    voted_for: Option<Uuid>,

    /// Identifier of a process which is thought to be the leader.
    leader_id: Option<Uuid>,
}

/// State of a Raft process with a corresponding (volatile) information.
enum ProcessType {
    Follower,
    Candidate { votes_received: HashSet<Uuid> },
    Leader,
}

struct Timeout;

struct Init {
    self_ref: ModuleRef<Raft>,
}

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, msg: Init) {
        
        self.self_ref = Some(msg.self_ref);
        self.reset_timer(self.election_timeout);
        
    }
}

async fn run_timer(raft_ref: ModuleRef<Raft>, interval: Duration, abort: Arc<AtomicBool>) {
    let mut interval = tokio::time::interval(interval);
    interval.tick().await;
    interval.tick().await;
    while !abort.load(Ordering::Relaxed) {
        raft_ref.send(Timeout).await;
        interval.tick().await;
    }
}

/// Handle timer timeout.
#[async_trait::async_trait]
impl Handler<Timeout> for Raft {
    async fn handle(&mut self, _: Timeout) {
        
        match &mut self.process_type {
            ProcessType::Follower {}        => { self.init_vote().await; }
            ProcessType::Candidate { .. }   => { self.init_vote().await; }
            ProcessType::Leader             => { 

                /* The handler for `RunHeartbeat` sends exactly 10 
                    * heartbeats and stops. The `RunHeartbeat` will be resent 
                    * on every timeout (if the process is till a leader)
                    */
                //self.self_ref.as_ref().unwrap().send(RunHeartbeat).await;
            }
        }
        
    }
}

