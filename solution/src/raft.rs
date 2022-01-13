use std::ops::RangeInclusive;
use std::time::{Duration, SystemTime};
use std::collections::{HashSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::cmp::min;

use uuid::Uuid;
use async_channel::Sender;

use executor::{Handler, ModuleRef, System};

use crate::domain::*;
use crate::utils::*;

const STATE_KEY: &str = "STATE";

pub struct Raft {
    // TODO you can add fields to this struct.
    persistent_state:   PersistentState,
    volatile_state:     VolatileState,
    config:             ServerConfig,
    storage:            Box<dyn StableStorage>,
    sender:             Box<dyn RaftSender>,
    timer_abort:        Arc<AtomicBool>,
    heartbeat_abort:    Arc<AtomicBool>,
    self_ref:           Option<ModuleRef<Self>>,
    state_machine:      Box<dyn StateMachine>,

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

        let first_entry = LogEntry {
            content: LogEntryContent::Configuration { servers: config.servers.clone() },
            term: 0,
            timestamp: first_log_entry_timestamp,
        };

        let persistent_state = match stable_storage.get(STATE_KEY).await {
            Some(data) => bincode::deserialize(&data[..]).unwrap(),
            None => PersistentState {
                current_term:       0,
                vote:               Vote::NoVote,
                log:                vec![first_entry],
            }
        };

        let volatile_state = VolatileState {
            commit_index:   0,
            last_applied:   0,
            
            process_type:   ProcessType::Follower,
            leader_data:    None
        };

        let self_ref = system
            .register_module(Self {
                persistent_state:   persistent_state,
                volatile_state:     volatile_state,
                config:             config,
                storage:            stable_storage,
                sender:             message_sender,
                timer_abort:        Arc::new(AtomicBool::new(false)),
                heartbeat_abort:    Arc::new(AtomicBool::new(false)),
                self_ref:           None,
                state_machine:      state_machine,
            })
            .await;
        self_ref
            .send(Init {
                self_ref: self_ref.clone(),
            })
            .await;
        self_ref
    }

    // timers -----------------------------------------------------------------
    
    fn reset_timer(&mut self, interval_range: RangeInclusive<Duration>) {
        self.timer_abort.store(true, Ordering::Relaxed);
        self.timer_abort = Arc::new(AtomicBool::new(false));
        tokio::spawn(run_timer(
            self.self_ref.as_ref().unwrap().clone(),
            interval_range,
            self.timer_abort.clone(),
        ));
    }

    fn reset_heartbeat(&mut self) {
        self.heartbeat_abort.store(true, Ordering::Relaxed);
        self.heartbeat_abort = Arc::new(AtomicBool::new(false));
        tokio::spawn(run_heartbeat(
            self.self_ref.as_ref().unwrap().clone(),
            self.config.heartbeat_timeout,
            self.heartbeat_abort.clone(),
        ));
    }

    // other utils ------------------------------------------------------------

    /// Set the process's term to the higher number.
    fn update_term(&mut self, new_term: u64) {
        assert!(self.persistent_state.current_term < new_term);
        self.persistent_state.current_term = new_term;
        self.persistent_state.vote = Vote::NoVote;
        // No reliable state update called here, must be called separately.
    }

    /// Reliably save the state.
    async fn update_state(&mut self) {
        self.storage.put(STATE_KEY, &bincode::serialize(&self.persistent_state).unwrap()[..]).await.unwrap();
    }

    /// Common message processing.
    fn msg_received(&mut self, msg: &RaftMessage) {
        if msg.header.term > self.persistent_state.current_term {
            self.update_term(msg.header.term);
            self.volatile_state.process_type = ProcessType::Follower;
            self.volatile_state.leader_data = None;
        }
    }

    // send / broadcast -------------------------------------------------------

    /// Broadcast a message
    async fn broadcast(&mut self, content: RaftMessageContent) {
        for id in &self.config.servers {

            // Don't send a message to self
            if *id == self.config.self_id { continue; }

            self.sender
            .send(
                &id,
                RaftMessage {
                    header: RaftMessageHeader {
                        source: self.config.self_id,
                        term:   self.persistent_state.current_term,
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
                        term:   self.persistent_state.current_term,
                    },
                    content: content
                })
            .await;
        
    }

    /// Broadcast an empty `AppendEntries` message to all other servers
    async fn broadcast_heartbeat(&self) {
        for id in &self.config.servers {

            // Don't send a message to self
            if *id == self.config.self_id { continue; }

            // Check if `self` is still a leader in every iteration to avoid a 
            // panic in `get_next_index` call
            match &self.volatile_state.process_type {
                ProcessType::Leader {} => {
                    let prev_index = self.get_next_index(&id) - 1;
                    self.sender
                    .send(
                        &id,
                        RaftMessage {
                            header: RaftMessageHeader {
                                source: self.config.self_id,
                                term:   self.persistent_state.current_term,
                            },

                            // TODO: what to put in `prev_log_index` and `prev_log_term`?
                            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                                prev_log_index: prev_index,
                                prev_log_term:  self.get_term(prev_index),
                                entries:        vec![],
                                leader_commit:  self.volatile_state.commit_index,
                            })
                        },
                    )
                    .await;
                }
                
                _ => { /* Only leader sends heartbeats */ }
            }
            
        }
    }


    // getters ----------------------------------------------------------------
    fn get_last_log_index(&self) -> usize {
        self.persistent_state.log.len() - 1
    }

    fn get_last_log_term(&self) -> u64 {
        self.get_term(self.persistent_state.log.len() - 1)
    }

    fn get_term(&self, index: usize) -> u64 {
        match index {
            0 => 0,
            _ => self.persistent_state.log[index].term
        }
    }

    fn update_next_index(&mut self, id: Uuid, index: usize) {
        match &mut self.volatile_state.leader_data {
            None => panic!("leader_data undefined"),
            Some(LeaderData { next_index, .. }) => {
                next_index.insert(id, index);
            }
        }
    }

    fn update_match_index(&mut self, id: Uuid, index: usize) {
        match &mut self.volatile_state.leader_data {
            None => panic!("leader_data undefined"),
            Some(data) => {
                let old_index = data.match_index[&id];
                data.match_index.insert(id, index);
                
                if index > self.volatile_state.commit_index 
                && self.volatile_state.commit_index >= old_index {
                    
                    data.num_updated_servers += 1;
                    if data.num_updated_servers > self.config.servers.len() {
                        self.update_commit_index();
                    }
                }
            }
        }
    }

    fn get_next_index(&self, id: &Uuid) -> usize {
        match &self.volatile_state.leader_data {
            None => panic!("leader_data undefined"),
            Some(data) => {
                data.next_index[id]
            }
        }
    }

    fn get_match_index(&self, id: &Uuid) -> usize {
        match &self.volatile_state.leader_data {
            None => panic!("leader_data undefined"),
            Some(data) => {
                data.match_index[id]
            }
        }
    }


    fn update_commit_index(&mut self) {

        let mut num_updated_servers = None;

        match &self.volatile_state.leader_data {
            None => { /* Only a leader should call this method */ }
            Some(data) => {
                // Find a maximal index such that a majority of servers 
                // matches the leaders log up to that index
                let mut match_index: Vec<usize> = data.match_index.values().cloned().collect();
                match_index.sort();
                let i = match_index.len() / 2;
                let max_commit_index = match_index[i];
                
                if max_commit_index > self.volatile_state.commit_index
                && self.get_term(max_commit_index) == self.persistent_state.current_term {

                    // Update `commit_index`
                    self.volatile_state.commit_index = max_commit_index;
                    
                    // Update `num_updated_servers`
                    let newer_commits: Vec<usize>
                        = match_index.into_iter().filter(|id| *id > max_commit_index).collect();
                    
                        num_updated_servers = Some(newer_commits.len());
                }

                
                
                
            }
        }

        // Update `num_updated_servers`
        match &mut self.volatile_state.leader_data {
            None => {}
            Some(data) => {
                match num_updated_servers {
                    None => {},
                    Some(n) => { data.num_updated_servers = n; },
                }
            }
        }
        
        
    }


    // -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
    // HANDLERS 
    // -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*

    // Raft handlers ----------------------------------------------------------

    async fn handle_append_entries(
        &mut self,
        msg_header: RaftMessageHeader,
        args: AppendEntriesArgs,
    ) {

        if msg_header.term < self.persistent_state.current_term {
            self.send(
                &msg_header.source,
                RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                    success: false,
                    last_log_index: self.get_last_log_index(),
                }),
            ).await;

            // TODO is return necessary?
            return;

        } else if self.prev_log_entry_matches(args.prev_log_index, args.prev_log_term) {

            self.add_log_entries(args.entries, args.prev_log_index);
            self.send(
                &msg_header.source, 
                RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                    success: true,
                    last_log_index: self.get_last_log_index(),
                }),
            ).await;

        } else {
            
            self.send(
                &msg_header.source, 
                RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                    success: false,
                    last_log_index: self.get_last_log_index(),
                }),
            ).await;

            // TODO is return necessary?
            return;
        }

        if args.leader_commit > self.volatile_state.commit_index {
            self.commit_up_to_index(args.leader_commit).await;
        }
    }

    async fn handle_append_entries_resp(
        &mut self,
        msg_header: RaftMessageHeader,
        args: AppendEntriesResponseArgs,
    ) {

        match self.volatile_state.process_type {
            ProcessType::Leader {} => {

                // TODO: why this was in the algo?
                /*
                if args.last_log_index > self.get_next_index(&msg_header.source) {
                    self.send_entries(&msg_header.source).await;
                }
                */

                if args.success {
                    self.update_next_index(msg_header.source, args.last_log_index + 1);
                    self.update_match_index(msg_header.source, args.last_log_index);

                    if args.last_log_index < self.get_last_log_index() {
                        self.send_entries(&msg_header.source).await;
                    }

                } else if args.last_log_index + 1 < self.get_next_index(&msg_header.source) {
                    // this means the follower has a shorter log
                    self.update_next_index(msg_header.source, args.last_log_index + 1);
                    self.send_entries(&msg_header.source).await;

                } else {
                    // this means the entries of the follower do not match
                    self.update_next_index(msg_header.source, self.get_next_index(&msg_header.source) - 1);
                    self.send_entries(&msg_header.source).await;
                }
                
            },

            _ => { /* nothing to do */ },
        }
        
    }

    async fn handle_request_vote(
        &mut self,
        msg_header: RaftMessageHeader,
        args: RequestVoteArgs,
    ) {

        

        let last_log_term = self.get_last_log_term();
        let candidate_up_to_date 
            =       args.last_log_term  >   last_log_term
            ||  (   args.last_log_term  ==  last_log_term
                &&  args.last_log_index >=  self.get_last_log_index()
                );

        let granted
            =   msg_header.term >= self.persistent_state.current_term 
            &&  candidate_up_to_date
            &&  match &mut self.volatile_state.process_type {
                
                ProcessType::Follower => { 
                    match &mut self.persistent_state.vote {
                        Vote::NoVote    => true,
                        _               => false,
                    }
                },
                
                ProcessType::Candidate { .. } => false,
                
                ProcessType::Leader { .. } => { 
                    /* here `msg_header.term == self.persistent_state.current_term`,
                    * because otherwise the program wouldn' reach this place or 
                    * it would make these values equal
                    */

                    // TODO: why this line was here?
                    //self.volatile_state.process_type = ProcessType::Follower;
                    //self.volatile_state.leader_data = None;
                    false
                },
            };

        if granted { 
            self.persistent_state.vote = Vote::VotedFor(msg_header.source);
            self.update_state().await;
        }

        self.send(
            &msg_header.source, 
            RaftMessageContent::RequestVoteResponse(
                RequestVoteResponseArgs {
                    vote_granted: granted,
                }
            )
        ).await;
    
    }

    async fn handle_request_vote_resp(
        &mut self,
        msg_header: RaftMessageHeader,
        args: RequestVoteResponseArgs,
    ) {
        // Ignore zombie responses
        if msg_header.term < self.persistent_state.current_term { return; }

        match &mut self.volatile_state.process_type {

            ProcessType::Candidate { votes_received } => {
                if args.vote_granted {
                    
                    votes_received.insert(msg_header.source);

                    if votes_received.len() > self.config.servers.len() / 2 {
                        
                        self.volatile_state.process_type = ProcessType::Leader {};
                        self.volatile_state.leader_data = Some(LeaderData {
                            next_index:             self.init_next_index(),
                            match_index:            self.init_match_index(),
                            num_updated_servers:    0,
                        });

                        // `AppendEntries` will be sent when handling the next `Heartbeat` message
                        self.reset_heartbeat();
                    }
                }
            },
            
            _ => { /* nothing to do */ },
        }
    }

    // Client handlers --------------------------------------------------------
    async fn handle_command(
        &mut self,
        reply_to: Sender<ClientRequestResponse>,
        command: Vec<u8>,
        client_id: Uuid,
        sequence_num: u64,
        lowest_sequence_num_without_response: u64,
    ) {
        todo!()
    }

    // -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
    // More Utils
    // -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*

    /// Initialize the vote
    async fn init_vote(&mut self) {

        self.update_term(self.persistent_state.current_term + 1);
        self.reset_timer(self.config.election_timeout_range.clone());

        let mut votes = HashSet::new();
        votes.insert(self.config.self_id);
        self.volatile_state.process_type = ProcessType::Candidate { votes_received: votes };
        self.volatile_state.leader_data = None;
        
        self.persistent_state.vote = Vote::VotedFor(self.config.self_id);
        {self.update_state().await;}
        
        self.broadcast(
            RaftMessageContent::RequestVote (
                RequestVoteArgs {
                    last_log_index: self.get_last_log_index(),
                    last_log_term:  self.get_last_log_term(),
                }
            ),
        )
        .await;
    }
    
    /// Commit uncommited entries up to `commit_index` (if they are present in the log)
    async fn commit_up_to_index(&mut self, commit_index: usize) {
        assert!(self.volatile_state.commit_index <= commit_index);

        let commit_index = min(commit_index, self.get_last_log_index());

        let from = self.volatile_state.commit_index + 1;
        let to = commit_index + 1;

        self.volatile_state.commit_index = commit_index;

        for entry in &self.persistent_state.log[from..to] {
            let serialized = bincode::serialize(&entry).unwrap();
            {self.state_machine.apply(&serialized[..]).await;}
            self.volatile_state.last_applied += 1;
        }

        self.update_state().await;   
    }

    /// Send log entries to `target` with indexes greater or equal to `next_index[&target]`
    async fn send_entries(&self, target: &Uuid) {
        match &self.volatile_state.leader_data {
            Some(LeaderData { next_index, .. }) => {
                
                let from = next_index[target];
                let num_entries = min(
                    self.get_last_log_index() - from + 1,
                    self.config.append_entries_batch_size,
                );
                let to = from + num_entries;

                let to_send = self.persistent_state.log[from..to].to_vec();
                self.send(
                    target, 
                    RaftMessageContent::AppendEntries(AppendEntriesArgs {
                        prev_log_index: from - 1,
                        prev_log_term: self.get_term(from - 1),
                        entries: to_send,
                        leader_commit: self.volatile_state.commit_index,
                    })
                ).await;
            }
            
            _ => { /* Only a leader sends log entries */ }
        }
        
    }

    /*
    /// Returns latest common entry index assumming that commit indexes are the same
    async fn latest_common_entry_index(&self, entries: Vec<LogEntry>) -> usize {
        let mut common_index = self.volatile_state.commit_index;
        for (entry1, entry2) in self.state.uncommited_entries.iter().zip(entries.iter()) {
            if entry1 == entry2 {
                common_index += 1;
            } else {
                break;
            }
        }
        common_index
    }
    */

    /// Adds an entry to the log, but does not commit it yet
    fn add_log_entry(&mut self, entry: LogEntry) {
        self.persistent_state.log.push(entry);
    }

    /// Adds entries to the log, but does not commit them yet, 
    /// overwiting all entries after the entry with index `last_index`
    fn add_log_entries(&mut self, entries: Vec<LogEntry>, last_index: usize) {

        self.persistent_state.log.truncate(last_index + 1);
        self.persistent_state.log.append(&mut entries.clone());
        
    }

    /// Deletes log entries with index greater or equal to `from`
    fn delete_log_entries(&mut self, from: usize) {
        self.persistent_state.log.truncate(from);
    }
    

    fn init_next_index(&self) -> HashMap<Uuid, usize> {

        let last_log_index = self.get_last_log_index();
        let mut match_index = HashMap::new();
        for id in &self.config.servers {
            match_index.insert(*id, last_log_index);
        }
        match_index
    }

    fn init_match_index(&self) -> HashMap<Uuid, usize> {
        let mut match_index = HashMap::new();
        for id in &self.config.servers {
            match_index.insert(*id, 0);
        }
        match_index
    }

    fn prev_log_entry_matches(&self, prev_log_index: usize, prev_log_term: u64) -> bool {
        if self.persistent_state.log.len() <= prev_log_index {
            false
        } else if self.persistent_state.log[prev_log_index].term == prev_log_term {
            true
        } else {
            false
        }
    }



}

// -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
// `Handler` implementations
// -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) {
        
        self.msg_received(&msg);

        match msg.content {
            RaftMessageContent::AppendEntries(args)
                => self.handle_append_entries(msg.header, args).await,

            RaftMessageContent::AppendEntriesResponse(args)
                => self.handle_append_entries_resp(msg.header, args).await,

            RaftMessageContent::RequestVote(args)
                => self.handle_request_vote(msg.header, args).await,

            RaftMessageContent::RequestVoteResponse(args)
                => self.handle_request_vote_resp(msg.header, args).await,

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
            ClientRequestContent::Command {
                command,
                client_id,
                sequence_num,
                lowest_sequence_num_without_response,
            } => self.handle_command(
                msg.reply_to,
                command,
                client_id,
                sequence_num,
                lowest_sequence_num_without_response
            ).await,

            ClientRequestContent::Snapshot            => unimplemented!("Snapshots omitted"),
            ClientRequestContent::AddServer { .. }    => unimplemented!("Cluster membership changes omitted"),
            ClientRequestContent::RemoveServer { .. } => unimplemented!("Cluster membership changes omitted"),
            ClientRequestContent::RegisterClient      => todo!(),
        }
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, msg: Init) {
        
        self.self_ref = Some(msg.self_ref);
        self.reset_timer(self.config.election_timeout_range.clone());
        self.reset_heartbeat();
        
    }
}



/// Handle timer timeout.
#[async_trait::async_trait]
impl Handler<Timeout> for Raft {
    async fn handle(&mut self, _: Timeout) {
        
        match &mut self.volatile_state.process_type {
            ProcessType::Follower {}        => { self.init_vote().await; }
            ProcessType::Candidate { .. }   => { self.init_vote().await; }
            ProcessType::Leader { .. }      => { }
        }
        
    }
}

#[async_trait::async_trait]
impl Handler<Heartbeat> for Raft {
    async fn handle(&mut self, _: Heartbeat) {
        
        match &mut self.volatile_state.process_type {
            ProcessType::Leader { .. } => { 

                self.broadcast_heartbeat().await;
            }

            // Only a leader sends a heartbeat
            _ => {}
        }
        
    }
}


