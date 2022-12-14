use std::time::SystemTime;
use std::collections::{HashSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::cmp::min;
use std::fmt::{Debug, Display};

use uuid::Uuid;
use async_channel::Sender;

use executor::{Handler, ModuleRef, System};

use crate::domain::*;
use crate::utils::*;

const TERM_KEY:         &str = "TERM";
const VOTE_KEY:         &str = "VOTE";
const NUM_ENTRIES_KEY:  &str = "NUM_ENTRIES";

#[allow(non_snake_case)]
fn MK_LOG_ENTRY_KEY(index: usize) -> String {
    format!("LOG{}", index)
}


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
    last_leader_msg:    SystemTime,

    start:              SystemTime,
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

        let current_term = from_option_deserialize(0, &stable_storage.get(TERM_KEY).await);
        let vote = from_option_deserialize(Vote::NoVote, &stable_storage.get(VOTE_KEY).await);
        let num_entries: usize = from_option_deserialize(0, &stable_storage.get(NUM_ENTRIES_KEY).await);

        let mut log: Vec<LogEntry> = vec![];
        if num_entries == 0 {
            log.push(first_entry);
        } else {
            for index in 0..num_entries {
                let entry: LogEntry = bincode::deserialize(
                    &stable_storage.get(&MK_LOG_ENTRY_KEY(index)).await.unwrap()[..]
                ).unwrap();
                log.push(entry);
            }
        }
        

        let persistent_state = PersistentState { current_term, vote, log, };

        let volatile_state = VolatileState {
            commit_index:   0,
            last_applied:   0,
            
            process_type:   ProcessType::Follower,
            leader_data:    None
        };

        let minimum_election_timeout = *config.election_timeout_range.start();
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
                last_leader_msg:    SystemTime::now() - minimum_election_timeout,

                start:              SystemTime::now(),
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
    fn reset_timer(&mut self) {
        self.timer_abort.store(true, Ordering::Relaxed);
        self.timer_abort = Arc::new(AtomicBool::new(false));
        tokio::spawn(run_timer(
            self.self_ref.as_ref().unwrap().clone(),
            self.config.election_timeout_range.clone(),
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
    async fn update_term(&mut self, new_term: u64) {
        assert!(self.persistent_state.current_term < new_term);
        self.persistent_state.current_term = new_term;
        self.update_vote(Vote::NoVote).await;
        
        self.storage.put(TERM_KEY, &bincode::serialize(&new_term).unwrap()[..]).await.unwrap();
    }

    async fn update_vote(&mut self, vote: Vote) {
        self.persistent_state.vote = vote;
        self.storage.put(VOTE_KEY, &bincode::serialize(&vote).unwrap()[..]).await.unwrap();
    }

    /// Stores a log entry nut does not update number of log entries
    async fn update_log_entry(&mut self, log_index: usize) {
        let entry = &self.persistent_state.log[log_index];
        self.storage.put(&MK_LOG_ENTRY_KEY(log_index), &bincode::serialize(entry).unwrap()[..]).await.unwrap();
    }

    async fn update_num_entries(&mut self) {
        let index = self.get_last_log_index() + 1;
        self.storage.put(NUM_ENTRIES_KEY, &bincode::serialize(&index).unwrap()[..]).await.unwrap();
    }

    /// Common message processing.
    async fn msg_received(&mut self, msg_header: &RaftMessageHeader) {
        if msg_header.term > self.persistent_state.current_term {
            self.update_term(msg_header.term).await;

            self.convert_to_follower();
            self.update_vote(Vote::Leader(msg_header.source)).await;
        }
    }

    fn convert_to_follower(&mut self) {
        
        self.volatile_state.process_type = ProcessType::Follower;
        self.volatile_state.leader_data = None;
    }

    fn is_leader(&self) -> bool {
        match self.volatile_state.process_type {
            ProcessType::Leader => true,
            _                   => false,
        }
    }

    async fn reply(&self, reply_to: &Sender<ClientRequestResponse>, response: ClientRequestResponse) {
        
        match reply_to.send(response).await {
            _ => { /* Ignore errors, nothing to do if success */ },
        }
    }

    // send / broadcast -------------------------------------------------------

    /// Broadcast a message
    async fn broadcast(&mut self, content: RaftMessageContent) {
        for id in &self.config.servers {

            // Don't send a message to self
            if *id == self.config.self_id { continue; }

            self.send(
                &id,
                content.clone(),
            )
            .await;
        }
    }

    /// Send a message
    async fn send(&self, target: &Uuid, content: RaftMessageContent) {

        let msg = RaftMessage {
            header: RaftMessageHeader {
                source: self.config.self_id,
                term:   self.persistent_state.current_term,
            },
            content: content
        };

        self.sender.send(
            target, 
            msg,
        )
        .await;
    }

    /// Broadcast an empty `AppendEntries` message to all other servers
    async fn broadcast_heartbeat(&self) {
        for id in &self.config.servers {

            // Don't send a message to self
            if *id == self.config.self_id { continue; }

            // Check if `self` is still a leader in every iteration to avoid a 
            // panic in `get_next_index` call
            if self.is_leader() {
                let prev_index = self.get_next_index(&id) - 1;
                self.send(
                    &id,
                    
                    RaftMessageContent::AppendEntries(AppendEntriesArgs {
                            prev_log_index: prev_index,
                            prev_log_term:  self.get_term(prev_index),
                            entries:        vec![],
                            leader_commit:  self.volatile_state.commit_index,
                    }),
                )
                .await;
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

    fn get_leader_data(&self) -> &LeaderData {
        match &self.volatile_state.leader_data {
            None => panic!("leader_data undefined"),
            Some(data) => data
        }
    }

    fn get_leader_data_mut(&mut self) -> &mut LeaderData {
        match &mut self.volatile_state.leader_data {
            None => panic!("leader_data undefined"),
            Some(data) => data
        }
    }

    fn get_next_index(&self, id: &Uuid) -> usize {
        let data = self.get_leader_data();
        data.next_index[id]
    }

    fn get_match_index(&self, id: &Uuid) -> usize {
        let data = self.get_leader_data();
        data.match_index[id]
    }

    // updates ----------------------------------------------------------------
    fn update_next_index(&mut self, id: Uuid, index: usize) {
        let data = self.get_leader_data_mut(); 
        data.next_index.insert(id, index);
    }

    async fn update_match_index(&mut self, id: Uuid, index: usize) {
        
        let commit_index = self.volatile_state.commit_index;

        let data = self.get_leader_data_mut();
        let old_index = data.match_index[&id];
        data.match_index.insert(id, index);
        
        if index > commit_index && commit_index >= old_index {
            
            data.num_updated_servers += 1;
            if data.num_updated_servers > self.config.servers.len() / 2 {
                self.update_commit_index().await;
            }

        } else if index < commit_index && commit_index <= old_index {
            // This should not be possible but let's put it here just in case 
            data.num_updated_servers -= 1;
        }
    }

    fn mark_successful_heartbeat(&mut self, source: Uuid) {
        let data = self.get_leader_data_mut();
        data.successes.insert(source);
    }

    /// Calculates the highest log index that is replicated on the majority of 
    /// servers and commits entries up to that index (inclusive)
    async fn update_commit_index(&mut self) {

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
                    self.commit_up_to_index(max_commit_index).await;
                    
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

            return;

        }
        
        self.reset_timer();
        self.last_leader_msg = SystemTime::now();

        { self.update_vote(Vote::Leader(msg_header.source)).await; }

        if self.prev_log_entry_matches(args.prev_log_index, args.prev_log_term) {

            { self.add_log_entries(args.entries, args.prev_log_index).await; }
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

        if self.is_leader() {

            if args.success {
                self.mark_successful_heartbeat(msg_header.source);
                self.update_next_index(msg_header.source, args.last_log_index + 1);
                { self.update_match_index(msg_header.source, args.last_log_index).await; }

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
                
                ProcessType::Follower => match &mut self.persistent_state.vote {
                    Vote::NoVote                    => true,
                    Vote::Leader(leader_id)         => msg_header.source == *leader_id,
                    Vote::VotedFor(candidate_id)    => msg_header.source == *candidate_id,
                },
                
                ProcessType::Candidate { .. } => false,
                
                ProcessType::Leader => { 
                    /* here `msg_header.term == self.persistent_state.current_term`,
                    * because otherwise the program wouldn' reach this place or 
                    * it would make these values equal
                    */

                    false
                },
            };

        if granted { self.update_vote(Vote::VotedFor(msg_header.source)).await; }

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

                    self.check_if_elected();
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

        match self.volatile_state.process_type {
            ProcessType::Leader {} => {
                let entry = LogEntry {
                    content: LogEntryContent::Command {
                        data: command,
                        client_id,
                        sequence_num,
                        lowest_sequence_num_without_response,
                    },
                    term: self.persistent_state.current_term,
                    timestamp: SystemTime::now(),
                };
        
                { self.add_log_entry_with_sender(entry, reply_to).await; }
                { self.send_entries_to_all().await; }
            }

            _ => {
                let leader_hint = match self.persistent_state.vote {
                    Vote::Leader(leader_id) => Some(leader_id),
                    _                       => None,
                };

                let response = ClientRequestResponse::CommandResponse(CommandResponseArgs {
                    client_id,
                    sequence_num,
                    content: CommandResponseContent::NotLeader { leader_hint },
                });

                self.reply(&reply_to, response).await;
            }
        }
        
    }

    async fn handle_register_client(&mut self, reply_to: Sender<ClientRequestResponse>) {

        match self.volatile_state.process_type {
            ProcessType::Leader {} => {
                let entry = LogEntry {
                    content: LogEntryContent::RegisterClient,
                    term: self.persistent_state.current_term,
                    timestamp: SystemTime::now(),
                };
        
                { self.add_log_entry_with_sender(entry, reply_to).await; }
                { self.send_entries_to_all().await; }
            }

            _ => {
                let leader_hint = match self.persistent_state.vote {
                    Vote::Leader(leader_id) => Some(leader_id),
                    _                       => None,
                };

                let response = ClientRequestResponse::RegisterClientResponse(
                    RegisterClientResponseArgs {
                        content: RegisterClientResponseContent::NotLeader { leader_hint },
                    }
                );

                self.reply(&reply_to, response).await;
            }
        }
    }

    // -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
    // More Utils
    // -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*

    /// Initialize the vote
    async fn init_vote(&mut self) {

        {
            self.update_term(self.persistent_state.current_term + 1).await;
            self.reset_timer();

            let mut votes = HashSet::new();
            votes.insert(self.config.self_id);
            self.volatile_state.process_type = ProcessType::Candidate { votes_received: votes };
            self.volatile_state.leader_data = None;
            self.check_if_elected();
            
            self.update_vote(Vote::VotedFor(self.config.self_id)).await;
        }
        
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
    
    fn check_if_elected(&mut self) {
        match &self.volatile_state.process_type{
            ProcessType::Candidate { votes_received } => {
                if votes_received.len() > self.config.servers.len() / 2 {
                        
                    self.volatile_state.process_type = ProcessType::Leader;
                    self.volatile_state.leader_data = Some(LeaderData {
                        next_index:             self.init_next_index(),
                        match_index:            self.init_match_index(),
                        num_updated_servers:    0,
                        reply_to:               HashMap::new(),
                        successes:              HashSet::new(),
                    });
            
                    // `AppendEntries` will be sent when handling the next `Heartbeat` message
                    self.reset_heartbeat();
                }
            }

            _ => { /* Nothing to do */ }
        }
        
    }

    /// Commit uncommited entries up to `commit_index` (if they are present in the log)
    async fn commit_up_to_index(&mut self, commit_index: usize) {
        assert!(self.volatile_state.commit_index <= commit_index);

        let commit_index = min(commit_index, self.get_last_log_index());

        let from = self.volatile_state.commit_index + 1;
        let to = commit_index + 1;

        self.volatile_state.commit_index = commit_index;

        for log_index in from..to {

            self.commit(log_index).await;
            
        }
    }

    /// Commits an entry with index `log_index` and 
    /// responds to the client if `self` is the leader
    async fn commit(&mut self, log_index: usize) {
        let entry = &self.persistent_state.log[log_index];
        match &entry.content {

            LogEntryContent::Command {
                data,
                client_id,
                sequence_num,
                lowest_sequence_num_without_response: _,
            } => {

                let machine_state;
                {machine_state = self.state_machine.apply(&data[..]).await;}
                self.volatile_state.last_applied = log_index;
                
                let response = ClientRequestResponse::CommandResponse(CommandResponseArgs {
                    client_id: *client_id,
                    sequence_num: *sequence_num,
                    content: CommandResponseContent::CommandApplied { output: machine_state },
                });

                self.respond_if_leader(log_index, response).await;
                

            },

            LogEntryContent::RegisterClient => {
                
                let response = ClientRequestResponse::RegisterClientResponse(
                    RegisterClientResponseArgs {
                        content: RegisterClientResponseContent::ClientRegistered { 
                            client_id: Uuid::from_u128(log_index as u128),
                        },
                    }
                );

                self.respond_if_leader(log_index, response).await;
            },

            LogEntryContent::Configuration { .. } => { /* Nothing to respond to */ },
        }
    }
    
    async fn respond_if_leader(&self, log_index: usize, response: ClientRequestResponse) {
        
        if self.is_leader() {

            let leader_data = self.get_leader_data();
            let reply_to = leader_data.reply_to.get(&log_index);

            match reply_to {

                None => { },

                Some(reply_to) => { self.reply(reply_to, response).await; },
            }
        }
        
    }

    /// Send log entries to `target` with indexes greater or equal to `next_index[&target]`
    async fn send_entries(&mut self, target: &Uuid) {
        match &self.volatile_state.leader_data {
            Some(LeaderData { next_index, .. }) => {
                
                let from = next_index[target];
                let num_entries = min(
                    self.get_last_log_index() - from + 1,
                    self.config.append_entries_batch_size,
                );
                let to = from + num_entries;
                self.update_next_index(*target, to);

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

    async fn send_entries_to_all(&mut self) {
        for id in self.config.servers.clone() {

            // Don't send message to self
            if id == self.config.self_id { continue; }

            self.send_entries(&id).await;
        }
        
    }

    /// Adds an entry to the log, but does not commit it yet
    async fn add_log_entry(&mut self, entry: LogEntry) {
        self.persistent_state.log.push(entry.clone());
        let index = self.get_last_log_index();
        self.update_log_entry(index).await;
        self.update_num_entries().await;
    }

    /// Adds an entry to the log and saves the reply sender
    async fn add_log_entry_with_sender(
        &mut self,
        entry: LogEntry,
        reply_to: Sender<ClientRequestResponse>,
    ) {
        self.add_log_entry(entry).await;
        let index = self.get_last_log_index();

        if self.is_leader() {
            let leader_data = self.get_leader_data_mut();
            leader_data.reply_to.insert(index, reply_to);
            
            self.update_next_index(self.config.self_id, self.get_last_log_index() + 1);
            self.update_match_index(self.config.self_id, self.get_last_log_index()).await;
            
        }
        
    }

    /// Adds entries to the log, but does not commit them yet, 
    /// overwiting all entries after the entry with index `last_index`
    async fn add_log_entries(&mut self, entries: Vec<LogEntry>, last_index: usize) {

        self.persistent_state.log.truncate(last_index + 1);
        self.persistent_state.log.append(&mut entries.clone());

        for index in last_index + 1..self.get_last_log_index() {
            self.update_log_entry(index).await;
        }
        self.update_num_entries().await;
        
    }

    fn init_next_index(&self) -> HashMap<Uuid, usize> {

        let init_index = self.get_last_log_index() + 1;
        let mut next_index = HashMap::new();
        for id in &self.config.servers {
            next_index.insert(*id, init_index);
        }
        next_index
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

    fn debug(&self, title: impl Display, msg: impl Debug) {
        if true {
            println!("-------------------------------------------------------------------------------");
            println!("elapsed: {}", self.start.elapsed().unwrap().as_millis());
            println!("ID: {:?}, TERM: {}, STATE: {:#?}", self.config.self_id, self.persistent_state.current_term, self.volatile_state.process_type);
            match &self.volatile_state.leader_data {
                None => {},
                Some(data) => {
                    println!("LEADER DATA: {:#?}", data);
                }
            }
            println!("{}:\n{:#?}\n\n\n", title, msg);
        }
    }

}

// -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
// `Handler` implementations
// -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) {
        
        match msg.content {
            RaftMessageContent::AppendEntries(args)
                => {
                    { self.msg_received(&msg.header).await; }
                    self.handle_append_entries(msg.header, args).await;
                },

            RaftMessageContent::AppendEntriesResponse(args)
                => {
                    { self.msg_received(&msg.header).await; }
                    self.handle_append_entries_resp(msg.header, args).await;
                },

            RaftMessageContent::RequestVote(args)
                => {
                    if self.is_leader()
                    || self.last_leader_msg.elapsed().unwrap() < *self.config.election_timeout_range.start() {
                        /* Ignore a `RequestVote` received within the minimum 
                         * election timeout of hearing from a current leader 
                         */
                    } else {
                        { self.msg_received(&msg.header).await; }
                        self.handle_request_vote(msg.header, args).await;
                    }
                },

            RaftMessageContent::RequestVoteResponse(args)
                => {
                    { self.msg_received(&msg.header).await; }
                    self.handle_request_vote_resp(msg.header, args).await;
                },

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

            ClientRequestContent::Snapshot              => unimplemented!("Snapshots omitted"),
            ClientRequestContent::AddServer { .. }      => unimplemented!("Cluster membership changes omitted"),
            ClientRequestContent::RemoveServer { .. }   => unimplemented!("Cluster membership changes omitted"),
            
            ClientRequestContent::RegisterClient        => self.handle_register_client(msg.reply_to).await,
        }
    }
}

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, msg: Init) {
        
        self.self_ref = Some(msg.self_ref);
        self.reset_timer();
        self.reset_heartbeat();
        
    }
}

/// Handle timer timeout.
#[async_trait::async_trait]
impl Handler<Timeout> for Raft {
    async fn handle(&mut self, _: Timeout) {
        
        match &mut self.volatile_state.process_type {
            ProcessType::Follower           => { self.init_vote().await; }
            ProcessType::Candidate { .. }   => { self.init_vote().await; }
            ProcessType::Leader             => {

                let num_servers = self.config.servers.len();
                let data = self.get_leader_data_mut();

                if num_servers > 1 {
                    // `self` is not included in `data.successs`, 
                    // therefore subtract 1`
                    if data.successes.len() <= (num_servers / 2) - 1 {
                        self.convert_to_follower();
                        self.update_term(self.persistent_state.current_term + 1).await;
                        
                    } else {
                        data.successes = HashSet::new();
                    }
                }
            }
        }
        
    }
}

#[async_trait::async_trait]
impl Handler<Heartbeat> for Raft {
    async fn handle(&mut self, _: Heartbeat) {
        
        if self.is_leader() { self.broadcast_heartbeat().await; }
    }
}


