use assignment_3_solution::{
    AppendEntriesArgs, ClientRequest, ClientRequestContent, ClientRequestResponse,
    CommandResponseArgs, CommandResponseContent, Raft, RaftMessage, RaftMessageContent,
    RaftMessageHeader, RaftSender, RegisterClientResponseArgs, RegisterClientResponseContent,
    ServerConfig, StableStorage, StateMachine,
};
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use async_channel::{Receiver, Sender};
use tokio::sync::Mutex;
use uuid::Uuid;

use executor::{Handler, Message, ModuleRef, System};

pub mod distributed_set;

#[derive(Clone, Default)]
pub struct ExecutorSender {
    processes: Arc<Mutex<HashMap<Uuid, BoxedRecipient<RaftMessage>>>>,
    broken_links: Arc<Mutex<HashSet<(Uuid, Uuid)>>>,
}

impl ExecutorSender {
    pub async fn insert(&self, id: Uuid, addr: BoxedRecipient<RaftMessage>) {
        self.processes.lock().await.insert(id, addr);
    }

    pub async fn break_link(&self, from: Uuid, to: Uuid) {
        self.broken_links.lock().await.insert((from, to));
    }

    pub async fn fix_link(&self, from: Uuid, to: Uuid) {
        self.broken_links.lock().await.remove(&(from, to));
    }
}

#[async_trait::async_trait]
impl RaftSender for ExecutorSender {
    async fn send(&self, target: &Uuid, msg: RaftMessage) {
        if let Some(addr) = self.processes.lock().await.get(target) {
            if !self
                .broken_links
                .lock()
                .await
                .contains(&(msg.header.source, *target))
            {
                let addr = addr.clone_to_box();
                addr.send(msg).await;
            }
        }
    }
}

#[derive(Default, Clone)]
pub struct RamStorage {
    content: HashMap<String, Vec<u8>>,
}

#[async_trait::async_trait]
impl StableStorage for RamStorage {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        self.content.insert(key.to_string(), value.into());
        Ok(())
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.content.get(key).cloned()
    }
}

pub struct SharedRamStorage {
    pub content: Arc<std::sync::Mutex<HashMap<String, Vec<u8>>>>,
}

#[async_trait::async_trait]
impl StableStorage for SharedRamStorage {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        self.content
            .lock()
            .unwrap()
            .insert(key.to_string(), value.into());
        Ok(())
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.content.lock().unwrap().get(key).cloned()
    }
}

type BoxedRecipient<M> = Box<dyn Recipient<M>>;

#[async_trait::async_trait]
pub trait Recipient<M>: Send + Sync + 'static
where
    M: Message,
{
    async fn send(&self, msg: M);
    fn clone_to_box(&self) -> BoxedRecipient<M>;
}

#[async_trait::async_trait]
impl<M, T> Recipient<M> for ModuleRef<T>
where
    M: Message,
    T: Handler<M> + Send,
{
    async fn send(&self, msg: M) {
        self.send(msg).await;
    }

    fn clone_to_box(&self) -> BoxedRecipient<M> {
        Box::new(self.clone())
    }
}

pub fn singleton_range<T: Copy>(value: T) -> RangeInclusive<T> {
    value..=value
}

pub fn append_entries_from_leader(
    msgs: &[RaftMessage],
) -> Vec<(RaftMessageHeader, AppendEntriesArgs)> {
    let mut res = Vec::new();
    for msg in msgs {
        if let RaftMessageContent::AppendEntries(args) = &msg.content {
            res.push((msg.header.clone(), args.clone()));
        }
    }
    res
}

pub fn extract_messages<T>(rx: &async_channel::Receiver<T>) -> Vec<T> {
    let mut msgs = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        msgs.push(msg);
    }
    msgs
}

pub const CHUNK_SIZE: usize = 3;
pub const SESSION_EXPIRATION: Duration = Duration::from_millis(1000);

pub fn make_config(self_id: Uuid, election_timeout: Duration, servers: Vec<Uuid>) -> ServerConfig {
    ServerConfig {
        self_id,
        election_timeout_range: singleton_range(election_timeout),
        heartbeat_timeout: election_timeout / 5,
        servers: servers.iter().cloned().collect(),
        append_entries_batch_size: 10,
        snapshot_chunk_size: CHUNK_SIZE,
        catch_up_rounds: 10,
        session_expiration: SESSION_EXPIRATION,
    }
}

pub struct RaftSpy {
    pub raft: Option<ModuleRef<Raft>>,
    pub tx: async_channel::Sender<RaftMessage>,
}

impl RaftSpy {
    pub async fn new(
        system: &mut System,
        raft: Option<ModuleRef<Raft>>,
        tx: async_channel::Sender<RaftMessage>,
    ) -> ModuleRef<Self> {
        system.register_module(Self { raft, tx }).await
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for RaftSpy {
    async fn handle(&mut self, msg: RaftMessage) {
        self.tx.send(msg.clone()).await.unwrap();
        if let Some(raft) = &self.raft {
            raft.send(msg).await;
        }
    }
}

pub struct RamSender {
    pub tx: Sender<RaftMessage>,
}

#[async_trait::async_trait]
impl RaftSender for RamSender {
    async fn send(&self, _: &Uuid, msg: RaftMessage) {
        self.tx.send(msg).await.unwrap();
    }
}

pub struct DummyMachine;

#[async_trait::async_trait]
impl StateMachine for DummyMachine {
    async fn initialize(&mut self, _state: &[u8]) {}

    async fn apply(&mut self, _command: &[u8]) -> Vec<u8> {
        vec![]
    }

    async fn serialize(&self) -> Vec<u8> {
        vec![]
    }
}

#[derive(Default)]
pub struct LogMachine(pub Vec<Vec<u8>>);

#[async_trait::async_trait]
impl StateMachine for LogMachine {
    async fn initialize(&mut self, state: &[u8]) {
        self.0 = bincode::deserialize(state).unwrap();
    }

    async fn apply(&mut self, command: &[u8]) -> Vec<u8> {
        self.0.push(command.into());
        command.into()
    }

    async fn serialize(&self) -> Vec<u8> {
        bincode::serialize(&self.0).unwrap()
    }
}

pub struct IdentityMachine;

#[async_trait::async_trait]
impl StateMachine for IdentityMachine {
    async fn initialize(&mut self, _state: &[u8]) {}

    async fn apply(&mut self, command: &[u8]) -> Vec<u8> {
        command.into()
    }

    async fn serialize(&self) -> Vec<u8> {
        vec![]
    }
}

pub struct InitDetectorMachine {
    pub init_sender: Sender<Vec<u8>>,
}

impl InitDetectorMachine {
    pub fn get_state() -> Vec<u8> {
        vec![42]
    }
}

#[async_trait::async_trait]
impl StateMachine for InitDetectorMachine {
    async fn initialize(&mut self, state: &[u8]) {
        self.init_sender.send(state.into()).await.unwrap();
    }

    async fn apply(&mut self, _command: &[u8]) -> Vec<u8> {
        vec![]
    }

    async fn serialize(&self) -> Vec<u8> {
        Self::get_state()
    }
}

pub struct SpyMachine {
    pub apply_sender: Sender<Vec<u8>>,
}

#[async_trait::async_trait]
impl StateMachine for SpyMachine {
    async fn initialize(&mut self, _state: &[u8]) {}

    async fn apply(&mut self, command: &[u8]) -> Vec<u8> {
        self.apply_sender.send(command.into()).await.unwrap();
        command.into()
    }

    async fn serialize(&self) -> Vec<u8> {
        vec![]
    }
}

pub async fn register_client(
    raft_leader: &ModuleRef<Raft>,
    result_sender: &Sender<ClientRequestResponse>,
    result_receiver: &Receiver<ClientRequestResponse>,
) -> Uuid {
    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::RegisterClient,
        })
        .await;
    if let ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
        content: RegisterClientResponseContent::ClientRegistered { client_id },
    }) = result_receiver.recv().await.unwrap()
    {
        client_id
    } else {
        panic!("Client registration failed");
    }
}

pub fn unwrap_output(response: &ClientRequestResponse) -> &Vec<u8> {
    if let ClientRequestResponse::CommandResponse(CommandResponseArgs {
        content: CommandResponseContent::CommandApplied { output },
        ..
    }) = response
    {
        output
    } else {
        panic!("Not a command reponse: {:?}", response);
    }
}
