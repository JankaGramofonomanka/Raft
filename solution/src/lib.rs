use std::time::SystemTime;

use executor::{Handler, ModuleRef, System};

pub use domain::*;

mod domain;

pub struct Raft {
    // TODO you can add fields to this struct.

}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        first_log_entry_timestamp: SystemTime,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        todo!()
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
