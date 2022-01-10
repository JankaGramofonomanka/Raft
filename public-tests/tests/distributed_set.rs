use std::time::{Duration, SystemTime};

use async_channel::unbounded;
use ntest::timeout;
use uuid::Uuid;

use executor::System;

use assignment_3_solution::{
    ClientRequest, ClientRequestContent, ClientRequestResponse, CommandResponseArgs,
    CommandResponseContent, Raft, RegisterClientResponseArgs, RegisterClientResponseContent,
    ServerConfig,
};
use assignment_3_test_utils::distributed_set::{DistributedSet, SetOperation, SetResponse};
use assignment_3_test_utils::{singleton_range, ExecutorSender, RamStorage};

#[tokio::test]
#[timeout(1000)]
async fn can_remove_from_set() {
    // given
    let mut system = System::new().await;

    let ident_leader = Uuid::new_v4();
    let servers = vec![ident_leader];
    let sender = ExecutorSender::default();

    let first_log_entry_timestamp = SystemTime::now();
    let raft_leader = Raft::new(
        &mut system,
        ServerConfig {
            self_id: ident_leader,
            election_timeout_range: singleton_range(Duration::from_millis(50)),
            heartbeat_timeout: Duration::from_millis(10),
            servers: servers.into_iter().collect(),
            append_entries_batch_size: 10,
            snapshot_chunk_size: 10,
            catch_up_rounds: 10,
            session_expiration: Duration::from_secs(20),
        },
        first_log_entry_timestamp,
        Box::new(DistributedSet::new()),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender
        .insert(ident_leader, Box::new(raft_leader.clone()))
        .await;
    // Raft leader election
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (result_sender, result_receiver) = unbounded();

    // when
    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::RegisterClient,
        })
        .await;
    let client_id =
        if let ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
            content: RegisterClientResponseContent::ClientRegistered { client_id },
        }) = result_receiver.recv().await.unwrap()
        {
            client_id
        } else {
            panic!("Client registration failed");
        };

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: bincode::serialize(&SetOperation::Add(7_i64)).unwrap(),
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: bincode::serialize(&SetOperation::Remove(7_i64)).unwrap(),
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: bincode::serialize(&SetOperation::IsPresent(7_i64)).unwrap(),
                client_id,
                sequence_num: 2,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    result_receiver.recv().await.unwrap();
    result_receiver.recv().await.unwrap();
    let is_7_present_response = result_receiver.recv().await.unwrap();
    let is_7_present_output = if let ClientRequestResponse::CommandResponse(CommandResponseArgs {
        content: CommandResponseContent::CommandApplied { output },
        ..
    }) = is_7_present_response
    {
        output
    } else {
        panic!("Received wrong response: {:?}", is_7_present_response);
    };

    // then
    assert_eq!(
        bincode::deserialize::<SetResponse>(&is_7_present_output).unwrap(),
        SetResponse::IsPresent(false)
    );

    system.shutdown().await;
}

#[tokio::test]
#[timeout(1000)]
async fn single_node_adds_to_set() {
    // given
    let mut system = System::new().await;

    let ident_leader = Uuid::new_v4();
    let servers = vec![ident_leader];
    let sender = ExecutorSender::default();

    let first_log_entry_timestamp = SystemTime::now();
    let raft_leader = Raft::new(
        &mut system,
        ServerConfig {
            self_id: ident_leader,
            election_timeout_range: singleton_range(Duration::from_millis(50)),
            heartbeat_timeout: Duration::from_millis(10),
            servers: servers.into_iter().collect(),
            append_entries_batch_size: 10,
            snapshot_chunk_size: 10,
            catch_up_rounds: 10,
            session_expiration: Duration::from_secs(20),
        },
        first_log_entry_timestamp,
        Box::new(DistributedSet::new()),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender
        .insert(ident_leader, Box::new(raft_leader.clone()))
        .await;
    // Raft leader election
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (result_sender, result_receiver) = unbounded();

    // when
    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::RegisterClient,
        })
        .await;
    let client_id =
        if let ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
            content: RegisterClientResponseContent::ClientRegistered { client_id },
        }) = result_receiver.recv().await.unwrap()
        {
            client_id
        } else {
            panic!("Client registration failed");
        };
    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: bincode::serialize(&SetOperation::Add(7_i64)).unwrap(),
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: bincode::serialize(&SetOperation::IsPresent(7_i64)).unwrap(),
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    result_receiver.recv().await.unwrap();
    let is_7_present_response = result_receiver.recv().await.unwrap();
    let is_7_present_output = if let ClientRequestResponse::CommandResponse(CommandResponseArgs {
        content: CommandResponseContent::CommandApplied { output },
        ..
    }) = is_7_present_response
    {
        output
    } else {
        panic!("Received wrong response: {:?}", is_7_present_response);
    };

    // then
    assert_eq!(
        bincode::deserialize::<SetResponse>(&is_7_present_output).unwrap(),
        SetResponse::IsPresent(true)
    );

    system.shutdown().await;
}
