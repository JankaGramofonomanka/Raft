use std::time::{Duration, SystemTime};

use async_channel::unbounded;
use ntest::timeout;
use uuid::Uuid;

use executor::System;

use assignment_3_solution::*;
use assignment_3_test_utils::*;

#[tokio::test]
#[timeout(500)]
async fn client_sessions_give_exactly_once_semantics() {
    let mut system = System::new().await;
    let leader_id = Uuid::new_v4();
    let follower_id = Uuid::new_v4();
    let processes = vec![leader_id, follower_id];
    let (apply_sender, apply_receiver) = unbounded();
    let sender = ExecutorSender::default();
    let first_log_entry_timestamp = SystemTime::now();
    let leader = Raft::new(
        &mut system,
        make_config(leader_id, Duration::from_millis(100), processes.clone()),
        first_log_entry_timestamp,
        Box::new(SpyMachine { apply_sender }),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, Duration::from_millis(300), processes),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;
    let (result_sender, result_receiver) = unbounded();

    tokio::time::sleep(Duration::from_millis(150)).await;

    let client_id = register_client(&leader, &result_sender, &result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    let mut responses = vec![];
    responses.push(result_receiver.recv().await.unwrap());
    responses.push(result_receiver.recv().await.unwrap());
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    responses.push(result_receiver.recv().await.unwrap());

    for response in responses {
        assert_eq!(
            response,
            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                client_id,
                sequence_num: 0,
                content: CommandResponseContent::CommandApplied {
                    output: vec![1, 2, 3, 4]
                },
            })
        );
    }
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![1, 2, 3, 4]);
    assert!(apply_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test]
#[timeout(2000)]
async fn client_sessions_are_expired() {
    let mut system = System::new().await;
    let leader_id = Uuid::new_v4();
    let follower_id = Uuid::new_v4();
    let processes = vec![leader_id, follower_id];
    let sender = ExecutorSender::default();
    let first_log_entry_timestamp = SystemTime::now();
    let leader = Raft::new(
        &mut system,
        make_config(leader_id, Duration::from_millis(100), processes.clone()),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, Duration::from_millis(300), processes),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;
    let (result_sender, result_receiver) = unbounded();

    tokio::time::sleep(Duration::from_millis(150)).await;

    let client_id_1 = register_client(&leader, &result_sender, &result_receiver).await;
    let client_id_2 = register_client(&leader, &result_sender, &result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id: client_id_1,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    tokio::time::sleep(SESSION_EXPIRATION + Duration::from_millis(200)).await;
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id: client_id_2,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![9, 10, 11, 12],
                client_id: client_id_1,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_1,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            }
        })
    );
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_2,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![5, 6, 7, 8]
            }
        })
    );
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_1,
            sequence_num: 1,
            content: CommandResponseContent::SessionExpired
        })
    );
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test]
#[timeout(2000)]
async fn acknowledged_outputs_are_discarded() {
    let mut system = System::new().await;
    let leader_id = Uuid::new_v4();
    let follower_id = Uuid::new_v4();
    let processes = vec![leader_id, follower_id];
    let sender = ExecutorSender::default();
    let first_log_entry_timestamp = SystemTime::now();
    let leader = Raft::new(
        &mut system,
        make_config(leader_id, Duration::from_millis(100), processes.clone()),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, Duration::from_millis(300), processes),
        first_log_entry_timestamp,
        Box::new(IdentityMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;
    let (result_sender, result_receiver) = unbounded();

    tokio::time::sleep(Duration::from_millis(150)).await;

    let client_id = register_client(&leader, &result_sender, &result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    let response_1 = result_receiver.recv().await.unwrap();
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 1,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    let response_2 = result_receiver.recv().await.unwrap();
    let response_3 = result_receiver.recv().await.unwrap();

    assert_eq!(
        response_1,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            }
        })
    );
    assert_eq!(
        response_2,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::CommandApplied {
                output: vec![5, 6, 7, 8]
            }
        })
    );
    assert_eq!(
        response_3,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::SessionExpired
        })
    );
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}
