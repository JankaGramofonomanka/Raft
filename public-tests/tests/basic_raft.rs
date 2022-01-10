use std::time::{Duration, SystemTime};

use async_channel::unbounded;
use ntest::timeout;
use uuid::Uuid;

use executor::System;

use assignment_3_solution::*;
use assignment_3_test_utils::*;

#[tokio::test]
#[timeout(1000)]
async fn system_makes_progress_when_there_is_a_majority() {
    // given
    let mut system = System::new().await;

    let entry_data = vec![1, 2, 3, 4, 5];
    let ident_leader = Uuid::new_v4();
    let ident_follower = Uuid::new_v4();
    let processes = vec![ident_leader, ident_follower, Uuid::new_v4()];
    let sender = ExecutorSender::default();

    let first_log_entry_timeout = SystemTime::now();
    let raft_leader = Raft::new(
        &mut system,
        make_config(ident_leader, Duration::from_millis(100), processes.clone()),
        first_log_entry_timeout,
        Box::new(IdentityMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    let raft_follower = Raft::new(
        &mut system,
        make_config(ident_follower, Duration::from_millis(300), processes),
        first_log_entry_timeout,
        Box::new(IdentityMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender
        .insert(ident_leader, Box::new(raft_leader.clone()))
        .await;
    sender.insert(ident_follower, Box::new(raft_follower)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (result_sender, result_receiver) = unbounded();

    // when
    let client_id = register_client(&raft_leader, &result_sender, &result_receiver).await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: entry_data.clone(),
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // then
    assert_eq!(
        entry_data,
        *unwrap_output(&result_receiver.recv().await.unwrap())
    );

    system.shutdown().await;
}

#[tokio::test]
#[timeout(1500)]
async fn system_does_not_make_progress_without_majority() {
    // given
    let mut system = System::new().await;

    let entry_data = vec![1, 2, 3, 4, 5];
    let ident_leader = Uuid::new_v4();
    let ident_follower = Uuid::new_v4();
    let processes = vec![ident_leader, ident_follower, Uuid::new_v4()];
    let sender = ExecutorSender::default();

    let first_log_entry_timestamp = SystemTime::now();
    let raft_leader = Raft::new(
        &mut system,
        make_config(ident_leader, Duration::from_millis(100), processes.clone()),
        first_log_entry_timestamp,
        Box::new(DummyMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    let raft_follower = Raft::new(
        &mut system,
        make_config(
            ident_follower,
            Duration::from_millis(300),
            processes.clone(),
        ),
        first_log_entry_timestamp,
        Box::new(DummyMachine),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender
        .insert(ident_leader, Box::new(raft_leader.clone()))
        .await;
    sender
        .insert(ident_follower, Box::new(raft_follower.clone()))
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (result_sender, result_receiver) = unbounded();

    // when
    let client_id = register_client(&raft_leader, &result_sender, &result_receiver).await;

    sender.break_link(ident_follower, ident_leader).await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: entry_data,
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // then
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test]
#[timeout(1500)]
async fn follower_denies_vote_for_candidate_with_outdated_log() {
    // given
    let mut system = System::new().await;

    let (tx, rx) = unbounded();
    let storage = RamStorage::default();
    let other_ident_1 = Uuid::new_v4();
    let other_ident_2 = Uuid::new_v4();
    let ident_follower = Uuid::new_v4();
    let first_log_entry_timestamp = SystemTime::now();
    let raft_follower = Raft::new(
        &mut system,
        make_config(
            ident_follower,
            Duration::from_millis(500),
            vec![other_ident_1, other_ident_2, ident_follower],
        ),
        first_log_entry_timestamp,
        Box::new(DummyMachine),
        Box::new(storage),
        Box::new(RamSender { tx }),
    )
    .await;
    let client_id = Uuid::from_u128(1);

    // when

    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 2,
                source: other_ident_1,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        content: LogEntryContent::RegisterClient,
                        term: 2,
                        timestamp: SystemTime::now(),
                    },
                    LogEntry {
                        content: LogEntryContent::Command {
                            data: vec![1],
                            client_id,
                            sequence_num: 0,
                            lowest_sequence_num_without_response: 0,
                        },
                        term: 2,
                        timestamp: SystemTime::now(),
                    },
                ],
                leader_commit: 0,
            }),
        })
        .await;

    // Wait longer than election timeout so that the follower does not ignore the vote request
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Older term of the last message.
    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 4,
                source: other_ident_2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 2,
                last_log_term: 1,
            }),
        })
        .await;

    // Shorter log in candidate.
    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 5,
                source: other_ident_2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 1,
                last_log_term: 2,
            }),
        })
        .await;

    // then
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: ident_follower,
                term: 2,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: true,
                last_log_index: 2,
            })
        }
    );
    for _ in 0..2 {
        rx.recv().await.unwrap();
    }
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: ident_follower,
                term: 4,
            },
            content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs {
                vote_granted: false,
            })
        }
    );
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: ident_follower,
                term: 5,
            },
            content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs {
                vote_granted: false,
            })
        }
    );

    system.shutdown().await;
}

#[tokio::test]
#[timeout(500)]
async fn follower_rejects_inconsistent_append_entry() {
    // given
    let mut system = System::new().await;

    let (tx, rx) = unbounded();
    let storage = RamStorage::default();
    let first_log_entry_timestamp = SystemTime::now();
    let other_ident = Uuid::new_v4();
    let ident_follower = Uuid::new_v4();
    let raft_follower = Raft::new(
        &mut system,
        make_config(
            ident_follower,
            Duration::from_secs(10),
            vec![other_ident, ident_follower],
        ),
        first_log_entry_timestamp,
        Box::new(DummyMachine),
        Box::new(storage),
        Box::new(RamSender { tx }),
    )
    .await;

    // when
    let client_id = Uuid::from_u128(1);

    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 1,
                source: other_ident,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        content: LogEntryContent::RegisterClient,
                        term: 1,
                        timestamp: SystemTime::now(),
                    },
                    LogEntry {
                        content: LogEntryContent::Command {
                            data: vec![1, 2, 3, 4],
                            client_id,
                            sequence_num: 0,
                            lowest_sequence_num_without_response: 0,
                        },
                        term: 1,
                        timestamp: SystemTime::now(),
                    },
                ],
                leader_commit: 0,
            }),
        })
        .await;

    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 2,
                source: other_ident,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index: 2,
                prev_log_term: 2,
                entries: vec![LogEntry {
                    content: LogEntryContent::Command {
                        data: vec![5, 6, 7, 8],
                        client_id,
                        sequence_num: 0,
                        lowest_sequence_num_without_response: 0,
                    },
                    term: 2,
                    timestamp: SystemTime::now(),
                }],
                leader_commit: 0,
            }),
        })
        .await;

    // then
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                term: 1,
                source: ident_follower,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: true,
                last_log_index: 2
            })
        }
    );
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                term: 2,
                source: ident_follower,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: false,
                last_log_index: 2
            })
        }
    );

    system.shutdown().await;
}

#[tokio::test]
#[timeout(500)]
async fn follower_redirects_to_leader() {
    // given
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
    tokio::time::sleep(Duration::from_millis(200)).await;

    // when
    let (follower_result_sender, follower_result_receiver) = unbounded();
    let (leader_result_sender, leader_result_receiver) = unbounded();

    let client_id = register_client(&leader, &leader_result_sender, &leader_result_receiver).await;

    follower
        .send(ClientRequest {
            reply_to: follower_result_sender,
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: leader_result_sender,
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // then
    assert_eq!(
        follower_result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::NotLeader {
                leader_hint: Some(leader_id)
            }
        })
    );
    assert_eq!(
        leader_result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![5, 6, 7, 8]
            },
        })
    );

    system.shutdown().await;
}

#[tokio::test]
#[timeout(500)]
async fn leader_steps_down_without_heartbeat_responses_from_majority() {
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

    sender.break_link(follower_id, leader_id).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    let response_2 = result_receiver.recv().await.unwrap();

    assert_eq!(
        response_1,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            },
        })
    );
    assert_eq!(
        response_2,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::NotLeader { leader_hint: None }
        })
    );

    system.shutdown().await;
}

#[tokio::test]
#[timeout(1000)]
async fn follower_ignores_request_vote_within_election_timeout_of_leader_heartbeat() {
    // given
    let mut system = System::new().await;
    let leader_id = Uuid::new_v4();
    let follower_id = Uuid::new_v4();
    let spy_id = Uuid::new_v4();
    let processes = vec![leader_id, follower_id, spy_id];
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
    tokio::time::sleep(Duration::from_millis(150)).await;

    // when
    let (spy_sender, spy_receiver) = unbounded();
    sender
        .insert(
            spy_id,
            Box::new(RaftSpy::new(&mut system, None, spy_sender).await),
        )
        .await;

    follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                source: spy_id,
                term: 2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 0,
                last_log_term: 0,
            }),
        })
        .await;
    leader
        .send(RaftMessage {
            header: RaftMessageHeader {
                source: spy_id,
                term: 2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 0,
                last_log_term: 0,
            }),
        })
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // then
    while let Ok(msg) = spy_receiver.try_recv() {
        assert_eq!(
            msg,
            RaftMessage {
                header: RaftMessageHeader {
                    source: leader_id,
                    term: 1,
                },
                content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: 0,
                })
            }
        );
    }

    system.shutdown().await;
}
