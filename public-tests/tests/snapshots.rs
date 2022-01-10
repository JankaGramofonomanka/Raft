use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_channel::unbounded;
use ntest::timeout;
use uuid::Uuid;

use executor::System;

use assignment_3_solution::*;
use assignment_3_test_utils::*;

#[tokio::test]
#[timeout(500)]
async fn snapshot_is_sent_to_follower_correctly() {
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
        Box::new(LogMachine::default()),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, Duration::from_millis(300), processes.clone()),
        first_log_entry_timestamp,
        Box::new(LogMachine::default()),
        Box::new(RamStorage::default()),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;
    let (result_sender, result_receiver) = unbounded();

    tokio::time::sleep(Duration::from_millis(150)).await;

    let client_id = register_client(&leader, &result_sender, &result_receiver).await;

    for i in 0..4 {
        leader
            .send(ClientRequest {
                reply_to: result_sender.clone(),
                content: ClientRequestContent::Command {
                    command: vec![i],
                    client_id,
                    sequence_num: i.into(),
                    lowest_sequence_num_without_response: 0,
                },
            })
            .await;
    }
    for _ in 0..4 {
        result_receiver.recv().await.unwrap();
    }
    sender.break_link(follower_id, leader_id).await;
    for i in 4..8 {
        leader
            .send(ClientRequest {
                reply_to: result_sender.clone(),
                content: ClientRequestContent::Command {
                    command: vec![i],
                    client_id,
                    sequence_num: i.into(),
                    lowest_sequence_num_without_response: 0,
                },
            })
            .await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Snapshot,
        })
        .await;
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::SnapshotResponse(SnapshotResponseArgs {
            content: SnapshotResponseContent::SnapshotCreated {
                last_included_index: 5
            }
        })
    );

    let (spy_tx, spy_rx) = unbounded();
    let spy = {
        let inner_raft = Raft::new(
            &mut system,
            make_config(spy_id, Duration::from_millis(300), processes.clone()),
            first_log_entry_timestamp,
            Box::new(LogMachine::default()),
            Box::new(RamStorage::default()),
            Box::new(sender.clone()),
        )
        .await;
        RaftSpy::new(&mut system, Some(inner_raft), spy_tx).await
    };
    sender.insert(spy_id, Box::new(spy.clone())).await;

    let mut expected_state_machine = LogMachine::default();
    for i in 0..4 {
        expected_state_machine.apply(&vec![i]).await;
    }
    let expected_snapshot = expected_state_machine.serialize().await;

    for (chunk_idx, chunk) in expected_snapshot.chunks(CHUNK_SIZE).enumerate() {
        loop {
            let msg = spy_rx.recv().await.unwrap();
            if let RaftMessageContent::InstallSnapshot(args @ InstallSnapshotArgs { offset, .. }) =
                msg.content
            {
                if offset == chunk_idx * CHUNK_SIZE {
                    assert_eq!(
                        msg.header,
                        RaftMessageHeader {
                            source: leader_id,
                            term: 1
                        }
                    );
                    if offset == 0 {
                        assert_eq!(
                            args.last_config.as_ref().unwrap(),
                            &processes.iter().cloned().collect::<HashSet<Uuid>>()
                        );
                        assert_eq!(args.client_sessions.as_ref().unwrap().len(), 1);
                        let session = &args.client_sessions.as_ref().unwrap()[&client_id];
                        assert_eq!(
                            session.responses,
                            (0..4_u8).map(|i| (i.into(), vec![i])).collect()
                        );
                        assert_eq!(session.lowest_sequence_num_without_response, 0);
                    }
                    assert_eq!(args.last_included_index, 5);
                    assert_eq!(args.last_included_term, 1);
                    assert_eq!(args.data, chunk);
                    assert_eq!(
                        args.done,
                        expected_snapshot.len() - chunk_idx * CHUNK_SIZE <= CHUNK_SIZE
                    );
                    break;
                }
            }
        }
    }
    loop {
        let msg = spy_rx.recv().await.unwrap();
        if let RaftMessageContent::AppendEntries(args) = msg.content {
            if args.prev_log_index == 5 {
                assert_eq!(
                    msg.header,
                    RaftMessageHeader {
                        source: leader_id,
                        term: 1,
                    }
                );
                assert_eq!(args.prev_log_term, 1);
                assert_eq!(args.entries.len(), 4);
                for (i, entry) in args.entries.iter().enumerate() {
                    assert_eq!(entry.term, 1);
                    assert_eq!(
                        entry.content,
                        LogEntryContent::Command {
                            data: vec![(i + 4).try_into().unwrap()],
                            client_id,
                            sequence_num: (i + 4).try_into().unwrap(),
                            lowest_sequence_num_without_response: 0,
                        }
                    )
                }
                assert_eq!(args.leader_commit, 5);
                break;
            }
        }
    }

    system.shutdown().await;
}

#[tokio::test]
#[timeout(500)]
async fn state_machine_is_initialized_with_snapshot() {
    // given
    let leader_id = Uuid::new_v4();
    let processes = vec![leader_id];
    let first_log_entry_timestamp = SystemTime::now();
    let stable_storage_content = Arc::new(std::sync::Mutex::new(HashMap::new()));
    {
        let mut system = System::new().await;
        let sender = ExecutorSender::default();
        let (init_sender, _init_receiver) = unbounded();
        let leader = Raft::new(
            &mut system,
            make_config(leader_id, Duration::from_millis(100), processes.clone()),
            first_log_entry_timestamp,
            Box::new(InitDetectorMachine {
                init_sender: init_sender.clone(),
            }),
            Box::new(SharedRamStorage {
                content: stable_storage_content.clone(),
            }),
            Box::new(sender.clone()),
        )
        .await;
        let (result_sender, result_receiver) = unbounded();
        leader
            .send(ClientRequest {
                reply_to: result_sender,
                content: ClientRequestContent::Snapshot,
            })
            .await;
        result_receiver.recv().await.unwrap();
        system.shutdown().await;
    }

    // when
    let mut system = System::new().await;
    let sender = ExecutorSender::default();
    let (init_sender, init_receiver) = unbounded();
    let _leader = Raft::new(
        &mut system,
        make_config(leader_id, Duration::from_millis(100), processes.clone()),
        first_log_entry_timestamp,
        Box::new(InitDetectorMachine { init_sender }),
        Box::new(SharedRamStorage {
            content: stable_storage_content.clone(),
        }),
        Box::new(sender.clone()),
    )
    .await;

    // then
    assert_eq!(
        init_receiver.recv().await.unwrap(),
        InitDetectorMachine::get_state()
    );

    system.shutdown().await;
}
