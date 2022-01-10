use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use assignment_3_solution::StateMachine;

pub struct DistributedSet {
    /// The current (committed) state of the set.
    integers: HashSet<i64>,
}

impl DistributedSet {
    pub fn new() -> Self {
        Self {
            integers: HashSet::new(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum SetOperation {
    Add(i64),
    Remove(i64),
    IsPresent(i64),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub enum SetResponse {
    InvalidOperation,
    IsPresent(bool),
}

#[async_trait::async_trait]
impl StateMachine for DistributedSet {
    async fn initialize(&mut self, state: &[u8]) {
        self.integers = bincode::deserialize(state).unwrap();
    }

    async fn apply(&mut self, command: &[u8]) -> Vec<u8> {
        match bincode::deserialize::<SetOperation>(command) {
            Ok(command) => match command {
                SetOperation::Add(num) => {
                    self.integers.insert(num);
                    vec![]
                }
                SetOperation::Remove(num) => {
                    self.integers.remove(&num);
                    vec![]
                }
                SetOperation::IsPresent(num) => {
                    bincode::serialize(&SetResponse::IsPresent(self.integers.contains(&num)))
                        .unwrap()
                }
            },
            Err(_) => bincode::serialize(&SetResponse::InvalidOperation).unwrap(),
        }
    }

    async fn serialize(&self) -> Vec<u8> {
        bincode::serialize(&self.integers).unwrap()
    }
}
