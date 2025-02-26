use std::cmp::Ordering;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::types::row_value::RowValue;
use crate::types::safe_f64::SafeF64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SafeHashMap(pub(crate) HashMap<RowValue, SafeF64>);

impl SafeHashMap {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub(crate) fn insert(&mut self, p0: RowValue, p1: SafeF64) {
        self.0.insert(p0, p1);
    }
}

impl PartialEq for SafeHashMap {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for SafeHashMap {} // Only valid if PartialEq is implemented correctly

impl PartialOrd for SafeHashMap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut self_entries: Vec<_> = self.0.iter().collect();
        let mut other_entries: Vec<_> = other.0.iter().collect();

        self_entries.sort_by(|a, b| a.0.cmp(b.0));
        other_entries.sort_by(|a, b| a.0.cmp(b.0));

        self_entries.partial_cmp(&other_entries)
    }
}

impl Ord for SafeHashMap {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut self_entries: Vec<_> = self.0.iter().collect();
        let mut other_entries: Vec<_> = other.0.iter().collect();

        self_entries.sort_by(|a, b| a.0.cmp(b.0));
        other_entries.sort_by(|a, b| a.0.cmp(b.0));

        self_entries.cmp(&other_entries)
    }
}