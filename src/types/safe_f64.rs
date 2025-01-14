use std::cmp::Ordering;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct SafeF64(pub(crate) f64);

impl PartialEq for SafeF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for SafeF64 {}  // Only valid if PartialEq is implemented correctly

impl PartialOrd for SafeF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for SafeF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Greater)  // Treat NaN as the largest value
    }
}
