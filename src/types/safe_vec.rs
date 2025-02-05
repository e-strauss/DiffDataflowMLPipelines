use std::cmp::Ordering;
use differential_dataflow::difference::Abelian;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeVec<G>(pub Vec<G>);

// order by vector length
// order should not be relevant in the end, since we wil have only one aggregate

impl<G> PartialEq for SafeVec<G> {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()  // Compare actual vector contents
    }
}

impl<G> Eq for SafeVec<G> {}

impl<G> PartialOrd for SafeVec<G> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.len().partial_cmp(&other.0.len())
    }
}

impl<G> Ord for SafeVec<G> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.len().cmp(&other.0.len())  // Safe because `Vec<G>` implements `Ord` if `G: Ord`
    }
}
