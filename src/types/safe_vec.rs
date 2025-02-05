use std::cmp::Ordering;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeVec<G>(pub Vec<G>);

impl<G: PartialEq> PartialEq for SafeVec<G> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<G: Eq> Eq for SafeVec<G> {}

impl<G: PartialOrd> PartialOrd for SafeVec<G> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<G: Ord> Ord for SafeVec<G> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)  // Safe because `Vec<G>` implements `Ord` if `G: Ord`
    }
}
