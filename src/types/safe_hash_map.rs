use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SafeHashMap<T: Ord + Clone + Hash + PartialEq, V: Clone + PartialEq >(pub(crate) HashMap<T, V>);

impl<T: Ord + Clone + Hash + PartialEq, V: Clone + PartialEq> SafeHashMap<T, V>  {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub(crate) fn insert(&mut self, key: T, value: V) {
        self.0.insert(key, value);
    }

    pub(crate) fn get(&self, key: &T) -> Option<&V> {
        self.0.get(key)
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &T> {
        self.0.keys()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&T, &V)> {
        self.0.iter()
    }

}

impl<T: Ord + Clone + Hash + PartialEq, V: Clone + PartialEq> PartialEq for SafeHashMap<T, V>{
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Ord + Clone + Hash + PartialEq, V: Clone + PartialEq> Eq for SafeHashMap<T, V> {}

impl<T: Ord + Clone + Hash + PartialEq, V: Clone + PartialEq + PartialOrd> PartialOrd for SafeHashMap<T, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut self_entries: Vec<_> = self.0.iter().collect();
        let mut other_entries: Vec<_> = other.0.iter().collect();

        self_entries.sort_by(|a, b| a.0.cmp(b.0));
        other_entries.sort_by(|a, b| a.0.cmp(b.0));

        self_entries.into_iter().partial_cmp(other_entries)
    }
}

impl<T: Ord + Clone + Hash + PartialEq, V: Clone + PartialEq + PartialOrd + Ord> Ord for SafeHashMap<T, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut self_entries: Vec<_> = self.0.iter().collect();
        let mut other_entries: Vec<_> = other.0.iter().collect();

        self_entries.sort_by(|a, b| a.0.cmp(b.0));
        other_entries.sort_by(|a, b| a.0.cmp(b.0));

        self_entries.into_iter().cmp(other_entries)
    }
}
