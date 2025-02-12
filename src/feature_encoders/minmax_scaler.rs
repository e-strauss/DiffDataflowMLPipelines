use std::cmp::Ordering;
use std::collections::BTreeMap;
use differential_dataflow::Collection;
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Threshold, Count};
use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::feature_encoders::standard_scaler::apply_scaling;
use crate::types::row_value::RowValue;
use crate::types::safe_f64::SafeF64;
use crate::types::safe_vec::SafeVec;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MinMaxAggregate {
    pub counts : BTreeMap<SafeF64, isize>,
    max_pq : PriorityQueue<SafeF64, SafeF64>,
    min_pq : PriorityQueue<SafeF64, SafeF64>
}

impl MinMaxAggregate {
    pub(crate) fn new(value: f64, count: isize) -> Self {
        let value = SafeF64(value);
        let mut new = Self::zero();
        new.counts.insert(value.clone(), count);
        if count > 0 {
            new.max_pq.push_increase(value.clone(), value.clone());
            new.min_pq.push_increase(value.clone(), SafeF64(-value.clone().0));
        }
        new
    }

    pub(crate) fn get(&self) -> (SafeF64, SafeF64) {
        let len = match self.max_pq.len() {
            0 => panic!("empty aggregate"),
            len => len

        };
        let min = *self.min_pq.peek().unwrap().0;
        let max = *self.max_pq.peek().unwrap().0;

        let range: f64 = max.0 - min.0;
        (SafeF64(min.0), SafeF64(range))
    }
}

impl IsZero for MinMaxAggregate {
    fn is_zero(&self) -> bool { self.max_pq.len() == 0 }
}

impl Semigroup for MinMaxAggregate {
    fn plus_equals(&mut self, other: &Self) {

        for (&key, &value) in &other.counts {
            // Merge counts
            let mut new_count = *(self.counts.get(&key)).unwrap_or(&0);
            new_count += value;
            self.counts.insert(key.clone(), new_count);

            // Ensure only positive counts remain in PQs
            if new_count > 0 {
                self.max_pq.push_increase(key.clone(), key.clone());
                self.min_pq.push_increase(key.clone(), SafeF64(-key.clone().0));
            } else {
                self.max_pq.remove(&key);
                self.min_pq.remove(&key);
            }
        }
    }
}

impl Monoid for MinMaxAggregate {
    fn zero() -> Self {
        Self { counts:BTreeMap::new(), max_pq:PriorityQueue::new(), min_pq:PriorityQueue::new() }
    }
}

impl Abelian for MinMaxAggregate {
    fn negate(&mut self) {
        for (_, count) in self.counts.iter_mut() {
            *count = -*count;
        }
    }
}

impl PartialOrd for MinMaxAggregate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.counts.partial_cmp(&other.counts)
    }
}

impl Ord for MinMaxAggregate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.counts.cmp(&other.counts)
    }
}

impl Serialize for MinMaxAggregate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.counts.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MinMaxAggregate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let counts: BTreeMap<SafeF64, isize> = BTreeMap::deserialize(deserializer)?;

        let mut new = Self::zero();
        new.counts = counts.clone();

        for (key, &count) in &counts {
            if count > 0 {
                new.max_pq.push_increase(key.clone(), key.clone());
                new.min_pq.push_increase(key.clone(), SafeF64(-key.clone().0));
            }
        }
        Ok(new)
    }
}

pub struct MinMaxScaler<G: Scope> {
    meta: Option<Collection<G, (usize, (SafeF64, SafeF64))>>,
}

impl<G: Scope> MinMaxScaler<G> {
    pub fn new() -> Self{
        Self{meta:None}
    }
}

fn insert_sorted_list(current: &mut SafeVec<(SafeF64, isize)>, other: &SafeVec<(SafeF64, isize)>) {
    for tuple in other.0.iter() {
        let pos =current.0.binary_search_by(|&(v, _)| v.partial_cmp(&tuple.0).unwrap())
            .unwrap_or_else(|e| e);

        if pos < current.0.len() && current.0[pos].0 == tuple.0 {
            // Merge counts
            current.0[pos].1 += tuple.1;

            // Remove if count <= 0
            if current.0[pos].1 <= 0 {
                current.0.remove(pos);
            }
        } else {
            // Insert maintaining order
            current.0.insert(pos, *tuple);
        }
    }
}

impl<G: Scope> ColumnEncoder<G> for MinMaxScaler<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let meta = get_meta(&data.map(|x| (1, x)))
            .inspect(|(record, time, change)| {
                println!("MinMaxScaler Meta: {:?}, time: {:?}, change: {:?}", record, time, change)
            });
        self.meta = Some(meta);
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let meta = match &self.meta {
            None => panic!("called transform before fit"),
            Some(m) => m
        };
        apply_scaling(&data.map(|x| (1, x)), &meta)
    }
}

pub(crate) fn get_meta<G: Scope>(data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G, (usize, (SafeF64, SafeF64))>
where G::Timestamp: Lattice+Ord {
    data.threshold(|(_k, (_ix, value)), c| {
            MinMaxAggregate::new((*value).get_float(), *c)
        })
        .map(|(column_id, _value)| column_id)
        .count()
        .map(|(column, agg)| (column, agg.get()))
}
