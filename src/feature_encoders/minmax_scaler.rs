use differential_dataflow::Collection;
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Threshold, Count};
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::feature_encoders::standard_scaler::apply_scaling;
use crate::types::row_value::RowValue;
use crate::types::safe_f64::SafeF64;
use crate::types::safe_vec::SafeVec;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MinMaxAggregate {
    sorted_values: SafeVec<(SafeF64, isize)>,

}

impl MinMaxAggregate {
    pub(crate) fn new(value: f64, count: isize) -> Self {
        Self { sorted_values: SafeVec(vec![(SafeF64(value), count)]) }
    }

    pub(crate) fn get(&self) -> (SafeF64, SafeF64) {
        let len = match self.sorted_values.0.len() {
            0 => panic!("empty aggregate"),
            len => len

        };
        let range: f64 = self.sorted_values.0[len - 1].0.0 - self.sorted_values.0[0].0.0;
        (SafeF64(self.sorted_values.0[0].0.0), SafeF64(range))
    }
}

impl IsZero for MinMaxAggregate {
    fn is_zero(&self) -> bool { self.sorted_values.0.len() == 0 }
}

impl Semigroup for MinMaxAggregate {
    fn plus_equals(&mut self, other: &Self) {
        // insert each tuple into the sorted list
        insert_sorted_list(&mut self.sorted_values, &other.sorted_values);
    }
}

impl Monoid for MinMaxAggregate {
    fn zero() -> Self {
        Self { sorted_values: SafeVec(vec![]) }
    }
}

impl Abelian for MinMaxAggregate {
    fn negate(&mut self) {
        for (_, count) in self.sorted_values.0.iter_mut() {
            *count = -*count;
        }
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
        let meta = get_meta(&data.map(|x| (1, x)));
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
        .inspect(|x| println!("{:?}", x))
}
