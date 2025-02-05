use differential_dataflow::Collection;
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Threshold, Count, Join};
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::dense_vector::DenseVector;
use crate::types::row_value::RowValue;
use crate::types::safe_f64::SafeF64;
use crate::types::safe_vec::SafeVec;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct MinMaxAggregate {
    sorted_values: SafeVec<(f64, isize)>,

}

impl MinMaxAggregate {
    fn new(value: f64, count: isize) -> Self {
        Self { sorted_values: SafeVec(vec![(value, count)]) }
    }

    fn get(&self) -> (SafeF64, SafeF64) {
        let len = match self.sorted_values.0.len() {
            0 => panic!("empty aggregate"),
            len => len

        };
        let range: f64 = self.sorted_values.0[len - 1].0 - self.sorted_values.0[0].0;
        (SafeF64(self.sorted_values.0[0].0), SafeF64(range))
    }
}

impl IsZero for MinMaxAggregate {
    fn is_zero(&self) -> bool { self.sorted_values.0.len() == 0 }
}

impl Semigroup for MinMaxAggregate {
    fn plus_equals(&mut self, other: &Self) {
        println!("{:?} merged with {:?}", self.sorted_values.0, other.sorted_values.0);

        // insert each tuple into the sorted list
        for tuple in other.sorted_values.0.iter() {
            let pos = self.sorted_values.0.binary_search_by(|&(v, _)| v.partial_cmp(&tuple.0).unwrap())
                .unwrap_or_else(|e| e);

            if pos < self.sorted_values.0.len() && self.sorted_values.0[pos].0 == tuple.0 {
                // Merge counts
                self.sorted_values.0[pos].1 += tuple.1;

                // Remove if count <= 0
                if self.sorted_values.0[pos].1 <= 0 {
                    self.sorted_values.0.remove(pos);
                }
            } else {
                // Insert maintaining order
                self.sorted_values.0.insert(pos, *tuple);
            }
        }
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

impl<G: Scope> ColumnEncoder<G> for MinMaxScaler<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, (usize, RowValue))>) {
        let meta = data
            .threshold(|(_k, (_ix, value)), c| {
                MinMaxAggregate::new((*value).get_float(), *c)
            })
            .map(|(column_id, _value)| column_id)
            .count()
            .map(|(column, agg)| (column, agg.get()))
            .inspect(|x| println!("{:?}", x));
        self.meta = Some(meta);
    }

    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G, (usize, DenseVector)> {
        let meta = match &self.meta {
            None => panic!("called transform before fit"),
            Some(m) => m
        };
        data.join(&meta)
            .map(|(_key, ((ix, val), (min, range)))|
                (ix, DenseVector::Scalar((val.get_float() - min.0)/range.0)) )
    }
}
