use std::collections::BTreeMap;
use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count, Join, Threshold};
use timely::dataflow::{Scope};
use crate::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::safe_hash_map::SafeHashMap;
use crate::types::integer_assignment_aggregate::PositionAssignmentAggregate;

pub struct OneHotEncoder <G: Scope> {
    value_positions: Option<Collection<G, ((), (SafeHashMap<RowValue, usize>, usize))>>,
}

impl<G: Scope> OneHotEncoder<G> {
    pub fn new() -> Self<>{
        Self{value_positions:None}
    }
}

impl<G: Scope> ColumnEncoder<G> for OneHotEncoder<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let distinct = data.map(|(_, row_value)| row_value).distinct();
        self.value_positions = Some(distinct
            .threshold(|value, multiplicity| {
                PositionAssignmentAggregate::new_with_val(value, *multiplicity)
            }).map(|_vector| ()).count().map(|agg| ((), (agg.1.val_to_index, agg.1.len))));
    }

    fn transform(&self, data: &Collection<G,(usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let value_positions = match &self.value_positions {
            None => panic!("called transform before fit"),
            Some(m) => m
        };
        let value_pos_pairs = value_positions.flat_map(|(_, (btree, len))| {
            btree.0.into_iter().map(move |(key, value)| (key, (value, len)))
        }).inspect(|(record, time, change)| {
            println!("OneHot Meta: {:?}, time: {:?}, change: {:?}", record, time, change)
        });

        let len_collection = value_positions.map(|(_, (_ , len))| ((), len));
        let data = data.map(|(i, v) | (v, i));

        let inner_join = data.join(&value_pos_pairs).map(|(value, (row_id, (vector_index, len)))| {
            let mut vec = vec![0f64; len];
            vec[vector_index] = 1.0;
            (row_id, RowValue::Vec(vec))
        });

        let unmatched = data
            .antijoin(&value_pos_pairs.map(|(value, _)| value))
            .map(|(v, i)| ((), (v, i)))
            .join(&len_collection)
            .map(|(_, ((value, row_id), len))| {
                let vec = vec![0f64; len];
                (row_id, RowValue::Vec(vec))
        });

        inner_join.concat(&unmatched)
    }
}