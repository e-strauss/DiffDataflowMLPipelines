use std::collections::BTreeMap;
use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count, Join, Threshold};
use timely::dataflow::{Scope};
use crate::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::vector_position_aggregate::PositionAssignmentAggregate;

pub struct OrdinalEncoder <G: Scope> {
    value_map: Option<Collection<G, ((), (BTreeMap<RowValue, usize>, usize))>>,
}

impl<G: Scope> OrdinalEncoder<G> {
    pub fn new() -> Self<>{
        Self{value_map:None}
    }
}

impl<G: Scope> ColumnEncoder<G> for OrdinalEncoder<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let distinct = data.map(|(_, row_value)| row_value).distinct();
        self.value_map = Some(distinct
            .threshold(|value, multiplicity| {
                PositionAssignmentAggregate::new_with_val(value, *multiplicity)
            }).map(|_vector| ()).count().map(|agg| ((), (agg.1.val_to_index, agg.1.len))));
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let value_map = match &self.value_map {
            None => panic!("called transform before fit"),
            Some(m) => m
        };

        let data = data.map(|(i, v) | ((), (i, v)));
        let joined = data.join(&value_map);

        joined.map(|(_, ((row_id, v), (val_to_index, _n)))| {
            let i = val_to_index.get(&v);
            if let Some(i) = i {
                (row_id, RowValue::Float(*i as f64))
            } else{
                (row_id, RowValue::Float(-1f64)) //TODO how to deal with this?
            }
        })
    }
}