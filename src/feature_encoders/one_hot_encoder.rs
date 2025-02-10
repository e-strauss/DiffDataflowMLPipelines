use std::collections::BTreeMap;
use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count, Join, Reduce, Threshold};
use timely::dataflow::{Scope};
use crate::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::vector_position_aggregate::PositionAssignmentAggregate;

pub struct OneHotEncoder <G: Scope> {
    value_positions: Option<Collection<G, ((), (BTreeMap<RowValue, usize>, usize))>>,
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
            }).map(|vector| ()).count().map(|agg| ((), (agg.1.val_to_index, agg.1.next_index))));
    }

    fn transform(&self, data: &Collection<G,(usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let value_positions = match &self.value_positions {
            None => panic!("called transform before fit"),
            Some(m) => m
        };

        let data = data.map(|(i, v) | ((), (i, v)));
        let joined = data.join(&value_positions);

        joined.map(|(_, ((row_id, v), (val_to_index, n)))| {
            let mut vec = vec![0f64; n];
            let i = val_to_index.get(&v);
            if let Some(i) = i {
                vec[*i] = 1.0;
            } else{
                //value not in map
            }
            (row_id, RowValue::Vec(vec))
        })
    }
}