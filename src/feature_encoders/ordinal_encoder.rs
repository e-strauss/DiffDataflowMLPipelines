use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count, Join, Threshold};
use timely::dataflow::{Scope};
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::integer_assignment_aggregate::PositionAssignmentAggregate;

pub struct OrdinalEncoder <G: Scope> {
    value_map: Option<Collection<G, ((RowValue, RowValue))>>,
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
            .threshold(|value, multiplicity| PositionAssignmentAggregate::new_with_val(value, *multiplicity))
            .map(|_vector| ())
            .count()
            .flat_map(|((), agg)| agg.val_to_index.0.iter().map(|(k,v)| (k.clone(), v.clone())).collect::<Vec<_>>())
            .map(|(k, v)| (k, RowValue::Float(v as f64)))
            //.inspect(|x| {println!("{:?}", x)})
        );
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let value_map = match &self.value_map {
            None => panic!("called transform before fit"),
            Some(m) => m
        };

        let joined = data.map(|(rix, v) | (v, rix) )
            .join(value_map);

        let matched = joined.map(|(k, (rix, v))| (rix, v));

        let unmatched = data.map(|(row_id, v)| (v, row_id))
            .antijoin(&value_map.map(|(value, _)| value))
            .map(|(_, row_id)| {
                (row_id, RowValue::Float(-1f64))
            });

        matched.concat(&unmatched)
    }
}

