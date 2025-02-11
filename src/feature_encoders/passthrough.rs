use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use timely::dataflow::Scope;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::safe_f64::SafeF64;

pub struct Passthrough < G: Scope> {
    phantom_data: Option<Collection<G, (usize, RowValue)>>
}

impl<G: Scope> Passthrough<G> {
    pub fn new() -> Self{
        Self { phantom_data: None }
    }
}

impl<G: Scope> ColumnEncoder<G> for Passthrough<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {

    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        data.clone()
    }
}