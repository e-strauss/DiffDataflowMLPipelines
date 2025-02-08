use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use timely::dataflow::{Scope};
use crate::ColumnEncoder;
use crate::types::row_value::RowValue;

pub struct Pipeline<'a, G : Scope> {
    config: Vec<Box<dyn ColumnEncoder<G> + 'a>>,
}

impl<'a, G: Scope> Pipeline<'a, G> {
    pub fn new(config: Vec<Box<dyn ColumnEncoder<G> + 'a>>) -> Self<>{
        Self{config}
    }
}

impl<'a, G: Scope> ColumnEncoder<G> for Pipeline<'a, G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let mut intermediate = data.clone();
        for encoder in &mut self.config {
            encoder.fit(&intermediate);
            intermediate = encoder.transform(&intermediate);
        }
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let mut intermediate = data.clone();
        for encoder in &self.config {
            intermediate = encoder.transform(&intermediate);
        }
        intermediate
    }
}