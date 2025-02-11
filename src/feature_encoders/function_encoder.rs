use std::rc::Rc;
use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use timely::dataflow::Scope;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::row_value::RowValue;

pub struct FunctionEncoder < G: Scope> {
    udf: Rc<dyn Fn(RowValue) -> RowValue>,
    phantom_data: Option<Collection<G, (usize, RowValue)>>

}

impl<G: Scope> FunctionEncoder<G> {
    pub fn new(udf: impl Fn(RowValue) -> RowValue + 'static) -> Self{
        Self {
            udf: Rc::new(udf), phantom_data: None
        }
    }
}

impl<G: Scope> ColumnEncoder<G> for FunctionEncoder<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {

    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let udf = self.udf.clone();
        data.map(move |(idx, val)| (idx, (udf)(val)))
    }
}