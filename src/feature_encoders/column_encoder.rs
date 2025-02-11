use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use timely::dataflow::{Scope};
use crate::types::row_value::RowValue;

pub trait ColumnEncoder<G: Scope>
where
    G::Timestamp: Lattice+Ord,
{
    /// Fits the encoder on the input data and stores metadata internally (in the struct)
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>);

    /// Transforms the input data using the internally stored metadata
    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G,  (usize, RowValue)>;
}
