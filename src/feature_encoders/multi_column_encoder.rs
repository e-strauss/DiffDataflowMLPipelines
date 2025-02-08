use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Reduce};
use timely::dataflow::Scope;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::row::Row;
use crate::types::row_value::RowValue;

// multi_column_encoder = sklearn's ColumnTransformer
pub fn multi_column_encoder<'a, G: Scope>(
    data: &Collection<G, (usize, Row)>,
    config: Vec<(usize, Box<dyn ColumnEncoder<G> + 'a>)>
) -> Collection<G, RowValue>
where
    G::Timestamp: Lattice+Ord,{
    let _est_cols = config.len() - 1;
    let mut col_iterator = config.into_iter();
    // Handle the first element init out with Row with one value
    // slice out current column
    let mut out = data.map(move | (ix, row)| (ix, RowValue::Vec(vec![])));
    // Process the rest using the same iterator
    for (col_id, mut enc) in col_iterator {
        // slice out current column
        let col = data.map(move | (ix, row)|
            (ix, row.values[col_id].clone()));
        enc.fit(&col);

        out = out.join(&enc.transform(&col))
            .map(|(ix, (row, row_val))| {
                (ix, row.vector_append(&row_val))
            });
    }
    out.map(|(_ix, val)| val)
}