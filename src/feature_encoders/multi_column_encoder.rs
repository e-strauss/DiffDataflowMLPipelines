use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Join;
use timely::dataflow::Scope;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::dense_vector::DenseVector;
use crate::types::row::Row;

// multi_column_encoder = sklearn's ColumnTransformer
pub fn multi_column_encoder<G: Scope, Enc>(
    data: &Collection<G, (usize, Row)>,
    config: Vec<(usize, Enc)>
) -> Collection<G, DenseVector>
where
    G::Timestamp: Lattice+Ord,
    Enc: ColumnEncoder<G> {
    let est_cols = config.len() - 1;
    let mut col_iterator = config.into_iter();
    if let Some((col_id, mut first_enc)) = col_iterator.next() {
        // Handle the first element init out with Row with one value
        // slice out current column
        let col = data.map(move | (ix, row)| (1,(ix, row.values[col_id].clone())));
        first_enc.fit(&col);
        let mut out = first_enc
            .transform(&col);
        // Process the rest using the same iterator
        for (col_id, mut enc) in col_iterator {
            // slice out current column
            let col = data.map(move | (ix, row)|
                (1, (ix, row.values[col_id].clone())));
            enc.fit(&col);
            out = out.join(&enc.transform(&col))
                .map(|(ix, (row, row_val))|
                    (ix, row.concat_dense_vector(row_val)));
        }
        out.map(|(_ix, val)| val)
    } else {
        panic!("no column encoder specified")
    }
}