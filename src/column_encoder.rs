use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Threshold};
use differential_dataflow::operators::Reduce;
use timely::dataflow::{Scope, ScopeParent};
use crate::types::{Row, RowValue, DenseVector};

pub trait ColumnEncoder<G: Scope>
where
    G::Timestamp: Lattice+Ord,
{
    /// Fits the encoder on the input data and stores metadata internally (in the struct)
    fn fit(&mut self, data: &Collection<G, (usize, (usize, RowValue))>);

    /// Transforms the input data using the internally stored metadata
    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G,  (usize, DenseVector)>;
}

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

/****************************************/
/*      ENCODER IMPLEMENTATIONS:        */
/****************************************/

pub struct StandardScaler <G: Scope> {
    mean: Option<Collection<G, (usize, DenseVector)>>,
}

impl<G: Scope> StandardScaler<G> {
    pub fn new() -> Self<>{
        Self{mean:None}
    }
}

impl<G: Scope> ColumnEncoder<G> for StandardScaler<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, (usize, RowValue))>) {
        let m = data.reduce(|_key, input, output| {
            let mut sum: f64 = 0.0;
            for ((_ix, v), _count) in input {
                sum += (*v).get_float();
            }
            sum = sum / input.len() as f64;
            output.push((DenseVector::Scalar(sum), 1));
        });
        self.mean = Some(m);
    }

    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G, (usize, DenseVector)> {
        let mean = match &self.mean {
            None => panic!("called transform before fit"),
            Some(m) => m
        };
        data.join(&mean)
            .map(|(_key, ((ix, val), mean))| (ix, DenseVector::Scalar(val.get_float() - mean.get_first_value()))  )
    }
}

pub fn standard_scale_fit<G: Scope>(
    data: &Collection<G, (usize, isize)>,
) -> Collection<G, (usize, isize)>
where G::Timestamp: Lattice+Ord
{
    data.reduce(|_key, input, output| {
        let mut sum = 0;
        let mut counts= 0;
        //println!("{}", input.len());
        for (v, count) in input {
            sum += *v * count;
            counts += count;
        }
        sum = sum / counts;
        output.push((sum, 1));
    })
}

pub fn standard_scale_transform<G: Scope>(
    data: &Collection<G, (usize, isize)>,
    meta: &Collection<G, (usize, isize)>,
) -> Collection<G, isize>
where G::Timestamp: Lattice+Ord
{
    data.join(meta)
        .map(|(_key, val)| val.0 - val.1)
}

pub fn recode_fit<G: Scope, D>(
    data: &Collection<G, (usize, D)>,
) -> Collection<G, (usize, (D, usize))>
where
    G::Timestamp: Lattice+Ord,
    D: differential_dataflow::ExchangeData + std::hash::Hash
{
    data.distinct()
        .reduce(|_key, input, output| {

            println!("{}", input.len());
            for (code, (val, _count)) in input.iter().enumerate(){
                output.push((((*val).clone(), code), 1));
            };
        })
}

// static encoder is just for testing purposes
// same as multi_column_encoder but with predefined, static encoding scheme
pub fn static_encoder<G: Scope>(
    data: &Collection<G, (usize, Row)>,
) -> Collection<G, DenseVector>
where G::Timestamp: Lattice+Ord{
    let tmp = data.map(| (ix, row)| (1, (ix, row.values[0].clone())));
    let mut scaler = StandardScaler::new();
    scaler.fit(&tmp);
    scaler.transform(&tmp).map(|(_ix, val)| val)
}
