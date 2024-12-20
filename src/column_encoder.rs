use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Threshold};
use differential_dataflow::operators::Reduce;
use timely::dataflow::Scope;
use crate::row::{Row, RowValue};

pub trait ColumnEncoder<G: Scope>
where
    G::Timestamp: Lattice+Ord,
{
    /// Fits the encoder on the input data and stores metadata internally (in the struct)
    fn fit(&mut self, data: &Collection<G, (usize, (usize, RowValue))>);

    /// Transforms the input data using the internally stored metadata
    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G,  (usize, RowValue)>;
}

// multi_column_encoder = sklearn's ColumnTransformer
pub fn multi_column_encoder<G: Scope, Enc>(
    data: &Collection<G, (usize, Row)>,
    config: Vec<(usize, Enc)>
) -> Collection<G, Row>
where
    G::Timestamp: Lattice+Ord,
    Enc: ColumnEncoder<G> {
    let mut col_iterator = config.into_iter();

    if let Some((col_id, mut first_enc)) = col_iterator.next() {
        // Handle the first element init out with Row with one value
        // slice out current column
        let col = data.map(move | (ix, row)| (1,(ix, row.values[col_id].clone())));
        first_enc.fit(&col);
        let mut out = first_enc
            .transform(&col)
            .map(|(ix, val)| (ix, Row::with_row_value(val)));

        // Process the rest using the same iterator
        for (col_id, mut enc) in col_iterator {
            // slice out current column
            let col = data.map(move | (ix, row)|
                (1, (ix, row.values[col_id].clone())));
            enc.fit(&col);
            out = out.join(&enc.transform(&col))
                .map(|(ix, (row, row_val))|
                    (ix, row.append_row_value(row_val)));
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
    mean: Option<Collection<G, (usize, i64)>>,
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
            let mut sum: i64 = 0;
            for ((_ix, v), _count) in input {
                sum += (*v).get_integer();
            }
            sum = sum / input.len() as i64;
            output.push((sum, 1));
        });
        self.mean = Some(m);
    }

    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G, (usize, RowValue)> {
        let mean = match &self.mean {
            None => panic!("called transform before fit"),
            Some(m) => m
        };
        data.join(&mean)
            .map(|(_key, ((ix, val), mean))| (ix, RowValue::Float((val.get_integer() - mean) as f64))  )
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
) -> Collection<G, RowValue>
where G::Timestamp: Lattice+Ord{
    let tmp = data.map(| (ix, row)| (1, (ix, row.values[0].clone())));
    let mut scaler = StandardScaler::new();
    scaler.fit(&tmp);
    scaler.transform(&tmp).map(|(_ix, val)| val)
}
