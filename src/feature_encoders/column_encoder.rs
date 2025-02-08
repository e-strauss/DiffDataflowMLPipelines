use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Threshold};
use differential_dataflow::operators::Reduce;
use timely::dataflow::{Scope};
use crate::feature_encoders::standard_scaler::StandardScaler;
use crate::types::row::Row;
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



/****************************************/
/*      ENCODER IMPLEMENTATIONS:        */
/****************************************/


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
/*pub fn static_encoder<G: Scope>(
    data: &Collection<G, (usize, Row)>,
) -> Collection<G, RowValue>
where G::Timestamp: Lattice+Ord{
    let tmp = data.map(| (ix, row)| (1, (ix, row.values[0].clone())));
    let mut scaler = StandardScaler::new();
    scaler.fit(&tmp);
    //scaler.transform(&tmp).map(|(_ix, val)| val)
}*/
