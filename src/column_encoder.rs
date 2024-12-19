use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Threshold};
use differential_dataflow::operators::Reduce;
use timely::dataflow::Scope;
use crate::row::{Row, RowValue};

pub trait ColumnEncoder<G: Scope, Meta> {
    /// Fits the encoder on the input data and returns metadata.
    fn fit(&self, data: &Collection<G, (usize, RowValue)>) -> Meta;

    /// Transforms the input data using the provided metadata.
    fn transform(&self, data: &Collection<G, (usize, RowValue)>, meta: &Meta) -> Collection<G, (usize, i64)>;
}

pub fn standard_scale_fit<G: Scope>(
    data: &Collection<G, (usize, isize)>,
) -> Collection<G, (usize, isize)>
where G::Timestamp: Lattice+Ord
{
    data.reduce(|_key, input, output| {
        let mut sum: isize = 0;
        //println!("{}", input.len());
        for (v, count) in input {
            sum += *v * count;
        }
        sum = sum / input.len() as isize;
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

pub fn static_encoder<G: Scope>(
    data: &Collection<G, (usize, Row)>,
) -> Collection<G, (usize, i64)>
where G::Timestamp: Lattice+Ord{
    data.reduce(|_key, input, output| {
        let mut sum = 0;
        for (row, count) in input {
            sum += (*row).values[0].get_integer()*(i64::try_from(*count).unwrap());
        }
        output.push((sum, 1));
    })
}
