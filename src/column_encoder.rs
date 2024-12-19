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
    /// Fits the encoder on the input data and returns metadata.
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>);

    /// Transforms the input data using the provided metadata.
    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G,  i64>;
}

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
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let m = data.reduce(|_key, input, output| {
            let mut sum: i64 = 0;
            for (v, _count) in input {
                sum += (*v).get_integer();
            }
            sum = sum / input.len() as i64;
            output.push((sum, 1));
        });
        self.mean = Some(m);
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, i64> {
        let mean = match &self.mean {
            None => panic!("called transform before fit"),
            Some(m) => m
        };
        data.join(&mean)
            .map(|(_key, (val, mean))| val.get_integer() - mean )
    }
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
) -> Collection<G, i64>
where G::Timestamp: Lattice+Ord{
    // data.reduce(|_key, input, output| {
    //     let mut sum = 0;
    //     for (row, count) in input {
    //         sum += (*row).values[0].get_integer()*(i64::try_from(*count).unwrap());
    //     }
    //     output.push((sum, 1));
    // })
    let tmp = data.map(| (k, row)| (k, row.values[0].clone()));
    let mut scaler = StandardScaler::new();
    scaler.fit(&tmp);
    scaler.transform(&tmp)


}
