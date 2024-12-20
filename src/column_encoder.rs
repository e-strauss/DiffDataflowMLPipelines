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
    fn fit(&mut self, data: &Collection<G, (usize, (usize, RowValue))>);

    /// Transforms the input data using the provided metadata.
    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G,  (usize, RowValue)>;
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

pub fn static_encoder<G: Scope>(
    data: &Collection<G, (usize, Row)>,
) -> Collection<G, RowValue>
where G::Timestamp: Lattice+Ord{
    let tmp = data.map(| (ix, row)| (1, (ix, row.values[0].clone())));
    let mut scaler = StandardScaler::new();
    scaler.fit(&tmp);
    scaler.transform(&tmp).map(|(_ix, val)| val)
}

// pub fn multi_column_encoder<G: Scope, Enc>(
//     data: &Collection<G, (usize, Row)>,
//     config: &mut [(usize, &Enc)]
// ) -> Collection<G, Row>
// where
//     G::Timestamp: Lattice+Ord,
//     Enc: ColumnEncoder<G> + Copy {
//     let (first_col, first_enc) = (*config)[0];
//     let col = data.map(move | (k, row)| (1,(k, row.values[first_col].clone())));
//     let mut first_enc = first_enc.clone();
//     first_enc.fit(&col);
//     let mut out = first_enc
//         .transform(&col)
//         .map(|(ix, val)| (ix, Row::with_row_value(val)));
//     for (col_id, enc) in & mut config[1..] {
//         let col = data.map(| (k, row)| (1, (k, row.values[*col_id].clone())));
//         let mut enc = enc.clone();
//         enc.fit(&col);
//         let num_col = enc.transform(&col);
//         let tmp = out.join(&num_col);
//         out = tmp.map(|(ix, (row, row_val))| (ix, row.append_row_value(row_val)));
//     }
//     return out.map(|(_ix, val)| val);
// }
