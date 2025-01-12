use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count, Join, Reduce, Threshold};
use timely::dataflow::{Scope};
use crate::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::dense_vector::DenseVector;

pub struct OneHotEncoder <G: Scope> {
    distinct_enumerated: Option<Collection<G, (RowValue, (usize, usize))>>,
    epoch : i64
}

impl<G: Scope> OneHotEncoder<G> {
    pub fn new() -> Self<>{
        Self{distinct_enumerated:None, epoch: 0}
    }
}

impl<G: Scope> ColumnEncoder<G> for OneHotEncoder<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, (usize, RowValue))>) {
        let distinct = data.map(|(_, (_, row_value))| row_value).distinct().map(|val| (1, val));
        /*let distinct = match &self.distinct {
            None => distinct.map(|(_, val)| {(1, val, self.epoch)}),
            Some(m) => distinct.map(|(_, val)| (1, val.clone(), m.filter(|(_, val2, _)| val.eq(val2)).() > 0))
        }; */
        //TODO better ordering using inspect based on insertion time (for sparse vectors)
        self.distinct_enumerated = Some(distinct.reduce(|_key, input, output| {
            let n = input.len();
            let mut i = 0;
            //let mut sorted = input.clone();
            //sorted.sort_by(|a, b| a.0.cmp(b.0));
            for (v, _count) in input {
                let owned_v = (*v).clone();
                output.push(((owned_v, i.clone(), n.clone()), 1));
                i = i+1;
            }
        }).map(|(_key, (v, i, n))| (v, (i, n))));
    }

    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G, (usize, DenseVector)> {
        let distinct_enumerated = match &self.distinct_enumerated {
            None => panic!("called transform before fit"),
            Some(m) => m
        };

        let data_inv = data.map(|(key, (i, v)) | (v, (key, i)));
        let joined = data_inv.join(&distinct_enumerated);

        joined.map(|(_value, ((_key, i), (vector_idx, n)))| {
            let mut vec = vec![0f64; n];
            vec[vector_idx] = 1.0;
            (i, DenseVector::Vector(vec))
        })
    }
}