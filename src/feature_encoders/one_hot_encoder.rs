use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Reduce, Threshold};
use timely::dataflow::{Scope, ScopeParent};
use crate::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::dense_vector::DenseVector;

pub struct OneHotEncoder <G: Scope> {
    distinct: Option<Collection<G, (usize, RowValue)>>,
}

impl<G: Scope> OneHotEncoder<G> {
    pub fn new() -> Self<>{
        Self{distinct:None}
    }
}

impl<G: Scope> ColumnEncoder<G> for OneHotEncoder<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, (usize, RowValue))>) {
        self.distinct = Some(data.map(|(_, (_, row_value))| row_value).distinct().map(|val| (1, val)));

    }

    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G, (usize, DenseVector)> {
        let distinct = match &self.distinct {
            None => panic!("called transform before fit"),
            Some(m) => m
        };
        let enumerated = distinct.reduce(|_key, input, output| {
            let n = input.len();
            let mut i = 0;
            //let mut sorted = input.clone();
            //sorted.sort_by(|a, b| a.0.cmp(b.0));
            for (v, _count) in input {
                let owned_v = (*v).clone();
                output.push(((owned_v, i.clone(), n.clone()), 1));
                i = i+1;
            }
        }).map(|(_key, (v, i, n))| (v, (i, n)));

        //enumerated.inspect(|x| println!("DISTINCT: {:?}", x));

        let data_inv = data.map(|(key, (i, v)) | (v, (key, i)));
        let joined = data_inv.join(&enumerated);

        joined.map(|(value, ((_key, i), (vector_idx, n)))| {
            let mut vec = vec![0f64; n];
            vec[vector_idx] = 1.0;
            (i, DenseVector::Vector(vec))
        })
    }
}