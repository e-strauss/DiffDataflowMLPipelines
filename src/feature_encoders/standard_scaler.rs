use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::Join;
use differential_dataflow::operators::Reduce;
use timely::dataflow::Scope;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::dense_vector::DenseVector;
use crate::types::row_value::RowValue;

pub struct StandardScaler < G: Scope> {
    mean: Option<Collection<G, (usize, DenseVector)>>,
}

impl<G: Scope> StandardScaler<G> {
    pub fn new() -> Self{
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