use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::BatchContainer;
use timely::dataflow::{Scope, ScopeParent};
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::dense_vector::DenseVector;
use crate::types::row_value::RowValue;
use crate::types::row_value::RowValue::{Float, Integer};

pub struct PolynomialFeaturesEncoder {
    min_degree : usize,
    max_degree : usize,
    combinations : Option<Vec<Vec<usize>>>
}

impl PolynomialFeaturesEncoder {
    pub fn new(min_degree : usize, max_degree : usize) -> Self<>{
        Self{min_degree, max_degree, combinations : None}
    }

    fn polynomials(&self, input_vec: Vec<f64>) -> DenseVector {
        let combinations = match &self.combinations {
            Some(v) => v,
            None => panic!("called transform before fit")
        };
        let mut output_vec = vec![0f64; combinations.len()];
        for i in 0..combinations.len() {
            let combination = &combinations[i];
            let mut x = 1f64;
            for j in combination.iter() {
                x *= input_vec[*j];
            }
            output_vec[i] = x;
        }
        DenseVector::Vector(output_vec)
    }

    fn polynomials_1d(&self, value: f64, min_degree: usize, max_degree: usize) -> DenseVector {
        let mut vec = vec![0f64; 1 + max_degree - min_degree];
        let mut x = value.powi(min_degree as i32);
        for idx in min_degree..=max_degree {
            vec[idx - min_degree] = x;
            x *= value;
        }
        DenseVector::Vector(vec)
    }
}

impl<G: Scope> ColumnEncoder<G> for PolynomialFeaturesEncoder
where G::Timestamp: Lattice+Ord {


    fn fit(&mut self, data: &Collection<G, (usize, (usize, RowValue))>) {
        data
            .consolidate()
            .inspect(|x| match x {
                _ => {}
                _ => { //TODO set this to vector later
                    let vec = vec![0f64, 10 as f64]; //dummy for extracted vector
                    self.combinations = Some(combinations_with_replacement(vec.len(), self.min_degree, self.max_degree))
                }
            });
    }

    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G, (usize, DenseVector)> {




        let min_degree = self.min_degree.clone();
        let max_degree = self.max_degree.clone();
        data.map(move |(_, (i, row_value))| {
            (i, match &row_value {
                Integer(i) => self.polynomials_1d(*i as f64, min_degree, max_degree),
                Float(f) => self.polynomials_1d(*f, min_degree, max_degree),
                //TODO allow processing on Vectors
                _ => panic!("cant apply to this rowvalue type"),
            })
        })
    }
}

fn combinations_with_replacement(len: usize, min_degree: usize, max_degree: usize) -> Vec<Vec<usize>> {
    let mut combinations = Vec::new();
    for degree in min_degree..=max_degree {
        let mut current_combination = Vec::new();
        recurse(0, 0, degree, len, &combinations, &current_combination);
    }
    combinations
}

fn recurse(i : usize, current_degree : usize, target_degree : usize, n_features : usize, mut combinations: &Vec<Vec<usize>>, mut current_combination : &Vec<usize>) {
    if(current_degree == target_degree){
        combinations.push(current_combination.clone());
        return;
    }
    if(i == n_features){
        return;
    }
    recurse(i + 1, current_degree, target_degree, n_features, combinations, current_combination);

    current_combination.push(i);
    recurse(i, current_degree + 1, target_degree, n_features, combinations, current_combination);
    current_combination.pop();

}