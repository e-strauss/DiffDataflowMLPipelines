use std::hash::{DefaultHasher, Hash, Hasher};
use std::ptr::hash;
use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Reduce, Threshold};
use timely::dataflow::{Scope};
use crate::ColumnEncoder;
use crate::feature_encoders::feature_extraction::utils::default_tokenizer;
use crate::types::row_value::RowValue;
use crate::types::row_value::RowValue::Text;

pub struct HashVectorizer {
    n_features : usize,
    binary : bool
}

impl HashVectorizer {
    pub fn new(n_features : usize, binary : bool) -> Self<>{
        Self{n_features, binary}
    }
}

impl<G: Scope> ColumnEncoder<G> for HashVectorizer
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let n_features = self.n_features.clone();
        let binary = self.binary.clone();
        data.map(move |(i, row_value)| {
            let text = match &row_value {
                Text(s) => s,
                _ => panic!("can only apply to text features"),
            };
            let mut vec = vec![0f64; n_features];
            let tokens = default_tokenizer(&text);
            for token in tokens {
                let mut hasher = DefaultHasher::new();
                token.hash(&mut hasher);
                let hash_value = hasher.finish() as usize % n_features;
                if binary {
                    vec[hash_value] = 1.0;
                } else {
                    vec[hash_value] += 1.0;
                }
            }
            (i, RowValue::Vec(vec))
        })
    }
}