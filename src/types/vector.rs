use serde::{Deserialize, Serialize};
use crate::types::dense_vector::DenseVector;
//use crate::types::sparse_vector::SparseVector;
//use crate::types::vector::Vector::{Dense, Sparse};

/*#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Vector {
    Dense(DenseVector),
    Sparse(SparseVector),
}

impl Vector {
    pub fn get_dense(&self) -> i64 {
        match *self {
            Dense(d) => {d}
            _ => panic!("get_dense called on sparse vector"),
        }
    }

    pub fn get_sparse(&self) -> i64 {
        match *self {
            Sparse(s) => {s}
            _ => panic!("get_dense called on sparse vector"),
        }
    }
} */