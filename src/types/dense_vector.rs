use std::cmp::Ordering;
use std::ops::{AddAssign, Neg};
use serde::{Deserialize, Serialize};

// struct DenseNumVec {
//     values: Vec<f64>,
//     size: usize,
// }
//
// impl DenseNumVec {
//     pub fn new(size: usize) -> Self {
//         DenseNumVec {
//             size,
//             values: vec![0.0; size],
//         }
//     }
//
//     pub fn with_value(val: f64, size: usize) -> Self {
//         let tmp = DenseNumVec {
//             size,
//             values: vec![0.0; size],
//         };
//         tmp.values[0] = val;
//         tmp
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DenseVector {
    Scalar(f64),
    Vector(Vec<f64>),
}

impl Eq for DenseVector {}

impl PartialEq<Self> for DenseVector {
    fn eq(&self, other: &Self) -> bool {
        match (self,other) {
            (DenseVector::Scalar(a), DenseVector::Scalar(b)) => a == b,
            (DenseVector::Vector(a), DenseVector::Vector(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd<Self> for DenseVector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (DenseVector::Scalar(a), DenseVector::Scalar(b)) => a.partial_cmp(b),
            (DenseVector::Vector(a), DenseVector::Vector(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Ord for DenseVector {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (DenseVector::Scalar(a), DenseVector::Scalar(b)) => a.partial_cmp(b).unwrap(),
            (DenseVector::Vector(a), DenseVector::Vector(b)) => a.partial_cmp(b).unwrap(),
            _ => panic!("LHS Vector"),
        }
    }
}

impl DenseVector {
    fn new_with_capacity(capacity: usize) -> Self {
        match capacity {
            1 => DenseVector::Scalar(0.0),
            _ => DenseVector::Vector(Vec::with_capacity(capacity)), }
    }

    pub fn get_first_value(&self) -> f64 {
        match self {
            DenseVector::Scalar(a) => *a,
            DenseVector::Vector(a) => a[0],
        }
    }

    pub fn increase_capacity(self, capacity: usize) -> Self {
        match (self, capacity) {
            (vec, 0) => vec,
            (DenseVector::Scalar(a), cap) => {
                let mut vec = Vec::with_capacity(1 + cap);
                println!("new capacity: {}", vec.capacity());
                vec.push(a);
                DenseVector::Vector(vec)
            },
            (DenseVector::Vector(mut a), cap) => {
                a.reserve(cap + a.capacity());
                DenseVector::Vector(a)
            }
        }
    }

    pub fn concat_dense_vector(self, other: DenseVector) -> Self {
        match (self, other) {
            (DenseVector::Scalar(a), DenseVector::Scalar(b)) => {
                DenseVector::Vector(vec![a, b])
            }
            (DenseVector::Scalar(a), DenseVector::Vector(mut b)) => {
                b.insert(0, a);
                DenseVector::Vector(b)
            }
            (DenseVector::Vector(mut a), DenseVector::Scalar(b)) => {
                a.push(b);
                DenseVector::Vector(a)
            }
            (DenseVector::Vector(mut a), DenseVector::Vector(b)) => {
                a.extend(b);
                DenseVector::Vector(a)
            }
        }
    }

    pub fn scale(&mut self, factor : f64) {
        match self {
            DenseVector::Scalar(s) => {
                *s *= factor;
            }
            DenseVector::Vector(vec) => {
                for x in vec.iter_mut() {
                    *x *= factor;
                }
            }
        }
    }

    pub fn binarize(&mut self) {
        let epsilon = 1e-10;
        match self {
            DenseVector::Scalar(s) => {
                *s = if (*s - 0.0).abs() < epsilon { 1f64 } else { 0f64 };
            }
            DenseVector::Vector(vec) => {
                for x in vec.iter_mut() {
                    *x = if (*x - 0.0).abs() < epsilon { 1f64 } else { 0f64 };
                }
            }
        }
    }
}

impl AddAssign for DenseVector {
    fn add_assign(&mut self, rhs: DenseVector) {
        match (self, rhs) {
            (DenseVector::Scalar(lhs), DenseVector::Scalar(rhs)) => {
                *lhs += rhs;
            }
            (DenseVector::Vector(lhs), DenseVector::Vector(rhs)) => {
                if lhs.len() != rhs.len() {
                    panic!("Vectors must have the same length to add");
                }
                for (l, r) in lhs.iter_mut().zip(rhs.iter()) {
                    *l += r;
                }
            }
            _ => panic!("cant add scalar and vector"),
        }
    }
}

impl Neg for DenseVector {
    type Output = DenseVector;

    fn neg(self) -> DenseVector {
        match self {
            DenseVector::Scalar(v) => DenseVector::Scalar(-v),
            DenseVector::Vector(v) => DenseVector::Vector(v.into_iter().map(|x| -x).collect()),
        }
    }
}