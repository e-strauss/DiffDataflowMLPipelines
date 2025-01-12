use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::ops::{Add, AddAssign};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RowValue {
    Integer(i64),
    Text(String),
    Float(f64),
}

impl RowValue {
    pub fn get_integer(&self) -> i64 {
        match *self {
            RowValue::Integer(a) => {a}
            _ => panic!("get_integer called on non-integer row value"),
        }
    }

    pub fn get_float(&self) -> f64 {
        match *self {
            RowValue::Integer(a) => {a as f64}
            RowValue::Float(a) => {a}
            _ => panic!("get_float called on non-numeric row value"),
        }
    }
}



// impl Sub for RowValue {
//     type Output = Self;
//
//     fn sub(self, rhs: Self) -> Self::Output {
//         match (self, rhs) {
//             (RowValue::Integer(i), RowValue::Integer(j)) => {RowValue::Integer(i - j)}
//             (_, _) => panic!("sub - called on non-integer row value"),
//         }
//     }
// }

impl Eq for RowValue {}

impl PartialEq<Self> for RowValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RowValue::Integer(a), RowValue::Integer(b)) => a == b,
            (RowValue::Float(a), RowValue::Float(b)) => a == b,
            (RowValue::Text(a), RowValue::Text(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd<Self> for RowValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (RowValue::Integer(a), RowValue::Integer(b)) => a.partial_cmp(b),
            (RowValue::Float(a), RowValue::Float(b)) => a.partial_cmp(b),
            (RowValue::Text(a), RowValue::Text(b)) => a.partial_cmp(b),
            _ => panic!("Cannot compare RowValue of different types!"),
        }
    }
}

impl Ord for RowValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (RowValue::Integer(a), RowValue::Integer(b)) => a.cmp(b),
            (RowValue::Float(a), RowValue::Float(b)) => a.partial_cmp(b).expect("Comparison failed"),
            (RowValue::Text(a), RowValue::Text(b)) => a.cmp(b),
            _ => panic!("Cannot compare RowValue of different types!"),
        }
    }
}

impl Hash for RowValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            RowValue::Integer(a) => {
                0.hash(state);
                a.hash(state);
            },
            RowValue::Text(a) => {
                1.hash(state);
                a.hash(state);
            },
            _ => panic!("Can only hash integers and strings!"),
        }
    }

}

impl Add for RowValue {
    type Output = RowValue;

    fn add(self, rhs: RowValue) -> RowValue {
        match (self, rhs) {
            (RowValue::Integer(lhs), RowValue::Integer(rhs)) => RowValue::Integer(lhs + rhs),
            (RowValue::Float(lhs), RowValue::Float(rhs)) => RowValue::Float(lhs + rhs),
            (RowValue::Integer(lhs), RowValue::Float(rhs)) => RowValue::Float(lhs as f64 + rhs),
            (RowValue::Float(lhs), RowValue::Integer(rhs)) => RowValue::Float(lhs + rhs as f64),
            _ => panic!("Cannot add mismatched RowValue types"),
        }
    }
}