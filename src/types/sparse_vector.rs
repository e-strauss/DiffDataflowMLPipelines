/*use std::cmp::max;
use std::collections::BTreeMap;
use std::ops::{AddAssign, Neg};
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use crate::types::row_value::RowValue;

#[derive(Debug, Clone)]
pub struct SparseVector {
    values : BTreeMap<usize, RowValue>,
    size: usize,
}

impl SparseVector {
    pub fn new(size: usize) -> Self {
        SparseVector {
            values: BTreeMap::new(),
            size,
        }
    }
}




impl AddAssign for SparseVector {
    fn add_assign(&mut self, rhs: Self) {
        if self.size != rhs.size {
            panic!("SparseVectors must have the same size to add");
        }
        for (index, value) in rhs.values {
            self.values
                .entry(index)
                .and_modify(|v|  *v = v.clone() + value.clone())
                .or_insert(value.clone());
        }
    }
}

impl Neg for SparseVector {
    type Output = SparseVector;

    fn neg(self) -> SparseVector {
        let mut negated_values = BTreeMap::new();

        for (index, value) in self.values {
            let negated_value = match value {
                RowValue::Integer(v) => RowValue::Integer(-v),
                RowValue::Float(v) => RowValue::Float(-v),
                RowValue::Text(_) => panic!("Cannot negate a Text value"),
            };
            negated_values.insert(index, negated_value);
        }

        SparseVector {
            values: negated_values,
            size: self.size,
        }
    }
}

*/
