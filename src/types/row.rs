use std::cmp::Ordering;
use serde::{Deserialize, Serialize};
use crate::types::row_value::RowValue;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<RowValue>,
    pub size: usize,
}
impl Row {
    // pub fn append_integer(&mut self, value: i64) {
    //     self.values.push(RowValue::Integer(value));
    //     self.size += 1;
    // }

    // pub fn append_row_value(mut self, val: RowValue) -> Self {
    //     self.values.push(val);
    //     self.size += 1;
    //     self
    // }

    // Create a new Row with one integer, float, and string
    pub fn with_values(integer: i64, float: f64, text: String) -> Self {
        Row {
            values: vec![
                RowValue::Integer(integer),
                RowValue::Float(float),
                RowValue::Text(text), ],
            size: 3,
        }
    }

    pub fn with_integer_vec(integers: Vec<i64>) -> Self {
        Row {
            size: integers.len(),
            values: integers.into_iter().map(RowValue::Integer).collect(),
        }
    }

    pub fn with_row_value(val: RowValue) -> Self{
        Row {
            values: vec![val],
            size: 1,
        }
    }

    pub fn with_row_values(vals: Vec<RowValue>) -> Self{
        Row {
            size: vals.len(),
            values: vals,
        }
    }


    pub fn find_indices<F>(&self, predicate: F) -> Vec<usize>
    where
        F: Fn(&RowValue) -> bool,
    {
        self.values
            .iter()
            .enumerate()
            .filter_map(|(i, value)| if predicate(value) { Some(i) } else { None })
            .collect()
    }
}

impl Eq for Row {}

impl PartialEq<Self> for Row {
    fn eq(&self, other: &Self) -> bool {
        if self.size != other.size {
            false;
        }
        self.values.eq(&other.values)
    }
}

impl PartialOrd<Self> for Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.values.partial_cmp(&other.values)
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare rows based on their `values` vector
        self.values.iter().cmp(other.values.iter())
    }
}