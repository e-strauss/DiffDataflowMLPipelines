use std::cmp::Ordering;
use timely::{Data, ExchangeData};
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
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<RowValue>,
    pub size: usize,
}
impl Row {
    // Create a new empty Row
    pub fn new() -> Self {
        Row {
            values: Vec::new(),
            size: 0,
        }
    }
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