use std::hash::Hash;
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use serde::{Deserialize, Serialize};
use crate::types::safe_hash_map::SafeHashMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PositionAssignmentAggregate<T>
where T: Ord + Clone + Hash {
    val_to_index: SafeHashMap<T, usize>,
    val_to_count: SafeHashMap<T, isize>,

    free_indices: Vec<usize>,
    next_index: usize,
    len : usize,
    neg : bool,
    row_count : isize
}


impl<T> PositionAssignmentAggregate<T>
where T: Ord + Clone + Hash {
    pub fn get_map_and_len(self) -> (SafeHashMap<T, usize>, usize) {
        (self.val_to_index, self.len)
    }

    pub fn new_with_vec(tokens : &Vec<T>, mult : isize) -> Self {
        let mut agg = Self::zero();
        agg.row_count = mult;
        for token in tokens {
            agg.plus_equals_value_count(token, mult);
        }
        agg
    }

    pub fn new_with_val(val : &T, mult : isize) -> Self {
        let mut agg = Self::zero();
        agg.row_count = mult;
        agg.plus_equals_value_count(val, mult);
        agg
    }


    fn assign_index(&mut self) -> usize {
        if self.free_indices.len() > 0 {
            self.free_indices.pop().unwrap()
        } else {
            self.next_index += 1;
            self.next_index-1
        }
    }

    fn compress(&mut self) {
        let keys: Vec<T> = self.val_to_index.keys().cloned().collect();

        let mut new_map = SafeHashMap::new();
        for (new_index, key) in keys.iter().enumerate() {
            new_map.insert(key.clone(), new_index);
        }

        self.val_to_index = new_map;
    }

    fn plus_equals_value_count(&mut self, value: &T, count_to_add: isize) {
        match self.val_to_count.get(&value) {
            Some(c) => {
                let count = c.clone();
                self.val_to_count.insert(value.clone(), count + count_to_add);
                if count > 0 && count + count_to_add <= 0 {
                    let index = *self.val_to_index.get(&value).unwrap();
                    self.free_indices.push(index);
                } else if count <= 0 && count + count_to_add > 0 {
                    let new_index = self.assign_index();
                    self.val_to_index.insert(value.clone(), new_index);
                }
            },
            _ => {
                self.val_to_count.insert(value.clone(), count_to_add);
                if count_to_add > 0 {
                    let new_index = self.assign_index();
                    self.val_to_index.insert(value.clone(), new_index);
                }
            }
        }
        if self.value_count() > self.len {
            while self.value_count() > self.len {
                self.len = (self.len as f64 * 1.5).round() as usize;
            }
        } else if self.value_count() < (self.len as f64 * 0.66).floor() as usize {
            self.compress();
            while self.value_count() < (self.len as f64 * 0.66).floor()  as usize {
                self.len = (self.len as f64 * 0.66).ceil() as usize;
            }
        }

    }

    fn value_count(&self) -> usize {
        self.val_to_count.len() - self.free_indices.len()
    }



}

impl<T> IsZero for PositionAssignmentAggregate<T>
where T: Ord + Clone + Hash {
    fn is_zero(&self) -> bool {
        self.row_count == 0
    }
}

impl<T> Semigroup for PositionAssignmentAggregate<T>
where T: Ord + Clone + Hash {
    fn plus_equals(&mut self, other: &Self) {
        for (value, _) in other.val_to_index.iter() {
            let mut other_count = *other.val_to_count.get(value).unwrap();
            other_count =  if !(self.neg ^ other.neg) {other_count} else {-other_count};
            self.plus_equals_value_count(&value, other_count)
        }
        //println!("len 1: {:x} , len2: {:x}", self.value_count(), other.value_count());
        self.row_count += other.row_count;
    }
}

impl<T> Monoid for PositionAssignmentAggregate<T>
where T: Ord + Clone + Hash {
    fn zero() -> Self {
        Self { val_to_index: SafeHashMap::new(), val_to_count: SafeHashMap::new(), free_indices: Vec::new(), next_index: 0, neg: false, row_count: 0, len:1}
    }
}

impl<T> Abelian for PositionAssignmentAggregate<T>
where T: Ord + Clone + Hash {
    fn negate(&mut self) {
        self.neg = !self.neg;
        self.row_count *= -1;
    }
}