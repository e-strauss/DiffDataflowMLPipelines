use std::collections::{BTreeMap, HashMap};
use differential_dataflow::Collection;
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count, Join, Threshold};
use serde::{Deserialize, Serialize};
use timely::dataflow::{Scope};
use crate::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::safe_f64::SafeF64;
use crate::types::safe_hash_map::SafeHashMap;


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct HashMapAggregate {
    map: SafeHashMap,
    count: isize
}

impl HashMapAggregate {
    fn new(value: &RowValue) -> Self {
        let mut map: SafeHashMap = SafeHashMap::new();
        map.0.insert(value.clone(), SafeF64(1.0));
        Self { map, count: 1, }
    }

    fn get(&self) -> SafeHashMap {
        // variance = M2 / count
        self.map.clone()
    }
}

impl Monoid for HashMapAggregate {
    fn zero() -> Self {
        Self{map: SafeHashMap::new(), count: 0}
    }
}

impl Semigroup for HashMapAggregate {
    fn plus_equals(&mut self, rhs: &Self) {
        //println!("{:?} + {:?}",self, rhs);
        match (self.count, rhs.count) {
            (_, 0) => {},
            (0, rhs_count) => {
                self.count = rhs_count;
                self.map = rhs.map.clone();},
            (count, 1) => {
                self.count = count + 1;
                let val = rhs.map.0.iter().next().unwrap();
                self.map.insert(val.0.clone(), SafeF64(self.count as f64));},
            _ => {}
        }
    }
}

impl IsZero for HashMapAggregate {
    fn is_zero(&self) -> bool {
        self.count == 0
    }
}

impl Abelian for HashMapAggregate {
    fn negate(&mut self) {
        panic!("Removal of values is not implemented yet")
    }
}


pub struct OrdinalEncoderHash <G: Scope> {
    value_map: Option<Collection<G, ((RowValue, RowValue))>>,
}

impl<G: Scope> OrdinalEncoderHash<G> {
    pub fn new() -> Self<>{
        Self{value_map:None}
    }
}

impl<G: Scope> ColumnEncoder<G> for OrdinalEncoderHash<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let distinct = data.map(|(_, row_value)| row_value).distinct();
        self.value_map = Some(distinct
            .threshold(|value, _| HashMapAggregate::new(value))
            .map(|_vector| ())
            .count()
            .flat_map(|((), agg)| agg.get().0.iter().map(|(k,v)| (k.clone(), v.clone())).collect::<Vec<_>>())
            .map(|(k, v)| (k, RowValue::Float(v.0)))
            //.inspect(|x| {println!("{:?}", x)})
        );
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let value_map = match &self.value_map {
            None => panic!("called transform before fit"),
            Some(m) => m
        };

        let joined = data.map(|(rix, v) | (v, rix) )
            .join(value_map);

        joined.map(|(k, (rix, v))| (rix, v))
    }
}