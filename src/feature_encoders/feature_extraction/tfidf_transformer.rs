use std::cmp::max;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::{AddAssign, Neg};
use differential_dataflow::Collection;
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count, Join, Threshold};
use serde::{Deserialize, Serialize};
use timely::dataflow::{Scope};
use crate::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::dense_vector::DenseVector;



pub struct TfidfTransformer<G: Scope> {
    frequencies : Option<Collection<G, ((), DocumentFrequencyAggregate)>>,
}

impl<G: Scope> TfidfTransformer<G> {
    pub fn new() -> TfidfTransformer<G> {
        Self{frequencies:None}
    }
}



impl<G: Scope> ColumnEncoder<G> for TfidfTransformer<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let transformed = data
            .map(|(_, vector)| {
                match &vector {
                    RowValue::Vec(v) => {
                        let epsilon = 1e-10;
                        let v: Vec<isize> = v.iter()
                            .map(|&x| if (x - 0.0).abs() < epsilon { 1 } else { 0 })
                            .collect();
                        return v;
                    }
                    _ => panic!("this should not happen in theory (backend doesnt yield Vec)")
                };
            });
        let frequencies = transformed
            .threshold(|vector, multiplicity| {
                DocumentFrequencyAggregate::new(vector.clone(), *multiplicity)
            })
            .map(|vector| ())
            .count();
        self.frequencies = Some(frequencies);
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let frequencies = match &self.frequencies {
            None => panic!("called transform before fit"),
            Some(f) => f
        };
        data
            .map(|x| ((), x))
            .join(&frequencies)
            .map(|(_, ((id, dense), frequencies))| {
                let freq_vector = match frequencies.get_frequencies() {
                    None => panic!("this should not happen in theory (would mean that the aggregate is empty)"),
                    Some(v) => v
                };
                let doc = match &dense {
                    RowValue::Vec(v) => v,
                    _ => panic!("this should not happen in theory (backend doesnt yield Vec)"),
                };
                let tfidf = doc
                    .iter()
                    .zip(freq_vector.iter())
                    .map(|(&doc_count, &freq)| {
                        if doc_count == 0.0 || freq == 0 {
                            0.0
                        } else {
                            let tf = doc_count;
                            let idf = (frequencies.count as f64 / freq as f64).ln();
                            tf * idf
                        }
                    })
                    .collect();

                (id, RowValue::Vec(tfidf))
            })
    }
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct DocumentFrequencyAggregate {
    frequencies: Option<Vec<isize>>,
    count: isize
}

impl DocumentFrequencyAggregate {
    fn new(mut document: Vec<isize>, multiplicity: isize) -> Self {
        for value in &mut document {
            *value *= multiplicity;
        }
        Self { frequencies: Some(document), count: multiplicity, }
    }

    fn get_frequencies(&self) -> &Option<Vec<isize>> {
        &self.frequencies
    }

    fn get_count(&self) -> isize {
        self.count.clone()
    }
}

impl IsZero for DocumentFrequencyAggregate {
    fn is_zero(&self) -> bool {
        self.count == 0
    }
}

impl Semigroup for DocumentFrequencyAggregate {
    fn plus_equals(&mut self, other: &Self) {
        match (&mut self.frequencies, &other.frequencies) {
            (Some(lhs), Some(rhs)) => {
                let max_len = max(lhs.len(), rhs.len());
                lhs.resize(max_len, 0);
                for i in 0..rhs.len(){
                    lhs[i] += rhs[i];
                }
            }
            (None, Some(rhs)) => {
                self.frequencies = Some(rhs.clone());
            }
            _ => {}
        }
        self.count += other.count;
    }
}

impl Monoid for DocumentFrequencyAggregate {
    fn zero() -> Self {
        Self { frequencies: None, count: 0, }
    }
}

impl Abelian for DocumentFrequencyAggregate {
    fn negate(&mut self) {

        match &mut self.frequencies {
            Some(m) => {
                for value in m {
                    *value *= -1;
                }
            }
            _ => {}
        };
        self.count *= -1;
    }
}

impl Hash for DenseVector { //workaround thats never actually executed, but necessary to define to call threshold
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i8(1);
    }

    fn hash_slice<H: Hasher>(data: &[Self], state: &mut H)
    where
        Self: Sized
    {
        state.write_i8(1);
    }
}
