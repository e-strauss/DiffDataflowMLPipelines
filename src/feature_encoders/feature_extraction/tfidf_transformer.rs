use std::cmp::max;
use differential_dataflow::Collection;
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count, Join, Threshold};
use serde::{Deserialize, Serialize};
use timely::dataflow::{Scope};
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::row_value::RowValue;

pub struct TfidfTransformer<G: Scope> {
    frequencies : Option<Collection<G, ((), DocumentFrequencyAggregate)>>,
    round_to: Option<i32>,
}

impl<G: Scope> TfidfTransformer<G> {
    pub fn new() -> TfidfTransformer<G> {
        Self{frequencies:None, round_to:None}
    }
    pub fn new_with_rounding(n: i32) -> Self{
        Self{frequencies:None, round_to: Some(n)}
    }
}



impl<G: Scope> ColumnEncoder<G> for TfidfTransformer<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let round_to = self.round_to.clone();
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
            .threshold(move |vector, multiplicity| {
                DocumentFrequencyAggregate::new(vector.clone(), *multiplicity, round_to)
            })
            .map(|_vector| ())
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
    count: isize,
    round_to: Option<i32>,
}

impl DocumentFrequencyAggregate {
    fn new(mut document: Vec<isize>, multiplicity: isize, round_to: Option<i32>) -> Self {
        for value in &mut document {
            *value *= multiplicity;
        }
        Self { frequencies: Some(document), count: multiplicity, round_to}
    }

    fn get_frequencies(&self) -> Option<Vec<isize>> {
        match self.frequencies {
            None => None,
            Some(ref v) => match self.round_to {
                Some(n) => Some(round_to_decimal(v, n)),
                None => Some(v.clone())
            }
        }
    }

    fn get_count(&self) -> isize {
        self.count.clone()
    }
}

fn round_to_decimal(vec : &Vec<isize>, n: i32) -> Vec<isize> {
    let factor = 10f64.powi(n);
    vec.iter().map(|&x| ((x as f64 /factor).round() * factor) as isize).collect::<Vec<isize>>()
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
        self.round_to = match self.round_to {
            None => other.round_to,
            Some(_) => self.round_to,
        };
        self.count += other.count;
    }
}

impl Monoid for DocumentFrequencyAggregate {
    fn zero() -> Self {
        Self { frequencies: None, count: 0, round_to: None}
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

