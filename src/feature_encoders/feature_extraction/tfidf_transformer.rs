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
    backend : Box<dyn ColumnEncoder<G>>
}

impl<G: Scope> TfidfTransformer<G> {
    pub fn new(backend : Box<dyn ColumnEncoder<G>>) -> TfidfTransformer<G> {
        Self{frequencies:None, backend}
    }
}



impl<G: Scope> ColumnEncoder<G> for TfidfTransformer<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, (usize, RowValue))>) {
        self.backend.fit(data);
        let transformed = self.backend.transform(data)
            .map(|(_column_id, mut vector)| {
                vector.binarize();
                vector
            });
        let frequencies = transformed
            .threshold(|vector, multiplicity| {
                DocumentFrequencyAggregate::new(vector.clone(), *multiplicity)
            })
            .map(|vector| ())
            .count();
        self.frequencies = Some(frequencies);
    }

    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G, (usize, DenseVector)> {
        let frequencies = match &self.frequencies {
            None => panic!("called transform before fit"),
            Some(f) => f
        };
        self.backend.transform(data)
            .map(|x| ((), x))
            .join(&frequencies)
            .map(|(_, ((id, dense), frequencies))| {
                let freq_vector = match frequencies.get_frequencies() {
                    None => panic!("this should not happen in theory (would mean that the aggregate is empty)"),
                    Some(f) => match &f {
                        DenseVector::Scalar(_) => panic!("if this happens, you fcked up badly"),
                        DenseVector::Vector(ref v) => v
                    }
                };
                let doc = match &dense {
                    DenseVector::Scalar(_) => panic!("this should not happen in theory (backend yields scalar then)"),
                    DenseVector::Vector(v) => v
                };
                let tfidf = doc
                    .iter()
                    .zip(freq_vector.iter())
                    .map(|(&doc_count, &freq)| {
                        if doc_count == 0.0 || freq == 0.0 {
                            0.0
                        } else {
                            let tf = doc_count;
                            let idf = (frequencies.count as f64 / freq).ln();
                            tf * idf
                        }
                    })
                    .collect();

                (id, DenseVector::Vector(tfidf))
            })
    }
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct DocumentFrequencyAggregate {
    frequencies: Option<DenseVector>,
    count: isize
}

impl DocumentFrequencyAggregate {
    fn new(mut document: DenseVector, multiplicity: isize) -> Self {
        document.scale(multiplicity as f64);
        Self { frequencies: Some(document), count: multiplicity, }
    }

    fn get_frequencies(&self) -> &Option<DenseVector> {
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
                lhs.add_assign(rhs.clone());
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
        self.frequencies = match &self.frequencies {
            None => None,
            Some(m) => Some(m.clone())
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
