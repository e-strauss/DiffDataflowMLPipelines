use std::collections::BTreeMap;
use differential_dataflow::Collection;
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count, Join, Threshold};
use serde::{Deserialize, Serialize};
use timely::dataflow::{Scope};
use timely::dataflow::operators::Map;
use crate::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::row_value::RowValue::Text;
use crate::feature_encoders::feature_extraction::utils::{default_tokenizer};


pub struct CountVectorizer <G: Scope> {
    corpus: Option<Collection<G, ((), (BTreeMap<String, usize>, usize))>>, //(BTreeMap<Token -> Index, max_index)
    binary: bool
}

impl<G: Scope> CountVectorizer<G> {
    pub fn new(binary : bool) -> Self<>{
        Self{corpus:None, binary}
    }
}

impl<G: Scope> ColumnEncoder<G> for CountVectorizer<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let tokenized = data
            .map(|(_, val)| {
                match val {
                    Text(text) => {default_tokenizer(&text)}
                    _ => !panic!("count vectorizer called on non-text column")
                }
            });
        self.corpus = Some(tokenized
            .threshold(|tokens, multiplicity| {
                CorpusAggregate::new(tokens, *multiplicity)
            }).map(|vector| ()).count().map(|agg| ((), (agg.1.word_to_index, agg.1.next_index))));
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let corpus = match &self.corpus {
            None => panic!("called transform before fit"),
            Some(c) => c
        };
        let binary = self.binary.clone();

        data.map(|(id, val)| {
            let tokens : Vec<String>= match val {
                Text(text) => {default_tokenizer(&text)}
                _ => !panic!("count vectorizer called on non-text column")
            };
            ((), (id, tokens))
        }).join(&corpus).map(move |(_, ((id, tokens), (word_to_index, len)))| {
            let mut vec = vec![0f64; len];
            for token in tokens {
                let i = word_to_index.get(&token);
                if let Some(i) = i {
                    if binary {
                        vec[*i] = 1.0;
                    } else {
                        vec[*i] += 1.0;
                    }
                } else{
                    //token not in corpus
                }
            }
            (id, RowValue::Vec(vec))
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct CorpusAggregate {
    word_to_index: BTreeMap<String, (usize)>,
    word_to_count: BTreeMap<String, (isize)>,

    free_indices: Vec<usize>,
    next_index: usize,
    neg : bool,
    row_count : isize
}


impl CorpusAggregate {
    fn new(tokens : &Vec<String>, mult : isize) -> Self {
        let mut agg = Self::zero();
        agg.row_count = mult;
        for token in tokens {
            agg.plus_equals_word_count(token.to_string(), mult);
        }
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

    fn plus_equals_word_count(&mut self, word: String, count_to_add: isize) {
        match self.word_to_count.get(&word) {
            Some(c) => {
                let count = c.clone();
                self.word_to_count.insert(word.clone(), count + count_to_add);
                if (count > 0 && count + count_to_add <= 0){
                    let index = *self.word_to_index.get_mut(&word).unwrap();
                    //self.word_to_index.remove(word); TODO remove neccessary?
                    self.free_indices.push(index);
                } else if(count <= 0 && count + count_to_add > 0) {
                    let new_index = self.assign_index();
                    self.word_to_index.insert(word.clone(), new_index);
                }
            },
            _ => {
                self.word_to_count.insert(word.clone(), count_to_add);
                if count_to_add > 0 {
                    let new_index = self.assign_index();
                    self.word_to_index.insert(word.clone(), new_index);
                }
            }
        }
    }


}

impl IsZero for CorpusAggregate {
    fn is_zero(&self) -> bool {
        self.row_count == 0
    }
}

impl Semigroup for CorpusAggregate {
    fn plus_equals(&mut self, other: &Self) {
        for (word, _) in other.word_to_index.iter() {
            let mut other_count = *other.word_to_count.get(word).unwrap();
            other_count =  if !(self.neg ^ other.neg) {other_count} else {-other_count};
            self.plus_equals_word_count(word.clone(), other_count)
        }
        self.row_count += other.row_count;
    }
}

impl Monoid for CorpusAggregate {
    fn zero() -> Self {
        Self { word_to_index: BTreeMap::new(), word_to_count: BTreeMap::new(), free_indices: Vec::new(), next_index: 0, neg: false, row_count: 0}
    }
}

impl Abelian for CorpusAggregate {
    fn negate(&mut self) {
        self.neg = !self.neg;
        self.row_count *= -1;
    }
}
