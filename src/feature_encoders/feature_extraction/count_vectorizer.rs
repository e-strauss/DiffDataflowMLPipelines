use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Count, Join, Threshold};
use timely::dataflow::{Scope};
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::row_value::RowValue::Text;
use crate::feature_encoders::feature_extraction::utils::{default_tokenizer};
use crate::types::safe_hash_map::SafeHashMap;
use crate::types::integer_assignment_aggregate::PositionAssignmentAggregate;

pub struct CountVectorizer <G: Scope> {
    corpus: Option<Collection<G, ((), (SafeHashMap<String, usize>, usize))>>, //(HashMap<Token -> Index, max_index)
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
                    Text(text) => {default_tokenizer(&text)},
                    _ => !panic!("count vectorizer called on non-text column")
                }
            });
        self.corpus = Some(tokenized
            .threshold(|tokens, multiplicity| {
                PositionAssignmentAggregate::new_with_vec(tokens, *multiplicity)
            }).map(|_vector| ()).count().map(|agg| ((), (agg.1.val_to_index, agg.1.len))));
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
