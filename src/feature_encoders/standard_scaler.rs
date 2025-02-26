use differential_dataflow::Collection;
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Threshold,Count};
use serde::{Deserialize, Serialize};
use timely::dataflow::{Scope};
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::row_value::RowValue;
use crate::types::safe_f64::SafeF64;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct VarianceAggregate {
    mean: SafeF64,
    m2: SafeF64,
    count: isize
}

impl VarianceAggregate {
    fn new(value: f64, multiplicity: isize) -> Self {
        Self { mean: SafeF64(value), m2: SafeF64(0.0), count: multiplicity, }
    }

    fn get(&self) -> (SafeF64, SafeF64) {
        // variance = M2 / count
        (SafeF64(self.mean.0),SafeF64( self.m2.0 / (self.count as f64)))
    }
}

impl IsZero for VarianceAggregate {
    fn is_zero(&self) -> bool {
        self.count == 0
    }
}

impl Semigroup for VarianceAggregate {
    fn plus_equals(&mut self, other: &Self) {
        let c1 = self.count as f64;
        let c2 = other.count as f64;
        self.count += other.count;
        if self.count <= 0 {
            println!("Negative variance count: {}", self.count);
        }
        let c_new = self.count as f64;
        let delta = self.mean.0 - other.mean.0;
        self.mean = SafeF64((self.mean.0*c1 + other.mean.0*c2) / c_new);
        self.m2 = SafeF64(self.m2.0 + other.m2.0 + (delta*delta)*c1*c2/c_new);
    }
}

impl Monoid for VarianceAggregate {
    fn zero() -> Self {
        Self { mean: SafeF64(0.0), m2: SafeF64(0.0), count: 0, }
    }
}

impl Abelian for VarianceAggregate {
    fn negate(&mut self) {
        self.m2 = SafeF64(self.m2.0 * -1.0);
        self.count *= -1;
    }
}

pub struct StandardScaler < G: Scope> {
    mean: Option<Collection<G, (usize, (SafeF64, SafeF64))>>,
    round_to: Option<(i32, i32)>,
}

impl<G: Scope> StandardScaler<G> {
    pub fn new() -> Self{
        Self{mean:None, round_to:None}
    }

    pub fn new_with_rounding(n_mean: i32, n_var: i32) -> Self{
        Self{mean:None, round_to: Some((n_mean,n_var))}
    }
}

impl<G: Scope> ColumnEncoder<G> for StandardScaler<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let mean_var_raw = data.map(|x| (1, x))
            .threshold(|(_k, (_ix, value)), c| {
                VarianceAggregate::new((*value).get_float(), *c)
            })
            .map(|(column_id, _value)| column_id)
            .count();
        let mean_var = match self.round_to {
            None => mean_var_raw
                .map(|(column, mean_aggregate)| (column, mean_aggregate.get())),
            Some((n1, n2)) => mean_var_raw.
                map(move |(column, mean_aggregate)| (column, round_to_decimal(mean_aggregate.get(), n1, n2)))
        };
        let mean_var = mean_var
            .inspect(|(record, time, change)| {
                println!("StandardScaler Meta: {:?}, time: {:?}, change: {:?}", record, time, change)
            });
        self.mean = Some(mean_var);
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let mean = match &self.mean {
            None => panic!("called transform before fit"),
            Some(m) => m
        };
        apply_scaling(&data.map(|x| (1, x)), &mean)
    }
}

pub(crate) fn apply_scaling<G: Scope>(data: &Collection<G, (usize, (usize, RowValue))>, mean: &Collection<G, (usize, (SafeF64, SafeF64))>) -> Collection<G, (usize, RowValue)>
where G::Timestamp: Lattice+Ord {
    data.join(&mean)
        .map(|(_key, ((ix, val), (mean, var)))| (ix, RowValue::Float((val.get_float() - mean.0) / var.0)))
}


fn round_to_decimal((m,v): (SafeF64, SafeF64), n1: i32, n2: i32) -> (SafeF64, SafeF64) {
    let factor1 = 10f64.powi(n1);
    let factor2 = 10f64.powi(n2);
    (SafeF64((m.0 / factor1).round() * factor1), SafeF64((v.0 / factor2).round() * factor2))
}


#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use differential_dataflow::input::InputSession;
    use super::*;
    #[test]
    fn standard_scaler_works() {
        let result = timely::execute(timely::Config::process(1), move |worker| {
            let mut input = InputSession::new();
            let output = Arc::new(Mutex::new(Vec::new()));
            let probe = worker.dataflow(|scope| {
                let input_df = input.to_collection(scope);
                let mut enc = StandardScaler::new();
                enc.fit(&input_df);
                let output_clone = Arc::clone(&output); // Clone Arc for use inside closure

                enc.transform(&input_df)
                    .inspect(move |((_, x),_,_)| {
                        let mut out = output_clone.lock().unwrap();
                        out.push(x.get_float());
                    })
                    .probe()
            });

            input.advance_to(0);
            for person in 0 .. 10 {
                let person_int = person as i64;
                input.insert((person,RowValue::Integer(person_int)));
            }

            input.advance_to(1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));

            // lock the Mutex to access data
            let output = output.lock().unwrap();

            // Check the output
            assert!(!output.is_empty(), "No output was generated");
            let expected_values: Vec<f64> = (0..10).map(|i| (i as f64 - 4.5)/8.25).collect();
            assert_eq!(&*output, &expected_values, "Transformed output is incorrect");
        });
        assert!(result.is_ok(), "Timely execution failed");
    }
}