use differential_dataflow::Collection;
use differential_dataflow::difference::{Abelian, IsZero, Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Threshold,Count};
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::types::dense_vector::DenseVector;
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
    fn fit(&mut self, data: &Collection<G, (usize, (usize, RowValue))>) {
        let mean_var_raw = data
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
                println!("Mean: {:?}, time: {:?}, change: {:?}", record, time, change)
            });
        self.mean = Some(mean_var);
    }

    fn transform(&self, data: &Collection<G, (usize, (usize, RowValue))>) -> Collection<G, (usize, DenseVector)> {
        let mean = match &self.mean {
            None => panic!("called transform before fit"),
            Some(m) => m
        };
        data.join(&mean)
            .map(|(_key, ((ix, val), (mean, var)))| (ix, DenseVector::Scalar((val.get_float() - mean.0)/var.0))  )
    }
}

fn round_to_decimal((m,v): (SafeF64, SafeF64), n1: i32, n2: i32) -> (SafeF64, SafeF64) {
    let factor1 = 10f64.powi(n1);
    let factor2 = 10f64.powi(n2);
    (SafeF64((m.0 / factor1).round() * factor1), SafeF64((v.0 / factor2).round() * factor2))
}