use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join};
use timely::dataflow::Scope;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::feature_encoders::minmax_scaler::{get_meta};
use crate::types::row_value::RowValue;
use crate::types::safe_f64::SafeF64;

pub struct KBinsDiscretizer<G: Scope> {
    meta: Option<Collection<G, (usize, (SafeF64, SafeF64))>>,
    k: usize,
}

impl<G: Scope> KBinsDiscretizer<G> {
    pub fn new(k: usize) -> Self{
        Self{meta:None, k}
    }
}

impl<G: Scope> ColumnEncoder<G> for KBinsDiscretizer<G>
where G::Timestamp: Lattice+Ord {
    fn fit(&mut self, data: &Collection<G, (usize, RowValue)>) {
        let meta = get_meta(&data.map(|x| (1, x)))
            .inspect(|(record, time, change)| {
                println!("KBins Meta: {:?}, time: {:?}, change: {:?}", record, time, change)
            });
        self.meta = Some(meta);
    }

    fn transform(&self, data: &Collection<G, (usize, RowValue)>) -> Collection<G, (usize, RowValue)> {
        let meta = match &self.meta {
            None => panic!("called transform before fit"),
            Some(m) => m
        };
        let tmp_k = self.k; // make tmp_k outlive self.k -> avoid lifetime error
        data.map(|x| (1, x)).join(meta)
            .map(move |(_key, ((ix, val), (mean, var)))| {
                let scaled = (val.get_float() - mean.0) / var.0;
                let mut bin_id = (scaled * (tmp_k as f64)) as isize;
                // handle edge case when scaled is exactly 1.0
                // avoid branch mis-prediction
                bin_id -= (bin_id - tmp_k as isize + 1)*((bin_id >= tmp_k as isize) as isize);
                return (ix, RowValue::Float(bin_id as f64));
            })
    }
}