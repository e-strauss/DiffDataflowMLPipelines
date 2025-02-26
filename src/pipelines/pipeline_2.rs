use std::collections::HashSet;
use std::process::exit;
use std::time::Instant;
use differential_dataflow::input::InputSession;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::feature_encoders::multi_column_encoder::multi_column_encoder;
use crate::feature_encoders::one_hot_encoder::OneHotEncoder;
use crate::feature_encoders::passthrough::Passthrough;
use crate::pipelines::adult_dataset_reader::read_adult_csv;
use crate::pipelines::pipeline_3_diabetes::print_demo_separator;
use crate::types::row::Row;
use crate::types::row_value::RowValue;

fn difference(a: &[usize], b: &[usize]) -> Vec<usize> {
    let set_b: HashSet<_> = b.iter().collect();
    a.iter().filter(|&&x| !set_b.contains(&x)).copied().collect()
}

pub fn run_pipeline2(dataset: Vec<Row>, r1: i32, r2: i32, size: f32) {
    println!("PIPELINE 2\n");
    //let dataset = read_adult_csv("data/adult_data.csv").unwrap();
    let timer = Instant::now();
    let protected_attributes = [8, 9].to_vec();
    let proxy_attributes = [12].to_vec(); //hardcoded instead of computed
    let split = (dataset.len()as f32*size) as usize;

    let mut excluded_attributes: Vec<usize> = protected_attributes
        .iter()
        .chain(proxy_attributes.iter())
        .copied()
        .collect();
    excluded_attributes.push(14); //exclude target column salary

    let categorical_columns = difference(
        &(&dataset[0]).find_indices(|v| matches!(v, RowValue::Text(_))),
        &excluded_attributes,
    );
    let numerical_columns = difference(
        &(&dataset[0]).find_indices(|v| !matches!(v, RowValue::Text(_))),
        &excluded_attributes,
    );


    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        let probe = worker.dataflow(|scope| {
            let mut input_df = input.to_collection(scope);

            let config: Vec<(usize, Box<dyn ColumnEncoder<_>>)> = categorical_columns.iter()
                .map(|&i| (i, Box::new(OneHotEncoder::new()) as Box<dyn ColumnEncoder<_>>))
                .chain(numerical_columns.iter().map(|&i| (i, Box::new(Passthrough::new()) as Box<dyn ColumnEncoder<_>>)))
                .collect();

            multi_column_encoder(&input_df, config)
                .probe()
        });
        input.advance_to(0);
        for rix in 0..split {
            input.insert((rix, dataset[rix].clone()));
        }
        input.advance_to(1);
        input.flush();
        //println!("\n-- time 0 -> 1 --------------------");
        let timer1 = Instant::now();
        worker.step_while(|| probe.less_than(input.time()));
        println!("\nInit Computation took: {:?}", timer1.elapsed().as_micros());
        println!("Number of Updates: {}", dataset.len() - split);
        let mut time = 2;
        for rix in split..dataset.len() {
            input.insert((rix, dataset[rix].clone()));
            input.advance_to(time);
            input.flush();
            //println!("\n-- time {} -> {} --------------------", time-1, time);
            time += 1;
            let timer_tmp = Instant::now();
            worker.step_while(|| probe.less_than(input.time()));
            //println!("{:?}", timer_tmp.elapsed().as_micros());
        }
        println!("\nComputation took: {:?}", timer1.elapsed().as_micros());
        // input.insert((7,Row::with_values(7, 2.0, "7".to_string())));
    }).expect("Computation terminated abnormally");

    print_demo_separator()
}