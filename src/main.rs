mod types;
mod feature_encoders;

use std::error::Error;
use types::row::{Row};

extern crate timely;
extern crate differential_dataflow;

use std::thread;
use std::time::Instant;
use csv::Reader;
use rand::Rng;
use differential_dataflow::input::{InputSession};
use serde::Deserialize;
use timely::communication::allocator::Generic;
use timely::dataflow::operators::probe::Handle;
use timely::worker::Worker;
use feature_encoders::column_encoder::{*};
use feature_encoders::one_hot_encoder::OneHotEncoder;
use feature_encoders::multi_column_encoder::multi_column_encoder;
use crate::feature_encoders::feature_extraction::count_vectorizer::CountVectorizer;
use crate::feature_encoders::ordinal_encoder::OrdinalEncoder;
use crate::feature_encoders::standard_scaler::StandardScaler;
use crate::feature_encoders::minmax_scaler::MinMaxScaler;
use crate::feature_encoders::feature_extraction::hash_vectorizer::HashVectorizer;
use crate::feature_encoders::feature_extraction::tfidf_transformer::TfidfTransformer;
use crate::feature_encoders::kbins_discretizer;
use crate::feature_encoders::kbins_discretizer::KBinsDiscretizer;
use crate::feature_encoders::pipeline::Pipeline;
use crate::feature_encoders::polynomial_features_encoder::PolynomialFeaturesEncoder;
use crate::types::row_value::RowValue;

const SLEEPING_DURATION: u64 = 250;

fn main() {
    print_demo_separator();
    // demo_multi_column_encoder(false);
    // demo_multi_column_encoder2(false);
    // demo_multi_column_encoder3(false);
    // text_encoder_demo(false);
    // micro_benchmark_standard_scaler();
    // micro_benchmark1();
    diabetes_pipeline(false);

}

fn print_demo_separator() {
    println!("---------------------------------------------------------------------------");
}

fn make_steps(worker: &mut Worker<Generic>, t: isize, q: bool) {
    if !q {println!("step 0, time {}", t)}
    worker.step();
    thread::sleep(std::time::Duration::from_millis(SLEEPING_DURATION));
    if !q {println!("step 1, time {}", t)}
    worker.step();
    thread::sleep(std::time::Duration::from_millis(SLEEPING_DURATION));
    if !q {println!("step 2, time {}", t)}
    worker.step();
    thread::sleep(std::time::Duration::from_millis(SLEEPING_DURATION));
}

fn demo_multi_column_encoder(quiet: bool) {
    println!("DEMO MULTI COLUMN ENCODER\n");
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        worker.dataflow(|scope| {
            let mut input_df = input.to_collection(scope);
            if !quiet {
                input_df = input_df.inspect(|x| println!("IN: {:?}", x));
            }

            let mut config: Vec<(usize, Box<dyn ColumnEncoder< _>>)>  = Vec::new();
            //config.push((0, Box::new(StandardScaler::new_with_rounding(-1, 0))));
            config.push((0, Box::new(PolynomialFeaturesEncoder::new(1,3))));

            multi_column_encoder(&input_df, config)
                .inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        input.advance_to(0);
        for person in 0 .. 100 {
            input.insert((person,Row::with_values((person % 10) as i64, 2.0, person.to_string())));
        }
        input.advance_to(1);
        input.flush();
        make_steps(worker, 0, quiet);


        input.insert((7,Row::with_values(7, 2.0, "7".to_string())));


    }).expect("Computation terminated abnormally");
    print_demo_separator()
}

fn demo_multi_column_encoder2(quiet: bool) {
    println!("DEMO MULTI COLUMN ENCODER2\n");
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        worker.dataflow(|scope| {
            let mut input_df = input.to_collection(scope);
            if !quiet {
                input_df = input_df.inspect(|x| println!("IN: {:?}", x));
            }
            //let config  = vec![(0, OrdinalEncoder::new()), (1, OrdinalEncoder::new())];
            let config: Vec<(usize, Box<dyn ColumnEncoder< _>>)> = vec![
                (0, Box::new(StandardScaler::new())),
                (1, Box::new(OneHotEncoder::new())),
            ];

            multi_column_encoder(&input_df, config)
                .inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        input.advance_to(0);
        for person in 0 .. 10 {
            let person_int = person as i64;
            input.insert((person,Row::with_integer_vec(vec![person_int, person_int%2])));
        }

    }).expect("Computation terminated abnormally");
    print_demo_separator()
}

fn demo_multi_column_encoder3(quiet: bool) {
    println!("DEMO MULTI COLUMN ENCODER2\n");
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        worker.dataflow(|scope| {
            let mut input_df = input.to_collection(scope);
            if !quiet {
                input_df = input_df.inspect(|x| println!("IN: {:?}", x));
            }
            //let config  = vec![(0, OrdinalEncoder::new()), (1, OrdinalEncoder::new())];


            let config: Vec<(usize, Box<dyn ColumnEncoder< _>>)> = vec![
                (0, Box::new(Pipeline::new(vec!
                [
                    Box::new(KBinsDiscretizer::new(5)),
                    Box::new(OrdinalEncoder::new())
                ]
                ))),
                (1, Box::new(Pipeline::new(vec!
                [
                    Box::new(MinMaxScaler::new()),
                    Box::new(KBinsDiscretizer::new(3)),
                    Box::new(OneHotEncoder::new())
                ]
                )))
            ];

            multi_column_encoder(&input_df, config)
                .inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        input.advance_to(0);
        for person in 0 .. 20 {
            let person_int = person as i64;
            input.insert((person,Row::with_integer_vec(
                vec![person_int, (person_int + 1),  person_int * 2 + 5,  person_int + 3])));
        }

    }).expect("Computation terminated abnormally");
    print_demo_separator()
}

fn generate_random_string(tokens : Vec<&str>) -> String {

    // Create a random number generator
    let mut rng = rand::rng();

    // Randomly choose a length between 5 and 10
    let length = rng.random_range(5..=10);

    // Generate a vector of randomly selected tokens
    let random_tokens: Vec<&str> = (0..length)
        .map(|_| tokens[rng.random_range(0..tokens.len())])
        .collect();

    // Join the tokens into a single string with a space separator
    random_tokens.join(" ")
}

fn text_encoder_demo(quiet: bool) {
    println!("DEMO TEXT ENCODER\n");
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        worker.dataflow(|scope| {
            let mut input_df = input.to_collection(scope);
            if !quiet {
                input_df = input_df.inspect(|x| println!("IN: {:?}", x));
            }

            let config: Vec<(usize, Box<dyn ColumnEncoder< _>>)> = vec![
                (0, Box::new(Pipeline::new(vec!
                [
                    Box::new(CountVectorizer::new(false)),
                    Box::new(TfidfTransformer::new_with_rounding(-2))
                ]
                )))
            ];
            multi_column_encoder(&input_df, config)
                .inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        input.advance_to(0);
        for person in 0 .. 10 {
            let tokens = vec!["EDML", "Benni", "Elias", "Berlin", "Bratwurst"];
            input.insert((person,Row::with_row_value(RowValue::Text(generate_random_string(tokens)))));
        }
        input.advance_to(1);
        input.flush();
        make_steps(worker, 0, quiet);
        for person in 11 .. 20 {
            let tokens = vec!["EDML1", "Benni1", "Elias1", "Berlin", "Bratwurst"];
            input.insert((person,Row::with_row_value(RowValue::Text(generate_random_string(tokens)))));
        }
        input.advance_to(2);
        input.flush();
        make_steps(worker, 1, quiet);


    }).expect("Computation terminated abnormally");
    print_demo_separator()
}

fn micro_benchmark_standard_scaler() {
    println!("DEMO MULTI COLUMN ENCODER\n");
    let size = std::env::args().nth(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(1000000);
    let size2 = std::env::args().nth(2).and_then(|s| s.parse::<usize>().ok()).unwrap_or(1);

    //let appends = std::env::args().nth(2).and_then(|s| s.parse::<usize>().ok()).unwrap_or(10);
    let appends = 1;
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        let probe = worker.dataflow(|scope| {
            let input_df = input.to_collection(scope);
            let mut config: Vec<(usize, Box<dyn ColumnEncoder< _>>)>  = Vec::new();
            config.push((0, Box::new(StandardScaler::new_with_rounding(-2,0))));

            multi_column_encoder(&input_df, config)
                //.inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        input.advance_to(0);
        let mut person = worker.index();
        while person < size {
            input.insert((person,Row::with_row_value(RowValue::Integer((person % 10) as i64))));
            person += worker.peers();
        }
        input.advance_to(1);
        input.flush();
        let timer = Instant::now();
        println!("\n-- time 0 -> 1 --------------------");
        worker.step_while(|| probe.less_than(input.time()));
        println!("\nInit Computation took: {:?}", timer.elapsed());
        let mut id = size+10;
        for i in 1 .. (1+ appends){
            for j in 0..size2{
                input.insert((id,Row::with_row_value(RowValue::Integer(1000000 as i64))));

                id += 1;
            }
            input.advance_to(1 + i);
            input.flush();
            let timer = Instant::now();
            println!("\n-- time {} -> {} --------------------", i, i + 1);
            worker.step_while(|| probe.less_than(input.time()));
            println!("\nUpdate Computation took: {:?}", timer.elapsed());
        }


        // input.insert((7,Row::with_values(7, 2.0, "7".to_string())));
    }).expect("Computation terminated abnormally");
    //println!("\nComputation took: {:?}", timer.elapsed());
    print_demo_separator()
}

fn micro_benchmark1() {
    println!("DEMO MULTI COLUMN ENCODER\n");
    let size = std::env::args().nth(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(10);
    let appends = std::env::args().nth(2).and_then(|s| s.parse::<usize>().ok()).unwrap_or(5);
    let timer = Instant::now();
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        let probe = worker.dataflow(|scope| {
            let input_df = input.to_collection(scope)
                .inspect(|x| println!("IN: {:?}", x));

            let config: Vec<(usize, Box<dyn ColumnEncoder< _>>)> = vec![
                (0, Box::new(StandardScaler::new())),
                (1, Box::new(MinMaxScaler::new())),
                (2, Box::new(KBinsDiscretizer::new(4))),
            ];

            multi_column_encoder(&input_df, config)
                .inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        init_collection(size, timer, worker, &mut input, &probe, 3);
        append_tuples(size, appends, worker, input, probe, 3);

        // input.insert((7,Row::with_values(7, 2.0, "7".to_string())));
    }).expect("Computation terminated abnormally");
    println!("\nComputation took: {:?}", timer.elapsed());
    print_demo_separator()
}

// CSV Reader for csv with floats returns Vec<Row>
fn read_csv2(file_path: &str) -> Result<Vec<Row>, Box<dyn Error>> {
    let mut rdr = Reader::from_path(file_path)?;
    let headers = rdr.headers()?;
    println!("Headers: {:?}", headers);

    let mut rows: Vec<Row> = Vec::new(); // Array to store rows
    for result in rdr.records() {
        let record = result?;
        let parsed_values: Vec<f64> = record.iter()
            .map(|s| s.trim().parse::<f64>().unwrap_or(-1.0)) // Trim spaces & convert to f64
            .collect();
        let row_vals: Vec<RowValue> = parsed_values.iter().map(|v| RowValue::Float(*v)).collect();
        let row = Row::with_row_values(row_vals);
        rows.push(row);
    }

    Ok(rows)
}

fn diabetes_pipeline(quiet: bool) {
    if let Err(err) = read_csv2("data/5050_split.csv") {
        eprintln!("Error reading CSV: {}", err);
    }
}

fn init_collection(size: usize, timer: Instant, worker: &mut Worker<Generic>, input: &mut InputSession<usize, (usize, Row), isize>, probe: &Handle<usize>, cols: usize) {
    input.advance_to(0);
    let mut person = worker.index();
    while person < size {
        let rv = RowValue::Integer((person % 10) as i64);
        input.insert((person, Row::with_row_values(vec![rv; cols])));
        person += worker.peers();
    }
    input.advance_to(1);
    input.flush();
    println!("\n-- time 0 -> 1 --------------------");
    worker.step_while(|| probe.less_than(input.time()));
    println!("\nInit Computation took: {:?}", timer.elapsed());
}

fn append_tuples(size: usize, appends: usize, worker: &mut Worker<Generic>, mut input: InputSession<usize, (usize, Row), isize>, probe: Handle<usize>, cols: usize) {
    for i in 0..appends {
        let rv = RowValue::Integer((i % 10) as i64);
        input.insert((size + i, Row::with_row_values(vec![rv; cols])));
        input.advance_to(1 + i);
        input.flush();
        println!("\n-- time {} -> {} --------------------", i, i + 1);
        worker.step_while(|| probe.less_than(input.time()));
    }
}