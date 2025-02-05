mod types;
mod feature_encoders;


use types::row::{Row};

extern crate timely;
extern crate differential_dataflow;

use std::thread;
use std::time::Instant;
use rand::Rng;
use differential_dataflow::input::{InputSession};
use differential_dataflow::operators::{Reduce};
use timely::communication::allocator::Generic;
use timely::dataflow::operators::probe::Handle;
use timely::worker::Worker;
use feature_encoders::column_encoder::{*};
use feature_encoders::one_hot_encoder::OneHotEncoder;
use feature_encoders::multi_column_encoder::multi_column_encoder;
use crate::feature_encoders::ordinal_encoder::OrdinalEncoder;
use crate::feature_encoders::standard_scaler::StandardScaler;
use crate::feature_encoders::minmax_scaler::MinMaxScaler;
use crate::feature_encoders::feature_extraction::hash_vectorizer::HashVectorizer;
use crate::feature_encoders::feature_extraction::tfidf_transformer::TfidfTransformer;
use crate::types::row_value::RowValue;

const SLEEPING_DURATION: u64 = 250;

fn main() {
    print_demo_separator();
    demo_standard_scale(false);
    demo_recode(false);
    demo_sum(false);
    demo_row_struct(false);
    demo_multi_column_encoder(false);
    demo_multi_column_encoder2(false);
    demo_multi_column_encoder3(false);
    text_encoder_demo(false);
    micro_benchmark_standard_scaler();
    micro_benchmark_minmax_scaler();
}

fn print_demo_separator() {
    println!("---------------------------------------------------------------------------");
}

fn demo_standard_scale(quiet: bool) {
    println!("DEMO STANDARD SCALE\n");
    // Input: Single Value
    timely::execute_from_args(std::env::args(), move |worker| {
        // create an input collection of data.
        let mut input = InputSession::new();
        // define a new computation.
        let _probe = worker.dataflow(|scope| {
            // create a new collection from our input.
            let mut input_df = input.to_collection(scope);
            if !quiet {
                input_df = input_df.inspect(|x| println!("IN: {:?}", x));
            }
            let input_df = input_df.map(|v| (1, v));

            let mut meta = standard_scale_fit(&input_df);
            if !quiet {
                meta = meta.inspect(|x| println!("FITTING META STATE: {:?}", x));
            }

            let mut transformed = standard_scale_transform(&input_df, &meta);
            if !quiet {
                transformed = transformed.inspect(|x| println!("OUT: {:?}", x));
            }

            return transformed.probe();

        });

        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert(person);
        }
        input.advance_to(1);
        input.flush();
        make_steps(worker, 0, quiet);

        for person in 0 .. 10 {
            input.insert(person);
        }
        input.advance_to(2);
        input.flush();
        make_steps(worker, 1, quiet);

    }).expect("Computation terminated abnormally");
    print_demo_separator()
}

fn demo_recode(quiet: bool) {
    println!("DEMO RECODE\n");
    // Input: Single Value
    timely::execute_from_args(std::env::args(), move |worker| {
        // create an input collection of data.
        let mut input = InputSession::new();
        // define a new computation.
        let _probe = worker.dataflow(|scope| {
            // create a new collection from our input.
            let mut input_df = input.to_collection(scope).map(|v| (1, v));
            if !quiet {
                input_df = input_df.inspect(|x| println!("IN: {:?}", x))
            }

            let mut meta = recode_fit(&input_df);
            if !quiet {
                meta = meta.inspect(|x| println!("FITTING META STATE: {:?}", x));
            }
            // TODO use join for OUT

            return meta.probe();
        });

        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert(person.to_string() + "Person");
        }
        input.advance_to(1);
        input.flush();
        make_steps(worker, 0, quiet);

        for person in 10 .. 20 {
            input.insert(person.to_string() + "Person");
        }
        input.advance_to(2);
        input.flush();
        make_steps(worker, 1, quiet);

        for person in 0 .. 5 {
            input.insert(person.to_string() + "Person");
        }
        input.advance_to(3);
        input.flush();
        make_steps(worker, 2, quiet);

    }).expect("Computation terminated abnormally");
    print_demo_separator()
}

fn demo_sum(quiet: bool) {
    println!("SUM DEMO\n");
    // Input: Single Value
    timely::execute_from_args(std::env::args(), move |worker| {
        // create an input collection of data.
        let mut input = InputSession::new();
        // define a new computation.
        let _probe = worker.dataflow(|scope| {
            // create a new collection from our input.
            let mut input_df = input.to_collection(scope).map(| v | (1, v));
            if !quiet {
                input_df = input_df.inspect(|x| println!("IN: {:?}", x));
            }

            input_df = input_df
                .reduce(|_key, input, output| {
                    let mut sum = 0;
                    println!("{}", input.len());
                    for (i, _c) in input {
                        sum += *i;
                    }
                    output.push((sum, 1));
                });
            if !quiet {
                input_df = input_df
                    .inspect(|x| println!("SUM: {:?}", x))
            }
            return input_df.probe();
        });

        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert(person);
        }
        input.advance_to(1);
        input.flush();
        make_steps(worker, 0, quiet);

        for person in 10 .. 20 {
            input.insert(person);
        }
        input.advance_to(2);
        input.flush();
        make_steps(worker, 1, quiet);

    }).expect("Computation terminated abnormally");
    print_demo_separator()
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

fn demo_row_struct(quiet: bool) {
    println!("DEMO STATIC ENCODER WITH ROW STRUCT\n");
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        worker.dataflow(|scope| {
            let mut input_df = input.to_collection(scope);
            if !quiet {
                input_df = input_df.inspect(|x| println!("IN: {:?}", x));
            }
            static_encoder(&input_df)
                .inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert((person,Row::with_values(person as i64, 2.0, person.to_string())));
        }
    }).expect("Computation terminated abnormally");
    print_demo_separator()
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
            config.push((0, Box::new(StandardScaler::new_with_rounding(-1, 0))));

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
                (0, Box::new(StandardScaler::new())),
                (1, Box::new(OneHotEncoder::new())),
                (2, Box::new(OrdinalEncoder::new())),
                (3, Box::new(OrdinalEncoder::new())),
            ];

            multi_column_encoder(&input_df, config)
                .inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        input.advance_to(0);
        for person in 0 .. 10 {
            let person_int = person as i64;
            input.insert((person,Row::with_integer_vec(
                vec![person_int, (person_int + 1) % 3,  person_int%2 + 2,  person_int%3 + 3])));
        }

    }).expect("Computation terminated abnormally");
    print_demo_separator()
}

fn generate_random_string() -> String {
    let tokens = ["EDML", "Benni", "Elias", "Berlin", "Bratwurst"];

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
    let n_features = 16;
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
                (0, Box::new(TfidfTransformer::new(Box::new(HashVectorizer::new(n_features, false)))))
            ];

            multi_column_encoder(&input_df, config)
                .inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert((person,Row::with_row_value(RowValue::Text(generate_random_string()))));
        }

    }).expect("Computation terminated abnormally");
    print_demo_separator()
}

fn micro_benchmark_standard_scaler() {
    println!("DEMO MULTI COLUMN ENCODER\n");
    // Set a size for our organization from the input.
    let size = std::env::args().nth(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(10);
    let appends = std::env::args().nth(2).and_then(|s| s.parse::<usize>().ok()).unwrap_or(10);
    let timer = Instant::now();
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

        init_collection(size, timer, worker, &mut input, &probe);
        append_tuples(appends, worker, input, probe);

        // input.insert((7,Row::with_values(7, 2.0, "7".to_string())));
    }).expect("Computation terminated abnormally");
    println!("\nComputation took: {:?}", timer.elapsed());
    print_demo_separator()
}

fn micro_benchmark_minmax_scaler() {
    println!("DEMO MULTI COLUMN ENCODER\n");
    // Set a size for our organization from the input.
    let size = std::env::args().nth(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(10);
    let appends = std::env::args().nth(2).and_then(|s| s.parse::<usize>().ok()).unwrap_or(5);
    let timer = Instant::now();
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        let probe = worker.dataflow(|scope| {
            let input_df = input.to_collection(scope)
                .inspect(|x| println!("IN: {:?}", x));
            let mut config: Vec<(usize, Box<dyn ColumnEncoder< _>>)>  = Vec::new();
            config.push((0, Box::new(MinMaxScaler::new())));

            multi_column_encoder(&input_df, config)
                .inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        init_collection(size, timer, worker, &mut input, &probe);
        append_tuples(appends, worker, input, probe);

        // input.insert((7,Row::with_values(7, 2.0, "7".to_string())));
    }).expect("Computation terminated abnormally");
    println!("\nComputation took: {:?}", timer.elapsed());
    print_demo_separator()
}

fn init_collection(size: usize, timer: Instant, worker: &mut Worker<Generic>, input: &mut InputSession<usize, (usize, Row), isize>, probe: &Handle<usize>) {
    input.advance_to(0);
    let mut person = worker.index();
    while person < size {
        input.insert((person, Row::with_row_value(RowValue::Integer((person % 10) as i64))));
        person += worker.peers();
    }
    input.advance_to(1);
    input.flush();
    println!("\n-- time 0 -> 1 --------------------");
    worker.step_while(|| probe.less_than(input.time()));
    println!("\nInit Computation took: {:?}", timer.elapsed());
}

fn append_tuples(appends: usize, worker: &mut Worker<Generic>, mut input: InputSession<usize, (usize, Row), isize>, probe: Handle<usize>) {
    for i in 1..(1 + appends) {
        input.insert((7, Row::with_row_value(RowValue::Integer((i % 10) as i64))));
        input.advance_to(1 + i);
        input.flush();
        println!("\n-- time {} -> {} --------------------", i, i + 1);
        worker.step_while(|| probe.less_than(input.time()));
    }
}