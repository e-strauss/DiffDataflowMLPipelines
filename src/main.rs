mod types;
mod feature_encoders;

use types::row::{Row};
// use feature_encoders::column_encoder::static_encoder;

extern crate timely;
extern crate differential_dataflow;

use std::thread;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Reduce};
use timely::communication::allocator::Generic;
use timely::worker::Worker;
use feature_encoders::column_encoder::{*};
use feature_encoders::one_hot_encoder::OneHotEncoder;
use feature_encoders::multi_column_encoder::multi_column_encoder;
use crate::feature_encoders::ordinal_encoder::OrdinalEncoder;
use crate::feature_encoders::standard_scaler::StandardScaler;

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
            config.push((0, Box::new(StandardScaler::new())));

            multi_column_encoder(&input_df, config)
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