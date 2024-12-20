mod row;
mod column_encoder;

use row::{Row}; // Import the Row struct and RowValue enum
use column_encoder::static_encoder;

extern crate timely;
extern crate differential_dataflow;

use std::thread;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Reduce};
use timely::communication::allocator::Generic;
use timely::worker::Worker;
use crate::column_encoder::{recode_fit, standard_scale_fit, standard_scale_transform};

const SLEEPING_DURATION: u64 = 250;

fn main() {
    demo_standard_scale(true);
    demo_recode(false);
    demo_sum(true);
    // a test using an input collection containing multiples columns using the new row struct
    demo_row_struct(false);
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
                input_df = input_df.inspect(|x| println!("START: {:?}", x));
            }
            let input_df = input_df.map(|v| (1, v));

            let mut meta = standard_scale_fit(&input_df);
            if !quiet {
                meta = meta.inspect(|x| println!("FITTING: {:?}", x));
            }

            let mut transformed = standard_scale_transform(&input_df, &meta);
            if !quiet {
                transformed = transformed.inspect(|x| println!("TRANSFORM: {:?}", x));
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

        for person in 10 .. 20 {
            input.insert(person);
        }
        input.advance_to(2);
        input.flush();
        make_steps(worker, 1, quiet);

    }).expect("Computation terminated abnormally");
    println!("--------------------------------------------------------");
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
                input_df = input_df.inspect(|x| println!("START: {:?}", x))
            }

            let mut meta = recode_fit(&input_df);
            if !quiet {
                meta = meta.inspect(|x| println!("FITTING: {:?}", x));
            }
            // TODO use join for transform

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
        input.advance_to(2);
        input.flush();
        make_steps(worker, 1, quiet);

    }).expect("Computation terminated abnormally");
    println!("--------------------------------------------------------");
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
                input_df = input_df.inspect(|x| println!("START: {:?}", x));
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
    println!("--------------------------------------------------------");
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
    println!("DEMO 2\n");
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
    println!("--------------------------------------------------------");
}
