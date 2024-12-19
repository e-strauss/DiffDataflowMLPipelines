mod row;
use row::{Row, RowValue}; // Import the Row struct and RowValue enum

extern crate timely;
extern crate differential_dataflow;

use std::thread;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Join, Reduce, Threshold};
use differential_dataflow::{Collection, Hashable};
use differential_dataflow::lattice::Lattice;
use timely::communication::allocator::Generic;
use timely::dataflow::Scope;
use timely::worker::Worker;

const SLEEPING_DURATION: u64 = 250;

fn main() {
    // demo1 is a first test using an input collection with only 1 numeric column
    demo_standard_scale(true);
    demo_recode(false);
    demo_sum(true);
    // demo2 is a test using an input collection containing multiples columns, but we only transform
    // the one numeric column
    demo2(false);
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

fn standard_scale_fit<G: Scope>(
    data: &Collection<G, (usize, isize)>,
) -> Collection<G, (usize, isize)>
where G::Timestamp: Lattice+Ord
{
    data.reduce(|_key, input, output| {
            let mut sum: isize = 0;
            //println!("{}", input.len());
            for (v, count) in input {
                sum += *v * count;
            }
            sum = sum / input.len() as isize;
            output.push((sum, 1));
        })
}

fn standard_scale_transform<G: Scope>(
    data: &Collection<G, (usize, isize)>,
    meta: &Collection<G, (usize, isize)>,
) -> Collection<G, isize>
where G::Timestamp: Lattice+Ord
{
    data.join(meta)
        .map(|(_key, val)| val.0 - val.1)
}

fn recode_fit<G: Scope, D>(
    data: &Collection<G, (usize, D)>,
) -> Collection<G, (usize, (D, usize))>
where
    G::Timestamp: Lattice+Ord,
    D: differential_dataflow::ExchangeData + std::hash::Hash
{
    data.distinct()
        .reduce(|_key, input, output| {

        println!("{}", input.len());
        for (code, (val, _count)) in input.iter().enumerate(){
            output.push((((*val).clone(), code), 1));
        };
    })
}

fn standard_scale_fit2<G: Scope, K>(
    data: &Collection<G, (usize, K)>,
    col_id: usize,
) -> Collection<G, (usize, K)>
where
    G::Timestamp: Lattice+Ord,
    K: Clone + 'static, K: Hashable, K: differential_dataflow::ExchangeData
{
    println!("{}", col_id);
    data.clone()
    //     .reduce(|_key, input, output| {
    //     let mut sum: isize = 0;
    //     for (vals, count) in input {
    //         let val = (*vals).1;
    //         sum += *val * count;
    //     }
    //     sum = sum / input.len() as isize;
    //     output.push((sum, 1));
    // })
}

fn demo2(quiet: bool) {
    println!("DEMO 2\n");
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        worker.dataflow(|scope| {
            let mut input_df = input.to_collection(scope)
                .map(| v | (1, v));
                //.inspect(|x| println!("START: {:?}", x));
                //.map(| v:(&str,isize,isize)| (1, v.1 ));
            if !quiet {
                input_df = input_df.inspect(|x| println!("START: {:?}", x));
            }
            // let meta = standard_scale_fit2(&input_df, 1)
            //     .inspect(|x| println!("FIT: {:?}", x));
            // input_df
            //     .reduce(|_key, input, output| {
            //         for (v, count) in input {
            //             println!("{:?}", (*v).1)
            //         }
            //         output.push((3, 1));
            //     })
            //     .inspect(|x| println!("FIT: {:?}", x));
            // standard_scale_transform(&input_df, &meta)
            //     .inspect(|x| println!("TRANSFORM: {:?}", x));
        });
        input.advance_to(0);
        for person in 0 .. 10 {

            input.insert(Row::with_values(1, 2.0, person.to_string()));
        }
    }).expect("Computation terminated abnormally");
    println!("--------------------------------------------------------");
}
