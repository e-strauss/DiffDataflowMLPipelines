extern crate timely;
extern crate differential_dataflow;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::thread;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Join, Reduce, Threshold};
use differential_dataflow::{Collection, Hashable};
use differential_dataflow::lattice::Lattice;
use timely::communication::allocator::Generic;
use timely::dataflow::Scope;
use timely::worker::Worker;

fn main() {
    // demo1 is a first test using an input collection with only 1 numeric column
    demo_standard_scaler();
    println!("--------------------------------------------------------");
    demo_recode();
    println!("--------------------------------------------------------");
    // demo2 is a test using an input collection containing multiples columns, but we only transform
    // the one numeric column
    demo_sum();
    println!("--------------------------------------------------------");
    demo2();
}

fn demo_standard_scaler() {
    // Input: Single Value
    timely::execute_from_args(std::env::args(), move |worker| {

        // create an input collection of data.
        let mut input = InputSession::new();

        // define a new computation.
        let probe = worker.dataflow(|scope| {

            // create a new collection from our input.
            let input_df = input.to_collection(scope)
                .inspect(|x| println!("START: {:?}", x))
                .map(| v | (1, v));

            let meta = standard_scale_fit(&input_df)
                .inspect(|x| println!("FITTING: {:?}", x));

            return standard_scale_transform(&input_df, &meta)
                .inspect(|x| println!("TRANSFORM: {:?}", x))
                .probe();
        });
        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert(person);
        }
        input.advance_to(1);
        input.flush();
        make_steps(worker, 0);
        for person in 10 .. 20 {
            input.insert(person);
        }
        input.advance_to(2);
        input.flush();
        make_steps(worker, 1);

    }).expect("Computation terminated abnormally");
}

fn demo_recode() {
    // Input: Single Value
    timely::execute_from_args(std::env::args(), move |worker| {

        // create an input collection of data.
        let mut input = InputSession::new();

        // define a new computation.
        let probe = worker.dataflow(|scope| {

            // create a new collection from our input.
            let input_df = input.to_collection(scope)
                .inspect(|x| println!("START: {:?}", x))
                .map(| v | (1, v));

            let meta = recode_fit(&input_df)
                .inspect(|x| println!("FITTING: {:?}", x));

            return meta.probe();
        });
        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert(person.to_string() + "Person");
        }
        input.advance_to(1);
        input.flush();
        make_steps(worker, 0);
        for person in 10 .. 20 {
            input.insert(person.to_string() + "Person");
        }
        input.advance_to(2);
        input.flush();
        make_steps(worker, 1);

    }).expect("Computation terminated abnormally");
}

fn demo_sum() {
    // Input: Single Value
    timely::execute_from_args(std::env::args(), move |worker| {

        // create an input collection of data.
        let mut input = InputSession::new();

        // define a new computation.
        let probe = worker.dataflow(|scope| {

            // create a new collection from our input.
            let input_df = input.to_collection(scope)
                .inspect(|x| println!("START: {:?}", x))
                .map(| v | (1, v));

            return input_df
                .reduce(|_key, input, output| {
                    let mut sum = 0;
                    println!("{}", input.len());
                    for (i, c) in input {
                        sum += *i;
                    }
                    output.push((sum, 1));
                })
                .inspect(|x| println!("SUM: {:?}", x))
                .probe();
        });
        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert(person);
        }
        input.advance_to(1);
        input.flush();
        make_steps(worker, 0);
        for person in 10 .. 20 {
            input.insert(person);
        }
        input.advance_to(2);
        input.flush();
        make_steps(worker, 1);

    }).expect("Computation terminated abnormally");
}

fn make_steps(worker: &mut Worker<Generic>, t: isize) {
    println!("step 0, time {}", t);
    worker.step();
    thread::sleep(std::time::Duration::from_millis(500));
    println!("step 1, time {}", t);
    worker.step();
    thread::sleep(std::time::Duration::from_millis(500));
    println!("step 2, time {}", t);
    worker.step();
    thread::sleep(std::time::Duration::from_millis(500));
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

fn recode_fit<G: Scope>(
    data: &Collection<G, (usize, String)>,
) -> Collection<G, (usize, (String, usize))>
where
    G::Timestamp: Lattice+Ord,
    //D: differential_dataflow::ExchangeData + std::hash::Hash
{
    data.distinct()
        .reduce(|_key, input, output| {
        let mut distinct = HashMap::new(); //OrdHashMap::new();

        println!("{}", input.len());
        for (v, _count) in input {
            if !distinct.contains_key(*v) {
                distinct.insert((*v).clone(), distinct.len());
            }
        }
        for pair in distinct{
            output.push((pair, 1));
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

fn demo2() {
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        worker.dataflow(|scope| {
            let input_df = input.to_collection(scope)
                .inspect(|x| println!("START: {:?}", x))
                .map(| v | (1, v));
                //.inspect(|x| println!("START: {:?}", x));
                //.map(| v:(&str,isize,isize)| (1, v.1 ));

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
            input.insert(("aa", person, 123));
        }
    }).expect("Computation terminated abnormally");
}
