extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Join, Reduce};
use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use timely::dataflow::Scope;

fn main() {
    // demo1 is a first test using an input collection with only 1 numeric column
    demo1();
    println!("--------------------------------------------------------");
    // demo1 is a test using an input collection containing multiples column, but we only transform
    // the one numeric column
    demo2();
}

fn demo1() {
    // Input: Single Value
    timely::execute_from_args(std::env::args(), move |worker| {

        // create an input collection of data.
        let mut input = InputSession::new();

        // define a new computation.
        worker.dataflow(|scope| {

            // create a new collection from our input.
            let input_df = input.to_collection(scope)
                .inspect(|x| println!("{:?}", x));

            // if (m2, m1) and (m1, p), then output (m1, (m2, p))
            let input_df = input_df
                .map(| v | (1, v));

            let meta = standard_scaler_fit(&input_df)
                .inspect(|x| println!("{:?}", x));
            standard_scaler_transform(&input_df, &meta)
                .inspect(|x| println!("{:?}", x));

        });
        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert(person);
        }
        input.flush(); // Flush the input to push changes to the computation

        worker.step(); // Step the computation to process all pending events
        input.advance_to(1);
        for person in 10 .. 20 {
            input.insert(person);
        }
        input.advance_to(2);

    }).expect("Computation terminated abnormally");
}

fn standard_scaler_fit<G: Scope>(
    data: &Collection<G, (usize, isize)>,
) -> Collection<G, (usize, isize)>
where G::Timestamp: Lattice+Ord
{
    data.reduce(|_key, input, output| {
            let mut sum: isize = 0;
            for (k, v) in input {
                sum += *k * v;
            }
            sum = sum / input.len() as isize;
            output.push((sum, 1));
        })
}

fn standard_scaler_transform<G: Scope>(
    data: &Collection<G, (usize, isize)>,
    meta: &Collection<G, (usize, isize)>,
) -> Collection<G, isize>
where G::Timestamp: Lattice+Ord
{
    data.join(meta)
        .map(|(_key, val)| val.0 - val.1)
}


fn demo2() {
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        worker.dataflow(|scope| {
            let manages = input.to_collection(scope);
            manages
                .inspect(|x| println!("{:?}", x))
                .map(| v:(&str,isize,isize)| ((), v.1))
                .reduce(|_key, input, output| {
                    let mut sum = 0;
                    for (k, v) in input {
                        sum += *k*v;
                    }
                    output.push((sum, 2));
                })
                .map(|(_k,v)| v)
                .inspect(|x| println!("{:?}", x));
        });
        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert(("aa", person, 123));
        }
    }).expect("Computation terminated abnormally");
}