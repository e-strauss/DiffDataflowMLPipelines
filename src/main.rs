extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Reduce};

fn main() {
    demo1();
    println!("--------------------------------------------------------");
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
            let manages = input.to_collection(scope);

            // if (m2, m1) and (m1, p), then output (m1, (m2, p))
            manages
                .map(| v | ((), v))
                .inspect(|x| println!("{:?}", x))
                .reduce(|_key, input, output| {
                    let mut sum = 0;

                    // Each element of input is a `(&Value, Count)`
                    for (k, v) in input {
                        sum += *k * v;
                    }

                    // Must produce outputs as `(Value, Count)`.
                    output.push((sum, 2));
                })
                .map(|(_k,v)| v)
                .inspect(|x| println!("{:?}", x));
        });
        input.advance_to(0);
        for person in 0 .. 10 {
            input.insert(person);
        }

    }).expect("Computation terminated abnormally");
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
