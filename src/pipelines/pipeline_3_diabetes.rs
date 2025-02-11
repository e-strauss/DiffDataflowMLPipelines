use std::time::Instant;
use differential_dataflow::input::InputSession;
use crate::feature_encoders::column_encoder::ColumnEncoder;
use crate::feature_encoders::multi_column_encoder::multi_column_encoder;
use crate::feature_encoders::standard_scaler::StandardScaler;
use crate::{print_demo_separator, Row};

pub fn diabetes(rows: Vec<Row>, r1: i32, r2: i32, size: f32) {
    let cols = rows[0].size;
    let split = (rows.len()as f32*size) as usize;
    // Input: Tuple
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();
        let probe = worker.dataflow(|scope| {
            let input_df = input.to_collection(scope);
            //.inspect(|x| println!("IN: {:?}", x));

            let mut config: Vec<(usize, Box<dyn ColumnEncoder< _>>)> = Vec::with_capacity(cols);
            for col in 0..cols {
                //config.push((col, Box::new(StandardScaler::new_with_rounding(-2,0))));
                config.push((col, Box::new(StandardScaler::new_with_rounding(r1, r2))));
            }

            multi_column_encoder(&input_df, config)
                //.inspect(|x| println!("OUT: {:?}", x))
                .probe()
        });

        input.advance_to(0);
        for rix in 0..split {
            input.insert((rix, rows[rix].clone()));
        }
        input.advance_to(1);
        input.flush();
        //println!("\n-- time 0 -> 1 --------------------");
        let timer1 = Instant::now();
        worker.step_while(|| probe.less_than(input.time()));
        println!("\nInit Computation took: {:?}", timer1.elapsed().as_micros());
        println!("Number of Updates: {}", rows.len() - split);
        let mut time = 2;
        for rix in split..rows.len() {
            input.insert((rix, rows[rix].clone()));
            input.advance_to(time);
            input.flush();
            //println!("\n-- time {} -> {} --------------------", time-1, time);
            time += 1;
            let timer_tmp = Instant::now();
            worker.step_while(|| probe.less_than(input.time()));
            println!("{:?}", timer_tmp.elapsed().as_micros());
        }
        println!("\nComputation took: {:?}", timer1.elapsed().as_micros());
        // input.insert((7,Row::with_values(7, 2.0, "7".to_string())));
    }).expect("Computation terminated abnormally");

    print_demo_separator()
}