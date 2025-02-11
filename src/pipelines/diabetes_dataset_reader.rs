use std::error::Error;
use csv::Reader;
use crate::types::row::Row;
use crate::types::row_value::RowValue;

// CSV Reader for csv with floats returns Vec<Row>
pub fn read_csv2(file_path: &str) -> Result<Vec<Row>, Box<dyn Error>> {
    let mut rdr = Reader::from_path(file_path)?;
    let headers = rdr.headers()?;
    println!("Headers: {:?}", headers);

    let mut rows: Vec<Row> = Vec::new(); // Array to store rows
    for result in rdr.records() {
        let record = result?;
        let r_vals: Vec<RowValue> = record.iter()
            .map(|s| RowValue::Float(s.trim().parse::<f64>().unwrap_or(-1.0)) ) // Trim spaces & convert to f64
            .collect();
        rows.push(Row::with_row_values(r_vals));
    }
    Ok(rows)
}