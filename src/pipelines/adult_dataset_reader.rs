use std::error::Error;
use csv::Reader;
use crate::types::row::Row;
use crate::types::row_value::RowValue;

pub fn read_adult_csv(file_path: &str) -> Result<Vec<Row>, Box<dyn Error>> {
    let mut rdr = Reader::from_path(file_path)?;
    let headers = rdr.headers()?;
    println!("Headers: {:?}", headers);

    let mut rows: Vec<Row> = Vec::new(); // Array to store rows
    for result in rdr.records() {
        let record = result?;
        let row_vals: Vec<RowValue> = record.iter()
            .map(|s| {
                let trimmed = s.trim();
                if let Ok(num) = trimmed.parse::<f64>() {
                    RowValue::Float(num) // Store as Float if parsable
                } else {
                    RowValue::Text(trimmed.to_string()) // Store as Text otherwise
                }
            })
            .collect();
        let row = Row::with_row_values(row_vals);
        rows.push(row);
    }

    Ok(rows)
}