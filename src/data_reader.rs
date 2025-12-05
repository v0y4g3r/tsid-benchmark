use std::collections::HashSet;
use std::fs::File;

use arrow::array::{Array, LargeStringArray, StringArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub fn read_parquet_files(
    path: &str,
    output_csv_path: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Open the parquet file
    let file = File::open(path)?;

    // Create a parquet reader builder
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Get the schema
    let schema = builder.schema().clone();

    // Find indices of columns to exclude
    let exclude_columns: HashSet<String> = ["greptime_value", "greptime_timestamp"]
        .into_iter()
        .map(|s| s.to_owned())
        .collect();
    let mut column_indices_to_keep = Vec::new();
    let mut column_names = Vec::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        if !exclude_columns.contains(field.name().as_str()) {
            column_indices_to_keep.push(idx);
            column_names.push(field.name().clone());
        }
    }

    // Build the reader
    let reader = builder.build()?;

    // Write distinct rows to CSV
    let mut writer = csv::Writer::from_path(output_csv_path)?;
    // Write header
    writer.write_record(&column_names)?;
    for batch_result in reader {
        let batch = batch_result?;

        // Extract only the columns we want to keep
        let columns: Vec<_> = column_indices_to_keep
            .iter()
            .map(|&idx| batch.column(idx).clone())
            .collect();

        // Assert all columns are string arrays
        for (col_idx, column) in columns.iter().enumerate() {
            match column.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    // Valid string type
                }
                _ => {
                    return Err(format!(
                        "Column '{}' is not a string array, found type: {:?}",
                        column_names[col_idx],
                        column.data_type()
                    )
                    .into());
                }
            }
        }

        // Extract rows as vectors of strings
        let num_rows = columns[0].len();
        for row_idx in 0..num_rows {
            let mut row = Vec::new();
            for column in &columns {
                let value = match column.data_type() {
                    DataType::Utf8 => {
                        let string_array = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or("Failed to downcast column to StringArray")?;
                        if string_array.is_null(row_idx) {
                            String::new()
                        } else {
                            string_array.value(row_idx).to_string()
                        }
                    }
                    DataType::LargeUtf8 => {
                        let string_array = column
                            .as_any()
                            .downcast_ref::<LargeStringArray>()
                            .ok_or("Failed to downcast column to LargeStringArray")?;
                        if string_array.is_null(row_idx) {
                            String::new()
                        } else {
                            string_array.value(row_idx).to_string()
                        }
                    }
                    _ => {
                        return Err(format!(
                            "Unexpected data type when extracting string value: {:?}",
                            column.data_type()
                        )
                        .into());
                    }
                };
                row.push(value);
            }
            writer.write_record(&row)?;
        }
    }

    writer.flush()?;
    Ok(())
}
