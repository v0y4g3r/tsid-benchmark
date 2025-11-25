use tsid_bench::data_reader;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: {} <input_parquet_path> <output_csv_path>", args[0]);
        eprintln!("Example: {} data.parquet output.csv", args[0]);
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    match data_reader::read_parquet_files(input_path, output_path) {
        Ok(()) => {
            println!(
                "Successfully processed parquet file and wrote distinct rows to {}",
                output_path
            );
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
