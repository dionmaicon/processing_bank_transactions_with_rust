use std::process;

use processing_bank_transactions_with_rust::{ read_csv, summarize_transactions, write_to_stdout };

fn main() {
    env_logger::init();

    let mut transactions_by_client = match read_csv() {
        Err(err) => {
            println!("Parsing process error: {}", err);
            process::exit(1);
        }
        Ok(values) => values
    };
    
    let accounts = match summarize_transactions(&mut transactions_by_client) {
        Ok(values) => values,
        Err(err) => {
            println!("Summarize process error: {}", err);
            process::exit(1);
        }
    };

    let _ = write_to_stdout(&accounts);
}
