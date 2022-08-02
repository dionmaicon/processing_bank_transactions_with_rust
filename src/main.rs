use std::collections::HashMap;
use std::process;
use std::thread;

use processing_bank_transactions_with_rust::Account;
use processing_bank_transactions_with_rust::{ read_csv, summarize_transactions, write_to_stdout };

fn main() {
    env_logger::init();

    let transactions_by_client = match read_csv() {
        Err(err) => {
            println!("Parsing process error: {}", err);
            process::exit(1);
        }
        Ok(values) => values
    };
    
    let list: Vec<_> = transactions_by_client.into_iter().collect();
    

    let chunk_len = (list.len() / 3) as usize + 1;
    let chunks: Vec<HashMap<_, _>> = list.chunks(chunk_len)
        .map(|c| c.iter().cloned().collect())
        .collect();
       
    let mut summarize_threads = Vec::new();

    
    chunks.into_iter().for_each(|set_chunk| {
         let th = thread::spawn( move || {
           return match summarize_transactions(&mut set_chunk.clone()) {
                Ok(values) => values,
                Err(err) => {
                    println!("Summarize process error: {}", err);
                    process::exit(1);
                }
            };
        });
        summarize_threads.push(th);
    });

    let mut accounts = Vec::new();
    summarize_threads.into_iter().for_each(|sub| {
        let result: Vec<Account> = sub.join().expect("Thread error");
        let accs: Vec<Account> = result.to_vec();
        accs.into_iter().for_each(|acc| {
            accounts.push(acc);
        });
    });

    let _ = write_to_stdout(&accounts);
}
