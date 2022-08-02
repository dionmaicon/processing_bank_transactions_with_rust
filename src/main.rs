use std::collections::HashMap;
use std::fmt::Debug;
use std::process;
use std::thread;

use processing_bank_transactions_with_rust::{ read_csv, summarize_transactions, write_to_stdout };


fn split<T>(arr: &[T], n: usize ) -> Vec<&[T]> where T: Debug {
    let chunks: Vec<&[T]> = arr.chunks(n).collect();
    chunks
}

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
    

    let chunk_len = (list.len() / 4) as usize + 1;
    let chunks: Vec<HashMap<_, _>> = list.chunks(chunk_len)
        .map(|c| c.iter().cloned().collect())
        .collect();

    let subsets_chunks = split(&chunks, 2);
           
    let mut summarize_threads = Vec::new();
    for set_chunk in subsets_chunks {
        let cloned_sets =  set_chunk.to_vec();
        for set in cloned_sets {
           let th = thread::spawn( move || {
           return match summarize_transactions(&mut set.clone()) {
                Ok(values) => values,
                Err(err) => {
                    println!("Summarize process error: {}", err);
                    process::exit(1);
                }
            };
        });
        summarize_threads.push(th);
        }
    }

    let mut accounts = Vec::new();
    for sub in summarize_threads {
        let result = sub.join().expect("Error");
        let accs = result.to_vec();
        for acc in accs {
            accounts.push(acc);
        }
    }

    let _ = write_to_stdout(&accounts);
}
