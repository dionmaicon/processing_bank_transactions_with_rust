extern crate csv;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use std::error::Error;
use std::ffi::OsString;
use std::env;
use std::fs::File;
use std::collections::HashMap;
use std::io::stdout;

use log::{debug, error, info, warn};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
#[non_exhaustive]
pub enum AccountError {
    #[error("Doesn't have enough funds to process the transaction")]
    BalanceIsLow,
    #[error("Account is locked, can't withdraw or deposit founds")]
    AccountIsLocked,
    #[error("The operation didn't find referenced transaction ID")]
    ReferenceNotFound,
    #[error("The amount is invalid")]
    InvalidAmount,
    #[error("The held amount has already been released")]
    InvalidHeld,
    #[error("Possible fraud, manual check is necessary")]
    InvalidAvailableAmount,
}

#[derive(Debug, Deserialize, Clone )]
pub struct Transaction {
    #[serde(rename = "type")]
    operation: String,
    client: u16,
    tax: u32,
    amount: Option<f64>,
}

#[derive(Debug, Default, Serialize, Clone )]
pub struct Account {
    client: u16,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

trait Operation {
    fn deposit(&mut self, transaction: &Transaction) -> Result<String,  AccountError>;
    fn withdraw(&mut self, transaction: &Transaction) -> Result<String, AccountError>;
    fn dispute(&mut self, transaction: &Transaction, transactions: &[Transaction]) -> Result<String, AccountError>; 
    fn resolve(&mut self, transaction: &Transaction, transactions: &[Transaction]) -> Result<String, AccountError>; 
    fn chargeback(&mut self, transaction: &Transaction, transactions: &[Transaction]) -> Result<String, AccountError>;
}

impl Operation for Account {
    fn deposit(&mut self, transaction: &Transaction ) -> Result<String, AccountError > {
        let value = parse_f64(transaction.amount.unwrap());

        if self.locked {
            return Err(AccountError::AccountIsLocked);
        }

        if !(value > 0.0) {
            return Err(AccountError::InvalidAmount);    
        }
        
        self.total = parse_f64(self.total) + value;
        self.available = parse_f64(self.available) + value;
            
        Ok(format!("Deposit sent, value: {}", value))
    }

    fn withdraw(&mut self, transaction: &Transaction ) -> Result<String, AccountError > {
        if  self.locked {
            return Err(AccountError::AccountIsLocked);
        }

        let value = parse_f64(transaction.amount.unwrap());
        

        if !(value > 0.0 && self.available >= value) {
            return Err(AccountError::BalanceIsLow)
        } 
        
        self.total = parse_f64(self.total) - value;
        self.available = parse_f64(self.available) - value;

        Ok(format!("Withdraw received, value: {}", value))
    }

    fn dispute(&mut self, transaction: &Transaction, transactions: &[Transaction]) -> Result<String, AccountError > {
        if  self.locked {
            return Err(AccountError::AccountIsLocked);
        }

        let result = transactions.iter().find( |&tr | 
            tr.tax == transaction.tax && tr.amount != None);

        let transaction_found = match result {
            None => return Err(AccountError::ReferenceNotFound),
            Some(value) => value
        };

        let value = parse_f64(transaction_found.amount.unwrap());
        self.held = parse_f64(self.held) + value;
        self.available = parse_f64(self.available) - value;
        
        Ok(format!("Open dispute for operation: {}", transaction_found.operation))
    }

    fn resolve(&mut self, transaction: &Transaction, transactions: &[Transaction]) -> Result<String, AccountError > {
        if  self.locked {
            return Err(AccountError::AccountIsLocked);
        }

        let result = transactions.iter().find( |&tr | 
            tr.tax == transaction.tax && tr.amount != None);
        
        let dispute = transactions.iter().find( |&tr | 
            tr.tax == transaction.tax && tr.operation == "dispute");
        
        match dispute {
            None => return Err(AccountError::ReferenceNotFound),
            Some(value) => value
        };

        let transaction_found = match result {
            None => return Err(AccountError::ReferenceNotFound),
            Some(value) => value
        };

        let value = parse_f64(transaction_found.amount.unwrap());

        if self.held - value < 0.0  {
            return Err(AccountError::InvalidHeld);
        }

        self.held = parse_f64(self.held) - value;
        self.available = parse_f64(self.available) + value;
        
        Ok(format!("Resolve dispute for operation: {}", transaction_found.operation))
    }

    fn chargeback(&mut self, transaction: &Transaction, transactions: &[Transaction]) -> Result<String, AccountError > {
        
        if  self.locked {
            return Err(AccountError::AccountIsLocked);
        }

        let result = transactions.iter().find( |&tr | 
            tr.tax == transaction.tax && tr.amount != None);
        
        let dispute = transactions.iter().find( |&tr | 
            tr.tax == transaction.tax && tr.operation == "dispute");
        
        match dispute {
            None => return Err(AccountError::ReferenceNotFound),
            Some(value) => value
        };

        let transaction_found = match result {
            None => return Err(AccountError::ReferenceNotFound),
            Some(value) => value
        };

        let value = parse_f64(transaction_found.amount.unwrap());

        if self.held - value < 0.0 {
            return Err(AccountError::InvalidHeld);
        }

        self.held = parse_f64(self.held) - value;
        self.locked = true;

        if transaction_found.operation == "withdraw" {
            self.available = parse_f64(self.total) + value;
        } else {
            self.available = parse_f64(self.total) - value;
        }

        self.total = parse_f64(self.held) + parse_f64(self.available);
        
        Ok(format!("Chargeback dispute for operation: {}", transaction_found.operation))
    }
}

pub fn summarize_transactions( transactions_by_client: &mut HashMap<u16, Vec<Transaction>>) -> Result<Vec<Account>, Box<dyn Error> > {
    let mut summarized = Vec::new();
    for (key, transactions ) in transactions_by_client {
        
        let mut account: Account = Account{ client: *key, ..Default::default()};
        
        let trs = transactions.clone();
        for transaction in  transactions {
            let operation = transaction.operation.as_str();
            
            match operation {
                "deposit" => {
                    match account.deposit(transaction) {
                        Ok( response ) => info!("{} - Client: {}", response, account.client),
                        Err(error) => warn!("{} - Client: {}", error.to_string(), account.client),
                    };
                },
                "withdraw" => {
                    match account.withdraw(transaction) {
                        Ok( response ) => info!("{} - Client: {}", response, account.client),
                        Err(error) => warn!("{} - Client: {}", error.to_string(), account.client),
                    };
                },
                "dispute" => {
                    match account.dispute(transaction, &trs) {
                        Ok( response ) => info!("{} - Client: {}", response, account.client),
                        Err(error) => warn!("{} - Client: {}", error.to_string(), account.client),
                    };
                },
                "resolve" => {
                    match account.resolve(transaction, &trs) {
                        Ok( response ) => info!("{} - Client: {}", response, account.client),
                        Err(error) => warn!("{} - Client: {}", error.to_string(), account.client),
                    };
                },
                "chargeback" => {
                    match account.chargeback(transaction, &trs) {
                        Ok( response ) => info!("{} - Client: {}", response, account.client),
                        Err(error) => warn!("{} - Client: {}", error.to_string(), account.client),
                    };
                },
                _ => ()
            }
            
        }
        summarized.push(account);
    }
    
    debug!("Summarized Accounts: \n {:#?}", summarized);
    Ok(summarized)
}


fn parse_f64( x: f64) -> f64 {
    let value = format!("{:.4}", x );
    value.parse::<f64>().unwrap()
}


fn get_first_arg() -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(1) {
        None => Err(From::from("expected 1 argument, but got none")),
        Some(file_path) => Ok(file_path),
    }
}

pub fn read_csv() -> Result<HashMap<u16, Vec<Transaction>>, Box<dyn Error>> {
    let file_path = get_first_arg()?;
    let file = File::open(file_path)?;
    
    let mut rdr = csv::Reader::from_reader(file);
    
    let mut transactions_by_client: HashMap<u16, Vec<Transaction>> = HashMap::new();

    for result in rdr.deserialize() {
        
        let entry: Transaction  = result?;

        if !transactions_by_client.contains_key(&entry.client) {
            transactions_by_client.insert(entry.client, Vec::new());
        }
        
        let transactions = transactions_by_client.get_mut( &entry.client).unwrap();
        
        transactions.push(entry);
    }
    
    debug!("Total of Clients: {}", transactions_by_client.len());

    Ok(transactions_by_client)
}

pub fn write_to_stdout(accounts: &[Account]) -> Result<(), Box<dyn Error>> {
    let mut wtr = csv::Writer::from_writer(stdout());

    wtr.write_record(&["client", "available", "held", "total", "locked"])?;

    for account in accounts {
        wtr.serialize((
            account.client, 
            parse_f64(account.available),
            parse_f64(account.held),
            parse_f64(account.total),
            account.locked,
        ))?;    
    }
    
    wtr.flush()?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    // 1. Deposit
    #[test]
    fn deposit() {
        let mut account: Account = Account{ client: 1, ..Default::default() };
        let mut transactions: Vec<Transaction> = Vec::new();
        
        transactions.push(Transaction { operation: "deposit".to_string(), client: 1, tax: 1, amount: Some(2.1) });
        transactions.push(Transaction { operation: "deposit".to_string(), client: 1, tax: 2, amount: Some(1.3) });

        let _ = Operation::deposit(&mut account, &transactions[0]);
        let _ = Operation::deposit(&mut account, &transactions[1]);

        assert_eq!(parse_f64(account.total), parse_f64(3.4));
    }

    // 2. Withdraw
    #[test]
    fn withdraw() {
        let mut account: Account = Account{ client: 2, ..Default::default() };
        let mut transactions: Vec<Transaction> = Vec::new();
        
        transactions.push(Transaction { operation: "deposit".to_string(), client: 2, tax: 1, amount: Some(2.1) });
        transactions.push(Transaction { operation: "withdraw".to_string(), client: 2, tax: 2, amount: Some(3.1) });
        transactions.push(Transaction { operation: "withdraw".to_string(), client: 2, tax: 3, amount: Some(2.0) });
        

        let _ = Operation::deposit(&mut account, &transactions[0]);
        let _ = Operation::withdraw(&mut account, &transactions[2]);
        
        assert_eq!(account.withdraw(&transactions[1]), Err(AccountError::BalanceIsLow));
        assert_eq!(parse_f64(account.total), parse_f64(0.1));
    }
    #[test]
    fn dispute_and_resolve_withdraw() {
        let mut account: Account = Account{ client: 2, ..Default::default() };
        let mut transactions: Vec<Transaction> = Vec::new();
        
        transactions.push(Transaction { operation: "deposit".to_string(), client: 2, tax: 1, amount: Some(2.1) });
        transactions.push(Transaction { operation: "withdraw".to_string(), client: 2, tax: 3, amount: Some(2.0) });
        transactions.push(Transaction { operation: "dispute".to_string(), client: 2, tax: 3, amount: None });
        transactions.push(Transaction { operation: "resolve".to_string(), client: 2, tax: 3, amount: None });


        let _ = Operation::deposit(&mut account, &transactions[0]);
        let _ = Operation::withdraw(&mut account, &transactions[1]);
        let _ = Operation::dispute(&mut account, &transactions[2], &transactions);
        
        assert_eq!(parse_f64(account.held), parse_f64(2.0));
        assert_eq!(parse_f64(account.available), parse_f64(-1.9));
        assert_eq!(parse_f64(account.total), parse_f64(0.1));

        
        let _ = Operation::resolve(&mut account, &transactions[3], &transactions);

        assert_eq!(parse_f64(account.held), parse_f64(0.0));
        assert_eq!(parse_f64(account.available), parse_f64(0.1));
        
    }

    #[test]
    fn dispute_and_resolve_deposit() {
        let mut account: Account = Account{ client: 2, ..Default::default() };
        let mut transactions: Vec<Transaction> = Vec::new();
        
        transactions.push(Transaction { operation: "deposit".to_string(), client: 2, tax: 1, amount: Some(2.1) });
        transactions.push(Transaction { operation: "withdraw".to_string(), client: 2, tax: 3, amount: Some(2.0) });
        transactions.push(Transaction { operation: "dispute".to_string(), client: 2, tax: 1, amount: None });
        transactions.push(Transaction { operation: "resolve".to_string(), client: 2, tax: 1, amount: None });


        let _ = Operation::deposit(&mut account, &transactions[0]);
        let _ = Operation::withdraw(&mut account, &transactions[1]);
        let _ = Operation::dispute(&mut account, &transactions[2], &transactions);
        
        assert_eq!(parse_f64(account.held), parse_f64(2.1));
        assert_eq!(parse_f64(account.available), parse_f64(-2.0));
        assert_eq!(parse_f64(account.total), parse_f64(0.1));

        let _ = Operation::resolve(&mut account, &transactions[3], &transactions);

        assert_eq!(parse_f64(account.held), parse_f64(0.0));
        assert_eq!(parse_f64(account.available), parse_f64(0.1));
        
    }

    #[test]
    fn dispute_and_chargeback_withdraw() {
        let mut account: Account = Account{ client: 2, ..Default::default() };
        let mut transactions: Vec<Transaction> = Vec::new();
        
        transactions.push(Transaction { operation: "deposit".to_string(), client: 2, tax: 1, amount: Some(2.1) });
        transactions.push(Transaction { operation: "withdraw".to_string(), client: 2, tax: 3, amount: Some(2.0) });
        transactions.push(Transaction { operation: "dispute".to_string(), client: 2, tax: 3, amount: None });
        transactions.push(Transaction { operation: "chargeback".to_string(), client: 2, tax: 3, amount: None });


        // Open dispute for withdraw
        let _ = Operation::deposit(&mut account, &transactions[0]);
        let _ = Operation::withdraw(&mut account, &transactions[1]);
        let _ = Operation::dispute(&mut account, &transactions[2], &transactions);
        
        assert_eq!(parse_f64(account.held), parse_f64(2.0));
        assert_eq!(parse_f64(account.total), parse_f64(0.1));
        assert_eq!(parse_f64(account.available), parse_f64(-1.9));
        
        let _ = Operation::chargeback(&mut account, &transactions[3], &transactions);

        assert_eq!(parse_f64(account.held), parse_f64(0.0));
        assert_eq!(parse_f64(account.available), parse_f64(2.1));
        assert_eq!(account.locked, true);
        
    }
     
    #[test]
    fn dispute_and_chargeback_deposit() {
        let mut account: Account = Account{ client: 2, ..Default::default() };
        let mut transactions: Vec<Transaction> = Vec::new();
        
        transactions.push(Transaction { operation: "deposit".to_string(), client: 2, tax: 1, amount: Some(2.1) });
        transactions.push(Transaction { operation: "withdraw".to_string(), client: 2, tax: 2, amount: Some(2.0) });
        transactions.push(Transaction { operation: "dispute".to_string(), client: 2, tax: 1, amount: None });
        transactions.push(Transaction { operation: "chargeback".to_string(), client: 2, tax: 1, amount: None });


        // Open dispute for deposit
        let _ = Operation::deposit(&mut account, &transactions[0]);
        let _ = Operation::withdraw(&mut account, &transactions[1]);
        let _ = Operation::dispute(&mut account, &transactions[2], &transactions);

        assert_eq!(parse_f64(account.held), parse_f64(2.1));
        assert_eq!(parse_f64(account.total), parse_f64(0.1));
        assert_eq!(parse_f64(account.available), parse_f64(-2.0));
        
        let _ = Operation::chargeback(&mut account, &transactions[3], &transactions);

        assert_eq!(parse_f64(account.held), parse_f64(0.0));
        assert_eq!(parse_f64(account.available), parse_f64(-2.0));
        assert_eq!(account.locked, true);
        
    }
}