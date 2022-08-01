
# Processing Bank Transactions Batch with RUST

You should be able to run this engine like
```
cargo run -- samples/transactions.csv > samples/test.output.csv 
```
The input file is the only argument. Output is std out.

To enable logger use

```
RUST_LOG=<logger-option> cargo run -- samples/transactions.csv > samples/test.output.csv
```
<<option>logger-option>: error, info and debug

To run tests use

```
RUST_LOG=debug cargo test -- --nocapture
```
Operations:

 - **deposit** - regular deposit transaction
 - **withdraw** - regular withdraw transaction
 - **dispute** - user open dispute
 - **resolve** - resolve dispute releasing the founds helded and increasing available balance
 - **chargeback** - resolve dispute reversing the transaction (withdraw or deposit) and block user to check fraud


## Approach 

Separate transactions by accounts, to avoid unnecessary costs in search operations. We have 3 different operations that do searches: **dispute**, **solve** and **reverse**.

## Next steps?

### To finish this challenge implementation

Spawn threads to handle the summary function quickly (use subsets of accounts)
Try different approach to read CSV's 
Library Documentation
Integration tests

### Scalability
In case you receive many requests to this program, you must use an SQS or similar to handle the process in the background, for greater efficiency you could use a file provider to save ".csv" files (S3) and use a Worker/Consumer to read from the SQS and get these files from their location.

### Issues

**chargebacks** In my opinion chargebacks shouldn't be processed in batch files, to avoid fraud maybe it should use a warning in the logger and include the chargeback transactions in another file to be processed after careful verification.

