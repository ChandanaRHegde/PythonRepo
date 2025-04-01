I need a Python script to convert bank statement Excel files into a structured SQLite database. 
1.The source Excel contains transaction data with the following columns:
SrNo
TransID
ValueDate
TransactionDate
CheqNo
TransactionRemarks
Withdrawal(Dr)
Deposit(Cr)
Balance

2.However, I only need to extract and store the following fields in the final database:
SrNo
TransactionDate
Transaction Details (from TransactionRemarks)
Amount (consolidated from Withdrawal and Deposit columns)
BillingAmountSign (should be 'Dr' for withdrawals or 'Cr' for deposits)

3.The script should:
Remove any irrelevant information
Clean and structure the data
Ensure no transactions are missed
Store the results in a SQLite database
Handle any formatting issues in the source Excel

4.The script uses:
extract data from Excel
Data cleaning functions to handle amounts and dates
SQLite database creation with proper schema
Error handling and validation

5.To use that script, you would just need to:
Install the required libraries
Update the excel_path and db_path 

