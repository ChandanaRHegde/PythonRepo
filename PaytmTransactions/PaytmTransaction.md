I need a Python script to convert bank statement Excel files into a structured SQLite database. 
1.The source Excel contains transaction data with the following columns:
Date,	
Time,	
Transaction Details,	
Your Account,	
Amount,	
UPI Ref No.,	
Order ID,	
Remarks	Tags,	
Comment.


2.However, I only need to extract and store the following fields in the final database:
SrNo,
TransactionDate,
Transaction Details (from TransactionDetails),
Amount (consolidated from Amount column),
BillingAmountSign (should be 'Dr' or 'Cr' based on transaction details and amount ).

3.The script should:
Remove any irrelevant information
Remove unwanted columns
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

