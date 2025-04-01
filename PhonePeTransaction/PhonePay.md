I need a Python script to convert bank statement PDF files into a structured SQLite database. 
1.The source pdf contains transaction data with the following columns:
1.1 date
1.2 Transaction details
1.3 Type
1.4 Amount

2.However, I only need to extract and store the following fields in the final database:
2.1 SrNo
2.2 TransactionDate
2.3 Transaction Details (from TransactionRemarks)
2.4 Amount (consolidated from Withdrawal and Deposit columns)
2.5 BillingAmountSign (should be 'Dr' for withdrawals or 'Cr' for deposits)

3.The script should:
3.1 Remove any irrelevant information
3.2 Clean and structure the data
3.3 Ensure no transactions are missed
3.4 Store the results in a SQLite database
3.5 Handle any formatting issues in the source PDF

4.The script uses:
4.1 pdfplumber to extract data from PDF
4.2 Data cleaning functions to handle amounts and dates
4.3 SQLite database creation with proper schema
4.4 Error handling and validation

5.To use that script, you would just need to:
5.1 Install the required libraries
5.2 Update the pdf_path and db_path 

