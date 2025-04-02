import pdfplumber
import pandas as pd
import sqlite3
import re
from datetime import datetime

def clean_amount(amount_str):
    if pd.isna(amount_str) or amount_str == '':
        return 0.0
    try:
        # Remove currency symbol, commas, and spaces
        amount_str = re.sub(r'[^\d.-]', '', amount_str)
        return abs(float(amount_str))  # Return absolute value
    except:
        return 0.0

def determine_transaction_type(details, amount_str):
    """
    Determine if transaction is Credit (Cr) or Debit (Dr) for DBS Credit Card
    """
    details = str(details).upper()
    amount_str = str(amount_str).upper()

    # Credit card payment indicators - expanded list
    credit_keywords = [
        'PAYMENT', 'REFUND', 'CASHBACK', 'REVERSAL', 'CREDIT',
        'PAYMENT RECEIVED', 'PAYMENT THANK YOU', 'CREDIT CARD PAYMENT',
        'PAYMENT - THANK YOU', 'PAYMENT REVERSAL', 'PAYMENT MADE',
        'CREDIT RECEIVED', 'PAYMENT CREDITED', 'THANKYOU'
    ]

    # Check for explicit credit indicators in details
    if any(keyword in details for keyword in credit_keywords):
        return 'Cr'

    # Check for CR in amount string
    if 'CR' in amount_str:
        return 'Cr'

    # For credit card statements, most transactions are debits (purchases)
    return 'Dr'

def extract_transactions_from_pdf(pdf_path):
    transactions = []
    current_date = None
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            print(f"Processing PDF with {len(pdf.pages)} pages")
            
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
                lines = text.split('\n')
                
                for line in lines:
                    if not line.strip():
                        continue
                        
                    # Updated pattern to better match date and amount
                    date_pattern = r'(\d{2}-\d{2}-\d{4})\s+(.+?)\s+([\d,]+\.\d{2}(?:\s*(?:CR|DR))?)'
                    match = re.search(date_pattern, line)
                    
                    if match:
                        try:
                            date_str, details, amount_str = match.groups()
                            
                            # Convert date
                            date_obj = datetime.strptime(date_str, '%d-%m-%Y')
                            formatted_date = date_obj.strftime('%d-%b-%y')
                            
                            # Clean amount
                            amount = clean_amount(amount_str)
                            
                            # Determine transaction type
                            sign = determine_transaction_type(details, amount_str)
                            
                            transaction = {
                                'Date': formatted_date,
                                'TransactionDetails': details.strip(),
                                'Amount': amount,
                                'BillingAmountSign': sign,
                                '_date_obj': date_obj  # Temporary field for sorting
                            }
                            
                            transactions.append(transaction)
                            print(f"Processed: {formatted_date} | {details.strip()} | {amount} | {sign}")
                            
                        except Exception as e:
                            print(f"Error processing line: {line}")
                            print(f"Error details: {str(e)}")
                            continue
    
    except Exception as e:
        print(f"Error processing PDF: {str(e)}")
        raise

    # Sort transactions by date
    transactions.sort(key=lambda x: x['_date_obj'])
    
    # Add SrNo and remove temporary date object
    final_transactions = []
    for i, trans in enumerate(transactions, 1):
        del trans['_date_obj']
        final_trans = {
            'SrNo': i,
            'Date': trans['Date'],
            'TransactionDetails': trans['TransactionDetails'],
            'Amount': trans['Amount'],
            'BillingAmountSign': trans['BillingAmountSign']
        }
        final_transactions.append(final_trans)
    
    # Print detailed transaction list for verification
    print("\nDetailed Transaction List:")
    print("-" * 80)
    for trans in final_transactions:
        print(f"#{trans['SrNo']:02d} | {trans['Date']} | {trans['TransactionDetails'][:40]:40} | {trans['Amount']:10.2f} | {trans['BillingAmountSign']}")
    print("-" * 80)
    
    return final_transactions

def create_database(transactions, db_path):
    # Create DataFrame
    df = pd.DataFrame(transactions)
    
    # Ensure columns are in correct order
    columns = ['SrNo', 'Date', 'TransactionDetails', 'Amount', 'BillingAmountSign']
    df = df[columns]
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    
    try:
        # Create table with proper schema
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS transactions (
            SrNo INTEGER PRIMARY KEY,
            Date TEXT NOT NULL,
            TransactionDetails TEXT NOT NULL,
            Amount REAL NOT NULL,
            BillingAmountSign TEXT NOT NULL
        )
        '''
        conn.execute(create_table_sql)
        
        # Clear existing data
        conn.execute('DELETE FROM transactions')
        
        # Insert data
        df.to_sql('transactions', conn, if_exists='replace', index=False)
        
        # Verify data
        print("\nVerifying database contents:")
        cursor = conn.cursor()
        
        # Check first few records
        print("\nFirst 5 records:")
        cursor.execute("""
            SELECT SrNo, Date, TransactionDetails, Amount, BillingAmountSign 
            FROM transactions 
            ORDER BY SrNo 
            LIMIT 5
        """)
        for row in cursor.fetchall():
            print(row)
        
        # Check transaction types
        cursor.execute("SELECT BillingAmountSign, COUNT(*) FROM transactions GROUP BY BillingAmountSign")
        print("\nTransaction type summary:")
        for sign, count in cursor.fetchall():
            print(f"{sign}: {count} transactions")
        
        conn.commit()
        
    except Exception as e:
        print(f"Error creating database: {e}")
        raise
    finally:
        conn.close()

def append_new_transactions(pdf_path):
    """Append new transactions from PDF to existing database"""
    # Existing database path
    db_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\DBS_Card_Statementn2.db"
    
    try:
        # Extract transactions from new PDF
        print(f"Processing new PDF: {pdf_path}")
        new_transactions = extract_transactions_from_pdf(pdf_path)
        
        if not new_transactions:
            print("No new transactions found in PDF")
            return
            
        print(f"Found {len(new_transactions)} potential new transactions")
        
        # Connect to existing database
        conn = sqlite3.connect(db_path)
        
        # Get existing transactions
        existing_df = pd.read_sql_query("SELECT * FROM transactions", conn)
        
        # Get the last SrNo
        last_srno = existing_df['SrNo'].max() if not existing_df.empty else 0
        
        # Convert new transactions to DataFrame
        new_df = pd.DataFrame(new_transactions)
        
        # Check for duplicates
        merged_df = pd.concat([existing_df, new_df])
        duplicates = merged_df.duplicated(subset=['Date', 'TransactionDetails', 'Amount'], keep='first')
        unique_new_transactions = merged_df[~duplicates].iloc[len(existing_df):]
        
        if len(unique_new_transactions) == 0:
            print("No new unique transactions to add")
            conn.close()
            return
            
        # Add SrNo to new transactions
        unique_new_transactions['SrNo'] = range(last_srno + 1, last_srno + len(unique_new_transactions) + 1)
        
        # Insert only the new transactions
        unique_new_transactions.to_sql('transactions', conn, if_exists='append', index=False)
        
        print(f"\nSuccessfully added {len(unique_new_transactions)} new transactions")
        print("\nNewly added transactions:")
        print(unique_new_transactions[['Date', 'TransactionDetails', 'Amount', 'BillingAmountSign']].head())
        
        # Display summary statistics
        print("\nDatabase Summary:")
        total_transactions = len(existing_df) + len(unique_new_transactions)
        print(f"Previous transactions: {len(existing_df)}")
        print(f"New transactions added: {len(unique_new_transactions)}")
        print(f"Total transactions: {total_transactions}")
        
        # Show latest transactions
        print("\nLatest 5 transactions in database:")
        latest = pd.read_sql_query("""
            SELECT Date, TransactionDetails, Amount, BillingAmountSign 
            FROM transactions 
            ORDER BY Date DESC 
            LIMIT 5
        """, conn)
        print(latest)
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"Error appending transactions: {e}")
        raise

def verify_database(db_path):
    """Verify database contents and integrity"""
    try:
        conn = sqlite3.connect(db_path)
        
        # Get basic statistics
        cursor = conn.cursor()
        
        # Total transactions
        cursor.execute("SELECT COUNT(*) FROM transactions")
        total = cursor.fetchone()[0]
        
        # Date range
        cursor.execute("SELECT MIN(Date), MAX(Date) FROM transactions")
        date_range = cursor.fetchone()
        
        # Transaction types
        cursor.execute("""
            SELECT BillingAmountSign, COUNT(*), SUM(Amount)
            FROM transactions 
            GROUP BY BillingAmountSign
        """)
        type_summary = cursor.fetchall()
        
        print("\nDatabase Verification Summary:")
        print(f"Total Transactions: {total}")
        print(f"Date Range: {date_range[0]} to {date_range[1]}")
        print("\nTransaction Types:")
        for sign, count, amount in type_summary:
            print(f"{sign}: {count} transactions, Total: ₹{amount:,.2f}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error verifying database: {e}")

def main():
    pdf_path = r'C:\Users\seren\OneDrive\Desktop\PythonTransaction\DBS_Card_Statement.pdf'
    db_path = r'C:\Users\seren\OneDrive\Desktop\PythonTransaction\DBS_Card_Statementn2.db'
    
    print("Extracting transactions from PDF...")
    transactions = extract_transactions_from_pdf(pdf_path)
    
    if not transactions:
        print("No transactions found in the PDF!")
        return
    
    print(f"Found {len(transactions)} transactions")
    print("Creating database...")
    create_database(transactions, db_path)
    
    print(f"Processed {len(transactions)} transactions successfully!")
    
    # Display sample data
    try:
        conn = sqlite3.connect(db_path)
        print("\nFirst 5 transactions:")
        df = pd.read_sql_query("SELECT * FROM transactions ORDER BY TransactionDate LIMIT 5", conn)
        print(df)
        
        print("\nLast 5 transactions:")
        df = pd.read_sql_query("SELECT * FROM transactions ORDER BY TransactionDate DESC LIMIT 5", conn)
        print(df)
        conn.close()
    except Exception as e:
        print(f"Error displaying sample data: {e}")

if __name__ == "__main__":
    # Example usage
    pdf_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\new_dbs_statement.pdf"
    db_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\DBS_Card_Statementn2.db"
    
    # Append new transactions
    append_new_transactions(pdf_path)
    
    # Verify database after update

