import sqlite3
import pandas as pd
import os
from datetime import datetime
import pdfplumber
import re

def standardize_date(date_str):
    """Convert various date formats to a standard format"""
    try:
        if isinstance(date_str, str):
            for fmt in ['%d-%b-%y', '%d-%b-%Y', '%d %b %Y', '%Y-%m-%d', '%d/%m/%Y']:
                try:
                    return datetime.strptime(date_str, fmt).strftime('%d-%b-%y')
                except ValueError:
                    continue
        elif isinstance(date_str, pd.Timestamp):
            return date_str.strftime('%d-%b-%y')
        return date_str
    except Exception as e:
        print(f"Date conversion error for {date_str}: {e}")
        return date_str

def extract_transactions_from_pdf(pdf_path):
    """Extract transaction data from PhonePe statement PDF"""
    transactions = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                lines = text.split('\n')
                
                # Adjust this pattern based on your PhonePe PDF format
                pattern = r'(\d{2}/\d{2}/\d{4})\s+(.*?)\s+([\d,]+\.\d{2})\s+(CR|DR)'
                
                for line in lines:
                    match = re.search(pattern, line)
                    if match:
                        date, details, amount, trans_type = match.groups()
                        # Convert date format from DD/MM/YYYY to DD-MMM-YY
                        date_obj = datetime.strptime(date, '%d/%m/%Y')
                        formatted_date = date_obj.strftime('%d-%b-%y')
                        
                        # Remove commas from amount and convert to float
                        amount = float(amount.replace(',', ''))
                        
                        # Set billing sign based on transaction type
                        billing_sign = '+' if trans_type == 'CR' else '-'
                        
                        transactions.append({
                            'Date': formatted_date,
                            'TransactionDetails': details.strip(),
                            'Amount': amount,
                            'BillingAmountSign': billing_sign
                        })
    
    except Exception as e:
        print(f"Error processing PDF {pdf_path}: {e}")
        raise
    
    return pd.DataFrame(transactions)

def append_new_transactions(pdf_path):
    """Append new transactions from PDF to existing database"""
    # Existing database path
    db_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\PhonePeMerge(2023-25).db"
    
    try:
        # Extract transactions from new PDF
        print(f"Processing new PDF: {pdf_path}")
        new_transactions = extract_transactions_from_pdf(pdf_path)
        
        if new_transactions.empty:
            print("No new transactions found in PDF")
            return
            
        print(f"Found {len(new_transactions)} new transactions")
        
        # Connect to existing database
        conn = sqlite3.connect(db_path)
        
        # Get existing transactions
        existing_df = pd.read_sql_query("SELECT * FROM transactions", conn)
        
        # Get the last SrNo from existing database
        last_srno = existing_df['SrNo'].max() if not existing_df.empty else 0
        
        # Check for duplicates based on Date, Amount, and TransactionDetails
        merged_df = pd.concat([existing_df, new_transactions])
        duplicates = merged_df.duplicated(subset=['Date', 'TransactionDetails', 'Amount'], keep='first')
        unique_transactions = merged_df[~duplicates]
        
        # Sort by date
        unique_transactions['Date'] = pd.to_datetime(unique_transactions['Date'], format='%d-%b-%y')
        unique_transactions = unique_transactions.sort_values('Date')
        unique_transactions['Date'] = unique_transactions['Date'].dt.strftime('%d-%b-%y')
        
        # Reassign SrNo after sorting
        unique_transactions['SrNo'] = range(1, len(unique_transactions) + 1)
        
        # Save back to database
        unique_transactions.to_sql('transactions', conn, if_exists='replace', index=False)
        
        # Create index on date
        conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON transactions(Date)')
        
        conn.commit()
        
        # Print summary
        print(f"\nDatabase updated successfully:")
        print(f"Previous number of transactions: {len(existing_df)}")
        print(f"New transactions added: {len(unique_transactions) - len(existing_df)}")
        print(f"Total transactions after update: {len(unique_transactions)}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error appending transactions: {e}")
        raise

def verify_database(db_path):
    """Verify database contents and integrity"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get basic statistics
        cursor.execute("SELECT COUNT(*) FROM transactions")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(Date), MAX(Date) FROM transactions")
        date_range = cursor.fetchone()
        
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

if __name__ == "__main__":
    # Example usage
    pdf_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\new_phonepe_statement.pdf"
    db_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\PhonePeMerge(2023-25).db"
    
    # Append new transactions
    append_new_transactions(pdf_path)
    
    # Verify database after update

