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
            for fmt in ['%d-%b-%y', '%d-%b-%Y', '%d %b %Y', '%Y-%m-%d']:
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
    """Extract transaction data from SBI credit card statement PDF"""
    transactions = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                lines = text.split('\n')
                
                pattern = r'(\d{2}-[A-Za-z]{3}-\d{2})\s+(.*?)\s+([-+]?\d+\.?\d*)'
                
                for line in lines:
                    match = re.search(pattern, line)
                    if match:
                        date, details, amount = match.groups()
                        billing_sign = '-' if float(amount) < 0 else '+'
                        amount = abs(float(amount))
                        
                        transactions.append({
                            'Date': date,
                            'TransactionDetails': details.strip(),
                            'Amount': amount,
                            'BillingAmountSign': billing_sign
                        })
    
    except Exception as e:
        print(f"Error processing PDF {pdf_path}: {e}")
    
    return pd.DataFrame(transactions)

def append_new_transactions(pdf_path):
    """Append new transactions from PDF to existing database"""
    db_path = r"C:\Users\seren\OneDrive\Desktop\NewfolderOne\SBI_CCMerge_7670.db"
    
    try:
        # Extract transactions from new PDF
        print(f"Processing PDF: {pdf_path}")
        new_transactions = extract_transactions_from_pdf(pdf_path)
        
        if new_transactions.empty:
            print("No transactions found in PDF")
            return
            
        print(f"Found {len(new_transactions)} potential new transactions")
        
        # Connect to existing database
        conn = sqlite3.connect(db_path)
        
        # Get existing transactions
        existing_df = pd.read_sql_query("SELECT * FROM transactions", conn)
        
        # Get the last SrNo
        last_srno = existing_df['SrNo'].max() if not existing_df.empty else 0
        
        # Check for duplicates
        merged_df = pd.concat([existing_df, new_transactions])
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
        stats = pd.read_sql_query("""
            SELECT 
                COUNT(*) as total_transactions,
                MIN(Date) as earliest_date,
                MAX(Date) as latest_date,
                SUM(CASE WHEN BillingAmountSign = '-' THEN Amount ELSE 0 END) as total_debits,
                SUM(CASE WHEN BillingAmountSign = '+' THEN Amount ELSE 0 END) as total_credits
            FROM transactions
        """, conn)
        
        print("\nDatabase Statistics:")
        print(f"Total Transactions: {stats['total_transactions'][0]}")
        print(f"Date Range: {stats['earliest_date'][0]} to {stats['latest_date'][0]}")
        print(f"Total Debits: ₹{stats['total_debits'][0]:,.2f}")
        print(f"Total Credits: ₹{stats['total_credits'][0]:,.2f}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error verifying database: {e}")

if __name__ == "__main__":
    # Example usage
    pdf_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\new_statement.pdf"
    db_path = r"C:\Users\seren\OneDrive\Desktop\NewfolderOne\SBI_CCMerge_7670.db"
    
    # Append new transactions
    append_new_transactions(pdf_path)
    
    # Verify database after update

