import sqlite3
import pandas as pd
import os
from datetime import datetime
import pdfplumber
import re

def extract_transactions_from_pdf(pdf_path):
    """Extract transaction data from Paytm UPI statement PDF"""
    transactions = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            print(f"Processing PDF with {len(pdf.pages)} pages")
            
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
                lines = text.split('\n')
                
                for line in lines:
                    if not line.strip():
                        continue
                    
                    # Pattern for Paytm UPI transactions
                    # Format typically includes date, transaction details, and amount
                    pattern = r'(\d{2}(?:-|/)\w{3}(?:-|/)\d{2,4})\s+(.+?)\s+((?:CR|DR)\s*[\d,]+\.?\d*)'
                    match = re.search(pattern, line)
                    
                    if match:
                        try:
                            date_str, details, amount_str = match.groups()
                            
                            # Clean amount string and determine transaction type
                            amount_str = re.sub(r'[^\d.]', '', amount_str)
                            amount = float(amount_str)
                            
                            # Determine if it's credit (CR) or debit (DR)
                            billing_sign = 'CR' if 'CR' in line.upper() else 'DR'
                            
                            transaction = {
                                'Date': date_str,
                                'TransactionDetails': details.strip(),
                                'Amount': amount,
                                'BillingAmountSign': billing_sign
                            }
                            
                            transactions.append(transaction)
                            print(f"Processed: {date_str} | {details.strip()} | {amount} | {billing_sign}")
                            
                        except Exception as e:
                            print(f"Error processing line: {line}")
                            print(f"Error details: {str(e)}")
                            continue
    
    except Exception as e:
        print(f"Error processing PDF {pdf_path}: {e}")
        return pd.DataFrame()
    
    if not transactions:
        print("No transactions found in PDF")
        return pd.DataFrame()
        
    df = pd.DataFrame(transactions)
    print(f"\nExtracted {len(df)} transactions from PDF")
    return df

def standardize_date(date_str):
    """Convert various date formats to a standard format"""
    try:
        if isinstance(date_str, str):
            for fmt in ['%d-%b-%y', '%d-%b-%Y', '%d %b %Y', '%Y-%m-%d']:
                try:
                    return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                except ValueError:
                    continue
        elif isinstance(date_str, pd.Timestamp):
            return date_str.strftime('%Y-%m-%d')
        return date_str
    except Exception as e:
        print(f"Date conversion error for {date_str}: {e}")
        return date_str

def append_new_transactions(pdf_path):
    """Append new transactions from PDF to existing database"""
    db_path = r"C:\Users\seren\OneDrive\Desktop\NewfolderOne\PaytmUPIMerge(2023-25)11.db"
    
    try:
        # Extract transactions from new PDF
        print(f"Processing new PDF: {pdf_path}")
        new_transactions = extract_transactions_from_pdf(pdf_path)
        
        if new_transactions.empty:
            print("No new transactions found in PDF")
            return
            
        # Standardize dates in new transactions
        new_transactions['Date'] = new_transactions['Date'].apply(standardize_date)
        
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
                SUM(CASE WHEN BillingAmountSign = 'DR' THEN Amount ELSE 0 END) as total_debits,
                SUM(CASE WHEN BillingAmountSign = 'CR' THEN Amount ELSE 0 END) as total_credits
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
    pdf_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\new_phonepe_statement.pdf"
    db_path = r"C:\Users\seren\OneDrive\Desktop\NewfolderOne\PaytmUPIMerge(2023-25)11.db"
    
    # Append new transactions
    append_new_transactions(pdf_path)
    
    # Verify database after update
    verify_database(db_path)

