import sqlite3
import pandas as pd
import os
from datetime import datetime

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

def extract_transactions_from_excel(excel_path):
    """Extract transaction data from Excel sheet"""
    try:
        # Read Excel file
        df = pd.read_excel(excel_path)
        
        # Rename columns if needed
        column_mapping = {
            'Value Date': 'Date',
            'Description': 'TransactionDetails',
            'Debit': 'Debit',
            'Credit': 'Credit'
        }
        df = df.rename(columns=column_mapping)
        
        # Convert date format
        df['Date'] = df['Date'].apply(standardize_date)
        
        # Handle Amount and BillingAmountSign
        df['Amount'] = df.apply(
            lambda row: row['Debit'] if pd.notnull(row['Debit']) else row['Credit'],
            axis=1
        )
        
        df['BillingAmountSign'] = df.apply(
            lambda row: '-' if pd.notnull(row['Debit']) else '+',
            axis=1
        )
        
        return df[['Date', 'TransactionDetails', 'Amount', 'BillingAmountSign']]
        
    except Exception as e:
        print(f"Error processing Excel {excel_path}: {e}")
        return pd.DataFrame()

def append_new_transactions(excel_path):
    """Append new transactions from Excel to existing database"""
    # Existing database path
    db_path = r"C:\Users\seren\OneDrive\Desktop\NewfolderOne\ICICI_SA_0090(2023-25).db"
    
    try:
        # Extract transactions from Excel
        print(f"Processing Excel file: {excel_path}")
        new_transactions = extract_transactions_from_excel(excel_path)
        
        if new_transactions.empty:
            print("No new transactions found in Excel")
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
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"Error appending transactions: {e}")

if __name__ == "__main__":
    # Example usage
    excel_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\new_icici_statement.xlsx"

