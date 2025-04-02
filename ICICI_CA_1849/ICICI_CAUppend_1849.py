import pdfplumber
import pandas as pd
import sqlite3
import re
from datetime import datetime

def clean_amount(amount_str):
    """Clean and convert amount string to float"""
    if pd.isna(amount_str) or amount_str == 'NA':
        return 0.0
    try:
        # Remove all non-digit characters except decimal point
        cleaned = re.sub(r'[^\d.]', '', str(amount_str))
        return float(cleaned) if cleaned else 0.0
    except:
        return 0.0

def extract_transactions_from_pdf(pdf_path):
    """Extract transactions from PDF statement"""
    transactions = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                
                for table in tables:
                    for row in table:
                        if not row or 'SrNo' in str(row[0]):  # Skip header row
                            continue
                        
                        try:
                            # Extract and clean data
                            sr_no = row[0]
                            trans_date = row[3].replace('\n', '')
                            remarks = row[5].replace('\n', ' ').strip()
                            withdrawal = clean_amount(row[6])
                            deposit = clean_amount(row[7])
                            
                            # Skip empty or invalid rows
                            if not trans_date or not (withdrawal or deposit):
                                continue
                            
                            # Determine amount and sign
                            amount = withdrawal if withdrawal > 0 else deposit
                            sign = '-' if withdrawal > 0 else '+'
                            
                            # Standardize date format to DD-Mon-YY
                            try:
                                date_obj = datetime.strptime(trans_date, '%d-%b-%Y')
                                formatted_date = date_obj.strftime('%d-%b-%y')
                            except ValueError:
                                print(f"Invalid date format: {trans_date}")
                                continue
                            
                            transactions.append({
                                'Date': formatted_date,
                                'TransactionDetails': remarks,
                                'Amount': amount,
                                'BillingAmountSign': sign
                            })
                            
                        except Exception as e:
                            print(f"Error processing row: {row}")
                            print(f"Error details: {str(e)}")
                            continue
                            
        return pd.DataFrame(transactions)
        
    except Exception as e:
        print(f"Error processing PDF {pdf_path}: {e}")
        return pd.DataFrame()

def create_database(transactions, db_path):
    # Create DataFrame
    df = pd.DataFrame(transactions)
    
    # Ensure proper data types
    df = df.astype({
        'SrNo': str,
        'TransactionDate': str,
        'TransactionDetails': str,
        'Amount': float,
        'BillingAmountSign': str
    })
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    
    try:
        # Create table manually first
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS transactions (
            SrNo TEXT,
            TransactionDate TEXT,
            TransactionDetails TEXT,
            Amount REAL,
            BillingAmountSign TEXT
        )
        '''
        conn.execute(create_table_sql)
        
        # Clear existing data
        conn.execute('DELETE FROM transactions')
        
        # Insert data
        for _, row in df.iterrows():
            insert_sql = '''
            INSERT INTO transactions (SrNo, TransactionDate, TransactionDetails, Amount, BillingAmountSign)
            VALUES (?, ?, ?, ?, ?)
            '''
            conn.execute(insert_sql, (
                row['SrNo'],
                row['TransactionDate'],
                row['TransactionDetails'],
                row['Amount'],
                row['BillingAmountSign']
            ))
        
        # Create index
        conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON transactions(TransactionDate)')
        
        # Commit changes
        conn.commit()
        
    except Exception as e:
        print(f"Error creating database: {e}")
        raise
    finally:
        conn.close()

def append_new_transactions(pdf_path):
    """Append new transactions from PDF to existing database"""
    # Existing database path
    db_path = r"C:\Users\seren\OneDrive\Desktop\NewfolderOne\ICICI_CA_1849(2023-25).db"
    
    try:
        # Extract transactions from new PDF
        print(f"Processing new PDF: {pdf_path}")
        new_transactions = extract_transactions_from_pdf(pdf_path)
        
        if new_transactions.empty:
            print("No new transactions found in PDF")
            return
            
        print(f"Found {len(new_transactions)} potential new transactions")
        
        # Connect to existing database
        conn = sqlite3.connect(db_path)
        
        # Get existing transactions
        existing_df = pd.read_sql_query("SELECT * FROM transactions", conn)
        
        # Get the last SrNo
        last_srno = existing_df['SrNo'].max() if not existing_df.empty else 0
        
        # Check for duplicates based on Date, Amount, and TransactionDetails
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
        
        # Create or update index on date
        conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON transactions(Date)')
        
        print(f"\nSuccessfully added {len(unique_new_transactions)} new transactions")
        print("\nNewly added transactions:")
        print(unique_new_transactions[['Date', 'TransactionDetails', 'Amount', 'BillingAmountSign']].head())
        
        # Display summary statistics
        print("\nDatabase Summary:")
        print(f"Total transactions: {len(existing_df) + len(unique_new_transactions)}")
        
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
    pdf_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\new_icici_statement.pdf"
    
    # Append new transactions
    append_new_transactions(pdf_path)
    
    # Verify database after update
    db_path = r"C:\Users\seren\OneDrive\Desktop\NewfolderOne\ICICI_CA_1849(2023-25).db"
    verify_database(db_path)

