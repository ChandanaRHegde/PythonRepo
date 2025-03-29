import pdfplumber
import pandas as pd
import sqlite3
import re
from datetime import datetime

def clean_amount(amount_str):
    if pd.isna(amount_str) or amount_str == 'NA':
        return 0.0
    try:
        return float(re.sub(r'[^\d.]', '', str(amount_str)))
    except:
        return 0.0

def extract_transactions_from_pdf(pdf_path):
    transactions = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            
            for table in tables:
                for row in table:
                    if not row or 'SrNo' in str(row[0]):
                        continue
                    
                    try:
                        sr_no = row[0]
                        # Handle multi-line date format
                        trans_date = row[3].replace('\n', '')
                        remarks = row[5].replace('\n', ' ')  # Replace newlines with spaces
                        withdrawal = clean_amount(row[6])
                        deposit = clean_amount(row[7])
                        
                        # Determine amount and sign
                        amount = withdrawal if withdrawal > 0 else deposit
                        sign = 'Dr' if withdrawal > 0 else 'Cr'
                        
                        # Clean and format transaction date to DD-Mon-YY
                        try:
                            # First convert to datetime object
                            date_obj = datetime.strptime(trans_date, '%d-%b-%Y')
                            # Then format to DD-Mon-YY
                            trans_date = date_obj.strftime('%d-%b-%y')  # Note: using lowercase 'y' for 2-digit year
                        except Exception as e:
                            print(f"Date conversion error for {trans_date}: {e}")
                            continue
                        
                        # Clean transaction details
                        remarks = re.sub(r'\s+', ' ', str(remarks)).strip()
                        
                        transactions.append({
                            'SrNo': sr_no,
                            'TransactionDate': trans_date,
                            'TransactionDetails': remarks,
                            'Amount': amount,
                            'BillingAmountSign': sign
                        })
                        print(f"Processed transaction: {sr_no} on {trans_date}")
                        
                    except Exception as e:
                        print(f"Error processing row: {row}")
                        print(f"Error details: {str(e)}")
                        continue
    
    return transactions

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

def main():
    pdf_path = r'C:\Users\seren\Downloads\Transactions.pdf'
    db_path = r'C:\Users\seren\Downloads\transactionsN.db'
    
    print("Extracting transactions from PDF...")
    transactions = extract_transactions_from_pdf(pdf_path)
    
    if not transactions:
        print("No transactions found in the PDF!")
        return
    
    print(f"Found {len(transactions)} transactions")
    print("Creating database...")
    create_database(transactions, db_path)
    
    print(f"Processed {len(transactions)} transactions successfully!")
    
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM transactions LIMIT 5", conn)
        print("\nSample data:")
        print(df)
        conn.close()
    except Exception as e:
        print(f"Error displaying sample data: {e}")

if __name__ == "__main__":
    main()


