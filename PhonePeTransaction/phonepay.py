import pdfplumber
import pandas as pd
import sqlite3
import re
from datetime import datetime
import os

def clean_amount(amount_str):
    if pd.isna(amount_str) or amount_str == 'NA':
        return 0.0
    try:
        return float(re.sub(r'[^\d.]', '', str(amount_str)))
    except:
        return 0.0

def extract_transactions_from_pdf(pdf_path):
    transactions = []
    current_transaction = {}
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            print(f"Successfully opened PDF with {len(pdf.pages)} pages")
            
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
                lines = text.split('\n')
                
                for line in lines:
                    line = line.strip()
                    if not line or 'Date Transaction Details Type Amount' in line:
                        continue

                    # Modified date pattern to match format "Feb 16, 2024"
                    date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s+(\d{4})', line)
                    
                    if date_match:
                        # If we have a previous transaction, save it
                        if current_transaction and current_transaction.get('Amount') is not None:
                            transactions.append(current_transaction)
                        
                        # Extract transaction details
                        month = date_match.group(1)
                        day = date_match.group(2)
                        year = date_match.group(3)
                        
                        # Parse the date
                        date_str = f"{month} {day}, {year}"
                        date_obj = datetime.strptime(date_str, '%b %d, %Y')
                        
                        # Determine transaction type and amount
                        trans_type = 'Debit' if 'Debit' in line else 'Credit' if 'Credit' in line else None
                        
                        # Extract amount
                        amount = None
                        amount_match = re.search(r'INR\s*(\d+\.?\d*)', line)
                        if amount_match:
                            amount = clean_amount(amount_match.group(1))
                        
                        # Get description
                        description = line[date_match.end():].strip()
                        if trans_type:
                            description = description.split(trans_type)[0].strip()
                        
                        current_transaction = {
                            'SrNo': str(len(transactions) + 1),
                            'TransactionDate': date_obj.strftime('%d-%b-%y'),
                            'TransactionDetails': description,
                            'Amount': amount,
                            'BillingAmountSign': 'Dr' if trans_type == 'Debit' else 'Cr' if trans_type == 'Credit' else None
                        }
                        
                    # If amount was not on the same line, check for amount in this line
                    elif current_transaction and (current_transaction['Amount'] is None or current_transaction['Amount'] == 0):
                        # Try to find amount at the end of the line
                        amount_match = re.search(r'(\d+\.?\d*)\s*$', line)
                        if amount_match:
                            amount = clean_amount(amount_match.group(1))
                            if amount > 0:  # Only update if we found a valid amount
                                current_transaction['Amount'] = amount
                        
            # Don't forget to add the last transaction
            if current_transaction and current_transaction.get('Amount') is not None:
                transactions.append(current_transaction)
    
    except Exception as e:
        print(f"Error processing PDF: {e}")
        import traceback
        print(f"Full error details:\n{traceback.format_exc()}")
    
    # Print summary of extracted transactions
    print(f"\nTotal transactions found: {len(transactions)}")
    if transactions:
        print("\nSample transactions:")
        for t in transactions[:5]:  # Show first 5 transactions
            print(f"Found transaction: {t['TransactionDate']} - {t['TransactionDetails']} - {t['Amount']} {t['BillingAmountSign']}")
    
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
    pdf_path = r'C:\Users\91861\Downloads/PhonePe_Transaction_Statement 2024-25.pdf'
    db_path = r'C:\Users\91861\Desktop/PhonePe_Transaction_Statement 2024-25.db'
    
    print("Extracting transactions from PDF...")
    print(f"Reading PDF from: {pdf_path}")
    
    # Check if file exists
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at {pdf_path}")
        return
        
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



