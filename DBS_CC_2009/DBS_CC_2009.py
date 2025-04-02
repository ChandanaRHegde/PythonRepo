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
    main()


