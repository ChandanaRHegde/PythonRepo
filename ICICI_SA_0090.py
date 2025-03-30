
import pandas as pd
import sqlite3
import re
import os
from datetime import datetime

def clean_amount(amount_str):
    if pd.isna(amount_str) or amount_str == 'NA' or amount_str == '-':
        return 0.0
    try:
        if isinstance(amount_str, (int, float)):
            return float(amount_str)
        # Remove any currency symbols, commas and other non-numeric characters except decimal point
        cleaned = re.sub(r'[^\d.-]', '', str(amount_str))
        return float(cleaned) if cleaned else 0.0
    except:
        return 0.0

def extract_transactions_from_excel(excel_path):
    try:
        print(f"Reading Excel file: {excel_path}")
        
        # Read all sheets from Excel file
        all_sheets = pd.read_excel(excel_path, sheet_name=None, header=None)
        
        # Try each sheet until we find one with the required data
        df = None
        for sheet_name, sheet_df in all_sheets.items():
            print(f"\nChecking sheet: {sheet_name}")
            
            # Read the entire sheet without headers first
            full_df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)
            
            # Look for the actual transaction data header
            # Usually it contains words like "Sr No", "Date", "Particulars", etc.
            for idx in range(len(full_df)):
                row = full_df.iloc[idx].astype(str).str.lower()
                
                # Check if this row looks like a header row
                if (row.str.contains('sr|no|date|particular|debit|credit|amount|balance', regex=True).any()):
                    # Try reading the file again with this row as header
                    temp_df = pd.read_excel(excel_path, sheet_name=sheet_name, skiprows=idx)
                    temp_df.columns = temp_df.columns.astype(str).str.strip().str.lower()
                    
                    print(f"Potential header row found at index {idx}")
                    print("Columns:", temp_df.columns.tolist())
                    
                    # Look for required columns
                    found_columns = {}
                    required_columns = {
                        'srno': ['sr.no', 'sr no', 'srno', 'sr.no.', 'serial no', 'sno', 'no', 'no.'],
                        'date': ['date', 'transaction date', 'trans date', 'value date', 'posting date'],
                        'details': ['transaction details', 'particulars', 'details', 'remarks', 'narration', 'description'],
                        'amount': ['amount', 'withdrawal', 'debit', 'credit', 'transaction amount'],
                        'sign': ['dr/cr', 'type', 'billingamountsign', 'debit/credit']
                    }
                    
                    for req_col, possible_names in required_columns.items():
                        for col in temp_df.columns:
                            if any(name in col.lower() for name in possible_names):
                                found_columns[req_col] = col
                                break
                    
                    # Also check first row for column names
                    first_row = temp_df.iloc[0].astype(str).str.lower()
                    for req_col, possible_names in required_columns.items():
                        if req_col not in found_columns:
                            for col in temp_df.columns:
                                cell_value = first_row[col]
                                if any(name in str(cell_value).lower() for name in possible_names):
                                    found_columns[req_col] = col
                                    temp_df = temp_df.iloc[1:]  # Skip the header row
                                    break
                    
                    if len(found_columns) >= 3:  # We found most of our required columns
                        df = temp_df
                        print("Found required columns!")
                        print("Matched columns:", found_columns)
                        break
            
            if df is not None:
                break
        
        if df is None:
            raise ValueError("Could not find sheet with required columns")
        
        # Create new DataFrame with only required columns
        transactions = []
        for _, row in df.iterrows():
            try:
                # Extract date
                date_col = found_columns.get('date')
                trans_date = row[date_col] if date_col else None
                if pd.isna(trans_date):
                    continue
                
                try:
                    # Handle different date formats and convert to DD-Mon-YY
                    if isinstance(trans_date, str):
                        # Try multiple date formats
                        date_formats = ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%d/%m/%y']
                        date_obj = None
                        
                        for date_format in date_formats:
                            try:
                                date_obj = datetime.strptime(trans_date, date_format)
                                break
                            except ValueError:
                                continue
                        
                        if date_obj is None:
                            # If all formats fail, try pandas to_datetime
                            date_obj = pd.to_datetime(trans_date).to_pydatetime()
                    elif isinstance(trans_date, datetime):
                        date_obj = trans_date
                    elif isinstance(trans_date, pd.Timestamp):
                        date_obj = trans_date.to_pydatetime()
                    else:
                        # Try parsing with pandas
                        date_obj = pd.to_datetime(trans_date).to_pydatetime()
                    
                    # Convert to DD-Mon-YY format
                    formatted_date = date_obj.strftime('%d-%b-%y')  # This will give format like '01-Jan-24'
                except Exception as e:
                    print(f"Date conversion error for {trans_date}: {e}")
                    continue
                
                # Extract details
                details_col = found_columns.get('details')
                details = str(row[details_col]).strip() if details_col else ''
                details = re.sub(r'\s+', ' ', details)
                
                # Extract withdrawal and deposit amounts
                withdrawal_amount = 0.0
                deposit_amount = 0.0
                
                # Check for withdrawal/debit amount
                for col in df.columns:
                    if any(name in col.lower() for name in ['withdrawal', 'debit', 'dr']):
                        withdrawal_amount = clean_amount(row[col])
                        break
                
                # Check for deposit/credit amount
                for col in df.columns:
                    if any(name in col.lower() for name in ['deposit', 'credit', 'cr']):
                        deposit_amount = clean_amount(row[col])
                        break
                
                # Determine final amount and sign
                if withdrawal_amount > 0:
                    amount = withdrawal_amount
                    sign = 'Dr'
                elif deposit_amount > 0:
                    amount = deposit_amount
                    sign = 'Cr'
                else:
                    amount = 0.0
                    sign = 'Dr'
                
                # Extract SrNo and convert to integer
                srno_col = found_columns.get('srno')
                if srno_col and not pd.isna(row[srno_col]):
                    try:
                        srno = int(float(row[srno_col]))
                    except:
                        srno = str(row[srno_col]).strip()
                else:
                    srno = ''
                
                # Skip rows where all values are empty or zero
                if not details and amount == 0.0:
                    continue
                
                transaction = {
                    'SrNo': srno,
                    'Date': formatted_date,
                    'TransactionDetails': details,
                    'Amount': abs(amount),
                    'BillingAmountSign': sign
                }
                
                transactions.append(transaction)
                
            except Exception as e:
                print(f"Error processing row: {row}")
                print(f"Error details: {str(e)}")
                continue
        
        print(f"\nTotal transactions extracted: {len(transactions)}")
        return transactions
    
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        raise

def create_database(transactions, db_path):
    if not transactions:
        print("No transactions to process!")
        return
    
    print(f"\nCreating database with {len(transactions)} transactions...")
    
    # Create DataFrame with only required columns in specific order
    df = pd.DataFrame(transactions)
    df = df[[
        'SrNo',
        'Date',
        'TransactionDetails',
        'Amount',
        'BillingAmountSign'
    ]]
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    
    try:
        # Create table with only required columns
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS transactions (
            SrNo TEXT,
            Date TEXT,
            TransactionDetails TEXT,
            Amount REAL,
            BillingAmountSign TEXT
        )
        '''
        conn.execute(create_table_sql)
        
        # Clear existing data
        conn.execute('DELETE FROM transactions')
        
        # Insert data
        df.to_sql('transactions', conn, if_exists='replace', index=False)
        
        conn.commit()
        print(f"\nDatabase created successfully at {db_path}")
        
        # Display sample data
        print("\nSample data from database:")
        sample_df = pd.read_sql_query("SELECT * FROM transactions LIMIT 5", conn)
        print(sample_df.to_string())
        
    except Exception as e:
        print(f"Error creating database: {str(e)}")
        raise
    finally:
        conn.close()

def main():
    excel_path = r"C:\Users\seren\OneDrive\Desktop\PythonRepo\ICICI_SA_0090(24-25).xls"
    db_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\ICICI_SA_0090(24-25).db"
    
    if not os.path.exists(excel_path):
        print(f"Error: Excel file not found at: {excel_path}")
        return
    
    print("Starting transaction extraction...")
    transactions = extract_transactions_from_excel(excel_path)
    
    if transactions:
        create_database(transactions, db_path)
        print(f"\nSuccessfully processed {len(transactions)} transactions!")
    else:
        print("No transactions were extracted!")

if __name__ == "__main__":
    main()





