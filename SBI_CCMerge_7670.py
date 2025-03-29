import sqlite3
import pandas as pd
import os
from datetime import datetime

def standardize_date(date_str):
    """Convert various date formats to a standard format"""
    try:
        # Handle different date formats
        if isinstance(date_str, str):
            # Try different date formats
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

def merge_databases():
    # Define paths
    db_path1 = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\SBI_CC_7670(D).db"
    db_path2 = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\SBI_CC_7670(D1).db"
    output_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\SBI_CC_7670.db"

    all_transactions = []
    
    # Process first database
    if os.path.exists(db_path1):
        try:
            conn1 = sqlite3.connect(db_path1)
            df1 = pd.read_sql_query("SELECT * FROM transactions", conn1)
            print(f"Database 1: Found {len(df1)} transactions")
            all_transactions.append(df1)
            conn1.close()
        except Exception as e:
            print(f"Error reading first database: {e}")
    else:
        print(f"First database not found at: {db_path1}")

    # Process second database
    if os.path.exists(db_path2):
        try:
            conn2 = sqlite3.connect(db_path2)
            df2 = pd.read_sql_query("SELECT * FROM transactions", conn2)
            print(f"Database 2: Found {len(df2)} transactions")
            all_transactions.append(df2)
            conn2.close()
        except Exception as e:
            print(f"Error reading second database: {e}")
    else:
        print(f"Second database not found at: {db_path2}")

    if not all_transactions:
        print("No transactions found in either database!")
        return

    # Combine all transactions
    merged_df = pd.concat(all_transactions, ignore_index=True)
    
    # Standardize date column name
    date_column = 'Date' if 'Date' in merged_df.columns else 'TransactionDate'
    merged_df = merged_df.rename(columns={date_column: 'Date'})
    
    # Standardize all dates to YYYY-MM-DD format
    print("Standardizing dates...")
    merged_df['Date'] = merged_df['Date'].apply(standardize_date)
    
    # Convert to datetime for sorting
    merged_df['Date'] = pd.to_datetime(merged_df['Date'])
    
    # Sort by date
    print("Sorting transactions by date...")
    merged_df = merged_df.sort_values('Date')
    
    # Reset index and add SrNo column
    merged_df = merged_df.reset_index(drop=True)
    merged_df['SrNo'] = range(1, len(merged_df) + 1)
    
    # Convert date back to string in consistent format
    merged_df['Date'] = merged_df['Date'].dt.strftime('%d-%b-%y')

    # Create new database
    try:
        if os.path.exists(output_path):
            os.remove(output_path)
            print(f"Removed existing output database")

        conn = sqlite3.connect(output_path)
        
        # Create transactions table with proper schema
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS transactions (
            SrNo INTEGER,
            Date TEXT,
            TransactionDetails TEXT,
            Amount REAL,
            BillingAmountSign TEXT
        )
        '''
        conn.execute(create_table_sql)
        
        # Insert data
        merged_df.to_sql('transactions', conn, if_exists='replace', index=False)
        
        # Create index on date
        conn.execute('CREATE INDEX idx_date ON transactions(Date)')
        
        # Commit changes
        conn.commit()

        print(f"\nSuccessfully merged databases:")
        print(f"Total transactions: {len(merged_df)}")
        
        # Display sample data
        print("\nFirst 5 transactions:")
        sample = pd.read_sql_query("""
            SELECT SrNo, Date, TransactionDetails, Amount, BillingAmountSign 
            FROM transactions 
            ORDER BY Date 
            LIMIT 5""", conn)
        print(sample)

        print("\nLast 5 transactions:")
        sample = pd.read_sql_query("""
            SELECT SrNo, Date, TransactionDetails, Amount, BillingAmountSign 
            FROM transactions 
            ORDER BY Date DESC 
            LIMIT 5""", conn)
        print(sample)
        
        conn.close()

    except Exception as e:
        print(f"Error creating merged database: {e}")

if __name__ == "__main__":
    print("Starting database merge process...")
    merge_databases()




