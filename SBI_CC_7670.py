import pdfplumber
import pandas as pd
import re
import sqlite3
import os
from datetime import datetime

# Load PDF and extract transactions
pdf_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\SBICardStatement_7670_01-03-2024.pdf"
db_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\SBI_CC_7670(T1).db"
transactions = []
current_date = None

# Delete existing database file if it exists
if os.path.exists(db_path):
    try:
        os.remove(db_path)
        print(f"Removed existing database: {db_path}")
    except Exception as e:
        print(f"Error removing existing database: {e}")
        exit(1)

# Extract data from PDF
try:
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            lines = text.split('\n')
            for line in lines:
                # Pattern: Full line with date
                match = re.match(r"(\d{2} \w{3} \d{2}) (.+?) (\d{1,3}(?:,\d{3})*(?:\.\d{2})?) ([MDC])$", line)
                if match:
                    date, details, amount, sign = match.groups()
                    current_date = date
                else:
                    # Pattern: Line without date
                    match = re.match(r"(.+?) (\d{1,3}(?:,\d{3})*(?:\.\d{2})?) ([MDC])$", line)
                    if match and current_date:
                        details, amount, sign = match.groups()
                        date = current_date
                    else:
                        continue

                amount = float(amount.replace(',', ''))
                transactions.append({
                    'Date': date,
                    'Transaction_Details': details.strip(),
                    'Amount': amount,
                    'BillingAmountSign': sign
                })
except Exception as e:
    print(f"Error reading PDF: {e}")
    exit(1)

if not transactions:
    print("No transactions found in PDF!")
    exit(1)

# Convert to DataFrame
df = pd.DataFrame(transactions)

# Convert dates to datetime for proper sorting
df['Date'] = pd.to_datetime(df['Date'], format='%d %b %y')

# Sort by date
df = df.sort_values('Date')

# Convert back to original format
df['Date'] = df['Date'].dt.strftime('%d %b %y')

# Create and populate database
try:
    # Create new database connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Date TEXT NOT NULL,
        Transaction_Details TEXT NOT NULL,
        Amount REAL NOT NULL,
        BillingAmountSign TEXT NOT NULL
    )
    ''')

    # Insert data row by row (now in sorted order)
    for _, row in df.iterrows():
        cursor.execute('''
        INSERT INTO transactions (Date, Transaction_Details, Amount, BillingAmountSign)
        VALUES (?, ?, ?, ?)
        ''', (
            row['Date'],
            row['Transaction_Details'],
            row['Amount'],
            row['BillingAmountSign']
        ))

    # Create index
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON transactions(Date)')
    
    # Commit changes
    conn.commit()
    
    print(f"✅ Successfully saved {len(df)} transactions to database")
    
    # Verify data
    cursor.execute("SELECT COUNT(*) FROM transactions")
    count = cursor.fetchone()[0]
    print(f"Total records in database: {count}")
    
    # Show sample data
    print("\nSample data from database:")
    cursor.execute("SELECT * FROM transactions LIMIT 5")
    for row in cursor.fetchall():
        print(row)

except Exception as e:
    print(f"Error creating/populating database: {e}")
    if os.path.exists(db_path):
        os.remove(db_path)
    raise

finally:
    if 'conn' in locals():
        conn.close()
        print(f"✅ Successfully saved {len(df)} transactions to '{db_path}' in table 'transactions'")
