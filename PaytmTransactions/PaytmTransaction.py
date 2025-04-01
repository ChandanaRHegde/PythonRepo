import pandas as pd
import sqlite3

try:
    # === Step 1: Load Excel ===
    excel_file_path = r"C:\Users\seren\OneDrive\Desktop\PythonTransaction\PaytmUPIStatement01Apr23-31Mar24.xlsx"
    sheet_name = "Passbook Payment History"

    # Load the sheet
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
    
    print("Original columns:", df.columns.tolist())
    
    # === Step 2: Clean and Format Data ===
    df_cleaned = df.copy()

    # Convert Date to datetime for sorting
    df_cleaned["Date"] = pd.to_datetime(df_cleaned["Date"], format='%d/%m/%Y', errors='coerce')

    # Sort by date in ascending order
    df_cleaned = df_cleaned.sort_values("Date")

    # Format date to DD-Mon-YY
    df_cleaned["Date"] = df_cleaned["Date"].dt.strftime("%d-%b-%y")

    # Add SrNO after sorting
    df_cleaned.insert(0, "SrNO", range(1, len(df_cleaned) + 1))

    # Handle Transaction Details column
    if "Transaction Details" in df.columns:
        df_cleaned["TransactionDetails"] = df_cleaned["Transaction Details"]
    elif "Transaction_Details" in df.columns:
        df_cleaned["TransactionDetails"] = df_cleaned["Transaction_Details"]
    else:
        raise ValueError("Transaction Details column not found in Excel file")

    # Clean and convert Amount column
    df_cleaned["Amount"] = df_cleaned["Amount"].astype(str).str.replace(",", "").astype(float)

    # Determine transaction type (DR/CR) based on transaction description and amount
    def determine_transaction_type(row):
        amount = row["Amount"]
        details = str(row["TransactionDetails"]).lower()
        
        # Keywords indicating money going out (DR)
        debit_keywords = ["paid", "payment", "sent", "debited", "purchase", "withdrawn"]
        
        # Keywords indicating money coming in (CR)
        credit_keywords = ["received", "credited", "refund", "cashback", "added"]
        
        # Check for debit keywords
        if any(keyword in details for keyword in debit_keywords):
            return "DR"
        # Check for credit keywords
        elif any(keyword in details for keyword in credit_keywords):
            return "CR"
        # If no keywords found, use amount sign
        else:
            return "DR" if amount < 0 else "CR"

    # Apply transaction type determination
    df_cleaned["BillingAmountSign-DR,CR"] = df_cleaned.apply(determine_transaction_type, axis=1)

    # Convert Amount to absolute value
    df_cleaned["Amount"] = df_cleaned["Amount"].abs()

    # === Step 3: Keep only necessary columns ===
    final_df = df_cleaned[["SrNO", "Date", "TransactionDetails", "Amount", "BillingAmountSign-DR,CR"]]

    # Check for null values
    print("\nChecking for null values:")
    print(final_df.isnull().sum())

    # === Step 4: Export to SQLite ===
    db_path = r"C:\Users\seren\OneDrive\Desktop\NewfolderOne\PaytmUPIStatement.db"
    
    conn = sqlite3.connect(db_path)
    
    # Create table with correct schema
    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS transactions (
        SrNO INTEGER,
        Date TEXT,
        TransactionDetails TEXT,
        Amount REAL,
        "BillingAmountSign-DR,CR" TEXT
    )
    '''
    conn.execute(create_table_sql)
    
    # Clear existing data
    conn.execute("DELETE FROM transactions")
    
    # Insert data
    final_df.to_sql('transactions', conn, if_exists='replace', index=False)

    # Display sample data to verify
    print("\nFirst 5 transactions:")
    sample = pd.read_sql_query("""
        SELECT SrNO, Date, TransactionDetails, Amount, "BillingAmountSign-DR,CR"
        FROM transactions 
        ORDER BY Date 
        LIMIT 5""", conn)
    print(sample)

    print("\nLast 5 transactions:")
    sample = pd.read_sql_query("""
        SELECT SrNO, Date, TransactionDetails, Amount, "BillingAmountSign-DR,CR"
        FROM transactions 
        ORDER BY Date DESC 
        LIMIT 5""", conn)
    print(sample)

    # Verify DR/CR distribution
    print("\nTransaction type distribution:")
    type_dist = pd.read_sql_query("""
        SELECT "BillingAmountSign-DR,CR", COUNT(*) as count 
        FROM transactions 
        GROUP BY "BillingAmountSign-DR,CR"
        """, conn)
    print(type_dist)

    conn.close()
    print("\n✅ Data successfully processed and saved to database")

except Exception as e:
    print(f"Error processing data: {str(e)}")
    import traceback
    print(f"Full error details:\n{traceback.format_exc()}")
    if 'conn' in locals():
        conn.close()
