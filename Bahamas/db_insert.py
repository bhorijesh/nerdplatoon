import pandas as pd
import os
import sys

# Starting from the current script
current_dir = os.path.dirname(__file__)
Bahamas = os.path.abspath(os.path.join(current_dir, '..'))

# Add base directory to sys.path
sys.path.append(Bahamas)

# Corrected import statement
from base.db_connect import connect_database

def insert_into_table(cursor, table):
    # Load the CSV data
    df = pd.read_csv('details.csv')
    
    # Replace NaN values with None (which translates to NULL in MySQL)
    df = df.where(pd.notnull(df), None)
    
    # Escape column names with backticks to handle reserved words and spaces
    columns = ', '.join([f"`{col}`" for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))
    
    # Prepare the INSERT query
    insert_query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
    
    try:
        # Iterate through each row, converting NaNs to None explicitly
        for index, row in df.iterrows():
            row_data = [None if pd.isna(val) else val for val in row]  # Handle NaN in each cell
            cursor.execute(insert_query, tuple(row_data))
        print(f"Inserted {len(df)} rows into {table}.")
    except Exception as e:
        print(f'Error while inserting into Database: {e}')

def main():
    try:
        db_connection, cursor = connect_database()
    except Exception as e:
        print(f"Database connection error: {e}")
        return
    
    table = 'bahamas'
    
    insert_into_table(cursor, table)
    db_connection.commit()  # Commit changes to the database
    
    print('Data Insertion into Database Completed')
    
    # Close the database connection
    cursor.close()
    db_connection.close()

if __name__ == '__main__':
    main()
