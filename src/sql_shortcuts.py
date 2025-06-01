import sqlite3
import pandas as pd

# sqlite3 shortcuts
def table(table, db = "transfermarkt.db"):
    
    """Shortcut to load a sqlite3 table"""
    
    conn = sqlite3.connect(db)
    
    query = f"SELECT * FROM {table}"
    df = pd.read_sql_query(query, conn)

    conn.close()
    
    return df

def tables(db_path = "transfermarkt.db"):
    
    """This function lists all of the tables in a given database"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return tables

def drop_table(table_name, db_path = "transfermarkt.db"):
    
    """This function removes a table from the given database"""
    
    print(f"This action will permanently delete {table_name}. Continue? (y/n)")
    x = input()
    if x == "y":
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()

        print(f"Table '{table_name}' dropped successfully (if it existed).")

        conn.close()
    else:
        print("Aborted")
    
def load_keys(table, primary_key, db = "transfermarkt.db"):
    
    """ 
    This function loads primary kays and hash_keys from a given table.
    It's used to check for updates during historization.
    """
    conn = sqlite3.connect(db)

    # SQL query to fetch data
    query = f"SELECT DISTINCT {', '.join(primary_key)}, hash_key FROM {table}"

    # Load data into a DataFrame
    df = pd.read_sql_query(query, conn).assign(joined = 1).drop_duplicates().rename({"hash_key":"hash_key_current"}, axis = 1)

    # Close the connection
    conn.close()
    
    return df

def replace_table(table_name, df, db = "transfermarkt.db"):
    
    """The function drops (if exists) and replaces a table in the database"""
    
    print("Are you sure you want to overwrite this table? This will overwrite the existing data permanently (y/n)")
    x = input()
    
    if x == 'y':
        conn = sqlite3.connect(db)

        # Save DataFrame to a new table (schema) named "players"
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        # Close connection
        conn.close()
    
def append_table(df, table_name, db = "transfermarkt.db"):
    
    """This function appends data to an existing db"""
    
    conn = sqlite3.connect(db)

    # Save DataFrame to a new table (schema) named "players"
    df.to_sql(table_name, conn, if_exists="append", index=False)

    # Close connection
    conn.close()