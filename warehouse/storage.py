import sqlite3
import pandas as pd

class Storage:
    def __init__(self, db_path="warehouse.db"):
        self.db_path = db_path

    def list_tables(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables

    def write_table(self, table_name, df):
        conn = sqlite3.connect(self.db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()

    def describe_table(self, table_name):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 5", conn)
        count = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM {table_name}", conn)['cnt'][0]
        columns = df.columns.tolist()
        conn.close()
        return {
            "table": table_name,
            "rows": count,
            "columns": columns,
            "sample": df
        }
    
    def read_table(self, table_name):
        import pandas as pd
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df