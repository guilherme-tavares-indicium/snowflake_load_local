from snowflake.connector import pandas_tools as pt
import snowflake.connector
import pandas as pd
import os
from dotenv import dotenv_values

class DataLoader:
    def __init__(self, directory, config_file='.env'):
        self.directory = directory
        self.file_paths = self._get_file_paths()
        self.config = dotenv_values(config_file)
        self.connection = snowflake.connector.connect(
            account=self.config["account"],
            user=self.config["user"],
            password=self.config["password"],
            database=self.config["database"],
            schema=self.config["schema"],
            role=self.config["role"],
        )
        self.cur = self.connection.cursor()

    def _get_file_paths(self):
        file_paths = {}
        for root, _, files in os.walk(self.directory):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    file_name = os.path.splitext(file)[0]
                    file_paths[file_path] = file_name
        return file_paths

    def load_data(self):
        for filepath, file_name in self.file_paths.items():
            # Check if table already exists
            self.cur.execute(f"SHOW TABLES LIKE '{file_name}'")
            rows = self.cur.fetchall()
            if not rows:
                # If table does not exist, create it using the schema inferred from the pandas DataFrame
                df = pd.read_csv(filepath)
                schema = ', '.join([f'{col} STRING' for col in df.columns])
                # print(schema)
                self.cur.execute(f"CREATE TABLE {file_name} ({schema})")
            # Load data into table
            df = pd.read_csv(filepath)
            pt.write_pandas(conn=self.connection, df=df, table_name=file_name, quote_identifiers=False)
            print(f"{filepath} loaded successfully")

    def close(self):
        self.connection.close()