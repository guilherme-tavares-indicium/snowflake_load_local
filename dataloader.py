from snowflake.connector import pandas_tools as pt
import snowflake.connector
import pandas as pd
import os
from dotenv import dotenv_values

class DataLoader:
    def __init__(self, directory, config_file='.env'):
        self.directory = directory
        self.file_paths = self._get_directory_files()
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

    def _get_files(self, path):
        file_paths = {}

        # Check files in subdirectories 
        for dirpath, _, filenames in os.walk(path):
            for filename in filenames:
                if filename.endswith('.csv'):
                    filepath = os.path.join(dirpath, filename)
                    file_name = os.path.splitext(filename)[0]
                    file_paths[filepath] = file_name

        # Check files in the base directory
        for filename in os.listdir(path):
            if filename.endswith('.csv'):
                filepath = os.path.join(path, filename)
                file_name = os.path.splitext(filename)[0]
                file_paths[filepath] = file_name
        return file_paths

    def _get_directory_files(self):
        file_paths = {}
        file_paths.update(self._get_files(self.directory))
        for root, dirs, _ in os.walk(self.directory):
            for dir in dirs:
                path = os.path.join(root, dir)
                file_paths.update(self._get_files(path))
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