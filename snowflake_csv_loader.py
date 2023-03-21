from snowflake.connector import pandas_tools as pt
import snowflake.connector
import pandas as pd
import os
from dotenv import dotenv_values

config = dotenv_values(".env")  # read credential variables from .env.

def get_files(path):
    """
    Get a dictionary of file paths and their corresponding file names in a directory and its subdirectories.

    Args:
        path (str): The path to the directory.

    Returns:
        A dictionary where the keys are file paths and the values are file names (without extension).
    """
    file_paths = {}
    for dirpath, _, filenames in os.walk(path):
        for filename in filenames:
            if filename.endswith('.csv'):
                filepath = os.path.join(dirpath, filename)
                file_name = os.path.splitext(filename)[0]
                file_paths[filepath] = file_name
    return file_paths

def get_directory_files(directory):
    """
    Get a dictionary of file paths and their corresponding file names in a directory and its subdirectories.

    Args:
        directory (str): The path to the directory.

    Returns:
        A dictionary where the keys are file paths and the values are file names (without extension).
    """
    file_paths = {}
    for root, dirs, _ in os.walk(directory):
        for dir in dirs:
            path = os.path.join(root, dir)
            file_paths.update(get_files(path))
    return file_paths

directory = "./data"
file_paths = get_directory_files(directory)


print(file_paths)

connection = snowflake.connector.connect(
    account=config["account"],
    user=config["user"],
    password=config["password"],
    database=config["database"],
    schema=config["schema"],
    role=config["role"],
)

# create a cursor object
cur = connection.cursor()

# Set the schema for the session
cur.execute(f"USE SCHEMA {config['schema']}")

for filepath, file_name in file_paths.items():
    # Check if table already exists
    cur.execute(f"SHOW TABLES LIKE '{file_name}'")
    rows = cur.fetchall()
    if not rows:
        # If table does not exist, create it using the schema inferred from the pandas DataFrame
        df = pd.read_csv(filepath)
        schema = ', '.join([f'{col} STRING' for col in df.columns])
        print(schema)
        cur.execute(f"CREATE TABLE {file_name} ({schema})")
    # Load data into table
    df = pd.read_csv(filepath)
    pt.write_pandas(conn=connection, df=df, table_name=file_name, quote_identifiers=False)

connection.close()
