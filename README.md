# Snowflake CSV Loader

This script loads CSV files into Snowflake tables. For each CSV file in a specified directory, it checks if a table with the same name exists in Snowflake. If the table does not exist, it creates it using the schema inferred from the pandas DataFrame. It then loads the data into the table.

## Requirements

- Python 3.6 or higher
- `snowflake-connector` library
- `pandas` library
- `dotenv` library

## Setup

1. Clone the repository and navigate to the project directory.
2. Install the required libraries by running `pip install -r requirements.txt`.
3. Create a `.env` file in the project directory, containing the following variables:
    - `account`: the name of the Snowflake account to connect to
    - `user`: the username to use for authentication
    - `password`: the password to use for authentication
    - `database`: the name of the database to create tables in
    - `schema`: the name of the schema to create tables in
    - `role`: the name of the role to use for authentication


4. Replace the values in angle brackets with your Snowflake account details. There is no need to worry about loading environment variables, since this project uses the dotenv library to automate that.

## Usage

1. Set the `directory` variable in the `main` function to the path of the directory that contains the csv files to be loaded.
2. Run the script using `python main.py` to create tables for each CSV file in the Snowflake database and load the data into the corresponding tables, named after the csv file name.

The script will load each CSV file into a corresponding table in Snowflake. If a table with the same name already exists, the script will load the data into the existing table (append data).
