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
3. Create a `.env` file in the project directory with the following variables:

account="your-snowflake-account"
user="your-snowflake-user"
password="your-snowflake-password"
database="your-snowflake-database"
schema="your-snowflake-schema"
role="your-snowflake-role"


4. Replace the values in angle brackets with your Snowflake account details. There is no need to worry about loading environment variables, since this project uses the dotenv library to automate that.

## Usage

1. Place the CSV files to be loaded in a directory.
2. Edit the `directory` variable in the script to point to the directory containing the CSV files.
3. Run the script using `python snowflake_csv_loader.py`.

The script will load each CSV file into a corresponding table in Snowflake. If a table with the same name already exists, the script will load the data into the existing table (append data).
