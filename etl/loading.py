"""
Description: This script contains functions for loading data into a PostgreSQL database.

Prerequisites:
- PostgreSQL database must be set up.
- Required Python packages: psycopg2

Usage:
1. Set the 'db_url' environment variable with the PostgreSQL connection details.
2. Call the 'load_data' function with the appropriate arguments to load data into the database.
"""

import os
import logging
import pandas as pd
import psycopg2
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define mapping between pandas data types and expected database data types
PANDAS_TO_DB_DTYPES = {
    'int64': 'INT',
    'float64': 'FLOAT',
    'object': 'VARCHAR',
    'datetime64[ns]': 'TIMESTAMP'
}


def load_data(df, table_name):
  """
  Purpose: Load data from a DataFrame into a PostgreSQL table.
  Usage: Call this function with the DataFrame and table name as arguments.
  Returns: None
  """
  try:
      db_url = os.getenv("db_url")
      if db_url is None:
          raise ValueError("Missing db_url environment variable.")

      # Create a connection
      with psycopg2.connect(db_url) as connection:
          with connection.cursor() as cursor:
              # Create table if not exists
              create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} (").format(sql.Identifier(table_name))
              for col_name, dtype in df.dtypes.items():
                  db_dtype = PANDAS_TO_DB_DTYPES.get(str(dtype), 'VARCHAR')
                  create_table_query += sql.SQL("{} {},").format(sql.Identifier(col_name), sql.SQL(db_dtype))
              create_table_query = create_table_query[:-1]  # Remove trailing comma
              create_table_query += sql.SQL(")")
              cursor.execute(create_table_query)
              logger.info(f"Table '{table_name}' created successfully.")

              # Insert data into the table
              columns = ', '.join(df.columns)
              insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ").format(sql.Identifier(table_name), sql.Identifier(columns))
              values = ', '.join(['%s' for _ in range(len(df.columns))])
              insert_query += sql.SQL("(" + values + ")")
              psycopg2.extras.execute_batch(cursor, insert_query, df.values)
              connection.commit()
              logger.info("Data loaded successfully.")
  except Exception as e:
      logger.error(f"An error occurred: {e}")
