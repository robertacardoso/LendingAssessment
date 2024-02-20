"""
Description: This script contains a function to execute SQL scripts on a PostgreSQL database using psycopg2.

Prerequisites:
- PostgreSQL database must be set up.
- Required Python packages: psycopg2

Usage:
1. Set the 'db_url' environment variable with the PostgreSQL connection details.
2. Call the 'db_query' function with the path to the SQL file as argument.
3. The script will execute the SQL script on the specified database.
"""
import os
import psycopg2
from psycopg2 import sql

def db_query(file_path):
    try:
        db_url = os.getenv("db_url")
        if db_url is None:
            raise ValueError("Missing db_url environment variable.")

        with open(file_path, 'r') as file:
            sql_script = file.read()

        connection = psycopg2.connect(db_url)

        cursor = connection.cursor()
        cursor.execute(sql_script)
        connection.commit()

        # Fetch the result of the last query
        result = fetch_query_result(cursor)
        return result

    except ValueError as e:
        print(f"Error: {e}")
    except psycopg2.Error as e:
        print(f"Error: Unable to execute the SQL script. {e}")
        return None  # Return None on error
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None  # Return None on error
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def fetch_query_result(cursor):
  try:
      if cursor.description is not None:  # Check if the query generated results
          result = cursor.fetchall()
          return result if result else None  # Return None if result is empty
      else:
          return None  # Return None if the query did not generate results

  except psycopg2.Error as e:
      print(f"Error fetching query result. {e}")
      return None  # Return None on error
