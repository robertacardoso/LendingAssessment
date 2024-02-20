import os
import psycopg2
import psycopg2.pool
import traceback
import csv

# Define a global variable for the connection pool
connection_pool = None

def create_connection_pool(db_url):
    """
    Creates a connection pool to the PostgreSQL database.

    Args:
    - db_url (str): PostgreSQL database URL.

    Returns:
    - psycopg2.pool.SimpleConnectionPool: Connection pool object.
    """
    global connection_pool
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, dsn=db_url)

def get_connection():
    """
    Retrieves a connection from the connection pool.

    Returns:
    - psycopg2.extensions.connection: Database connection object.
    """
    if connection_pool is None:
        raise ValueError("Connection pool not initialized. Call create_connection_pool() first.")
    return connection_pool.getconn()

def release_connection(connection):
    """
    Releases a connection back to the connection pool.

    Args:
    - connection (psycopg2.extensions.connection): Database connection object.
    """
    connection_pool.putconn(connection)

def test_database_connection(db_url):
    """
    Test the connection to the PostgreSQL database.

    Args:
    - db_url (str): PostgreSQL database URL.

    Returns:
    - bool: True if connection successful, False otherwise.
    """
    try:
        connection = psycopg2.connect(db_url)
        connection.close()
        return True
    except psycopg2.Error:
        return False

def check_database_permissions(db_url):
    """
    Check if the user specified in the database URL has sufficient permissions.

    Args:
    - db_url (str): PostgreSQL database URL.

    Returns:
    - bool: True if user has sufficient permissions, False otherwise.
    """
    # You can customize this function based on your specific permissions requirements
    return True

def db_query(sql_script, db_url):
    """
    Executes the given SQL script on the specified PostgreSQL database.

    Args:
    - sql_script (str): Path to the SQL file containing the script to execute.
    - db_url (str): PostgreSQL database URL.

    Returns:
    - result (list): Result of the last query executed.
    """
    connection = None
    cursor = None
    try:
        if not os.path.exists(sql_script):
            raise FileNotFoundError(f"File '{sql_script}' not found.")

        with open(sql_script, 'r') as file:
            sql_query = file.read()

        print("Executing SQL script:")
        print(sql_query)

        connection = psycopg2.connect(db_url)
        cursor = connection.cursor()

        cursor.execute(sql_query)
        connection.commit()

        # Fetch the result of the last query
        result = fetch_query_result(cursor)
        print("Query executed successfully.")
        return result

    except Exception as e:
        print(f"Error executing SQL script: {e}")
        traceback.print_exc()
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def fetch_query_result(cursor):
    """
    Fetches the result of the last executed query.

    Args:
    - cursor: psycopg2 cursor object.

    Returns:
    - result (list): Result of the last query executed.
    """
    try:
        if cursor.description is not None:
            result = cursor.fetchall()
            return result if result else None
        else:
            return None

    except psycopg2.Error as e:
        print(f"Error fetching query result. {e}")
        return None

def debug_info(sql_script, db_url):
    """
    Print debug information about the SQL script and database URL.

    Args:
    - sql_script (str): Path to the SQL file containing the script to execute.
    """
    print("Debugging information:")
    print(f"SQL Script: {sql_script}")


def ingest_data_from_csv(csv_file, table_name, db_url):
  """
  Ingests data from a CSV file into the specified database table.

  Args:
  - csv_file (str): Path to the CSV file.
  - table_name (str): Name of the database table.
  - db_url (str): PostgreSQL database URL.

  Returns:
  - int: Number of rows inserted into the database table.
  """
  connection = None
  cursor = None
  try:
      connection = get_connection()
      cursor = connection.cursor()

      with open(csv_file, 'r', newline='', encoding='utf-8') as file:
          reader = csv.DictReader(file)
          headers = ','.join(reader.fieldnames)
          query = f"COPY {table_name} ({headers}) FROM STDIN WITH CSV HEADER"
          cursor.copy_expert(sql=query, file=file)

      connection.commit()

      return cursor.rowcount

  except psycopg2.Error as e:
      print(f"Error ingesting data from CSV into {table_name}: {e}")
      return 0

  finally:
      if cursor:
          cursor.close()
      if connection:
          release_connection(connection)