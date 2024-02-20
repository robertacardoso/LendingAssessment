import os
from db_mgmt.db_handlers import *

def db_setup():
    # Get database URL from environment variable
    db_url = os.getenv("db_url")
    if db_url is None:
        raise ValueError("Missing db_url environment variable.")

    # Test the database connection
    print("Database connection test result:", test_database_connection(db_url))

    # Check database permissions
    print("Database permissions check result:", check_database_permissions(db_url))
  
    # Execute the SQL script on the specified database
    print("Executing SQL script...")
    result = db_query("db_mgmt/queries/db_setup.sql", db_url)

    if result is not None:
        print("Result:", result)

if __name__ == "__main__":
    db_setup()
