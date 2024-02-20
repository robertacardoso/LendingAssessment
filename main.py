import pandas as pd
from etl.extraction import print_csv_schema
from etl.transformation import transform_clients_data, transform_loans_data
from etl.loading import load_data

def main():
    try:
        # Step 1: Extraction
        print("Extracting data from CSV files and printing schema...")
        clients_csv_url = "https://raw.githubusercontent.com/p-beraldin/lending-data-engineer-test/main/files/clients.csv"
        loans_csv_url = "https://raw.githubusercontent.com/p-beraldin/lending-data-engineer-test/main/files/loans.csv"

        clients_df = pd.read_csv(clients_csv_url, dtype={'batch': str})  # Specify 'batch' column as string due to mixed types
        loans_df = pd.read_csv(loans_csv_url, dtype={'user_id': int, 'loan_id': int})  # Specify data types for specific columns

        print_csv_schema(clients_df)
        print_csv_schema(loans_df)

        # Step 2: Transformation
        print("Transforming data...")
        clients_df = transform_clients_data(clients_df)
        loans_df = transform_loans_data(loans_df)

        # Step 3: Loading
        print("Loading transformed data into PostgreSQL database...")
        load_data(clients_df, 'clients')  # Load clients data
        load_data(loans_df, 'loans')  # Load loans data

        print("ETL process completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
