import os
from pipeline.extraction import download_csv
from infra.db_handlers import create_connection_pool, ingest_data_from_csv

def main():
    # Create a connection pool
    db_url = os.getenv('db_url')
    create_connection_pool(db_url)

    # URLs of the raw CSV files on GitHub
    clients_csv_url = "https://raw.githubusercontent.com/p-beraldin/lending-data-engineer-test/main/files/clients.csv"
    loans_csv_url = "https://raw.githubusercontent.com/p-beraldin/lending-data-engineer-test/main/files/loans.csv"

    # Local file paths to save the downloaded CSV files
    clients_csv = "infra/data/clients.csv"
    loans_csv = "infra/data/loans.csv"

    # Download the CSV files
    download_csv(clients_csv_url, clients_csv)
    download_csv(loans_csv_url, loans_csv)

    # Ingest data from CSV files into the database
    num_rows_inserted_clients = ingest_data_from_csv(clients_csv, "clients", db_url)
    print(f"Number of new rows inserted into clients table: {num_rows_inserted_clients}")

    num_rows_inserted_loans = ingest_data_from_csv(loans_csv, "loans", db_url)
    print(f"Number of new rows inserted into loans table: {num_rows_inserted_loans}")

if __name__ == "__main__":
    main()
