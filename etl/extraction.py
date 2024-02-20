"""
Description: This script contains functions for extracting data from CSV files and printing their schema.

Prerequisites:
- Required Python packages: pandas

Usage:
1. Call the 'print_csv_schema' function with the path to the CSV file as argument to print its schema.
"""

import pandas as pd


def print_csv_schema(csv_file_path):
  """
    Purpose: Print the schema of a CSV file.
    Usage: Call this function with the path to the CSV file as argument.
    Returns: None
    """
  try:
    df = pd.read_csv(csv_file_path)
    print("CSV Schema:")
    print(df.dtypes)
  except Exception as e:
    print(f"Error: {e}")
