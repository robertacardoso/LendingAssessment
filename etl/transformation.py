"""
Description: This script contains functions for transforming data for the Clients and Loans tables according to the final schema.

Prerequisites:
- Required Python packages: pandas, numpy

Usage:
1. Call the 'transform_clients_data' function with the clients DataFrame as argument.
2. Call the 'transform_loans_data' function with the loans DataFrame as argument.
"""
import pandas as pd
import numpy as np


def transform_clients_data(clients_df):
  """
    Purpose: Transform data for the Clients table according to the final schema.
    Usage: Call this function with the clients DataFrame as argument.
    Returns: Transformed clients DataFrame.
    """
  # Convert 'created_at' column to datetime
  clients_df['created_at'] = pd.to_datetime(clients_df['created_at'],
                                            errors='coerce')

  # Convert 'denied_at' column to datetime
  clients_df['denied_at'] = pd.to_datetime(clients_df['denied_at'],
                                           errors='coerce')

  # Convert 'status' column to string
  clients_df['status'] = clients_df['status'].astype(str)

  # Fill missing values in 'denied_reason' column with empty string
  clients_df['denied_reason'] = clients_df['denied_reason'].fillna('')

  return clients_df


def transform_loans_data(loans_df):
  """
    Purpose: Transform data for the Loans table according to the final schema.
    Usage: Call this function with the loans DataFrame as argument.
    Returns: Transformed loans DataFrame.
    """
  # Convert datetime columns to datetime, handling 'NaT' values
  date_columns = ['created_at', 'due_at', 'paid_at']
  for col in date_columns:
    loans_df[col] = pd.to_datetime(loans_df[col], errors='coerce')

  # Convert 'status' column to string
  loans_df['status'] = loans_df['status'].astype(str)

  # Calculate tax (3.8% of the principal amount + 0.0082% of the principal amount per day for 90 days)
  loans_df['tax'] = loans_df['loan_amount'] * 0.038 + loans_df[
      'loan_amount'] * 0.000082 * 90

  # Calculate due amount (loan_amount + tax + 90 days interest)
  loans_df['due_amount'] = loans_df['loan_amount'] + loans_df[
      'tax'] + loans_df['loan_amount'] * 0.000082 * 90

  # Update loan status based on specific conditions
  loans_df.loc[(loans_df['paid_at'].isnull()) &
               (loans_df['due_at'] < pd.Timestamp.now()), 'status'] = 'default'
  loans_df.loc[(loans_df['paid_at'].isnull()) &
               (loans_df['due_at'] >= pd.Timestamp.now()),
               'status'] = 'ongoing'
  loans_df.loc[loans_df['paid_at'].notnull(), 'status'] = 'paid'

  return loans_df
