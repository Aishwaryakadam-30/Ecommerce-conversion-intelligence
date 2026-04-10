"""
Data Cleaning Module for E-commerce Clickstream Data
=====================================================
Author: Aishwarya Kadam
Project: E-commerce Conversion Intelligence Platform

This module provides reusable functions to clean and validate
clickstream event data before analysis.
"""

import pandas as pd
import numpy as np
from datetime import datetime


def load_raw_data(file_path: str) -> pd.DataFrame:
    """
    Load raw clickstream CSV into a DataFrame.
    
    Args:
        file_path: Path to the raw CSV file
        
    Returns:
        DataFrame with raw clickstream events
    """
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df):,} rows from {file_path}")
    return df


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert column names to snake_case for consistency.
    
    Example: UserID -> user_id, EventType -> event_type
    """
    column_mapping = {
        'UserID': 'user_id',
        'SessionID': 'session_id',
        'Timestamp': 'timestamp',
        'EventType': 'event_type',
        'ProductID': 'product_id',
        'Amount': 'amount',
        'Outcome': 'outcome'
    }
    df = df.rename(columns=column_mapping)
    return df


def convert_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert timestamp column to pandas datetime type.
    Adds derived time features for analysis.
    """
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.day_name()
    df['month'] = df['timestamp'].dt.month
    df['year'] = df['timestamp'].dt.year
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove exact duplicate rows.
    """
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    removed = before - after
    if removed > 0:
        print(f"Removed {removed:,} duplicate rows ({(removed/before*100):.2f}%)")
    return df


def remove_bot_sessions(df: pd.DataFrame, max_events: int = 500) -> pd.DataFrame:
    """
    Remove sessions with unusually high event counts (likely bots).
    
    Args:
        df: Input DataFrame
        max_events: Sessions with more events than this are filtered
    
    Returns:
        DataFrame without bot sessions
    """
    session_counts = df.groupby('session_id').size()
    bot_sessions = session_counts[session_counts > max_events].index
    
    before = len(df)
    df = df[~df['session_id'].isin(bot_sessions)]
    after = len(df)
    
    if before > after:
        print(f"Removed {len(bot_sessions)} bot sessions ({before-after:,} events)")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values in critical columns.
    
    Strategy:
    - Drop rows missing user_id or event_type (critical)
    - Keep rows missing product_id or amount (expected for non-purchase events)
    """
    before = len(df)
    df = df.dropna(subset=['user_id', 'event_type', 'timestamp'])
    after = len(df)
    
    if before > after:
        print(f"Dropped {before-after:,} rows with missing critical fields")
    return df


def validate_event_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only valid, expected event types.
    """
    valid_events = [
        'page_view', 'product_view', 'click',
        'add_to_cart', 'purchase', 'login', 'logout'
    ]
    
    before = len(df)
    df = df[df['event_type'].isin(valid_events)]
    after = len(df)
    
    if before > after:
        print(f"Removed {before-after:,} rows with invalid event types")
    return df


def validate_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate purchase amounts:
    - Must be positive for purchase events
    - Cap extreme outliers at 99th percentile
    """
    purchase_mask = df['event_type'] == 'purchase'
    
    # Remove purchases with invalid amounts
    invalid = purchase_mask & (df['amount'].isna() | (df['amount'] <= 0))
    before = len(df)
    df = df[~invalid]
    after = len(df)
    
    if before > after:
        print(f"Removed {before-after:,} purchases with invalid amounts")
    
    # Cap extreme outliers
    if purchase_mask.any():
        p99 = df.loc[purchase_mask, 'amount'].quantile(0.99)
        outlier_mask = purchase_mask & (df['amount'] > p99)
        outlier_count = outlier_mask.sum()
        if outlier_count > 0:
            df.loc[outlier_mask, 'amount'] = p99
            print(f"Capped {outlier_count:,} amount outliers at ${p99:.2f}")
    
    return df


def clean_pipeline(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Run the complete cleaning pipeline.
    
    Args:
        df: Raw input DataFrame
        verbose: If True, print progress messages
    
    Returns:
        Cleaned DataFrame ready for analysis
    """
    if verbose:
        print("=" * 50)
        print("STARTING DATA CLEANING PIPELINE")
        print("=" * 50)
        print(f"Input: {len(df):,} rows")
        print()
    
    df = standardize_column_names(df)
    df = convert_timestamps(df)
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = validate_event_types(df)
    df = validate_amounts(df)
    df = remove_bot_sessions(df)
    
    if verbose:
        print()
        print(f"Output: {len(df):,} rows")
        print("=" * 50)
        print("CLEANING COMPLETE")
        print("=" * 50)
    
    return df


def data_quality_report(df: pd.DataFrame) -> dict:
    """
    Generate a data quality report for the cleaned dataset.
    """
    report = {
        'total_rows': len(df),
        'total_users': df['user_id'].nunique(),
        'total_sessions': df['session_id'].nunique(),
        'total_products': df['product_id'].nunique() if 'product_id' in df.columns else 0,
        'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}",
        'event_type_counts': df['event_type'].value_counts().to_dict(),
        'missing_values': df.isnull().sum().to_dict(),
        'duplicate_rows': df.duplicated().sum()
    }
    return report
