"""
Data preprocessing module for flight route finder.

This module handles the preprocessing of flight data tables including:
- Extracting city names from arrival and departure cities
- Extracts longitude and latitude from coordinate columns
- Converting times to a standardized format
- Calculate flight duration based on departure and arrival times
- Handling missing values, especially for prices
- Creating airport-city mappings
"""

import pandas as pd
import numpy as np
import ast


def extract_city_names(df):
    """
    Extracts English city names from specified columns containing dictionary-like strings.
    Creates new columns 'departure_city' and 'arrival_city'.

    :param df: DataFrame with columns like 'departure_city' and 'arrival_city'
    :return df: The original DataFrame with two new columns added:
                      'departure_city_name' and 'arrival_city_name'.

    >>> df_test = pd.DataFrame({
    ...     'departure_city': ['{"en": "Moscow", "ru": "Москва"}',
    ...                        '{"en": "St. Petersburg", "ru": "Санкт-Петербург"}',
    ...                        '{"ru": "Казань"}',
    ...                        None],
    ...     'arrival_city': ['{"en": "Sochi", "ru": "Сочи"}',
    ...                      '{"en": "Kazan", "ru": "Казань"}',
    ...                      '{"en": "Moscow"}',
    ...                      '{"en": "Novosibirsk", "ru": "Новосибирск"}']
    ... })
    >>> result = extract_city_names(df_test.copy())
    >>> result['departure_city_name'].tolist()
    ['Moscow', 'St. Petersburg', None, None]
    >>> result['arrival_city_name'].tolist()
    ['Sochi', 'Kazan', 'Moscow', 'Novosibirsk']
    """

    result_df = df.copy()

    def parse_and_extract(city_string):
        if pd.isna(city_string):
            return None
        try:
            city_dict = ast.literal_eval(str(city_string))
            if isinstance(city_dict, dict):
                return city_dict.get('en')
            else:
                return None
        except (ValueError, SyntaxError, TypeError):
            return None

    result_df['departure_city_name'] = result_df['departure_city'].apply(parse_and_extract)
    result_df['arrival_city_name'] = result_df['arrival_city'].apply(parse_and_extract)

    return result_df



def extract_coordinates(df):
    """
    Extracts longitude and latitude from coordinate columns and creates new columns.

    :param df: DataFrame with 'departure_coordinates' and 'arrival_coordinates' columns.
    :return df: The original DataFrame with four new columns added for longitude and latitude,
                or the original DataFrame if input columns are missing.

    >>> df_test = pd.DataFrame({
    ...     'departure_coordinates': ['(37.906, 55.408)', '37.261, 55.591', None],
    ...     'arrival_coordinates': ['(139.6917, 35.6895)', '-0.1278, 51.5074', '(90.0, -45.0)']
    ... })
    >>> result = extract_coordinates(df_test.copy())
    >>> result['departure_longitude'].tolist()
    [37.906, 37.261, nan]
    >>> result['departure_latitude'].tolist()
    [55.408, 55.591, nan]
    >>> result['arrival_longitude'].tolist()
    [139.6917, -0.1278, 90.0]
    >>> result['arrival_latitude'].tolist()
    [35.6895, 51.5074, -45.0]
    """
    result_df = df.copy()

    def _process_coordinate_series(coord_series):
        """Takes a Series of coordinate strings, returns lon and lat Series."""
        if coord_series is None or coord_series.empty:
            return pd.Series([np.nan] * len(result_df)), pd.Series([np.nan] * len(result_df))

        # Remove parentheses and split on comma into two parts
        coords = coord_series.astype(str).str.strip('()')
        parts = coords.str.split(',', n=1, expand=True)

        lon_str = parts[0]
        lat_str = parts[1]

        # Convert to numeric, coercing errors to NaN
        lon_numeric = pd.to_numeric(lon_str, errors='coerce')
        lat_numeric = pd.to_numeric(lat_str, errors='coerce')

        return lon_numeric, lat_numeric

    prefixes = ['departure', 'arrival']
    for prefix in prefixes:
        coord_col = f"{prefix}_coordinates"
        lon_col = f"{prefix}_longitude"
        lat_col = f"{prefix}_latitude"

        if coord_col in result_df.columns:
            lon_series, lat_series = _process_coordinate_series(result_df[coord_col])
            result_df[lon_col] = lon_series
            result_df[lat_col] = lat_series
        else:
            print("Warning: '{coord_col}' column not found.")
            result_df[lon_col] = np.nan
            result_df[lat_col] = np.nan

    return result_df