"""
Data preprocessing module for flight route finder.

This module handles the preprocessing of flight data tables including:
- Extracting city names from arrival and departure cities
- Extracts longitude and latitude from coordinate columns
- Converting times to datetime format
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


def process_time_columns(df):
    """
    Converts time-related string columns to datetime format and computes flight duration in hours.

    :param df: DataFrame with 'scheduled_departure' and 'scheduled_arrival' columns
    :return df: Updated DataFrame with:
                - datetime-converted departure and arrival columns
                - flight_duration_hours (as float)

    >>> df_test = pd.DataFrame({
    ...     'scheduled_departure': ['2017-09-02 08:55:00+03', '2017-09-02 12:00:00+03'],
    ...     'scheduled_arrival': ['2017-09-02 10:55:00+03', '2017-09-02 14:15:00+03']
    ... })
    >>> result = process_time_columns(df_test.copy())
    >>> result['flight_duration_hours'].round(2).tolist()
    [2.0, 2.25]
    """

    result_df = df.copy()

    # 转换时间为 datetime 类型
    result_df['scheduled_departure'] = pd.to_datetime(result_df['scheduled_departure'], errors='coerce')
    result_df['scheduled_arrival'] = pd.to_datetime(result_df['scheduled_arrival'], errors='coerce')

    # 计算飞行时长（单位：小时）
    result_df['flight_duration_hours'] = (
        (result_df['scheduled_arrival'] - result_df['scheduled_departure']).dt.total_seconds() / 3600
    )

    return result_df


def fill_missing_amount_by_route_type(df):
    """
    Fills missing 'amount' values based on route type (core vs. niche):
    - For core routes (those with flight_id count >= average), missing values are filled using the route's mean amount.
    - For niche routes (those with flight_id count < average), rows with missing amount are dropped.

    A route is defined by the combination of 'departure_airport' and 'arrival_airport'.
    Core routes are considered frequent routes, while niche routes are considered infrequent based on the number of unique flight IDs.

    :param df: DataFrame containing at least the following columns:
               - 'departure_airport'
               - 'arrival_airport'
               - 'flight_id'
               - 'amount'
    :return df: A cleaned DataFrame with all missing 'amount' values handled.

    >>> import numpy as np
    >>> df_test = pd.DataFrame({
    ...     'departure_airport': ['A', 'A', 'A', 'B', 'B', 'C'],
    ...     'arrival_airport':   ['X', 'X', 'X', 'Y', 'Y', 'Z'],
    ...     'flight_id':         [1, 2, 3, 4, 5, 6],
    ...     'amount':            [100, 200, np.nan, 300, 400, np.nan]
    ... })
    >>> cleaned = fill_missing_amount_by_route_type(df_test.copy())
    >>> cleaned = cleaned.sort_values(by='flight_id').reset_index(drop=True)
    >>> cleaned = cleaned.sort_index()
    >>> cleaned['amount'].tolist()
    [100.0, 200.0, 150.0, 300.0, 400.0]
    """

    result_df = df.copy()

    # Define route as the combination of departure and arrival airports
    result_df['route'] = result_df['departure_airport'] + ' → ' + result_df['arrival_airport']

    # Count unique flight IDs per route
    route_flight_counts = result_df.groupby('route')['flight_id'].nunique()

    # Use average flight count per route as the threshold to classify routes
    avg_flight_count = route_flight_counts.mean()

    # Classify routes as 'core' or 'niche'
    route_type_map = route_flight_counts.apply(lambda x: 'core' if x >= avg_flight_count else 'niche')
    result_df['route_type'] = result_df['route'].map(route_type_map)

    # Fill missing amounts in core routes with route mean
    route_mean_amount = result_df.groupby('route')['amount'].transform(
        lambda x: pd.to_numeric(x, errors='coerce').mean()
    )
    result_df['amount'] = result_df.apply(
        lambda row: route_mean_amount[row.name]
        if pd.isna(row['amount']) and row['route_type'] == 'core'
        else row['amount'],
        axis=1
    )

    # Drop rows from niche routes where amount is still missing
    result_df = result_df[~((result_df['route_type'] == 'niche') & (result_df['amount'].isna()))]

    return result_df


def city_to_airports_map(df):
    """
    Builds a mapping from English city names to all associated airport codes.

    :param df: DataFrame with columns:
               - 'departure_city_name', 'departure_airport'
               - 'arrival_city_name', 'arrival_airport'
    :return: Dictionary {city_name: [airport1, airport2, ...]}

    >>> df_test = pd.DataFrame({
    ...     'departure_city_name': ['Moscow', 'Beijing'],
    ...     'departure_airport': ['A', 'B'],
    ...     'arrival_city_name': ['Beijing', 'Moscow'],
    ...     'arrival_airport': ['C', 'D']
    ... })
    >>> mapping = city_to_airports_map(df_test)
    >>> sorted(mapping['Moscow'])
    ['A', 'D']
    >>> sorted(mapping['Beijing'])
    ['B', 'C']
    """
    # Select relevant columns for departure/arrival city-airport pairs and drop any rows with missing values
    dep = df[['departure_city_name', 'departure_airport']].dropna()
    arr = df[['arrival_city_name', 'arrival_airport']].dropna()

    # Rename both DataFrames to have the same column names for easy merging
    dep.columns = ['city', 'airport']
    arr.columns = ['city', 'airport']

    # Combine departure and arrival records into a single DataFrame and remove duplicates
    combined = pd.concat([dep, arr], ignore_index=True).drop_duplicates()

    # Group by city and collect all unique associated airports, sorted alphabetically
    mapping = (
        combined.groupby('city')['airport']
        .unique()
        .apply(sorted)
        .to_dict()
    )

    return mapping

