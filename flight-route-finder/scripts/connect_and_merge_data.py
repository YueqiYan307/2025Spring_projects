"""
Flight ticket summary extraction module.

This module loads flight schedule and fare data from a SQLite database, merges
them into a consolidated DataFrame, and writes the result out as a CSV file.
"""

import sqlite3
import pandas as pd
import os
import argparse
import sys

script_dir = os.path.dirname(__file__)
db_file = os.path.abspath(os.path.join(script_dir, os.pardir, 'data', 'travel.sqlite'))
output_dir = os.path.abspath(os.path.join(script_dir, os.pardir, 'data', 'processed'))
output_csv_file = os.path.join(output_dir, 'flight_ticket_summary.csv')

# --- SQL query ---
sql_select_summary = """
WITH table1 AS (
    SELECT
        f.flight_id,
        f.flight_no,
        f.scheduled_departure,
        f.scheduled_arrival,
        f.departure_airport,
        dep_air.city AS departure_city,
        dep_air.coordinates AS departure_coordinates,
        f.arrival_airport,
        arr_air.city AS arrival_city,
        arr_air.coordinates AS arrival_coordinates
    FROM
        flights AS f
    LEFT JOIN
        airports_data AS dep_air ON f.departure_airport = dep_air.airport_code
    LEFT JOIN
        airports_data AS arr_air ON f.arrival_airport = arr_air.airport_code
),
table2 AS (
    SELECT
        flight_id,
        fare_conditions,
        amount,
        COUNT(*) AS ticket_count
    FROM
        ticket_flights
    GROUP BY
        flight_id, fare_conditions, amount
)
SELECT
    t1.*,
    t2.fare_conditions,
    t2.amount,
    t2.ticket_count
FROM
    table1 AS t1
LEFT JOIN
    table2 AS t2
ON
    t1.flight_id = t2.flight_id;
"""

def load_data(db_file: str, sql: str) -> pd.DataFrame:
    if not os.path.exists(db_file):
        print(f"Error: Database file '{db_file}' not found.")
        sys.exit(1)
    with sqlite3.connect(db_file) as conn:
        df = pd.read_sql_query(sql, conn)
    print(f"Successfully loaded {len(df)} rows.")
    return df

def save_data(df: pd.DataFrame, output_file: str) -> None:
    output_dir = os.path.dirname(output_file)
    print(f"Ensuring output directory exists: '{output_dir}'")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Saving DataFrame to CSV file: '{output_file}'...")
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Data successfully saved to '{output_file}'.")

def main():
    parser = argparse.ArgumentParser(description="Load flight ticket summary from SQLite and save to CSV.")
    parser.add_argument("--db-file", default=db_file, help="Path to the SQLite database file.")
    parser.add_argument("--output-file", default=output_csv_file, help="Path to the output CSV file.")
    args = parser.parse_args()

    df = load_data(args.db_file, sql_select_summary)
    save_data(df, args.output_file)

if __name__ == "__main__":
    main()