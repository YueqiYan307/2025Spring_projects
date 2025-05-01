import sqlite3
import pandas as pd
import os

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

try:
    # 1. Check if the database file exists
    if not os.path.exists(db_file):
        print(f"Error: Database file '{db_file}' not found.")
    else:
        # 2. Ensure the output directory exists
        print(f"Ensuring output directory exists: '{output_dir}'")
        os.makedirs(output_dir, exist_ok=True)

        # 3. Connect to the database using 'with' to ensure automatic closure
        with sqlite3.connect(db_file) as connection:

            # 4. Execute the SQL query using pandas and load into a DataFrame
            print("Executing SQL query and loading results into Pandas DataFrame...")
            df = pd.read_sql_query(sql_select_summary, connection)
            print(f"Successfully loaded {len(df)} rows.")

            # 5. Save the DataFrame to a CSV file
            print(f"Saving DataFrame to CSV file: '{output_csv_file}'...")
            df.to_csv(output_csv_file, index=False, encoding='utf-8')
            print(f"Data successfully saved to '{output_csv_file}'.")

except sqlite3.Error as e:
    print(f"SQLite error during SQL execution or database connection: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

print("Script execution completed.")