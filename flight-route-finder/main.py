"""
Flight Route Finder

A command-line application to find flight routes between cities using preprocessed flight data.
This module serves as the entry point for the flight route finder application.
"""

import sys
import argparse
import pandas as pd
from datetime import datetime
import re
import difflib

from src.preprocessing import (
    extract_city_names,
    extract_coordinates,
    process_time_columns,
    fill_missing_amount_by_route_type,
    city_to_airports_map
)
from src.flight_functions import (
    build_flight_graph,
    find_all_paths,
    get_path_details,
    select_best_routes
)


def match_city(input_city, cities):
    """Case-insensitive and fuzzy match of input_city to cities list."""
    # Normalize: lowercase and remove non-letter characters
    input_norm = re.sub(r'[^a-z]', '', input_city.strip().lower())
    # Map normalized city keys to original names
    city_map = {re.sub(r'[^a-z]', '', city.lower()): city for city in cities}
    # Exact normalized match
    if input_norm in city_map:
        return city_map[input_norm]
    # Fuzzy match on normalized keys
    close = difflib.get_close_matches(input_norm, city_map.keys(), n=1, cutoff=0.7)
    if close:
        return city_map[close[0]]
    return None


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Flight Route Finder')
    parser.add_argument('--data', type=str, default='data/processed/flight_ticket_summary.csv',
                        help='Path to the flight data file (CSV)')
    return parser.parse_args()


def load_and_preprocess_data(file_path):
    """Load flight data from CSV and preprocess it."""
    try:
        print(f"Loading flight data from {file_path}...")
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

    print("Preprocessing flight data...")

    # Apply preprocessing functions
    df = extract_city_names(df)
    df = extract_coordinates(df)
    df = process_time_columns(df)
    df = fill_missing_amount_by_route_type(df)

    # Create city to airports mapping
    city_airports = city_to_airports_map(df)

    print(f"Data preprocessing complete. {len(df)} flights available.")
    return df, city_airports


def get_cities_list(city_airports):
    """Return sorted list of available cities."""
    return sorted(city_airports.keys())


def get_user_input(cities):
    """Get user input for departure and arrival cities and departure date/time."""
    while True:
        try:
            # Get departure city
            raw_dep = input("\nDeparture city (e.g., Moscow, Saint Petersburg, Novosibirsk): ")
            departure_city = match_city(raw_dep, cities)
            if not departure_city:
                print(f"Error: '{raw_dep}' is not in the available cities list.")
                continue

            # Get arrival city
            raw_arr = input("Arrival city (e.g., Moscow, Saint Petersburg, Novosibirsk): ")
            arrival_city = match_city(raw_arr, cities)
            if not arrival_city:
                print(f"Error: '{raw_arr}' is not in the available cities list.")
                continue

            if departure_city == arrival_city:
                print("Error: Departure and arrival cities cannot be the same.")
                continue

            # Get departure date and time
            date_str = input("Departure date (YYYY-MM-DD, default: today; between 2017-06-16 and 2017-09-14): ")
            time_str = input("Departure time (HH:MM, default: now): ")

            # Use current date/time as default
            now = datetime.now()
            if not date_str:
                date_str = now.strftime("%Y-%m-%d")
            if not time_str:
                time_str = now.strftime("%H:%M")

            # Parse date and time
            try:
                departure_time = pd.Timestamp(f"{date_str} {time_str}").tz_localize('UTC+03:00')
            except:
                print("Invalid date or time format. Please use YYYY-MM-DD for date and HH:MM for time.")
                continue

            # All inputs are valid
            return departure_city, arrival_city, departure_time

        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            sys.exit(0)


def display_route(route_type, route_details):
    """Display a single route with all details."""
    print(f"\n{route_type.upper()} ROUTE:")
    print(f"  Total price: {route_details['total_price']:.2f}")
    # Format duration into hours and minutes
    td = route_details['total_duration']
    total_seconds = td.total_seconds()
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    print(f"  Total duration: {hours}h{minutes}min")
    print(f"  Transfers: {route_details['transfers']}")

    print("\n  Flight segments:")
    for i, segment in enumerate(route_details['path'], 1):
        print(f"  {i}. {segment['from']} → {segment['to']}")
        dep = segment['departure'].strftime('%Y-%m-%d %H:%M')
        arr = segment['arrival'].strftime('%Y-%m-%d %H:%M')
        print(f"     Departure: {dep}")
        print(f"     Arrival: {arr}")
        print(f"     Price: {segment['price']:.2f}")


def display_results(best_routes):
    """Display the best routes found."""
    if not best_routes:
        print("\nNo routes found. Try different cities or departure time.")
        return

    for route_type, route_details in best_routes.items():
        display_route(route_type, route_details)


def main():
    """Main function for the flight route finder application."""
    print("=" * 60)
    print("             FLIGHT ROUTE FINDER")
    print("=" * 60)

    # Parse command line arguments
    args = parse_arguments()

    # Load and preprocess flight data
    df, city_airports = load_and_preprocess_data(args.data)

    while True:
        # Get list of cities
        cities = get_cities_list(city_airports)

        # Get user input
        departure_city, arrival_city, departure_time = get_user_input(cities)

        print(f"\nSearching for routes from {departure_city} to {arrival_city} on {departure_time}...")

        # Build flight graph
        flight_graph, airport_nodes = build_flight_graph(df, departure_time)

        # Find all possible paths
        try:
            paths = find_all_paths(flight_graph, city_airports, departure_city, arrival_city)

            # Get detailed information for each path
            path_details = []
            for path in paths:
                details = get_path_details(flight_graph, path)
                if details:
                    path_details.append(details)

            # Select the best routes
            best_routes = select_best_routes(path_details)

            # Display results
            display_results(best_routes)

        except ValueError as e:
            print(f"Error: {e}")

        # Ask if user wants to search again
        if input("\nSearch for another route? (y/n): ").lower() != 'y':
            break

    print("Thank you for using Flight Route Finder!")


if __name__ == "__main__":
    main()