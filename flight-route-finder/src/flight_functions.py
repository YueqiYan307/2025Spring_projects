"""
Flight route finder functions module.

This module provides functionality to build flight networks and
find paths based on different criteria using a functional approach.
"""

import networkx as nx
from datetime import datetime, timedelta
import pandas as pd
import pytz


def build_flight_graph(flights_df, departure_time):
    """
    Build a directed graph of flights that occur after the specified departure time.

    :param flights_df: DataFrame, preprocessed flight data
           departure_time : datetime, the earliest time a passenger can depart
    :return tuple: (networkx.DiGraph, set of airport nodes)

    >>> data = {
    ...     'flight_id': ['11', '12'],
    ...     'flight_no': ['PG1234', 'PG5678'],
    ...     'departure_airport': ['SVO', 'LED'],
    ...     'arrival_airport': ['LED', 'SVO'],
    ...     'scheduled_departure': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 14:00:00')],
    ...     'scheduled_arrival':   [pd.Timestamp('2023-01-01 12:00:00'), pd.Timestamp('2023-01-01 16:00:00')],
    ...     'flight_duration_hours': [2.0, 2.25],
    ...     'amount': [100, 120]
    ... }
    >>> df = pd.DataFrame(data)
    >>> departure_time = pd.Timestamp('2023-01-01 08:00:00')
    >>> G, airport_nodes = build_flight_graph(df, departure_time)
    >>> len(G.edges())
    2
    >>> 'SVO' in airport_nodes
    True
    """
    # Create a new directed graph
    G = nx.DiGraph()
    airport_nodes = set()

    # Filter flights by scheduled departure time
    if not isinstance(departure_time, pd.Timestamp):
        departure_time = pd.Timestamp(departure_time)
    valid_flights = flights_df[flights_df['scheduled_departure'] >= departure_time]

    # Create edges for each valid flight
    for _, flight in valid_flights.iterrows():
        departure_airport = flight['departure_airport']
        arrival_airport = flight['arrival_airport']

        # Store airport nodes for later reference
        airport_nodes.add(departure_airport)
        airport_nodes.add(arrival_airport)

        # Add nodes if they don't exist
        if not G.has_node(departure_airport):
            G.add_node(departure_airport, type='airport')

        if not G.has_node(arrival_airport):
            G.add_node(arrival_airport, type='airport')

        # Extract attributes for this flight edge
        edge_attrs = {
            'flight_id': flight['flight_id'],
            'flight_number': flight['flight_no'],
            'scheduled_arrival': flight['scheduled_arrival'],
            'duration_hours': flight['flight_duration_hours'],
            'price': flight['amount']
        }

        # Add edge with flight attributes
        G.add_edge(departure_airport, arrival_airport, **edge_attrs)

    return G, airport_nodes


def find_all_paths(G, city_to_airports_map, departure, arrival, max_segments=3):
    """
    Find all possible paths from origin city to destination city.

    :param G: networkx.DiGraph, Flight graph
           city_to_airports_map: dict, Mapping of cities to their airport codes
           departure: str, origin city
           arrival: str, destination city
           max_segments: int, default=3, Maximum number of flight segments to consider

    :return: List of valid paths, where each path is a list of flight edges

    >>> G = nx.DiGraph()
    >>> city_to_airports_map = {'Moscow': ['SVO', 'VKO'], 'St Petersburg': ['LED'], 'Kazan': ['KZN']}
    >>> G.add_edge('SVO', 'LED', departure=pd.Timestamp('2020-01-01 10:00'), arrival=pd.Timestamp('2020-01-01 11:00'), price=100)
    >>> G.add_edge('VKO', 'KZN', departure=pd.Timestamp('2020-01-01 11:30'), arrival=pd.Timestamp('2020-01-01 13:00'), price=200)
    >>> G.add_edge('LED', 'KZN', departure=pd.Timestamp('2020-01-01 12:00'), arrival=pd.Timestamp('2020-01-01 13:00'), price=200)
    >>> paths = find_all_paths(G, city_to_airports_map, 'Moscow', 'Kazan')
    >>> [p for p in paths]
    [['VKO', 'KZN'], ['SVO', 'LED', 'KZN']]
    """
    if G is None:
        raise ValueError("Flight graph not built")

    if city_to_airports_map is None:
        raise ValueError("City to airports mapping not provided")

    # Get all airports for origin and destination cities
    origin_airports = city_to_airports_map.get(departure, [])
    dest_airports = city_to_airports_map.get(arrival, [])

    if not origin_airports:
        raise ValueError(f"No airports found for origin city: {departure}")

    if not dest_airports:
        raise ValueError(f"No airports found for destination city: {arrival}")

    all_paths = []

    # For each origin-destination airport pair
    for origin_airport in origin_airports:
        for dest_airport in dest_airports:
            # Skip if same airport
            if origin_airport == dest_airport:
                continue

            # Find paths with limited segments
            for path in _find_time_aware_paths(G, origin_airport, dest_airport, max_segments):
                all_paths.append(path)

    return all_paths


def _find_time_aware_paths(G, origin_airport, dest_airport, max_segments):
    """
    Find all time-constrained paths between two airports.
    This respects the temporal sequence of flights (connection causality).

    :param G : networkx.DiGraph
           Flight graph
           origin_airport : str
           Origin airport code
           dest_airport : str
           Destination airport code
           max_segments : int
           Maximum number of flight segments

    :return: List of valid paths with flight details
    """
    if not G.has_node(origin_airport) or not G.has_node(dest_airport):
        return []

    # Use a modified BFS to find valid paths
    queue = [(origin_airport, [], None)]  # (airport, path_so_far, last_arrival_time)
    valid_paths = []

    while queue:
        current_airport, path_so_far, last_arrival_time = queue.pop(0)

        # Skip if path is too long
        if len(path_so_far) >= max_segments:
            # If we're at destination, add to valid paths
            if current_airport == dest_airport and path_so_far:
                valid_paths.append(path_so_far)
            continue

        # Explore next possible flights
        for neighbor in G.successors(current_airport):
            # Get all edges (flights) between current and neighbor
            edges = G.get_edge_data(current_airport, neighbor)

            # There might be multiple flights between the same airports
            for flight_id, flight_data in edges.items():
                departure_time = flight_data['departure_time']

                # Check if this flight departs after previous flight's arrival
                # (respecting connection time)
                if last_arrival_time is None or departure_time > last_arrival_time:
                    new_path = path_so_far + [(current_airport, neighbor, flight_data)]

                    # If reached destination, add to valid paths
                    if neighbor == dest_airport:
                        valid_paths.append(new_path)
                    else:
                        # Continue searching from this airport
                        queue.append((neighbor, new_path, flight_data['arrival_time']))

    return valid_paths
