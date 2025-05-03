"""
Flight route finder functions module.

This module provides functionality to build flight networks and
find paths based on different criteria using a functional approach.
"""

import networkx as nx
from datetime import timedelta
import pandas as pd


def build_flight_graph(flights_df, departure_time):
    """
    Build a directed graph of flights that occur after the specified departure time.

    :param flights_df: DataFrame, preprocessed flight data
           departure_time: datetime, the earliest time a passenger can depart
    :return tuple: (networkx. MultiDiGraph, set of airport nodes)

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
    G = nx.MultiDiGraph()
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
            'scheduled_departure': flight['scheduled_departure'],
            'scheduled_arrival': flight['scheduled_arrival'],
            'duration_hours': flight['flight_duration_hours'],
            'price': flight['amount']
        }

        # Add edge with flight attributes
        G.add_edge(departure_airport, arrival_airport, key=flight['flight_id'], **edge_attrs)

    return G, airport_nodes


def find_all_paths(G, city_to_airports_map, departure_city, arrival_city, max_segments=3):
    """
    Find all possible paths from origin city to destination city.

    :param G: networkx.MultiDiGraph, Flight graph from build_flight_graph function
           city_to_airports_map: dict, Mapping of cities to their airport codes
           departure_city: str, origin city
           arrival_city: str, destination city
           max_segments: int, default=3, Maximum number of flight segments to consider

    :return: List of valid paths, where each path is a list of flight edges

    >>> G = nx.MultiDiGraph()
    >>> city_to_airports_map = {'Moscow': ['SVO', 'VKO'], 'St Petersburg': ['LED'], 'Kazan': ['KZN']}
    >>> _ = G.add_edge('SVO', 'LED', scheduled_departure=pd.Timestamp('2020-01-01 10:00'), scheduled_arrival=pd.Timestamp('2020-01-01 11:00'), price=100)
    >>> _ = G.add_edge('VKO', 'KZN', scheduled_departure=pd.Timestamp('2020-01-01 11:30'), scheduled_arrival=pd.Timestamp('2020-01-01 13:00'), price=200)
    >>> _ = G.add_edge('LED', 'KZN', scheduled_departure=pd.Timestamp('2020-01-01 12:00'), scheduled_arrival=pd.Timestamp('2020-01-01 13:00'), price=200)
    >>> paths = find_all_paths(G, city_to_airports_map, 'Moscow', 'Kazan')
    >>> [p for p in paths]
    [['SVO', 'LED', 'KZN'], ['VKO', 'KZN']]
    """
    if G is None:
        raise ValueError("Flight graph not built")

    if city_to_airports_map is None:
        raise ValueError("City to airports mapping not provided")

    # Get all airports for origin and destination cities
    origin_airports = city_to_airports_map.get(departure_city, [])
    dest_airports = city_to_airports_map.get(arrival_city, [])

    if not origin_airports:
        raise ValueError(f"No airports found for origin city: {departure_city}")

    if not dest_airports:
        raise ValueError(f"No airports found for destination city: {arrival_city}")

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

    :param G: networkx.MultiDiGraph, flight graph from build_flight_graph function
           origin_airport : str
           dest_airport : str
           max_segments : int, maximum number of flight segments

    :return: List of valid paths with flight details
    """
    if not G.has_node(origin_airport) or not G.has_node(dest_airport):
        return []

    # Use a modified BFS to find valid paths
    queue = [(origin_airport, [origin_airport], None)]  # (airport, path_so_far, last_arrival_time)
    valid_paths = []

    # A state is a combination of (airport, last_arrival_time)
    visited = set()

    while queue:
        current_airport, path_so_far, last_arrival_time = queue.pop(0)

        # Create a state identifier for the visited set
        state = (current_airport, last_arrival_time)
        if state in visited:
            continue
        visited.add(state)

        # If we're at destination, add to valid paths
        if current_airport == dest_airport and path_so_far[0] == origin_airport:
            valid_paths.append(path_so_far)
            continue

        # Skip if path is already at max length and we're not at destination
        if len(path_so_far) > max_segments:
            continue

        # Process MultiDiGraph edges
        for neighbor, edge_dict in G.adj[current_airport].items():
            for edge_key, flight_data in edge_dict.items():
                # Get departure and arrival times
                departure_time = flight_data.get('scheduled_departure')
                arrival_time = flight_data.get('scheduled_arrival')

                # Check if this flight departs after previous flight's arrival
                if last_arrival_time is None or departure_time > last_arrival_time:
                    new_path = path_so_far + [neighbor]
                    queue.append((neighbor, new_path, arrival_time))

    return valid_paths


def get_path_details(G, path, min_layover=timedelta(hours=1)):
    """
    Compute detailed info for a given airport‐code path, including each segment’s
    departure/arrival times & price, plus the journey’s total price, total duration,
    and number of transfers.

    :param G: networkx.MultiDiGraph
              Each edge must carry at least:
                - 'departure_time': pandas.Timestamp
                - 'arrival_time'  : pandas.Timestamp
                - 'price'         : numeric
    :param path: list of airport codes, e.g. ['A','B','C']
    :param min_layover: timedelta, minimum required time between arrival and next departure

    :return: dict with:
             - 'path'           : list of segment‐dicts
             - 'total_price'    : sum of all segment prices
             - 'total_duration' : sum of all segment durations
             - 'transfers'      : number of connections (len(path)-2)
             or None if any segment fails the time‐connection rule.

    >>> G = nx. MultiDiGraph()
    >>> _ = G.add_edge('A','B',
    ...            scheduled_departure=pd.Timestamp('2020-01-01T10:00:00'),
    ...            scheduled_arrival=pd.Timestamp('2020-01-01T12:00:00'),
    ...            price=100)
    >>> _ = G.add_edge('B','C',
    ...            scheduled_departure=pd.Timestamp('2020-01-01T13:30:00'),
    ...            scheduled_arrival=pd.Timestamp('2020-01-01T15:00:00'),
    ...            price=150)
    >>> details = get_path_details(G, ['A','B','C'])
    >>> details['total_price']
    250
    >>> details['transfers']
    1
    >>> # invalid layover (<1h) returns None
    >>> _ = G.add_edge('B','D',
    ...            scheduled_departure=pd.Timestamp('2020-01-01T12:30:00'),
    ...            scheduled_arrival=pd.Timestamp('2020-01-01T13:30:00'),
    ...            price=200)
    >>> get_path_details(G, ['A','B','D']) is None
    True
    """
    path_segments = []
    total_price    = 0
    total_duration = timedelta(0)
    last_arrival   = None

    # walk through each hop in the given airport list
    for origin, dest in zip(path, path[1:]):
        data = G.get_edge_data(origin, dest)
        if data is None:
            # no flights on this leg
            return None

        # unpack candidates (MultiDiGraph vs DiGraph)
        if isinstance(G, nx.MultiDiGraph):
            candidates = [edge for _, edge in data.items()]
        else:
            candidates = [data]

        # pick first flight satisfying layover
        chosen = None
        for flight in candidates:
            dep = flight.get('scheduled_departure')
            # first leg has no layover constraint
            if last_arrival is None or dep >= last_arrival + min_layover:
                chosen = flight
                break

        if chosen is None:
            # no valid connecting flight
            return None

        # record this segment
        arr = chosen.get('scheduled_arrival')
        dur = arr - dep
        path_segments.append({
            'from':      origin,
            'to':        dest,
            'departure': dep,
            'arrival':   arr,
            'price':     chosen['price']
        })
        total_price    += chosen['price']
        total_duration += dur
        last_arrival    = arr

    return {
        'path':           path_segments,
        'total_price':    total_price,
        'total_duration': total_duration,
        'transfers':      len(path) - 2
    }


def select_best_routes(all_path_details):
    """
    Select three optimal routes from a list of path detail dicts:
      - 'cheapest': the route with minimal total_price
      - 'fastest': the route with minimal total_duration
      - 'least_transfers': the route with minimal transfers

    :param all_path_details: list of dicts, each dict must contain keys:
                             'path', 'total_price', 'total_duration', 'transfers'
    :return: dict with keys 'cheapest', 'fastest', 'least_transfers',
             each mapping to one of the input dicts. Returns None if input is empty.

    >>> paths = [
    ...     {'path': [], 'total_price': 100, 'total_duration': timedelta(hours=2),    'transfers': 1},
    ...     {'path': [], 'total_price': 150, 'total_duration': timedelta(hours=1, minutes=30), 'transfers': 0},
    ...     {'path': [], 'total_price':  80, 'total_duration': timedelta(hours=3),    'transfers': 2}
    ... ]
    >>> best = select_best_routes(paths)
    >>> best['cheapest']['total_price']
    80
    >>> best['fastest']['total_duration']
    datetime.timedelta(seconds=5400)
    >>> best['least_transfers']['transfers']
    0
    """
    if not all_path_details:
        return None

    # cheapest by price
    cheapest = min(all_path_details, key=lambda x: x['total_price'])
    # fastest by duration
    fastest = min(all_path_details, key=lambda x: x['total_duration'])
    # fewest transfers
    least_transfers = min(all_path_details, key=lambda x: x['transfers'])

    return {
        'cheapest': cheapest,
        'fastest': fastest,
        'least_transfers': least_transfers
    }