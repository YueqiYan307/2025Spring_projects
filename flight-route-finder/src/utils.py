# user应该输入四个变量，出发地目的地，目前所在地，目前所在地的时间
def standardize_user_time(time_str: str, user_tz_str: str):
    """
    Parses a user-provided time string and timezone, returning a UTC datetime object.

    :param time_str: The user's input time string (e.g., "2024-08-15 10:00").
           user_tz_str: The IANA timezone name for the user's location
                     (e.g., "Moscow").

    :return: A timezone-aware datetime object representing the input time in UTC.
    """


def load_city_timezone_map(filepath):
    """
    Loads the city name to timezone identifier map from a JSON file.
    Caches the result in memory after the first successful load.

    :param: filepath: The path to the JSON file containing the mapping.

    :return: A dictionary mapping city names (str) to timezone identifiers (str).
    """


def get_timezone_for_city(city_name: str):
    """
    Looks up the IANA timezone identifier for a given city name using a pre-built map.
    Performs a case-insensitive lookup after loading the map (if not already cached).

    :param: city_name: The city name to lookup.

    :return: The IANA timezone string (e.g., "Europe/Moscow") if the city is found.
    """