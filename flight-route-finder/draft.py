import pandas as pd
import pytz
from datetime import datetime
import dateutil.parser

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

# 项目数据和用户输入时间都假定为莫斯科时区（UTC+03）
DATASET_TZ = pytz.timezone("Europe/Moscow")

def main():
    # 1) 交互式获取输入
    origin      = input("Departure city (English): ").strip()
    destination = input("Arrival city (English): ").strip()
    user_time   = input('Current time (UTC+03), e.g. "2025-05-02 15:30": ').strip()
    current_city = input(f"Current city (default = {origin}): ").strip() or origin

    default_csv = "../data/processed/flight_ticket_summary.csv"
    data_file = input(f"Path to flight_ticket_summary.csv [default: {default_csv}]: ").strip() or default_csv

    seg_input = input("Max number of flight segments (hops) [default: 3]: ").strip()
    try:
        max_segments = int(seg_input) if seg_input else 3
    except ValueError:
        print("Invalid number for max segments, using default 3.")
        max_segments = 3

    # 2) 读取预处理后的 CSV
    df = pd.read_csv(data_file)

    # 3) 数据预处理流水线
    df = extract_city_names(df)
    df = extract_coordinates(df)
    df = process_time_columns(df)
    df = fill_missing_amount_by_route_type(df)

    # 4) 构建城市↔机场映射
    city_map = city_to_airports_map(df)

    # 5) 解析用户输入时间（假设已是 UTC+03）
    dep_time = dateutil.parser.parse(user_time)
    if dep_time.tzinfo is None:
        dep_time = DATASET_TZ.localize(dep_time)
    else:
        dep_time = dep_time.astimezone(DATASET_TZ)

    # 6) 构建航班图（只保留起飞时间 >= dep_time 的航班）
    G, _ = build_flight_graph(df, dep_time)

    # 7) 查找所有可行机场路径
    paths = find_all_paths(
        G,
        city_to_airports_map=city_map,
        departure_city=origin,
        arrival_city=destination,
        max_segments=max_segments
    )

    # 8) 计算每条路径详情并过滤无效项
    details = [get_path_details(G, p) for p in paths]
    details = [d for d in details if d is not None]

    # 9) 选出三种最优路线
    best = select_best_routes(details)
    if best is None:
        print("\nNo valid routes found.")
        return

    # 10) 打印结果
    print("\n=== Recommended Routes ===\n")
    print("Cheapest:         ", best['cheapest'])
    print("Fastest:          ", best['fastest'])
    print("Fewest Transfers: ", best['least_transfers'])

if __name__ == "__main__":
    main() 