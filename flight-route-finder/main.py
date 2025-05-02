# src/main.py

import pandas as pd
import pytz
from datetime import datetime
import dateutil.parser
import os

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
    origin = input("Departure city (English): ").strip()
    destination = input("Arrival city (English): ").strip()
    user_time = input('Current time (UTC+03), e.g. "2025-05-02 15:30": ').strip()
    current_city = input(f"Current city (default = {origin}): ").strip() or origin

    default_csv = "data/processed/flight_ticket_summary.csv"
    data_file = input(f"Path to flight_ticket_summary.csv [default: {default_csv}]: ").strip() or default_csv

    # 检查文件是否存在
    if not os.path.exists(data_file):
        print(f"错误: 找不到文件 '{data_file}'")
        return

    seg_input = input("Max number of flight segments (hops) [default: 3]: ").strip()
    try:
        max_segments = int(seg_input) if seg_input else 3
    except ValueError:
        print("Invalid number for max segments, using default 3.")
        max_segments = 3

    print("\n正在处理数据，请稍候...\n")

    try:
        # 2) 读取预处理后的 CSV
        df = pd.read_csv(data_file)

        # 3) 数据预处理流水线
        df = extract_city_names(df)
        df = extract_coordinates(df)
        df = process_time_columns(df)
        df = fill_missing_amount_by_route_type(df)

        # 4) 构建城市↔机场映射
        city_map = city_to_airports_map(df)

        # 检查输入城市是否存在于数据中
        if origin not in city_map:
            print(f"错误: 出发城市 '{origin}' 在数据中找不到。")
            print(f"可用的城市有: {', '.join(sorted(city_map.keys())[:10])}等")
            return

        if destination not in city_map:
            print(f"错误: 目的地城市 '{destination}' 在数据中找不到。")
            print(f"可用的城市有: {', '.join(sorted(city_map.keys())[:10])}等")
            return

        # 5) 解析用户输入时间（假设已是 UTC+03）
        try:
            dep_time = dateutil.parser.parse(user_time)
            if dep_time.tzinfo is None:
                dep_time = DATASET_TZ.localize(dep_time)
            else:
                dep_time = dep_time.astimezone(DATASET_TZ)
        except Exception as e:
            print(f"错误: 无法解析时间 '{user_time}'. 请使用格式 'YYYY-MM-DD HH:MM'")
            return

        print(f"正在查找从 {origin} 到 {destination} 的航班路线...")

        # 6) 构建航班图（只保留起飞时间 >= dep_time 的航班）
        G, airport_nodes = build_flight_graph(df, dep_time)

        if not G.number_of_edges():
            print(f"没有找到任何在 {dep_time} 之后起飞的航班。")
            return

        # 7) 查找所有可行机场路径
        paths = find_all_paths(
            G,
            city_to_airports_map=city_map,
            departure_city=origin,
            arrival_city=destination,
            max_segments=max_segments
        )

        if not paths:
            print(f"\n没有找到从 {origin} 到 {destination} 的路线 (最多 {max_segments} 段航班)。")
            print("尝试增加最大航段数或选择不同的城市。")
            return

        print(f"找到 {len(paths)} 条可能的路径，正在分析最佳选择...")

        # 8) 计算每条路径详情并过滤无效项
        details = [get_path_details(G, p) for p in paths]
        details = [d for d in details if d is not None]

        if not details:
            print("\n没有找到满足中转时间要求的有效路线。")
            return

        # 9) 选出三种最优路线
        best = select_best_routes(details)
        if best is None:
            print("\n没有找到有效路线。")
            return

        # 10) 打印结果
        print("\n=== 推荐路线 ===\n")

        # 打印最便宜路线
        print("最便宜路线:")
        print(f"  总价: {best['cheapest']['total_price']} 卢布")
        print(f"  总时长: {best['cheapest']['total_duration']}")
        print(f"  中转次数: {best['cheapest']['transfers']}")
        print("  路线详情:")
        for seg in best['cheapest']['path']:
            print(
                f"    {seg['from']} → {seg['to']}: 出发 {seg['departure']}, 到达 {seg['arrival']}, 价格 {seg['price']} 卢布")

        print("\n最快路线:")
        print(f"  总价: {best['fastest']['total_price']} 卢布")
        print(f"  总时长: {best['fastest']['total_duration']}")
        print(f"  中转次数: {best['fastest']['transfers']}")
        print("  路线详情:")
        for seg in best['fastest']['path']:
            print(
                f"    {seg['from']} → {seg['to']}: 出发 {seg['departure']}, 到达 {seg['arrival']}, 价格 {seg['price']} 卢布")

        print("\n最少中转路线:")
        print(f"  总价: {best['least_transfers']['total_price']} 卢布")
        print(f"  总时长: {best['least_transfers']['total_duration']}")
        print(f"  中转次数: {best['least_transfers']['transfers']}")
        print("  路线详情:")
        for seg in best['least_transfers']['path']:
            print(
                f"    {seg['from']} → {seg['to']}: 出发 {seg['departure']}, 到达 {seg['arrival']}, 价格 {seg['price']} 卢布")

    except Exception as e:
        print(f"程序运行出错: {e}")


if __name__ == "__main__":
    main()