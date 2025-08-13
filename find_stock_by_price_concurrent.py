#!/usr/bin/env python3
"""
查找指定历史价格的股票 - 并发版本
支持按时间区间查找，支持收盘价、最高价、最低价
使用多进程和多线程提高查找性能
"""
import argparse
import pandas as pd
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
import logging
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import os
from functools import partial
import time

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_single_stock_data(csv_file: Path) -> Optional[Tuple[str, pd.DataFrame]]:
    """加载单个股票数据文件"""
    try:
        stock_code = csv_file.stem
        df = pd.read_csv(csv_file, parse_dates=['date'])
        if not df.empty:
            return (stock_code, df)
    except Exception as e:
        logger.warning(f"读取文件 {csv_file} 失败: {e}")
    return None

def load_stock_data_concurrent(data_dir: Path, max_workers: Optional[int] = None) -> List[Tuple[str, pd.DataFrame]]:
    """并发加载所有股票数据"""
    if not data_dir.exists():
        logger.error(f"数据目录不存在: {data_dir}")
        return []
    
    csv_files = list(data_dir.glob("**/*.csv"))
    logger.info(f"找到 {len(csv_files)} 个CSV文件")
    
    if not csv_files:
        return []
    
    # 确定并发数量
    if max_workers is None:
        max_workers = min(mp.cpu_count(), len(csv_files))
    
    stock_data = []
    
    # 使用进程池并发读取文件
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_file = {executor.submit(load_single_stock_data, csv_file): csv_file 
                         for csv_file in csv_files}
        
        # 收集结果
        for future in as_completed(future_to_file):
            csv_file = future_to_file[future]
            try:
                result = future.result()
                if result is not None:
                    stock_data.append(result)
            except Exception as e:
                logger.warning(f"处理文件 {csv_file} 时出错: {e}")
    
    logger.info(f"成功加载 {len(stock_data)} 只股票的数据")
    return stock_data

def find_by_price_single_stock(
    stock_item: Tuple[str, pd.DataFrame],
    target_price: float,
    price_type: str,
    start_date: Optional[str],
    end_date: Optional[str],
    tolerance: float
) -> List[Tuple[str, float, str]]:
    """在单个股票数据中查找指定价格"""
    stock_code, df = stock_item
    results = []
    
    if df.empty:
        return results
    
    min_price = target_price - tolerance
    max_price = target_price + tolerance
    
    # 按日期筛选
    if start_date or end_date:
        df_filtered = df.copy()
        
        # 如果只指定了开始时间，将开始时间作为结束时间
        if start_date and end_date is None:
            end_date = start_date
        # 如果只指定了结束时间，将结束时间作为开始时间
        elif end_date and start_date is None:
            start_date = end_date
            
        if start_date:
            start_dt = pd.to_datetime(start_date)
            df_filtered = df_filtered[df_filtered['date'] >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            df_filtered = df_filtered[df_filtered['date'] <= end_dt]
        
        if df_filtered.empty:  # type: ignore
            return results
    else:
        df_filtered = df
    
    # 查找所有符合条件的价格
    mask = (df_filtered[price_type] >= min_price) & (df_filtered[price_type] <= max_price)
    matching_rows = df_filtered[mask]
    
    for _, row in matching_rows.iterrows():  # type: ignore
        results.append((stock_code, row[price_type], pd.to_datetime(row['date']).strftime('%Y-%m-%d')))
    
    return results

def find_by_price_concurrent(
    stock_data: List[Tuple[str, pd.DataFrame]], 
    target_price: float,
    price_type: str = 'close',
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    tolerance: float = 0.01,
    max_workers: Optional[int] = None
) -> List[Tuple[str, float, str]]:
    """
    并发查找历史价格等于指定价格的股票
    
    Args:
        stock_data: 股票数据列表
        target_price: 目标价格
        price_type: 价格类型 ('close', 'high', 'low')
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        tolerance: 价格容差
        max_workers: 最大并发数
        
    Returns:
        符合条件的股票列表 (代码, 价格, 日期)
    """
    # 验证价格类型
    valid_price_types = ['close', 'high', 'low']
    if price_type not in valid_price_types:
        raise ValueError(f"价格类型必须是: {', '.join(valid_price_types)}")
    
    if not stock_data:
        return []
    
    # 确定并发数量
    if max_workers is None:
        max_workers = min(mp.cpu_count(), len(stock_data))
    
    logger.info(f"使用 {max_workers} 个并发进程进行查找")
    
    # 创建偏函数，固定其他参数
    search_func = partial(
        find_by_price_single_stock,
        target_price=target_price,
        price_type=price_type,
        start_date=start_date,
        end_date=end_date,
        tolerance=tolerance
    )
    
    all_results = []
    
    # 使用进程池并发查找
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_stock = {executor.submit(search_func, stock_item): stock_item[0] 
                          for stock_item in stock_data}
        
        # 收集结果
        for future in as_completed(future_to_stock):
            stock_code = future_to_stock[future]
            try:
                result = future.result()
                all_results.extend(result)
            except Exception as e:
                logger.warning(f"处理股票 {stock_code} 时出错: {e}")
    
    return sorted(all_results, key=lambda x: (x[0], x[2]))  # 按股票代码和日期排序

def print_results(results: List[Tuple[str, float, str]], price_type: str):
    """打印搜索结果"""
    if not results:
        print("未找到符合条件的股票")
        return
    
    price_type_name = {
        'close': '收盘价',
        'high': '最高价', 
        'low': '最低价'
    }.get(price_type, price_type)
    
    print(f"\n找到 {len(results)} 条符合条件的记录:")
    print("-" * 50)
    print(f"{'股票代码':<10} {price_type_name:<10} {'日期':<12}")
    print("-" * 35)
    
    for code, price, date in results:
        print(f"{code:<10} {price:<10.2f} {date:<12}")

def main():
    parser = argparse.ArgumentParser(description="查找指定历史价格的股票 - 并发版本")
    parser.add_argument("price", type=float, help="目标价格")
    parser.add_argument("--data-dir", default="./data", help="数据目录路径 (默认: ./data)")
    parser.add_argument("--price-type", choices=['close', 'high', 'low'], default='close', 
                       help="价格类型 (默认: close)")
    parser.add_argument("--start-date", help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--tolerance", type=float, default=0.00, help="价格容差 (默认: 0.01)")
    parser.add_argument("--max-workers", type=int, help="最大并发数 (默认: CPU核心数)")
    parser.add_argument("--benchmark", action="store_true", help="显示性能基准测试")
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    # 加载数据
    data_dir = Path(args.data_dir)
    logger.info("开始并发加载股票数据...")
    stock_data = load_stock_data_concurrent(data_dir, args.max_workers)
    
    if not stock_data:
        logger.error("没有找到可用的股票数据")
        return
    
    load_time = time.time() - start_time
    logger.info(f"数据加载完成，耗时: {load_time:.2f}秒")
    
    # 执行搜索
    try:
        search_start_time = time.time()
        logger.info("开始并发查找...")
        results = find_by_price_concurrent(
            stock_data, 
            args.price, 
            args.price_type,
            args.start_date,
            args.end_date,
            args.tolerance,
            args.max_workers
        )
        search_time = time.time() - search_start_time
        logger.info(f"查找完成，耗时: {search_time:.2f}秒")
        
        print_results(results, args.price_type)
        
        if args.benchmark:
            total_time = time.time() - start_time
            print(f"\n性能统计:")
            print(f"数据加载时间: {load_time:.2f}秒")
            print(f"查找时间: {search_time:.2f}秒")
            print(f"总耗时: {total_time:.2f}秒")
            print(f"处理股票数量: {len(stock_data)}")
            print(f"找到结果数量: {len(results)}")
            
    except ValueError as e:
        logger.error(f"参数错误: {e}")
        return

if __name__ == "__main__":
    # 设置多进程启动方法
    mp.set_start_method('spawn', force=True)
    main() 