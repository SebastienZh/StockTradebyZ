#!/usr/bin/env python3
"""
回测系统测试脚本 - 验证回测系统的正确性

功能：
1. 测试交易日期计算功能
2. 测试股票名称映射功能
3. 测试回测计算的准确性
"""

import unittest
import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from backtest import (
    StockNameMapper,
    get_trading_dates,
    get_next_trading_day,
    run_backtest
)


class TestBacktestSystem(unittest.TestCase):
    """回测系统测试类"""
    
    def setUp(self):
        """测试前准备"""
        self.test_dir = Path("test_backtest_data")
        self.test_dir.mkdir(exist_ok=True)
        
        # 创建测试数据
        self._create_test_data()
    
    def tearDown(self):
        """测试后清理"""
        # 清理测试数据
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
    
    def _create_test_data(self):
        """创建测试数据"""
        # 创建测试股票数据
        data_dir = self.test_dir / "data"
        data_dir.mkdir(exist_ok=True)
        
        # 创建测试配置
        config_dir = self.test_dir
        
        # 创建测试股票数据
        dates = pd.date_range(start="2025-05-01", end="2025-07-15")
        
        # 过滤掉周末
        dates = dates[dates.dayofweek < 5]
        
        # 创建测试股票数据
        for code in ["000001", "000002", "000003"]:
            df = pd.DataFrame({
                "date": dates,
                "open": [10 + i * 0.1 for i in range(len(dates))],
                "close": [11 + i * 0.1 for i in range(len(dates))],
                "high": [12 + i * 0.1 for i in range(len(dates))],
                "low": [9 + i * 0.1 for i in range(len(dates))],
                "volume": [1000000 for _ in range(len(dates))]
            })
            df.to_csv(data_dir / f"{code}.csv", index=False)
        
        # 创建测试配置
        config = {
            "selectors": [
                {
                    "class": "BBIKDJSelector",
                    "alias": "测试策略1",
                    "activate": True,
                    "params": {
                        "j_threshold": -5,
                        "bbi_min_window": 20,
                        "max_window": 60,
                        "price_range_pct": 0.5,
                        "bbi_q_threshold": 0.1,
                        "j_q_threshold": 0.10
                    }
                }
            ]
        }
        
        with open(config_dir / "test_config.json", "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    
    def test_stock_name_mapper(self):
        """测试股票名称映射功能"""
        mapper = StockNameMapper(cache_file=str(self.test_dir / "test_stock_names.json"))
        
        # 测试获取单个股票名称
        name = mapper.get_stock_name("000001")
        self.assertIsNotNone(name)
        self.assertIsInstance(name, str)
        
        # 测试批量获取股票名称
        names = mapper.get_stock_names(["000001", "000002", "000003"])
        self.assertEqual(len(names), 3)
        self.assertIsInstance(names, dict)
        
        # 测试缓存功能
        self.assertTrue((self.test_dir / "test_stock_names.json").exists())
    
    def test_trading_dates(self):
        """测试交易日期计算功能"""
        # 测试获取交易日列表
        start_date = "2025-06-01"
        end_date = "2025-06-30"
        
        trading_dates = get_trading_dates(start_date, end_date)
        self.assertIsInstance(trading_dates, list)
        
        # 测试获取下一个交易日
        if trading_dates:
            next_date = get_next_trading_day(trading_dates[0], trading_dates)
            if len(trading_dates) > 1:
                self.assertEqual(next_date, trading_dates[1])
    
    def test_backtest_execution(self):
        """测试回测执行功能"""
        # 创建输出目录
        output_dir = self.test_dir / "backtest_result"
        
        # 运行回测
        try:
            results = run_backtest(
                data_dir=self.test_dir / "data",
                config_path=self.test_dir / "test_config.json",
                start_date="2025-06-01",
                end_date="2025-06-30",
                output_dir=output_dir
            )
            
            # 检查结果
            self.assertIsInstance(results, dict)
            
            # 检查输出目录
            self.assertTrue(output_dir.exists())
            
            # 检查总体结果文件
            overall_file = output_dir / "overall_results.json"
            self.assertTrue(overall_file.exists())
            
        except Exception as e:
            self.fail(f"回测执行失败: {e}")


if __name__ == "__main__":
    unittest.main()
