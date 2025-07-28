"""
股票数据抓取和选股系统GUI界面
支持K线数据抓取和量化选股功能
"""
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import sys
import os
from pathlib import Path
import datetime as dt
import subprocess
import json
import queue
import logging
import tempfile
import time
import signal
from typing import Optional, Dict, Any

# 添加当前目录到Python路径，以便导入本地模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class TextHandler(logging.Handler):
    """自定义日志处理器，将日志输出到GUI文本框"""
    def __init__(self, text_widget, queue_obj):
        super().__init__()
        self.text_widget = text_widget
        self.queue = queue_obj

    def emit(self, record):
        msg = self.format(record)
        self.queue.put(msg)

class StockToolGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("股票数据抓取和选股系统")
        self.root.geometry("1000x800")
        
        # 设置窗口图标和样式
        self.setup_styles()
        
        # 创建日志队列
        self.log_queue = queue.Queue()
        
        # 进程管理变量
        self.current_process = None
        self.fetch_thread = None
        self.select_thread = None
        
        # 创建主界面
        self.create_widgets()
        
        # 启动日志更新定时器
        self.update_log_display()
        
    def setup_styles(self):
        """设置界面样式"""
        style = ttk.Style()
        # 设置主题
        try:
            style.theme_use('clam')
        except:
            pass
            
    def create_widgets(self):
        """创建主界面控件"""
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(2, weight=1)
        
        # 创建选项卡
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # K线数据抓取选项卡
        self.fetch_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.fetch_frame, text="K线数据抓取")
        self.create_fetch_tab()
        
        # 选股选项卡
        self.select_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.select_frame, text="量化选股")
        self.create_select_tab()
        
        # 日志显示区域
        log_label = ttk.Label(main_frame, text="运行日志:", font=('Arial', 10, 'bold'))
        log_label.grid(row=1, column=0, sticky=tk.W, pady=(10, 5))
        
        # 创建日志文本框
        self.log_text = scrolledtext.ScrolledText(
            main_frame, 
            height=15, 
            width=100,
            font=('Consolas', 9),
            bg='#1e1e1e',
            fg='#ffffff',
            insertbackground='white'
        )
        self.log_text.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # 控制按钮框架
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=3, column=0, columnspan=2, pady=(0, 10))
        
        # 清空日志按钮
        clear_btn = ttk.Button(button_frame, text="清空日志", command=self.clear_log)
        clear_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # 保存日志按钮
        save_log_btn = ttk.Button(button_frame, text="保存日志", command=self.save_log)
        save_log_btn.pack(side=tk.LEFT)
        
    def create_fetch_tab(self):
        """创建K线数据抓取选项卡"""
        # 主框架
        fetch_main = ttk.Frame(self.fetch_frame, padding="10")
        fetch_main.pack(fill=tk.BOTH, expand=True)
        
        # 数据源设置
        datasource_frame = ttk.LabelFrame(fetch_main, text="数据源设置", padding="10")
        datasource_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(datasource_frame, text="数据源:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.datasource_var = tk.StringVar(value="tushare")
        datasource_combo = ttk.Combobox(datasource_frame, textvariable=self.datasource_var, 
                                       values=["tushare", "akshare", "mootdx"], state="readonly", width=15)
        datasource_combo.grid(row=0, column=1, sticky=tk.W, padx=(0, 20))
        
        ttk.Label(datasource_frame, text="K线频率:").grid(row=0, column=2, sticky=tk.W, padx=(0, 10))
        self.frequency_var = tk.StringVar(value="4")
        frequency_combo = ttk.Combobox(datasource_frame, textvariable=self.frequency_var,
                                     values=["0 (5分)", "1 (15分)", "2 (30分)", "3 (60分)", 
                                            "4 (日线)", "5 (周线)", "6 (月线)"], 
                                     state="readonly", width=15)
        frequency_combo.grid(row=0, column=3, sticky=tk.W)
        
        # Tushare Token设置
        ttk.Label(datasource_frame, text="Tushare Token:").grid(row=1, column=0, sticky=tk.W, padx=(0, 10), pady=(10, 0))
        self.ts_token_var = tk.StringVar(value="")
        self.ts_token_entry = ttk.Entry(datasource_frame, textvariable=self.ts_token_var, width=40, show="*")
        self.ts_token_entry.grid(row=1, column=1, columnspan=2, sticky=tk.W, padx=(0, 10), pady=(10, 0))
        
        # 显示/隐藏Token按钮
        self.show_token_var = tk.BooleanVar(value=False)
        self.show_token_btn = ttk.Checkbutton(datasource_frame, text="显示", variable=self.show_token_var,
                                             command=self.toggle_token_visibility)
        self.show_token_btn.grid(row=1, column=3, sticky=tk.W, pady=(10, 0))
        
        # Token说明
        token_info = ttk.Label(datasource_frame, text="(使用Tushare数据源时必填，请到 https://tushare.pro 注册获取)", 
                              font=('Arial', 8), foreground='gray')
        token_info.grid(row=2, column=1, columnspan=3, sticky=tk.W, pady=(5, 0))
        
        # 绑定数据源变化事件
        datasource_combo.bind('<<ComboboxSelected>>', self.on_datasource_changed)
        
        # 市值筛选设置
        market_frame = ttk.LabelFrame(fetch_main, text="市值筛选", padding="10")
        market_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(market_frame, text="最小市值(亿元):").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.min_mktcap_var = tk.StringVar(value="50")
        min_mktcap_entry = ttk.Entry(market_frame, textvariable=self.min_mktcap_var, width=15)
        min_mktcap_entry.grid(row=0, column=1, sticky=tk.W, padx=(0, 20))
        
        ttk.Label(market_frame, text="最大市值(亿元):").grid(row=0, column=2, sticky=tk.W, padx=(0, 10))
        self.max_mktcap_var = tk.StringVar(value="无限制")
        max_mktcap_entry = ttk.Entry(market_frame, textvariable=self.max_mktcap_var, width=15)
        max_mktcap_entry.grid(row=0, column=3, sticky=tk.W)
        
        self.exclude_gem_var = tk.BooleanVar(value=True)
        exclude_gem_check = ttk.Checkbutton(market_frame, text="排除创业板/科创板/北交所",
                                           variable=self.exclude_gem_var)
        exclude_gem_check.grid(row=1, column=0, columnspan=2, sticky=tk.W, pady=(10, 0))
        
        # 日期设置
        date_frame = ttk.LabelFrame(fetch_main, text="日期范围", padding="10")
        date_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(date_frame, text="开始日期:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.start_date_var = tk.StringVar(value="20200101")
        start_date_entry = ttk.Entry(date_frame, textvariable=self.start_date_var, width=15)
        start_date_entry.grid(row=0, column=1, sticky=tk.W, padx=(0, 20))
        
        ttk.Label(date_frame, text="结束日期:").grid(row=0, column=2, sticky=tk.W, padx=(0, 10))
        self.end_date_var = tk.StringVar(value="today")
        end_date_entry = ttk.Entry(date_frame, textvariable=self.end_date_var, width=15)
        end_date_entry.grid(row=0, column=3, sticky=tk.W)
        
        # 其他设置
        other_frame = ttk.LabelFrame(fetch_main, text="其他设置", padding="10")
        other_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(other_frame, text="输出目录:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.output_dir_var = tk.StringVar(value="./data")
        output_dir_entry = ttk.Entry(other_frame, textvariable=self.output_dir_var, width=30)
        output_dir_entry.grid(row=0, column=1, sticky=tk.W, padx=(0, 10))
        
        browse_btn = ttk.Button(other_frame, text="浏览", 
                               command=lambda: self.browse_directory(self.output_dir_var))
        browse_btn.grid(row=0, column=2, padx=(0, 20))
        
        ttk.Label(other_frame, text="并发线程数:").grid(row=0, column=3, sticky=tk.W, padx=(0, 10))
        self.workers_var = tk.StringVar(value="3")
        workers_spin = ttk.Spinbox(other_frame, from_=1, to=20, textvariable=self.workers_var, width=10)
        workers_spin.grid(row=0, column=4, sticky=tk.W)
        
        # 执行按钮
        fetch_btn_frame = ttk.Frame(fetch_main)
        fetch_btn_frame.pack(fill=tk.X, pady=(20, 0))
        
        self.fetch_btn = ttk.Button(fetch_btn_frame, text="开始抓取K线数据", 
                                   command=self.start_fetch_data, style="Accent.TButton")
        self.fetch_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # 停止抓取按钮
        self.stop_fetch_btn = ttk.Button(fetch_btn_frame, text="停止抓取", 
                                        command=self.stop_fetch_data, state='disabled')
        self.stop_fetch_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        self.fetch_progress = ttk.Progressbar(fetch_btn_frame, mode='indeterminate')
        self.fetch_progress.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(10, 0))
    
    def toggle_token_visibility(self):
        """切换Token显示/隐藏"""
        if self.show_token_var.get():
            self.ts_token_entry.config(show="")
        else:
            self.ts_token_entry.config(show="*")
    
    def on_datasource_changed(self, event=None):
        """数据源变化时的处理"""
        datasource = self.datasource_var.get()
        if datasource == "tushare":
            # 显示Token相关控件
            self.ts_token_entry.grid()
            self.show_token_btn.grid()
        else:
            # 隐藏Token相关控件（但不删除，保持值）
            pass  # 保持显示，但在验证时忽略
        
    def create_select_tab(self):
        """创建选股选项卡"""
        # 主框架
        select_main = ttk.Frame(self.select_frame, padding="10")
        select_main.pack(fill=tk.BOTH, expand=True)
        
        # 数据设置
        data_frame = ttk.LabelFrame(select_main, text="数据设置", padding="10")
        data_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(data_frame, text="数据目录:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.data_dir_var = tk.StringVar(value="./data")
        data_dir_entry = ttk.Entry(data_frame, textvariable=self.data_dir_var, width=30)
        data_dir_entry.grid(row=0, column=1, sticky=tk.W, padx=(0, 10))
        
        browse_data_btn = ttk.Button(data_frame, text="浏览", 
                                    command=lambda: self.browse_directory(self.data_dir_var))
        browse_data_btn.grid(row=0, column=2, padx=(0, 20))
        
        ttk.Label(data_frame, text="配置文件:").grid(row=0, column=3, sticky=tk.W, padx=(0, 10))
        self.config_file_var = tk.StringVar(value="./configs.json")
        config_file_entry = ttk.Entry(data_frame, textvariable=self.config_file_var, width=25)
        config_file_entry.grid(row=0, column=4, sticky=tk.W, padx=(0, 10))
        
        browse_config_btn = ttk.Button(data_frame, text="浏览", 
                                      command=lambda: self.browse_file(self.config_file_var, "JSON文件", "*.json"))
        browse_config_btn.grid(row=0, column=5)
        
        # 选股参数
        param_frame = ttk.LabelFrame(select_main, text="选股参数", padding="10")
        param_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(param_frame, text="交易日期:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.select_date_var = tk.StringVar(value="")
        select_date_entry = ttk.Entry(param_frame, textvariable=self.select_date_var, width=15)
        select_date_entry.grid(row=0, column=1, sticky=tk.W, padx=(0, 10))
        
        ttk.Label(param_frame, text="(留空使用最新日期，格式: YYYY-MM-DD)").grid(row=0, column=2, sticky=tk.W)
        
        ttk.Label(param_frame, text="股票代码:").grid(row=1, column=0, sticky=tk.W, padx=(0, 10), pady=(10, 0))
        self.tickers_var = tk.StringVar(value="all")
        tickers_entry = ttk.Entry(param_frame, textvariable=self.tickers_var, width=40)
        tickers_entry.grid(row=1, column=1, columnspan=2, sticky=tk.W, pady=(10, 0))
        
        ttk.Label(param_frame, text="(all表示全部股票，或输入逗号分隔的股票代码)").grid(row=2, column=1, columnspan=2, sticky=tk.W)
        
        # 策略配置显示
        strategy_frame = ttk.LabelFrame(select_main, text="当前策略配置", padding="10")
        strategy_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # 创建策略配置文本框
        self.strategy_text = scrolledtext.ScrolledText(
            strategy_frame, 
            height=10, 
            width=80,
            font=('Consolas', 9)
        )
        self.strategy_text.pack(fill=tk.BOTH, expand=True)
        
        # 加载配置按钮
        load_config_btn = ttk.Button(strategy_frame, text="重新加载配置", command=self.load_strategy_config)
        load_config_btn.pack(pady=(10, 0))
        
        # 执行按钮
        select_btn_frame = ttk.Frame(select_main)
        select_btn_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.select_btn = ttk.Button(select_btn_frame, text="开始选股", 
                                    command=self.start_stock_selection, style="Accent.TButton")
        self.select_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # 停止选股按钮
        self.stop_select_btn = ttk.Button(select_btn_frame, text="停止选股", 
                                         command=self.stop_stock_selection, state='disabled')
        self.stop_select_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        self.select_progress = ttk.Progressbar(select_btn_frame, mode='indeterminate')
        self.select_progress.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(10, 0))
        
        # 初始加载策略配置
        self.load_strategy_config()
        
    def browse_directory(self, var):
        """浏览选择目录"""
        directory = filedialog.askdirectory(initialdir=var.get())
        if directory:
            var.set(directory)
            
    def browse_file(self, var, file_desc, file_pattern):
        """浏览选择文件"""
        file_path = filedialog.askopenfilename(
            initialdir=os.path.dirname(var.get()),
            title=f"选择{file_desc}",
            filetypes=[(file_desc, file_pattern), ("所有文件", "*.*")]
        )
        if file_path:
            var.set(file_path)
            
    def load_strategy_config(self):
        """加载并显示策略配置"""
        try:
            config_path = self.config_file_var.get()
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                # 格式化显示配置
                formatted_config = json.dumps(config, indent=2, ensure_ascii=False)
                self.strategy_text.delete(1.0, tk.END)
                self.strategy_text.insert(1.0, formatted_config)
            else:
                self.strategy_text.delete(1.0, tk.END)
                self.strategy_text.insert(1.0, f"配置文件不存在: {config_path}")
        except Exception as e:
            self.strategy_text.delete(1.0, tk.END)
            self.strategy_text.insert(1.0, f"加载配置文件失败: {str(e)}")
            
    def log_message(self, message, level="INFO"):
        """添加日志消息到队列"""
        timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_msg = f"[{timestamp}] [{level}] {message}\n"
        self.log_queue.put(formatted_msg)
        
    def update_log_display(self):
        """更新日志显示"""
        try:
            while True:
                message = self.log_queue.get_nowait()
                self.log_text.insert(tk.END, message)
                self.log_text.see(tk.END)
        except queue.Empty:
            pass
        
        # 每100ms检查一次
        self.root.after(100, self.update_log_display)
        
    def clear_log(self):
        """清空日志"""
        self.log_text.delete(1.0, tk.END)
        
    def save_log(self):
        """保存日志到文件"""
        log_content = self.log_text.get(1.0, tk.END)
        if log_content.strip():
            file_path = filedialog.asksaveasfilename(
                defaultextension=".log",
                filetypes=[("日志文件", "*.log"), ("文本文件", "*.txt"), ("所有文件", "*.*")]
            )
            if file_path:
                try:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(log_content)
                    messagebox.showinfo("成功", f"日志已保存到: {file_path}")
                except Exception as e:
                    messagebox.showerror("错误", f"保存日志失败: {str(e)}")
        else:
            messagebox.showwarning("警告", "没有日志内容可保存")
            
    def start_fetch_data(self):
        """开始抓取K线数据"""
        if self.fetch_thread and self.fetch_thread.is_alive():
            messagebox.showwarning("警告", "数据抓取正在进行中，请等待完成")
            return
        
        # 验证Tushare Token
        datasource = self.datasource_var.get()
        if datasource == "tushare":
            token = self.ts_token_var.get().strip()
            if not token:
                messagebox.showerror("参数错误", "使用Tushare数据源时必须提供Token")
                return
            
        # 验证输入参数
        try:
            min_cap = float(self.min_mktcap_var.get()) * 1e8  # 转换为元
            max_cap_str = self.max_mktcap_var.get()
            if max_cap_str == "无限制" or max_cap_str.lower() == "inf":
                max_cap = float('+inf')
            else:
                max_cap = float(max_cap_str) * 1e8
                
            workers = int(self.workers_var.get())
            frequency = int(self.frequency_var.get().split()[0])
            
        except ValueError as e:
            messagebox.showerror("参数错误", f"请检查输入参数: {str(e)}")
            return
            
        # 构建命令参数
        cmd = [sys.executable, "fetch_kline.py"]
        cmd.extend(["--datasource", self.datasource_var.get()])
        cmd.extend(["--frequency", str(frequency)])
        if self.exclude_gem_var.get():
            cmd.append("--exclude-gem")
        cmd.extend(["--min-mktcap", str(min_cap)])
        cmd.extend(["--max-mktcap", str(max_cap)])
        cmd.extend(["--start", self.start_date_var.get()])
        cmd.extend(["--end", self.end_date_var.get()])
        cmd.extend(["--out", self.output_dir_var.get()])
        cmd.extend(["--workers", str(workers)])
        
        # 启动数据抓取线程
        self.fetch_thread = threading.Thread(target=self.run_subprocess, args=(cmd, "数据抓取", self.fetch_completed))
        self.fetch_thread.daemon = True
        self.fetch_thread.start()
        
        # 更新UI状态
        self.fetch_btn.config(state='disabled', text='抓取中...')
        self.stop_fetch_btn.config(state='normal')
        self.fetch_progress.start()
        
    def run_subprocess(self, cmd, task_name, completion_callback):
        """在后台线程中运行子进程并使用重定向获取输出"""
        temp_file = None
        temp_file_path = None
        try:
            self.log_message(f"开始{task_name}...")
            self.log_message(f"执行命令: {' '.join(cmd)}")
            
            # 创建临时文件用于重定向输出，使用二进制模式避免编码问题
            temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False)
            temp_file_path = temp_file.name
            temp_file.close()
            
            # 设置环境变量，如果是Tushare数据源
            env = os.environ.copy()
            if cmd[1] == "fetch_kline.py" and self.datasource_var.get() == "tushare":
                # 通过环境变量传递Token
                env['TUSHARE_TOKEN'] = self.ts_token_var.get().strip()
            
            # 启动子进程 - 将输出重定向到临时文件
            with open(temp_file_path, 'wb') as output_file:
                process = subprocess.Popen(
                    cmd,
                    stdout=output_file,
                    stderr=subprocess.STDOUT,
                    env=env,
                    cwd=os.path.dirname(os.path.abspath(__file__))
                )
                
            # 保存当前进程实例
            self.current_process = process
            
            # 实时监控临时文件内容
            last_position = 0
            while process.poll() is None:
                try:
                    # 读取文件新增内容（二进制模式）
                    with open(temp_file_path, 'rb') as f:
                        f.seek(last_position)
                        new_bytes = f.read()
                        if new_bytes:
                            # 尝试多种编码解码
                            decoded_content = self._decode_bytes(new_bytes)
                            if decoded_content:
                                self.log_queue.put(decoded_content)
                            last_position = f.tell()
                except (IOError, OSError):
                    # 文件可能还没准备好，稍等片刻
                    pass
                
                # 短暂等待避免过度占用CPU
                time.sleep(0.1)
            
            # 进程结束后，读取剩余内容
            try:
                with open(temp_file_path, 'rb') as f:
                    f.seek(last_position)
                    remaining_bytes = f.read()
                    if remaining_bytes:
                        decoded_content = self._decode_bytes(remaining_bytes)
                        if decoded_content:
                            self.log_queue.put(decoded_content)
            except (IOError, OSError):
                pass
            
            # 获取进程返回码
            return_code = process.returncode
            
            if return_code == 0:
                self.log_message(f"{task_name}完成!", "SUCCESS")
            elif return_code == -15 or return_code == 1:  # SIGTERM 或手动终止
                self.log_message(f"{task_name}已被停止", "WARNING")
            else:
                self.log_message(f"{task_name}失败，返回码: {return_code}", "ERROR")
                
        except Exception as e:
            self.log_message(f"{task_name}失败: {str(e)}", "ERROR")
        finally:
            # 清理进程引用
            self.current_process = None
            # 清理临时文件
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except:
                    pass
            # 更新UI状态
            self.root.after(0, completion_callback)
    
    def _decode_bytes(self, byte_data):
        """尝试多种编码方式解码字节数据"""
        if not byte_data:
            return ""
        
        # 尝试多种编码
        encodings = ['utf-8', 'gbk', 'cp936', 'gb2312', 'latin1']
        for encoding in encodings:
            try:
                return byte_data.decode(encoding)
            except UnicodeDecodeError:
                continue
        
        # 如果所有编码都失败，使用错误处理
        return byte_data.decode('utf-8', errors='replace')
            
    def fetch_completed(self):
        """数据抓取完成后的UI更新"""
        self.fetch_btn.config(state='normal', text='开始抓取K线数据')
        self.stop_fetch_btn.config(state='disabled')
        self.fetch_progress.stop()
    
    def stop_fetch_data(self):
        """停止数据抓取"""
        if self.current_process and self.current_process.poll() is None:
            try:
                # 尝试优雅地终止进程
                if sys.platform == "win32":
                    # Windows平台使用CTRL_BREAK_EVENT
                    self.current_process.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    # Unix平台使用SIGTERM
                    self.current_process.terminate()
                    
                self.log_message("正在停止数据抓取进程...", "WARNING")
                
                # 等待进程结束，最多等待5秒
                try:
                    self.current_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # 如果进程没有响应终止信号，强制杀死
                    self.current_process.kill()
                    self.log_message("强制终止数据抓取进程", "WARNING")
                    
            except Exception as e:
                self.log_message(f"停止进程时出错: {str(e)}", "ERROR")
                # 尝试强制终止
                try:
                    self.current_process.kill()
                except:
                    pass
        else:
            self.log_message("没有正在运行的抓取进程", "WARNING")
        
    def start_stock_selection(self):
        """开始股票选择"""
        if self.select_thread and self.select_thread.is_alive():
            messagebox.showwarning("警告", "选股正在进行中，请等待完成")
            return
            
        # 验证数据目录
        data_dir = self.data_dir_var.get()
        if not os.path.exists(data_dir):
            messagebox.showerror("错误", f"数据目录不存在: {data_dir}")
            return
            
        # 验证配置文件
        config_file = self.config_file_var.get()
        if not os.path.exists(config_file):
            messagebox.showerror("错误", f"配置文件不存在: {config_file}")
            return
            
        # 构建命令参数
        cmd = [sys.executable, "select_stock.py"]
        cmd.extend(["--data-dir", data_dir])
        cmd.extend(["--config", config_file])
        cmd.extend(["--tickers", self.tickers_var.get()])
        
        # 添加日期参数（如果指定）
        if self.select_date_var.get().strip():
            cmd.extend(["--date", self.select_date_var.get().strip()])
            
        # 启动选股线程
        self.select_thread = threading.Thread(target=self.run_subprocess, args=(cmd, "选股", self.selection_completed))
        self.select_thread.daemon = True
        self.select_thread.start()
        
        # 更新UI状态
        self.select_btn.config(state='disabled', text='选股中...')
        self.stop_select_btn.config(state='normal')
        self.select_progress.start()
        
    def selection_completed(self):
        """选股完成后的UI更新"""
        self.select_btn.config(state='normal', text='开始选股')
        self.stop_select_btn.config(state='disabled')
        self.select_progress.stop()
    
    def stop_stock_selection(self):
        """停止选股"""
        if self.current_process and self.current_process.poll() is None:
            try:
                # 尝试优雅地终止进程
                if sys.platform == "win32":
                    # Windows平台使用CTRL_BREAK_EVENT
                    self.current_process.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    # Unix平台使用SIGTERM
                    self.current_process.terminate()
                    
                self.log_message("正在停止选股进程...", "WARNING")
                
                # 等待进程结束，最多等待5秒
                try:
                    self.current_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # 如果进程没有响应终止信号，强制杀死
                    self.current_process.kill()
                    self.log_message("强制终止选股进程", "WARNING")
                    
            except Exception as e:
                self.log_message(f"停止进程时出错: {str(e)}", "ERROR")
                # 尝试强制终止
                try:
                    self.current_process.kill()
                except:
                    pass
        else:
            self.log_message("没有正在运行的选股进程", "WARNING")

def main():
    """主函数"""
    root = tk.Tk()
    app = StockToolGUI(root)
    
    # 设置窗口关闭事件
    def on_closing():
        if messagebox.askokcancel("退出", "确定要退出程序吗？"):
            root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        root.destroy()

if __name__ == "__main__":
    main()