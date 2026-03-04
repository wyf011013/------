"""
文件监控服务
使用watchdog库实时监控文件变化
"""

import os
import hashlib
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Set, List, Optional, Callable, Dict, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
import threading
from queue import Queue, Empty


class FileHasher:
    """文件哈希计算器"""
    
    CHUNK_SIZE = 64 * 1024  # 64KB 分块大小
    
    @staticmethod
    def calculate_md5(file_path: str, chunk_size: int = None) -> Optional[str]:
        """
        计算文件的MD5哈希
        
        Args:
            file_path: 文件路径
            chunk_size: 分块大小，默认为64KB
            
        Returns:
            MD5哈希字符串，如果失败返回None
        """
        if not os.path.exists(file_path):
            return None
        
        if chunk_size is None:
            chunk_size = FileHasher.CHUNK_SIZE
        
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logging.getLogger(__name__).error(f"计算文件哈希失败 {file_path}: {e}")
            return None
    
    @staticmethod
    def calculate_chunk_hashes(file_path: str, chunk_size: int = None) -> List[str]:
        """
        计算文件各分块的哈希
        
        Args:
            file_path: 文件路径
            chunk_size: 分块大小
            
        Returns:
            分块哈希列表
        """
        if not os.path.exists(file_path):
            return []
        
        if chunk_size is None:
            chunk_size = FileHasher.CHUNK_SIZE
        
        chunk_hashes = []
        try:
            with open(file_path, "rb") as f:
                chunk_index = 0
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    chunk_hash = hashlib.md5(chunk).hexdigest()
                    chunk_hashes.append(chunk_hash)
                    chunk_index += 1
        except Exception as e:
            logging.getLogger(__name__).error(f"计算分块哈希失败 {file_path}: {e}")
        
        return chunk_hashes


class BackupFileHandler(FileSystemEventHandler):
    """文件系统事件处理器"""
    
    def __init__(self, monitor_paths: List[str], exclude_patterns: Set[str] = None,
                 callback: Callable = None, logger: logging.Logger = None):
        """
        初始化处理器
        
        Args:
            monitor_paths: 监控路径列表
            exclude_patterns: 排除模式集合（如：*.tmp, *.log）
            callback: 文件变化回调函数
            logger: 日志记录器
        """
        super().__init__()
        self.monitor_paths = [Path(p).resolve() for p in monitor_paths]
        self.exclude_patterns = exclude_patterns or set()
        self.callback = callback
        self.logger = logger or logging.getLogger(__name__)
        
        # 文件修改防抖（避免频繁写入触发多次备份）
        self.file_modification_times: Dict[str, float] = {}
        self.debounce_interval = 2.0  # 2秒防抖间隔
        
        # 文件大小缓存（避免重复计算小文件的哈希）
        self.file_size_cache: Dict[str, int] = {}
        
        # 新增：文件状态缓存（修改时间+大小），用于过滤纯属性变化
        self.file_state_cache: Dict[str, tuple] = {}  # key=文件路径, value=(mtime_ns, size)
        
        # 待处理文件队列（用于批量处理）
        self.pending_files = Queue()
        self.process_thread = None
        self._stop_event = threading.Event()
        
        self._start_batch_processor()
    
    def _start_batch_processor(self):
        """启动批量处理线程"""
        self.process_thread = threading.Thread(target=self._process_pending_files, daemon=True)
        self.process_thread.start()
    
    def _process_pending_files(self):
        """批量处理待处理文件"""
        batch = []
        batch_timeout = 5.0  # 5秒批处理超时
        
        while not self._stop_event.is_set():
            try:
                # 等待文件事件
                file_info = self.pending_files.get(timeout=1.0)
                batch.append(file_info)
                
                # 收集更多事件或等待超时
                deadline = time.time() + 0.5  # 0.5秒收集窗口
                while time.time() < deadline and len(batch) < 10:
                    try:
                        file_info = self.pending_files.get(timeout=0.1)
                        batch.append(file_info)
                    except Empty:
                        break
                
                # 处理批次
                if batch:
                    self._process_batch(batch)
                    batch = []
                    
            except Empty:
                # 处理超时累积的批次
                if batch:
                    self._process_batch(batch)
                    batch = []
                continue
    
    def _process_batch(self, batch: List[Dict[str, Any]]):
        """处理一批文件事件（新增状态对比逻辑）"""
        # 去重（同一文件的多次修改只保留最后一次）
        file_dict = {}
        for item in batch:
            file_path = item['file_path']
            # 保留最新的事件
            if file_path not in file_dict or item['timestamp'] > file_dict[file_path]['timestamp']:
                file_dict[file_path] = item
        
        # 调用回调处理每个文件（添加状态对比）
        for item in file_dict.values():
            file_path = item['file_path']
            change_type = item['change_type']
            
            # 删除/移动事件直接处理（无状态可对比），并清理缓存
            if change_type in ["deleted", "moved"]:
                if file_path in self.file_state_cache:
                    del self.file_state_cache[file_path]
                if self.callback:
                    try:
                        self.callback(item)
                    except Exception as e:
                        self.logger.error(f"回调处理失败: {e}")
                continue
            
            # 仅文件存在时对比状态
            if not os.path.exists(file_path):
                continue
            
            # 获取当前文件状态（纳秒级修改时间+大小）
            try:
                stat = os.stat(file_path)
                current_mtime = stat.st_mtime_ns  # 纳秒级，精度更高
                current_size = stat.st_size
                current_state = (current_mtime, current_size)
            except Exception as e:
                self.logger.error(f"获取文件状态失败 {file_path}: {e}")
                continue
            
            # 获取历史状态
            history_state = self.file_state_cache.get(file_path)
            
            # 状态不一致 → 真实内容修改，触发回调
            if current_state != history_state:
                self.file_state_cache[file_path] = current_state  # 更新缓存
                if self.callback:
                    try:
                        self.callback(item)
                    except Exception as e:
                        self.logger.error(f"回调处理失败: {e}")
            else:
                # 状态一致 → 仅属性变化，跳过
                self.logger.debug(f"仅文件属性变化，跳过处理: {file_path}")
    
    def _should_ignore(self, file_path: str) -> bool:
        """
        检查是否应该忽略该文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            是否应该忽略
        """
        from fnmatch import fnmatch
        
        path = Path(file_path)
        
        # 检查排除模式
        for pattern in self.exclude_patterns:
            if fnmatch(path.name, pattern) or fnmatch(str(path), pattern):
                return True
        
        # 检查是否在监控路径下
        try:
            path_resolved = path.resolve()
        except OSError:
            return True
        
        for monitor_path in self.monitor_paths:
            try:
                if path_resolved.is_relative_to(monitor_path):
                    return False
            except ValueError:
                continue
        
        return True
    
    def _is_debounced(self, file_path: str) -> bool:
        """
        检查文件是否应该防抖
        
        Args:
            file_path: 文件路径
            
        Returns:
            是否应该忽略（在防抖期内）
        """
        current_time = time.time()
        last_time = self.file_modification_times.get(file_path, 0)
        
        if current_time - last_time < self.debounce_interval:
            return True
        
        self.file_modification_times[file_path] = current_time
        return False
    
    def _get_relative_path(self, file_path: str) -> str:
        """
        获取相对于监控路径的相对路径
        
        Args:
            file_path: 文件完整路径
            
        Returns:
            相对路径
        """
        path = Path(file_path).resolve()
        
        for monitor_path in self.monitor_paths:
            try:
                return str(path.relative_to(monitor_path))
            except ValueError:
                continue
        
        return str(path)
    
    def on_created(self, event: FileSystemEvent):
        """文件创建事件"""
        if event.is_directory:
            return
        
        file_path = event.src_path
        
        if self._should_ignore(file_path):
            return
        
        if self._is_debounced(file_path):
            return
        
        self.logger.info(f"文件创建: {file_path}")
        
        # 获取文件信息
        file_size = 0
        file_hash = ""
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            # 小文件立即计算哈希，大文件延迟计算
            if file_size < 10 * 1024 * 1024:  # 小于10MB
                file_hash = FileHasher.calculate_md5(file_path)
        
        # 添加到待处理队列
        self.pending_files.put({
            "file_path": file_path,
            "relative_path": self._get_relative_path(file_path),
            "change_type": "created",
            "file_size": file_size,
            "file_hash": file_hash,
            "timestamp": datetime.utcnow()
        })
    
    def on_modified(self, event: FileSystemEvent):
        """文件修改事件"""
        if event.is_directory:
            return
        
        file_path = event.src_path
        
        if self._should_ignore(file_path):
            return
        
        if self._is_debounced(file_path):
            return
        
        self.logger.info(f"文件修改: {file_path}")
        
        # 获取文件信息
        file_size = 0
        file_hash = ""
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            # 小文件立即计算哈希
            if file_size < 10 * 1024 * 1024:  # 小于10MB
                file_hash = FileHasher.calculate_md5(file_path)
        
        # 添加到待处理队列
        self.pending_files.put({
            "file_path": file_path,
            "relative_path": self._get_relative_path(file_path),
            "change_type": "modified",
            "file_size": file_size,
            "file_hash": file_hash,
            "timestamp": datetime.utcnow()
        })
    
    def on_deleted(self, event: FileSystemEvent):
        """文件删除事件"""
        if event.is_directory:
            return
        
        file_path = event.src_path
        
        if self._should_ignore(file_path):
            return
        
        self.logger.info(f"文件删除: {file_path}")
        
        # 添加到待处理队列
        self.pending_files.put({
            "file_path": file_path,
            "relative_path": self._get_relative_path(file_path),
            "change_type": "deleted",
            "file_size": 0,
            "file_hash": "",
            "timestamp": datetime.utcnow()
        })
    
    def on_moved(self, event: FileSystemEvent):
        """文件移动事件"""
        if event.is_directory:
            return
        
        src_path = event.src_path
        dest_path = event.dest_path
        
        if self._should_ignore(src_path) and self._should_ignore(dest_path):
            return
        
        self.logger.info(f"文件移动: {src_path} -> {dest_path}")
        
        # 处理为：删除原文件，创建新文件
        # 删除事件
        if not self._should_ignore(src_path):
            self.pending_files.put({
                "file_path": src_path,
                "relative_path": self._get_relative_path(src_path),
                "change_type": "deleted",
                "file_size": 0,
                "file_hash": "",
                "timestamp": datetime.utcnow()
            })
        
        # 创建事件
        if not self._should_ignore(dest_path) and os.path.exists(dest_path):
            file_size = os.path.getsize(dest_path)
            file_hash = FileHasher.calculate_md5(dest_path) if file_size < 10 * 1024 * 1024 else ""
            
            self.pending_files.put({
                "file_path": dest_path,
                "relative_path": self._get_relative_path(dest_path),
                "change_type": "created",
                "file_size": file_size,
                "file_hash": file_hash,
                "timestamp": datetime.utcnow()
            })
    
    def stop(self):
        """停止处理器"""
        self._stop_event.set()
        if self.process_thread and self.process_thread.is_alive():
            self.process_thread.join(timeout=5)


class FileMonitorService:
    """文件监控服务"""
    
    def __init__(self, config: Dict[str, Any], file_callback: Callable, 
                 logger: logging.Logger = None):
        """
        初始化文件监控服务
        
        Args:
            config: 配置字典
            file_callback: 文件变化回调函数
            logger: 日志记录器
        """
        self.config = config
        self.file_callback = file_callback
        self.logger = logger or logging.getLogger(__name__)
        
        # 监控配置
        self.monitor_paths = config.get('monitor_paths', [])
        self.exclude_patterns = set(config.get('exclude_patterns', [
            '*.tmp', '*.temp', '*.log', '*.lock', '*.swp', '*.swo',
            '~*', '.DS_Store', 'Thumbs.db', '*.bak', '*.cache'
        ]))
        
        # 监控器
        self.observer: Optional[Observer] = None
        self.event_handler: Optional[BackupFileHandler] = None
        
        # 状态
        self.is_running = False
    
    def start(self) -> bool:
        """
        启动文件监控
        
        Returns:
            是否成功启动
        """
        if not self.monitor_paths:
            self.logger.warning("没有配置监控路径")
            return False
        
        # 过滤有效路径
        valid_paths = []
        for path in self.monitor_paths:
            if os.path.exists(path) and os.path.isdir(path):
                valid_paths.append(path)
            else:
                self.logger.warning(f"监控路径不存在或不是目录: {path}")
        
        if not valid_paths:
            self.logger.error("没有有效的监控路径")
            return False
        
        try:
            # 创建事件处理器
            self.event_handler = BackupFileHandler(
                monitor_paths=valid_paths,
                exclude_patterns=self.exclude_patterns,
                callback=self.file_callback,
                logger=self.logger
            )
            
            # 创建监控器
            self.observer = Observer()
            
            # 为每个路径安排监控
            for path in valid_paths:
                self.observer.schedule(self.event_handler, path, recursive=True)
                self.logger.info(f"开始监控: {path}")
            
            # 启动监控
            self.observer.start()
            self.is_running = True
            
            self.logger.info("文件监控服务已启动")
            return True
            
        except Exception as e:
            self.logger.error(f"启动文件监控失败: {e}")
            return False
    
    def stop(self):
        """停止文件监控"""
        self.is_running = False
        
        if self.observer and self.observer.is_alive():
            self.observer.stop()
            self.observer.join(timeout=10)
            self.logger.info("文件监控器已停止")
        
        if self.event_handler:
            self.event_handler.stop()
            self.logger.info("事件处理器已停止")
        
        self.observer = None
        self.event_handler = None


# 测试代码
if __name__ == "__main__":
    import tempfile
    import shutil
    
    # 设置日志
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # 创建临时测试目录
    test_dir = tempfile.mkdtemp(prefix="file_monitor_test_")
    logger.info(f"测试目录: {test_dir}")
    
    try:
        # 文件变化回调
        def on_file_change(file_info):
            logger.info(f"文件变化: {file_info}")
        
        # 配置
        config = {
            'monitor_paths': [test_dir],
            'exclude_patterns': ['*.tmp', '*.log']
        }
        
        # 创建监控服务
        monitor = FileMonitorService(config, on_file_change, logger)
        
        # 启动监控
        if monitor.start():
            logger.info("文件监控已启动，开始测试...")
            
            # 测试1: 创建文件
            test_file = os.path.join(test_dir, "test.txt")
            with open(test_file, 'w') as f:
                f.write("Hello, World!")
            time.sleep(1)
            
            # 测试2: 修改文件属性（改权限）- 应该被过滤
            os.chmod(test_file, 0o777)
            logger.info("修改文件权限，预期不会触发回调")
            time.sleep(1)
            
            # 测试3: 修改文件内容 - 应该触发回调
            with open(test_file, 'a') as f:
                f.write("\nModified content")
            time.sleep(1)
            
            # 测试4: 移动文件
            new_file = os.path.join(test_dir, "renamed.txt")
            os.rename(test_file, new_file)
            time.sleep(1)
            
            # 测试5: 删除文件
            os.remove(new_file)
            time.sleep(2)
            
            logger.info("测试完成")
        else:
            logger.error("文件监控启动失败")
        
        # 停止监控
        monitor.stop()
        
    finally:
        # 清理测试目录
        shutil.rmtree(test_dir, ignore_errors=True)
        logger.info("测试目录已清理")