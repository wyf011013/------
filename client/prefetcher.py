"""
预读取和预哈希模块
后台预计算文件哈希，加速增量备份过程
"""

import os
import hashlib
import threading
import logging
import time
import queue
from typing import Dict, List, Optional, Set, Callable, Tuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from enum import IntEnum


class PrefetchPriority(IntEnum):
    """预取优先级"""
    LOW = 0      # 后台扫描
    NORMAL = 1   # 文件监控触发
    HIGH = 2     # 即将备份的文件


@dataclass
class PrefetchTask:
    """预取任务"""
    file_path: str
    priority: PrefetchPriority
    timestamp: float
    callback: Optional[Callable[[str, str, int], None]] = None


class FilePrefetcher:
    """
    文件预取器
    后台预计算文件哈希，减少备份时的等待时间
    """
    
    def __init__(self, 
                 max_workers: int = 4,
                 cache_size: int = 10000,
                 hash_algorithm: str = 'md5',
                 logger: Optional[logging.Logger] = None):
        self.max_workers = max_workers
        self.cache_size = cache_size
        self.hash_algorithm = hash_algorithm
        self.logger = logger or logging.getLogger(__name__)
        
        # 哈希缓存: file_path -> (hash, mtime, size, timestamp)
        self._hash_cache: Dict[str, Tuple[str, float, int, float]] = {}
        self._cache_lock = threading.RLock()
        
        # 任务队列 (优先级队列)
        self._task_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._pending_files: Set[str] = set()
        self._pending_lock = threading.Lock()
        
        # 工作线程
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        
        # 统计
        self.stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'prefetch_completed': 0,
            'prefetch_errors': 0,
        }
    
    def start(self):
        """启动预取器"""
        if self._running:
            return
        
        self._running = True
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        self.logger.info(f"文件预取器已启动 (工作线程: {self.max_workers})")
    
    def stop(self):
        """停止预取器"""
        self._running = False
        
        # 清空队列
        while not self._task_queue.empty():
            try:
                self._task_queue.get_nowait()
            except queue.Empty:
                break
        
        # 关闭线程池
        self._executor.shutdown(wait=False)
        
        self.logger.info("文件预取器已停止")
    
    def _worker_loop(self):
        """工作线程循环"""
        while self._running:
            try:
                # 获取任务 (优先级, 任务)
                priority, task = self._task_queue.get(timeout=1)
                
                # 从待处理集合移除
                with self._pending_lock:
                    self._pending_files.discard(task.file_path)
                
                # 执行预取
                self._process_task(task)
                
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"预取任务处理异常: {e}")
    
    def _process_task(self, task: PrefetchTask):
        """处理单个预取任务"""
        file_path = task.file_path
        
        try:
            # 检查文件是否存在
            if not os.path.exists(file_path):
                return
            
            # 获取文件信息
            stat = os.stat(file_path)
            mtime = stat.st_mtime
            size = stat.st_size
            
            # 检查缓存是否有效
            cached = self._get_cached_hash(file_path)
            if cached:
                cached_hash, cached_mtime, cached_size, _ = cached
                if cached_mtime == mtime and cached_size == size:
                    # 缓存有效
                    if task.callback:
                        task.callback(file_path, cached_hash, size)
                    return
            
            # 计算哈希
            file_hash = self._calculate_hash(file_path)
            if file_hash:
                # 更新缓存
                self._update_cache(file_path, file_hash, mtime, size)
                
                self.stats['prefetch_completed'] += 1
                
                # 调用回调
                if task.callback:
                    task.callback(file_path, file_hash, size)
                
                self.logger.debug(f"预计算完成: {file_path}")
            
        except Exception as e:
            self.stats['prefetch_errors'] += 1
            self.logger.warning(f"预计算失败 {file_path}: {e}")
    
    def _calculate_hash(self, file_path: str) -> Optional[str]:
        """计算文件哈希"""
        try:
            if self.hash_algorithm == 'md5':
                hasher = hashlib.md5()
            elif self.hash_algorithm == 'sha256':
                hasher = hashlib.sha256()
            else:
                hasher = hashlib.md5()
            
            with open(file_path, 'rb') as f:
                # 分块读取，避免大文件内存问题
                while chunk := f.read(1024 * 1024):  # 1MB chunks
                    hasher.update(chunk)
            
            return hasher.hexdigest()
            
        except Exception as e:
            self.logger.warning(f"计算哈希失败 {file_path}: {e}")
            return None
    
    def _get_cached_hash(self, file_path: str) -> Optional[Tuple[str, float, int, float]]:
        """获取缓存的哈希"""
        with self._cache_lock:
            return self._hash_cache.get(file_path)
    
    def _update_cache(self, file_path: str, file_hash: str, 
                      mtime: float, size: int):
        """更新缓存"""
        with self._cache_lock:
            # LRU清理: 如果缓存满了，移除最旧的条目
            if len(self._hash_cache) >= self.cache_size:
                # 找到最旧的条目
                oldest_key = min(
                    self._hash_cache.keys(),
                    key=lambda k: self._hash_cache[k][3]
                )
                del self._hash_cache[oldest_key]
            
            self._hash_cache[file_path] = (file_hash, mtime, size, time.time())
    
    def prefetch(self, file_path: str, 
                 priority: PrefetchPriority = PrefetchPriority.NORMAL,
                 callback: Optional[Callable[[str, str, int], None]] = None):
        """
        添加文件到预取队列
        
        Args:
            file_path: 文件路径
            priority: 优先级
            callback: 完成回调 (file_path, hash, size)
        """
        if not self._running:
            return
        
        # 检查是否已在处理中
        with self._pending_lock:
            if file_path in self._pending_files:
                return
            self._pending_files.add(file_path)
        
        # 创建任务
        task = PrefetchTask(
            file_path=file_path,
            priority=priority,
            timestamp=time.time(),
            callback=callback
        )
        
        # 添加到队列 (优先级队列，数值越小优先级越高)
        self._task_queue.put((-int(priority), task))
    
    def prefetch_batch(self, file_paths: List[str],
                       priority: PrefetchPriority = PrefetchPriority.NORMAL):
        """批量预取"""
        for file_path in file_paths:
            self.prefetch(file_path, priority)
    
    def get_hash(self, file_path: str) -> Optional[Tuple[str, int]]:
        """
        获取文件哈希（优先从缓存）
        
        Returns:
            (hash, size) 或 None
        """
        try:
            stat = os.stat(file_path)
            mtime = stat.st_mtime
            size = stat.st_size
            
            # 检查缓存
            cached = self._get_cached_hash(file_path)
            if cached:
                cached_hash, cached_mtime, cached_size, _ = cached
                if cached_mtime == mtime and cached_size == size:
                    self.stats['cache_hits'] += 1
                    return cached_hash, cached_size
            
            self.stats['cache_misses'] += 1
            
            # 缓存未命中，添加到预取队列
            self.prefetch(file_path, PrefetchPriority.HIGH)
            return None
            
        except Exception as e:
            self.logger.warning(f"获取哈希失败 {file_path}: {e}")
            return None
    
    def invalidate_cache(self, file_path: str):
        """使缓存失效"""
        with self._cache_lock:
            self._hash_cache.pop(file_path, None)
    
    def clear_cache(self):
        """清空缓存"""
        with self._cache_lock:
            self._hash_cache.clear()
        self.logger.info("哈希缓存已清空")
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        with self._cache_lock:
            cache_size = len(self._hash_cache)
        
        total_requests = self.stats['cache_hits'] + self.stats['cache_misses']
        hit_rate = (
            self.stats['cache_hits'] / total_requests * 100
            if total_requests > 0 else 0
        )
        
        return {
            **self.stats,
            'cache_size': cache_size,
            'cache_hit_rate': f"{hit_rate:.1f}%",
            'pending_tasks': len(self._pending_files),
        }


class BlockPrefetcher:
    """
    块预取器
    预计算文件块的哈希，用于块级增量备份
    """
    
    def __init__(self, 
                 chunk_size: int = 1024 * 1024,
                 max_workers: int = 4,
                 logger: Optional[logging.Logger] = None):
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.logger = logger or logging.getLogger(__name__)
        
        # 块哈希缓存: file_path -> [(chunk_hash, offset), ...]
        self._block_cache: Dict[str, List[Tuple[str, int]]] = {}
        self._cache_lock = threading.RLock()
        
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._running = False
    
    def start(self):
        """启动预取器"""
        self._running = True
        self.logger.info("块预取器已启动")
    
    def stop(self):
        """停止预取器"""
        self._running = False
        self._executor.shutdown(wait=False)
        self.logger.info("块预取器已停止")
    
    def prefetch_blocks(self, file_path: str) -> Future:
        """
        预取文件块哈希
        
        Returns:
            Future对象，可通过result()获取 [(chunk_hash, offset), ...]
        """
        return self._executor.submit(self._calculate_block_hashes, file_path)
    
    def _calculate_block_hashes(self, file_path: str) -> List[Tuple[str, int]]:
        """计算文件块哈希"""
        try:
            if not os.path.exists(file_path):
                return []
            
            chunk_hashes = []
            with open(file_path, 'rb') as f:
                offset = 0
                while chunk := f.read(self.chunk_size):
                    chunk_hash = hashlib.sha256(chunk).hexdigest()
                    chunk_hashes.append((chunk_hash, offset))
                    offset += len(chunk)
            
            # 更新缓存
            with self._cache_lock:
                self._block_cache[file_path] = chunk_hashes
            
            self.logger.debug(f"块哈希预计算完成: {file_path} ({len(chunk_hashes)}块)")
            return chunk_hashes
            
        except Exception as e:
            self.logger.warning(f"块哈希预计算失败 {file_path}: {e}")
            return []
    
    def get_block_hashes(self, file_path: str) -> Optional[List[Tuple[str, int]]]:
        """获取块哈希（优先从缓存）"""
        with self._cache_lock:
            return self._block_cache.get(file_path)
    
    def invalidate_blocks(self, file_path: str):
        """使块缓存失效"""
        with self._cache_lock:
            self._block_cache.pop(file_path, None)


class DirectoryScanner:
    """
    目录扫描器
    后台扫描目录，预取文件哈希
    """
    
    def __init__(self, 
                 prefetcher: FilePrefetcher,
                 exclude_patterns: Optional[List[str]] = None,
                 max_file_size: int = 100 * 1024 * 1024,
                 logger: Optional[logging.Logger] = None):
        self.prefetcher = prefetcher
        self.exclude_patterns = exclude_patterns or []
        self.max_file_size = max_file_size
        self.logger = logger or logging.getLogger(__name__)
        
        self._running = False
        self._scan_thread: Optional[threading.Thread] = None
    
    def start(self, paths: List[str], interval: int = 300):
        """
        启动后台扫描
        
        Args:
            paths: 要扫描的路径列表
            interval: 扫描间隔（秒）
        """
        if self._running:
            return
        
        self._running = True
        self._scan_thread = threading.Thread(
            target=self._scan_loop,
            args=(paths, interval),
            daemon=True
        )
        self._scan_thread.start()
        self.logger.info(f"目录扫描器已启动，间隔: {interval}s")
    
    def stop(self):
        """停止扫描"""
        self._running = False
        self.logger.info("目录扫描器已停止")
    
    def _scan_loop(self, paths: List[str], interval: int):
        """扫描循环"""
        while self._running:
            try:
                for path in paths:
                    if not self._running:
                        break
                    self._scan_path(path)
                
                # 等待下一次扫描
                for _ in range(interval):
                    if not self._running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                self.logger.error(f"扫描循环异常: {e}")
                time.sleep(10)
    
    def _scan_path(self, path: str):
        """扫描单个路径"""
        try:
            from fnmatch import fnmatch
            
            path_obj = Path(path)
            if not path_obj.exists():
                return
            
            files_to_prefetch = []
            
            for file_path in path_obj.rglob('*'):
                if not self._running:
                    break
                
                if not file_path.is_file():
                    continue
                
                file_path_str = str(file_path)
                
                # 排除模式检查
                skip = False
                for pattern in self.exclude_patterns:
                    if fnmatch(file_path.name, pattern) or fnmatch(file_path_str, pattern):
                        skip = True
                        break
                if skip:
                    continue
                
                # 文件大小检查
                try:
                    size = file_path.stat().st_size
                    if size > self.max_file_size:
                        continue
                except Exception:
                    continue
                
                files_to_prefetch.append(file_path_str)
            
            # 批量添加到预取队列
            if files_to_prefetch:
                self.prefetcher.prefetch_batch(
                    files_to_prefetch, 
                    PrefetchPriority.LOW
                )
                self.logger.debug(f"扫描完成: {path}, 发现 {len(files_to_prefetch)} 个文件")
                
        except Exception as e:
            self.logger.warning(f"扫描路径失败 {path}: {e}")
