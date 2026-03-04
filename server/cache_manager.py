"""
缓存管理模块
提供LRU缓存、热点块缓存和去重优化
"""

import hashlib
import threading
import logging
import time
from typing import Dict, Optional, Any, List, Tuple, Set
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
import json


@dataclass
class CacheEntry:
    """缓存条目"""
    key: str
    value: Any
    size: int
    access_count: int = 0
    last_access: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)


class LRUCache:
    """
    LRU (Least Recently Used) 缓存
    线程安全的LRU缓存实现
    """
    
    def __init__(self, max_size: int = 1000, max_memory_mb: int = 100,
                 logger: Optional[logging.Logger] = None):
        self.max_size = max_size
        self.max_memory = max_memory_mb * 1024 * 1024
        self.logger = logger or logging.getLogger(__name__)
        
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._current_memory = 0
        
        # 统计
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'inserts': 0,
        }
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存值
        
        Args:
            key: 缓存键
            
        Returns:
            缓存值或None
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry:
                # 更新访问信息
                entry.access_count += 1
                entry.last_access = time.time()
                
                # 移动到末尾（最近使用）
                self._cache.move_to_end(key)
                
                self.stats['hits'] += 1
                return entry.value
            
            self.stats['misses'] += 1
            return None
    
    def put(self, key: str, value: Any, size: Optional[int] = None):
        """
        添加缓存值
        
        Args:
            key: 缓存键
            value: 缓存值
            size: 值的大小（字节），用于内存管理
        """
        if size is None:
            # 估算大小
            try:
                size = len(value) if hasattr(value, '__len__') else 1024
            except:
                size = 1024
        
        with self._lock:
            # 如果已存在，更新
            if key in self._cache:
                old_entry = self._cache[key]
                self._current_memory -= old_entry.size
                del self._cache[key]
            
            # 检查是否需要清理
            while (len(self._cache) >= self.max_size or 
                   (self._current_memory + size) > self.max_memory):
                if not self._evict_oldest():
                    break
            
            # 添加新条目
            entry = CacheEntry(key=key, value=value, size=size)
            self._cache[key] = entry
            self._current_memory += size
            self.stats['inserts'] += 1
    
    def _evict_oldest(self) -> bool:
        """驱逐最旧的条目"""
        if not self._cache:
            return False
        
        # 移除第一个（最旧）
        key, entry = self._cache.popitem(last=False)
        self._current_memory -= entry.size
        self.stats['evictions'] += 1
        
        self.logger.debug(f"缓存驱逐: {key}")
        return True
    
    def invalidate(self, key: str):
        """使缓存条目失效"""
        with self._lock:
            entry = self._cache.pop(key, None)
            if entry:
                self._current_memory -= entry.size
    
    def clear(self):
        """清空缓存"""
        with self._lock:
            self._cache.clear()
            self._current_memory = 0
        self.logger.info("缓存已清空")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            total_requests = self.stats['hits'] + self.stats['misses']
            hit_rate = (
                self.stats['hits'] / total_requests * 100
                if total_requests > 0 else 0
            )
            
            return {
                **self.stats,
                'hit_rate': f"{hit_rate:.1f}%",
                'size': len(self._cache),
                'memory_usage_mb': self._current_memory / (1024 * 1024),
                'max_memory_mb': self.max_memory / (1024 * 1024),
            }


class BlockCache:
    """
    块缓存管理器
    缓存热点数据块，加速去重查询
    """
    
    def __init__(self, 
                 max_blocks: int = 10000,
                 max_memory_mb: int = 500,
                 hot_threshold: int = 3,
                 logger: Optional[logging.Logger] = None):
        self.max_blocks = max_blocks
        self.max_memory = max_memory_mb * 1024 * 1024
        self.hot_threshold = hot_threshold
        self.logger = logger or logging.getLogger(__name__)
        
        # 块缓存: block_hash -> block_data
        self._block_cache: Dict[str, bytes] = {}
        self._block_metadata: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        
        # 访问频率统计
        self._access_count: Dict[str, int] = {}
        
        self._current_memory = 0
        
        # 统计
        self.stats = {
            'block_hits': 0,
            'block_misses': 0,
            'hot_blocks': 0,
        }
    
    def get_block(self, block_hash: str) -> Optional[bytes]:
        """
        获取缓存的块数据
        
        Args:
            block_hash: 块哈希
            
        Returns:
            块数据或None
        """
        with self._lock:
            if block_hash in self._block_cache:
                self._access_count[block_hash] = self._access_count.get(block_hash, 0) + 1
                self.stats['block_hits'] += 1
                return self._block_cache[block_hash]
            
            self.stats['block_misses'] += 1
            return None
    
    def cache_block(self, block_hash: str, block_data: bytes,
                    is_hot: bool = False):
        """
        缓存块数据
        
        Args:
            block_hash: 块哈希
            block_data: 块数据
            is_hot: 是否热点块
        """
        with self._lock:
            # 如果已存在，跳过
            if block_hash in self._block_cache:
                return
            
            # 如果不是热点块且缓存已满，跳过
            if not is_hot and len(self._block_cache) >= self.max_blocks:
                return
            
            # 检查内存限制
            data_size = len(block_data)
            if self._current_memory + data_size > self.max_memory:
                self._evict_cold_blocks()
            
            # 再次检查
            if self._current_memory + data_size > self.max_memory:
                return
            
            # 添加缓存
            self._block_cache[block_hash] = block_data
            self._block_metadata[block_hash] = {
                'cached_at': time.time(),
                'is_hot': is_hot,
                'size': data_size,
            }
            self._current_memory += data_size
            
            if is_hot:
                self.stats['hot_blocks'] += 1
    
    def _evict_cold_blocks(self):
        """驱逐冷块"""
        with self._lock:
            # 找出访问次数最少的块
            cold_blocks = [
                (h, self._access_count.get(h, 0))
                for h in self._block_cache.keys()
                if not self._block_metadata[h].get('is_hot', False)
            ]
            
            # 按访问次数排序
            cold_blocks.sort(key=lambda x: x[1])
            
            # 驱逐10%的冷块
            to_evict = len(cold_blocks) // 10
            for block_hash, _ in cold_blocks[:to_evict]:
                self._remove_block(block_hash)
    
    def _remove_block(self, block_hash: str):
        """移除块缓存"""
        if block_hash in self._block_cache:
            data = self._block_cache.pop(block_hash)
            self._current_memory -= len(data)
            self._block_metadata.pop(block_hash, None)
            self._access_count.pop(block_hash, None)
    
    def mark_as_hot(self, block_hash: str):
        """标记块为热点"""
        with self._lock:
            if block_hash in self._block_metadata:
                self._block_metadata[block_hash]['is_hot'] = True
                self.stats['hot_blocks'] += 1
    
    def is_hot_block(self, block_hash: str) -> bool:
        """检查是否是热点块"""
        with self._lock:
            return self._block_metadata.get(block_hash, {}).get('is_hot', False)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            total_requests = self.stats['block_hits'] + self.stats['block_misses']
            hit_rate = (
                self.stats['block_hits'] / total_requests * 100
                if total_requests > 0 else 0
            )
            
            return {
                **self.stats,
                'hit_rate': f"{hit_rate:.1f}%",
                'cached_blocks': len(self._block_cache),
                'memory_usage_mb': self._current_memory / (1024 * 1024),
                'max_memory_mb': self.max_memory / (1024 * 1024),
            }


class DeduplicationIndex:
    """
    去重索引
    加速块去重查询
    """
    
    def __init__(self, 
                 cache_size: int = 100000,
                 bloom_filter_size: int = 1000000,
                 logger: Optional[logging.Logger] = None):
        self.cache_size = cache_size
        self.logger = logger or logging.getLogger(__name__)
        
        # 块存在性缓存
        self._existence_cache: Set[str] = set()
        self._cache_lock = threading.RLock()
        
        # 布隆过滤器（快速判断块可能不存在）
        self._bloom_filter = self._create_bloom_filter(bloom_filter_size)
        
        # 统计
        self.stats = {
            'index_hits': 0,
            'index_misses': 0,
            'bloom_negatives': 0,
            'bloom_false_positives': 0,
        }
    
    def _create_bloom_filter(self, size: int):
        """创建布隆过滤器"""
        try:
            from pybloom_live import BloomFilter
            return BloomFilter(capacity=size, error_rate=0.01)
        except ImportError:
            self.logger.warning("pybloom_live未安装，使用简单集合代替")
            return set()
    
    def check_block_exists(self, block_hash: str) -> Tuple[bool, bool]:
        """
        检查块是否可能存在
        
        Returns:
            (可能存在, 确定存在)
        """
        with self._cache_lock:
            # 检查缓存
            if block_hash in self._existence_cache:
                self.stats['index_hits'] += 1
                return True, True
        
        # 检查布隆过滤器
        if self._bloom_filter:
            if block_hash not in self._bloom_filter:
                self.stats['bloom_negatives'] += 1
                return False, False
        
        # 可能存在，需要查询数据库确认
        self.stats['index_misses'] += 1
        return True, False
    
    def add_block(self, block_hash: str):
        """添加块到索引"""
        with self._cache_lock:
            # 添加到缓存
            if len(self._existence_cache) < self.cache_size:
                self._existence_cache.add(block_hash)
        
        # 添加到布隆过滤器
        if self._bloom_filter and hasattr(self._bloom_filter, 'add'):
            self._bloom_filter.add(block_hash)
    
    def add_blocks_batch(self, block_hashes: List[str]):
        """批量添加块"""
        with self._cache_lock:
            for block_hash in block_hashes:
                if len(self._existence_cache) < self.cache_size:
                    self._existence_cache.add(block_hash)
                
                if self._bloom_filter and hasattr(self._bloom_filter, 'add'):
                    self._bloom_filter.add(block_hash)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            **self.stats,
            'cache_size': len(self._existence_cache),
        }


class CacheManager:
    """
    缓存管理器
    统一管理各种缓存
    """
    
    def __init__(self, 
                 lru_cache_size: int = 1000,
                 block_cache_mb: int = 500,
                 dedup_index_size: int = 100000,
                 logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        
        # 各种缓存
        self.lru_cache = LRUCache(max_size=lru_cache_size, logger=logger)
        self.block_cache = BlockCache(max_memory_mb=block_cache_mb, logger=logger)
        self.dedup_index = DeduplicationIndex(cache_size=dedup_index_size, logger=logger)
        
        # 元数据缓存
        self.metadata_cache: Dict[str, Dict[str, Any]] = {}
        self._meta_lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        """从LRU缓存获取"""
        return self.lru_cache.get(key)
    
    def put(self, key: str, value: Any, size: Optional[int] = None):
        """添加到LRU缓存"""
        self.lru_cache.put(key, value, size)
    
    def get_block(self, block_hash: str) -> Optional[bytes]:
        """获取块数据"""
        return self.block_cache.get_block(block_hash)
    
    def cache_block(self, block_hash: str, block_data: bytes, is_hot: bool = False):
        """缓存块数据"""
        self.block_cache.cache_block(block_hash, block_data, is_hot)
    
    def check_dedup(self, block_hash: str) -> Tuple[bool, bool]:
        """检查去重"""
        return self.dedup_index.check_block_exists(block_hash)
    
    def add_dedup_block(self, block_hash: str):
        """添加去重块"""
        self.dedup_index.add_block(block_hash)
    
    def get_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """获取元数据"""
        with self._meta_lock:
            return self.metadata_cache.get(key)
    
    def set_metadata(self, key: str, metadata: Dict[str, Any]):
        """设置元数据"""
        with self._meta_lock:
            self.metadata_cache[key] = metadata
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取所有缓存统计"""
        return {
            'lru_cache': self.lru_cache.get_stats(),
            'block_cache': self.block_cache.get_stats(),
            'dedup_index': self.dedup_index.get_stats(),
        }
    
    def clear_all(self):
        """清空所有缓存"""
        self.lru_cache.clear()
        with self._meta_lock:
            self.metadata_cache.clear()
        self.logger.info("所有缓存已清空")
