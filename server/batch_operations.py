"""
数据库批量操作优化模块
提供高效的批量插入、更新和查询功能
"""

import logging
import threading
import time
from typing import Dict, List, Any, Optional, Tuple, Callable
from collections import defaultdict
from dataclasses import dataclass, field
from queue import Queue
import json

from database import DatabaseManager


@dataclass
class BatchInsertTask:
    """批量插入任务"""
    table: str
    columns: List[str]
    values: List[Tuple]
    callback: Optional[Callable[[bool, int], None]] = None
    timestamp: float = field(default_factory=time.time)


class BatchInserter:
    """
    批量插入器
    自动聚合插入操作，定期批量执行
    """
    
    def __init__(self, db_manager: DatabaseManager,
                 batch_size: int = 1000,
                 flush_interval: float = 5.0,
                 logger: Optional[logging.Logger] = None):
        self.db = db_manager
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.logger = logger or logging.getLogger(__name__)
        
        # 按表分组的待插入数据
        self._pending: Dict[str, List[BatchInsertTask]] = defaultdict(list)
        self._pending_lock = threading.Lock()
        
        # 后台刷新线程
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None
        
        # 统计
        self.stats = {
            'total_inserted': 0,
            'total_batches': 0,
            'total_time': 0,
        }
    
    def start(self):
        """启动批量插入器"""
        if self._running:
            return
        
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        self.logger.info(f"批量插入器已启动 (batch_size={self.batch_size})")
    
    def stop(self):
        """停止批量插入器，刷新所有待处理数据"""
        self._running = False
        
        # 最终刷新
        self._flush_all()
        
        if self._flush_thread:
            self._flush_thread.join(timeout=10)
        
        self.logger.info("批量插入器已停止")
    
    def _flush_loop(self):
        """定期刷新循环"""
        while self._running:
            time.sleep(self.flush_interval)
            try:
                self._flush_all()
            except Exception as e:
                self.logger.error(f"批量刷新异常: {e}")
    
    def add(self, table: str, columns: List[str], values: Tuple,
            callback: Optional[Callable[[bool, int], None]] = None):
        """
        添加单条插入任务
        
        Args:
            table: 表名
            columns: 列名列表
            values: 值元组
            callback: 完成回调 (success, rowcount)
        """
        task = BatchInsertTask(table, columns, [values], callback)
        
        with self._pending_lock:
            self._pending[table].append(task)
            
            # 检查是否达到批量大小
            total_rows = sum(len(t.values) for t in self._pending[table])
            if total_rows >= self.batch_size:
                self._flush_table(table)
    
    def add_many(self, table: str, columns: List[str], values_list: List[Tuple],
                 callback: Optional[Callable[[bool, int], None]] = None):
        """
        添加多条插入任务
        
        Args:
            table: 表名
            columns: 列名列表
            values_list: 值元组列表
            callback: 完成回调
        """
        # 分割成多个批次
        for i in range(0, len(values_list), self.batch_size):
            batch = values_list[i:i + self.batch_size]
            task = BatchInsertTask(table, columns, batch, callback)
            
            with self._pending_lock:
                self._pending[table].append(task)
    
    def _flush_table(self, table: str):
        """刷新单个表的数据"""
        with self._pending_lock:
            tasks = self._pending[table]
            self._pending[table] = []
        
        if not tasks:
            return
        
        # 合并相同表结构的任务
        grouped = defaultdict(list)
        for task in tasks:
            key = tuple(task.columns)
            grouped[key].extend(task.values)
        
        # 执行批量插入
        for columns, values in grouped.items():
            self._execute_batch_insert(table, list(columns), values, tasks)
    
    def _execute_batch_insert(self, table: str, columns: List[str],
                              values: List[Tuple], tasks: List[BatchInsertTask]):
        """执行批量插入"""
        if not values:
            return
        
        start_time = time.time()
        
        try:
            # 构建INSERT语句
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
            
            # 使用executemany批量插入
            rowcount = self.db.execute_many(query, values)
            
            elapsed = time.time() - start_time
            
            # 更新统计
            self.stats['total_inserted'] += rowcount
            self.stats['total_batches'] += 1
            self.stats['total_time'] += elapsed
            
            self.logger.debug(
                f"批量插入完成: {table} {rowcount}行 "
                f"({elapsed:.3f}s, {len(values)}条/批)"
            )
            
            # 调用回调
            for task in tasks:
                if task.callback:
                    try:
                        task.callback(True, rowcount)
                    except Exception as e:
                        self.logger.warning(f"回调执行失败: {e}")
            
        except Exception as e:
            self.logger.error(f"批量插入失败 {table}: {e}")
            
            # 调用回调通知失败
            for task in tasks:
                if task.callback:
                    try:
                        task.callback(False, 0)
                    except Exception:
                        pass
    
    def _flush_all(self):
        """刷新所有表的数据"""
        with self._pending_lock:
            tables = list(self._pending.keys())
        
        for table in tables:
            try:
                self._flush_table(table)
            except Exception as e:
                self.logger.error(f"刷新表失败 {table}: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        avg_time = (
            self.stats['total_time'] / self.stats['total_batches']
            if self.stats['total_batches'] > 0 else 0
        )
        return {
            **self.stats,
            'avg_batch_time': f"{avg_time:.3f}s",
            'pending_tables': len(self._pending),
        }


class VersionBlocksBatchInserter:
    """
    version_blocks表专用批量插入器
    优化块映射记录的批量写入
    """
    
    def __init__(self, db_manager: DatabaseManager,
                 batch_size: int = 500,
                 logger: Optional[logging.Logger] = None):
        self.db = db_manager
        self.batch_size = batch_size
        self.logger = logger or logging.getLogger(__name__)
        
        self._buffer: List[Tuple] = []
        self._buffer_lock = threading.Lock()
        self._total_inserted = 0
    
    def add(self, version_id: int, block_index: int, block_hash: str,
            block_offset: int, block_size: int):
        """添加块记录"""
        with self._buffer_lock:
            self._buffer.append((
                version_id, block_index, block_hash,
                block_offset, block_size
            ))
            
            if len(self._buffer) >= self.batch_size:
                self._flush()
    
    def add_many(self, version_id: int, blocks: List[Dict[str, Any]]):
        """批量添加块记录"""
        with self._buffer_lock:
            for block in blocks:
                self._buffer.append((
                    version_id,
                    block['block_index'],
                    block['block_hash'],
                    block.get('block_offset', 0),
                    block.get('block_size', 0)
                ))
            
            if len(self._buffer) >= self.batch_size:
                self._flush()
    
    def _flush(self):
        """刷新缓冲区"""
        with self._buffer_lock:
            if not self._buffer:
                return
            
            batch = self._buffer[:self.batch_size]
            self._buffer = self._buffer[self.batch_size:]
        
        try:
            query = """
                INSERT INTO version_blocks
                    (version_id, block_index, block_hash, block_offset, block_size)
                VALUES (%s, %s, %s, %s, %s)
            """
            
            rowcount = self.db.execute_many(query, batch)
            self._total_inserted += rowcount
            
            self.logger.debug(f"version_blocks批量插入: {rowcount}行")
            
        except Exception as e:
            self.logger.error(f"version_blocks批量插入失败: {e}")
    
    def close(self):
        """关闭并刷新剩余数据"""
        with self._buffer_lock:
            while self._buffer:
                self._flush()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计"""
        return {
            'total_inserted': self._total_inserted,
            'buffer_size': len(self._buffer),
        }


class DataBlocksBatchManager:
    """
    data_blocks表批量管理器
    优化块去重记录的批量操作
    """
    
    def __init__(self, db_manager: DatabaseManager,
                 batch_size: int = 500,
                 logger: Optional[logging.Logger] = None):
        self.db = db_manager
        self.batch_size = batch_size
        self.logger = logger or logging.getLogger(__name__)
        
        # 待插入缓冲区
        self._insert_buffer: List[Tuple] = []
        # 待更新引用计数的块
        self._refcount_updates: Dict[str, int] = {}
        
        self._lock = threading.Lock()
        self._total_inserted = 0
        self._total_updated = 0
    
    def add_block(self, block_hash: str, block_size: int, stored_size: int,
                  storage_path: str, is_compressed: bool = False):
        """
        添加块记录
        如果块已存在，则增加引用计数
        """
        with self._lock:
            # 检查是否已在本次批次中
            for i, (bh, _, _, _, _) in enumerate(self._insert_buffer):
                if bh == block_hash:
                    # 已在插入缓冲区，增加引用计数
                    self._refcount_updates[block_hash] = self._refcount_updates.get(block_hash, 0) + 1
                    return
            
            # 添加到插入缓冲区
            self._insert_buffer.append((
                block_hash, block_size, stored_size,
                storage_path, 1 if is_compressed else 0, 1  # ref_count=1
            ))
            
            if len(self._insert_buffer) >= self.batch_size:
                self._flush_inserts()
    
    def increment_refcount(self, block_hash: str, count: int = 1):
        """增加引用计数"""
        with self._lock:
            self._refcount_updates[block_hash] = self._refcount_updates.get(block_hash, 0) + count
            
            if len(self._refcount_updates) >= self.batch_size:
                self._flush_refcount_updates()
    
    def _flush_inserts(self):
        """刷新插入缓冲区"""
        with self._lock:
            if not self._insert_buffer:
                return
            
            batch = self._insert_buffer[:self.batch_size]
            self._insert_buffer = self._insert_buffer[self.batch_size:]
        
        try:
            # 使用INSERT IGNORE避免重复键错误
            query = """
                INSERT IGNORE INTO data_blocks
                    (block_hash, block_size, stored_size, storage_path, is_compressed, ref_count)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            rowcount = self.db.execute_many(query, batch)
            self._total_inserted += rowcount
            
            self.logger.debug(f"data_blocks批量插入: {rowcount}行")
            
        except Exception as e:
            self.logger.error(f"data_blocks批量插入失败: {e}")
    
    def _flush_refcount_updates(self):
        """刷新引用计数更新"""
        with self._lock:
            if not self._refcount_updates:
                return
            
            updates = list(self._refcount_updates.items())
            self._refcount_updates = {}
        
        try:
            # 构建CASE语句批量更新
            cases = []
            hashes = []
            for block_hash, count in updates:
                cases.append(f"WHEN %s THEN ref_count + {count}")
                hashes.append(block_hash)
            
            if cases:
                query = f"""
                    UPDATE data_blocks
                    SET ref_count = CASE block_hash
                        {' '.join(cases)}
                    END
                    WHERE block_hash IN ({', '.join(['%s'] * len(hashes))})
                """
                
                params = hashes + hashes
                rowcount = self.db.execute_update(query, tuple(params))
                self._total_updated += rowcount
                
                self.logger.debug(f"data_blocks引用计数更新: {rowcount}行")
            
        except Exception as e:
            self.logger.error(f"data_blocks引用计数更新失败: {e}")
    
    def close(self):
        """关闭并刷新所有数据"""
        self._flush_inserts()
        self._flush_refcount_updates()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计"""
        with self._lock:
            return {
                'total_inserted': self._total_inserted,
                'total_updated': self._total_updated,
                'insert_buffer_size': len(self._insert_buffer),
                'refcount_buffer_size': len(self._refcount_updates),
            }


class BulkQueryOptimizer:
    """
    批量查询优化器
    使用IN语句优化批量查询
    """
    
    def __init__(self, db_manager: DatabaseManager,
                 logger: Optional[logging.Logger] = None):
        self.db = db_manager
        self.logger = logger or logging.getLogger(__name__)
    
    def query_blocks_exist(self, block_hashes: List[str]) -> Dict[str, bool]:
        """
        批量查询块是否存在
        
        Args:
            block_hashes: 块哈希列表
            
        Returns:
            {block_hash: exists}
        """
        if not block_hashes:
            return {}
        
        # 分批查询（避免SQL过长）
        batch_size = 1000
        results = {}
        
        for i in range(0, len(block_hashes), batch_size):
            batch = block_hashes[i:i + batch_size]
            
            try:
                placeholders = ', '.join(['%s'] * len(batch))
                query = f"""
                    SELECT block_hash FROM data_blocks
                    WHERE block_hash IN ({placeholders})
                """
                
                rows = self.db.execute_query(query, tuple(batch))
                found = {row['block_hash'] for row in rows}
                
                for bh in batch:
                    results[bh] = bh in found
                
            except Exception as e:
                self.logger.error(f"批量查询块存在性失败: {e}")
                # 默认不存在
                for bh in batch:
                    results[bh] = False
        
        return results
    
    def query_block_storage_paths(self, block_hashes: List[str]) -> Dict[str, str]:
        """
        批量查询块存储路径
        
        Returns:
            {block_hash: storage_path}
        """
        if not block_hashes:
            return {}
        
        batch_size = 1000
        results = {}
        
        for i in range(0, len(block_hashes), batch_size):
            batch = block_hashes[i:i + batch_size]
            
            try:
                placeholders = ', '.join(['%s'] * len(batch))
                query = f"""
                    SELECT block_hash, storage_path FROM data_blocks
                    WHERE block_hash IN ({placeholders})
                """
                
                rows = self.db.execute_query(query, tuple(batch))
                
                for row in rows:
                    results[row['block_hash']] = row['storage_path']
                
            except Exception as e:
                self.logger.error(f"批量查询块路径失败: {e}")
        
        return results


# 使用示例和便捷函数
def create_optimized_batch_inserters(db_manager: DatabaseManager) -> Dict[str, Any]:
    """
    创建优化的批量插入器集合
    
    Returns:
        {
            'batch_inserter': BatchInserter,
            'version_blocks': VersionBlocksBatchInserter,
            'data_blocks': DataBlocksBatchManager,
            'query_optimizer': BulkQueryOptimizer,
        }
    """
    return {
        'batch_inserter': BatchInserter(db_manager),
        'version_blocks': VersionBlocksBatchInserter(db_manager),
        'data_blocks': DataBlocksBatchManager(db_manager),
        'query_optimizer': BulkQueryOptimizer(db_manager),
    }
