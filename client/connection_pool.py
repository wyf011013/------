"""
TCP连接池模块
支持多连接并发传输，提高文件备份速度
"""

import socket
import struct
import threading
import logging
import time
from typing import Dict, List, Optional, Tuple, Any
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, Future

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.protocol import Message, MessageType


class PooledConnection:
    """池化连接封装"""
    
    def __init__(self, connection_id: int, host: str, port: int, 
                 client_id: str, logger: logging.Logger):
        self.connection_id = connection_id
        self.host = host
        self.port = port
        self.client_id = client_id
        self.logger = logger
        
        self.socket: Optional[socket.socket] = None
        self.is_connected = False
        self._send_lock = threading.Lock()
        self._last_used = time.time()
        
    def connect(self, timeout: int = 30) -> bool:
        """建立连接"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(timeout)
            # 优化TCP参数
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8 * 1024 * 1024)  # 8MB
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)  # 8MB
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # 启用TCP快速重传
            try:
                self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_QUICKACK, 1)
            except (AttributeError, OSError):
                pass
            
            self.socket.connect((self.host, self.port))
            self.socket.settimeout(None)
            self.is_connected = True
            self._last_used = time.time()
            self.logger.debug(f"连接池连接 #{self.connection_id} 已建立")
            return True
        except Exception as e:
            self.logger.error(f"连接池连接 #{self.connection_id} 建立失败: {e}")
            return False
    
    def send_message(self, message: Message) -> bool:
        """发送消息"""
        if not self.is_connected or not self.socket:
            return False
        
        with self._send_lock:
            try:
                data = message.to_bytes()
                # 动态计算超时
                send_timeout = max(30, 15 * (len(data) / (1024 * 1024)) + 10)
                self.socket.settimeout(send_timeout)
                self.socket.sendall(data)
                self.socket.settimeout(None)
                self._last_used = time.time()
                return True
            except Exception as e:
                self.logger.error(f"连接 #{self.connection_id} 发送失败: {e}")
                self.is_connected = False
                return False
    
    def close(self):
        """关闭连接"""
        self.is_connected = False
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
            self.socket = None
    
    def is_stale(self, timeout: int = 300) -> bool:
        """检查连接是否闲置过久"""
        return (time.time() - self._last_used) > timeout


class ConnectionPool:
    """
    TCP连接池
    管理多个并发连接，支持并行文件传输
    """
    
    def __init__(self, host: str, port: int, client_id: str,
                 max_connections: int = 4, logger: Optional[logging.Logger] = None):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.max_connections = max_connections
        self.logger = logger or logging.getLogger(__name__)
        
        self._connections: Dict[int, PooledConnection] = {}
        self._available: Queue[int] = Queue()
        self._lock = threading.Lock()
        self._connection_counter = 0
        self._shutdown = False
        
        # 启动维护线程
        self._maintenance_thread = threading.Thread(target=self._maintenance_loop, daemon=True)
        self._maintenance_thread.start()
    
    def _create_connection(self) -> Optional[PooledConnection]:
        """创建新连接"""
        with self._lock:
            if len(self._connections) >= self.max_connections:
                return None
            self._connection_counter += 1
            conn_id = self._connection_counter
        
        conn = PooledConnection(conn_id, self.host, self.port, self.client_id, self.logger)
        if conn.connect():
            with self._lock:
                self._connections[conn_id] = conn
            return conn
        return None
    
    def acquire(self, timeout: float = 30.0) -> Optional[PooledConnection]:
        """获取可用连接"""
        start_time = time.time()
        
        while not self._shutdown:
            # 尝试获取现有可用连接
            try:
                conn_id = self._available.get(timeout=0.1)
                conn = self._connections.get(conn_id)
                if conn and conn.is_connected:
                    return conn
            except Empty:
                pass
            
            # 尝试创建新连接
            conn = self._create_connection()
            if conn:
                return conn
            
            # 检查超时
            if time.time() - start_time > timeout:
                self.logger.warning("获取连接超时")
                return None
            
            time.sleep(0.05)
        
        return None
    
    def release(self, conn: PooledConnection):
        """释放连接回池"""
        if conn and conn.is_connected:
            self._available.put(conn.connection_id)
    
    def execute_parallel(self, messages: List[Message], 
                         max_workers: Optional[int] = None) -> List[bool]:
        """
        并行发送多条消息
        
        Args:
            messages: 要发送的消息列表
            max_workers: 最大并发数，默认使用连接池大小
            
        Returns:
            每条消息的发送结果列表
        """
        if not messages:
            return []
        
        max_workers = max_workers or min(len(messages), self.max_connections)
        results = [False] * len(messages)
        
        def send_task(idx_msg):
            idx, msg = idx_msg
            conn = self.acquire(timeout=10.0)
            if conn:
                try:
                    results[idx] = conn.send_message(msg)
                finally:
                    self.release(conn)
            return idx
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(send_task, enumerate(messages)))
        
        return results
    
    def broadcast_message(self, message: Message) -> int:
        """
        向所有连接广播消息
        
        Returns:
            成功发送的连接数
        """
        success_count = 0
        with self._lock:
            connections = list(self._connections.values())
        
        for conn in connections:
            if conn.send_message(message):
                success_count += 1
        
        return success_count
    
    def _maintenance_loop(self):
        """维护线程：清理闲置连接"""
        while not self._shutdown:
            time.sleep(60)  # 每分钟检查一次
            
            stale_connections = []
            with self._lock:
                for conn_id, conn in list(self._connections.items()):
                    if conn.is_stale(timeout=300):  # 5分钟闲置
                        stale_connections.append(conn_id)
            
            for conn_id in stale_connections:
                self.logger.debug(f"关闭闲置连接 #{conn_id}")
                with self._lock:
                    conn = self._connections.pop(conn_id, None)
                if conn:
                    conn.close()
    
    def close_all(self):
        """关闭所有连接"""
        self._shutdown = True
        with self._lock:
            connections = list(self._connections.values())
            self._connections.clear()
        
        for conn in connections:
            conn.close()
        
        self.logger.info("连接池已关闭")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取连接池统计信息"""
        with self._lock:
            total = len(self._connections)
            active = sum(1 for c in self._connections.values() if c.is_connected)
        
        return {
            'total_connections': total,
            'active_connections': active,
            'max_connections': self.max_connections,
            'available_queue_size': self._available.qsize()
        }


class ParallelFileTransfer:
    """
    并行文件传输器
    将大文件分割为多个范围，通过多个连接并行传输
    """
    
    def __init__(self, connection_pool: ConnectionPool, chunk_size: int = 1024 * 1024):
        self.connection_pool = connection_pool
        self.chunk_size = chunk_size
        self.logger = logging.getLogger(__name__)
    
    def transfer_file_parallel(self, file_path: str, file_size: int,
                                client_id: str, version: int,
                                diff_indices: Optional[set] = None) -> bool:
        """
        并行传输文件
        
        Args:
            file_path: 文件路径
            file_size: 文件大小
            client_id: 客户端ID
            version: 版本号
            diff_indices: 差异块索引集合，None表示全量
            
        Returns:
            是否全部传输成功
        """
        try:
            from shared.protocol import ProtocolHandler
            import hashlib
            import os
            
            # 计算总块数
            chunk_count = (file_size + self.chunk_size - 1) // self.chunk_size
            if chunk_count == 0:
                chunk_count = 1
            
            # 确定需要传输的块
            if diff_indices is None:
                chunks_to_send = list(range(chunk_count))
            else:
                chunks_to_send = sorted(diff_indices)
            
            if not chunks_to_send:
                self.logger.info(f"文件无需传输（无差异块）: {file_path}")
                return True
            
            # 计算文件哈希
            file_hash = ""
            try:
                hasher = hashlib.md5()
                with open(file_path, 'rb') as f:
                    while chunk := f.read(1024 * 1024):
                        hasher.update(chunk)
                file_hash = hasher.hexdigest()
            except Exception as e:
                self.logger.warning(f"计算文件哈希失败: {e}")
            
            # 准备所有消息
            messages = []
            chunk_data_cache = {}
            
            with open(file_path, 'rb') as f:
                for chunk_index in chunks_to_send:
                    f.seek(chunk_index * self.chunk_size)
                    data = f.read(self.chunk_size)
                    if not data:
                        break
                    
                    chunk_hash = hashlib.sha256(data).hexdigest()
                    chunk_data_cache[chunk_index] = (data, chunk_hash)
                    
                    # 压缩（如果需要）
                    import zlib
                    is_compressed = False
                    send_data = data
                    if len(data) > 1024:
                        compressed = zlib.compress(data, level=1)  # 快速压缩
                        if len(compressed) < len(data):
                            send_data = compressed
                            is_compressed = True
                    
                    msg = ProtocolHandler.create_file_chunk_message(
                        client_id, file_path, version,
                        chunk_index, chunk_count, chunk_hash,
                        file_hash, file_size, send_data, is_compressed
                    )
                    messages.append((chunk_index, msg))
            
            # 并行发送
            self.logger.info(f"开始并行传输: {file_path} ({len(messages)}块)")
            
            results = {}
            
            def send_chunk(idx_msg):
                idx, msg = idx_msg
                conn = self.connection_pool.acquire(timeout=30.0)
                if conn:
                    try:
                        success = conn.send_message(msg)
                        results[idx] = success
                        return success
                    finally:
                        self.connection_pool.release(conn)
                else:
                    results[idx] = False
                    return False
            
            # 使用线程池并行发送
            max_workers = min(len(messages), self.connection_pool.max_connections)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(send_chunk, messages)
            
            success_count = sum(1 for v in results.values() if v)
            self.logger.info(f"并行传输完成: {file_path} 成功={success_count}/{len(messages)}")
            
            return success_count == len(messages)
            
        except Exception as e:
            self.logger.error(f"并行传输失败: {e}")
            return False
