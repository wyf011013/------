"""
异步I/O客户端模块
使用asyncio实现高性能异步文件传输
"""

import asyncio
import hashlib
import logging
import struct
import zlib
from typing import Dict, List, Optional, Callable, Any, Tuple
from pathlib import Path
import time

# 尝试导入aiofiles，如果不存在则使用标准库
try:
    import aiofiles
    HAS_AIOFILES = True
except ImportError:
    HAS_AIOFILES = False

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.protocol import Message, MessageType, ProtocolHandler
from shared.compression import compress_data


class AsyncBackupClient:
    """
    异步备份客户端
    使用asyncio实现高并发文件传输
    """
    
    def __init__(self, host: str, port: int, client_id: str,
                 chunk_size: int = 1024 * 1024,
                 max_concurrent: int = 8,
                 logger: Optional[logging.Logger] = None):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.chunk_size = chunk_size
        self.max_concurrent = max_concurrent
        self.logger = logger or logging.getLogger(__name__)
        
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.connected = False
        
        # 信号量控制并发
        self._send_semaphore = asyncio.Semaphore(max_concurrent)
        
        # 统计
        self.stats = {
            'bytes_sent': 0,
            'bytes_received': 0,
            'files_sent': 0,
            'chunks_sent': 0,
        }
    
    async def connect(self):
        """连接到服务端"""
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=30
            )
            self.connected = True
            self.logger.info(f"已连接到服务端 {self.host}:{self.port}")
            
            # 发送客户端ID
            await self._send_message({
                'type': 'client_id',
                'client_id': self.client_id
            })
            
        except Exception as e:
            self.logger.error(f"连接失败: {e}")
            raise
    
    async def disconnect(self):
        """断开连接"""
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except:
                pass
        self.connected = False
        self.logger.info("已断开连接")
    
    async def _send_message(self, data: dict):
        """发送消息"""
        if not self.writer:
            raise ConnectionError("未连接到服务端")
        
        message = json.dumps(data).encode('utf-8')
        length = struct.pack('!I', len(message))
        
        self.writer.write(length + message)
        await self.writer.drain()
        self.stats['bytes_sent'] += len(length) + len(message)
    
    async def _read_message(self) -> Optional[dict]:
        """读取消息"""
        if not self.reader:
            return None
        
        try:
            # 读取长度
            length_data = await self.reader.readexactly(4)
            length = struct.unpack('!I', length_data)[0]
            
            # 读取消息
            message_data = await self.reader.readexactly(length)
            self.stats['bytes_received'] += 4 + length
            
            return json.loads(message_data.decode('utf-8'))
        except asyncio.IncompleteReadError:
            return None
        except Exception as e:
            self.logger.error(f"读取消息失败: {e}")
            return None
    
    async def send_file_async(self, file_path: str, 
                              compress: bool = True) -> bool:
        """
        异步发送单个文件
        
        Args:
            file_path: 文件路径
            compress: 是否压缩
            
        Returns:
            是否发送成功
        """
        path = Path(file_path)
        if not path.exists():
            self.logger.error(f"文件不存在: {file_path}")
            return False
        
        file_size = path.stat().st_size
        file_hash = await self._calculate_file_hash_async(file_path)
        
        # 发送文件开始消息
        await self._send_message({
            'type': 'file_start',
            'file_path': str(file_path),
            'file_size': file_size,
            'file_hash': file_hash,
        })
        
        # 等待服务端响应
        response = await self._read_message()
        if not response or response.get('status') != 'ready':
            self.logger.error(f"服务端未准备好接收文件: {file_path}")
            return False
        
        # 发送文件块
        chunks_sent = 0
        bytes_sent = 0
        
        if HAS_AIOFILES:
            # 使用aiofiles异步读取
            async with aiofiles.open(file_path, 'rb') as f:
                chunk_index = 0
                while True:
                    chunk = await f.read(self.chunk_size)
                    if not chunk:
                        break
                    
                    # 压缩块
                    if compress and len(chunk) > 1024:
                        chunk_data, was_compressed = compress_data(chunk)
                    else:
                        chunk_data = chunk
                        was_compressed = False
                    
                    # 发送块
                    async with self._send_semaphore:
                        await self._send_chunk(
                            chunk_index, chunk_data, was_compressed
                        )
                    
                    chunks_sent += 1
                    bytes_sent += len(chunk_data)
                    chunk_index += 1
        else:
            # 使用线程池异步读取
            loop = asyncio.get_event_loop()
            chunk_index = 0
            offset = 0
            
            while offset < file_size:
                # 在线程池中读取文件
                chunk = await loop.run_in_executor(
                    None, 
                    self._read_file_chunk_sync, 
                    file_path, 
                    offset, 
                    self.chunk_size
                )
                
                if not chunk:
                    break
                
                # 压缩块
                if compress and len(chunk) > 1024:
                    chunk_data, was_compressed = compress_data(chunk)
                else:
                    chunk_data = chunk
                    was_compressed = False
                
                # 发送块
                async with self._send_semaphore:
                    await self._send_chunk(
                        chunk_index, chunk_data, was_compressed
                    )
                
                chunks_sent += 1
                bytes_sent += len(chunk_data)
                chunk_index += 1
                offset += len(chunk)
        
        # 发送文件完成消息
        await self._send_message({
            'type': 'file_complete',
            'file_path': str(file_path),
            'chunks_sent': chunks_sent,
            'bytes_sent': bytes_sent,
        })
        
        self.stats['files_sent'] += 1
        self.stats['chunks_sent'] += chunks_sent
        
        self.logger.info(f"文件发送完成: {path.name} ({chunks_sent} 块)")
        return True
    
    def _read_file_chunk_sync(self, file_path: str, 
                              offset: int, size: int) -> bytes:
        """同步读取文件块（在线程池中执行）"""
        with open(file_path, 'rb') as f:
            f.seek(offset)
            return f.read(size)
    
    async def _calculate_file_hash_async(self, file_path: str) -> str:
        """异步计算文件哈希"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._calculate_file_hash_sync, file_path
        )
    
    def _calculate_file_hash_sync(self, file_path: str) -> str:
        """同步计算文件哈希"""
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(self.chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    async def _send_chunk(self, chunk_index: int, 
                          chunk_data: bytes, 
                          was_compressed: bool):
        """发送单个块"""
        await self._send_message({
            'type': 'chunk',
            'index': chunk_index,
            'size': len(chunk_data),
            'compressed': was_compressed,
            'data': chunk_data.hex() if was_compressed else chunk_data.decode('latin-1', errors='replace'),
        })
    
    async def send_files_batch(self, file_paths: List[str],
                               max_concurrent: int = 4) -> Dict[str, bool]:
        """
        批量异步发送文件
        
        Args:
            file_paths: 文件路径列表
            max_concurrent: 最大并发数
            
        Returns:
            发送结果字典 {file_path: success}
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results = {}
        
        async def send_with_limit(file_path: str):
            async with semaphore:
                success = await self.send_file_async(file_path)
                results[file_path] = success
        
        # 创建所有任务
        tasks = [send_with_limit(fp) for fp in file_paths]
        
        # 等待所有任务完成
        await asyncio.gather(*tasks, return_exceptions=True)
        
        return results
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            **self.stats,
            'has_aiofiles': HAS_AIOFILES,
        }


class AsyncFileMonitor:
    """
    异步文件监控器
    使用asyncio监控文件变化
    """
    
    def __init__(self, watch_paths: List[str],
                 callback: Optional[Callable[[str, str], Any]] = None,
                 logger: Optional[logging.Logger] = None):
        self.watch_paths = [Path(p) for p in watch_paths]
        self.callback = callback
        self.logger = logger or logging.getLogger(__name__)
        
        self._running = False
        self._file_states: Dict[str, dict] = {}
    
    async def start(self):
        """启动监控"""
        self._running = True
        self.logger.info(f"开始监控路径: {self.watch_paths}")
        
        # 初始扫描
        await self._scan_all()
        
        # 定期扫描
        while self._running:
            await asyncio.sleep(5)  # 每5秒扫描一次
            await self._check_changes()
    
    def stop(self):
        """停止监控"""
        self._running = False
        self.logger.info("文件监控已停止")
    
    async def _scan_all(self):
        """扫描所有监控路径"""
        for watch_path in self.watch_paths:
            if not watch_path.exists():
                continue
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._scan_path_sync, watch_path)
    
    def _scan_path_sync(self, path: Path):
        """同步扫描路径"""
        try:
            for file_path in path.rglob('*'):
                if file_path.is_file():
                    stat = file_path.stat()
                    self._file_states[str(file_path)] = {
                        'mtime': stat.st_mtime,
                        'size': stat.st_size,
                    }
        except Exception as e:
            self.logger.error(f"扫描路径失败 {path}: {e}")
    
    async def _check_changes(self):
        """检查文件变化"""
        loop = asyncio.get_event_loop()
        
        for watch_path in self.watch_paths:
            if not watch_path.exists():
                continue
            
            # 在线程池中检查变化
            changes = await loop.run_in_executor(
                None, self._check_changes_sync, watch_path
            )
            
            # 调用回调
            if self.callback:
                for change_type, file_path in changes:
                    try:
                        self.callback(change_type, file_path)
                    except Exception as e:
                        self.logger.error(f"回调执行失败: {e}")
    
    def _check_changes_sync(self, path: Path) -> List[Tuple[str, str]]:
        """同步检查变化"""
        changes = []
        current_files = set()
        
        try:
            for file_path in path.rglob('*'):
                if not file_path.is_file():
                    continue
                
                file_str = str(file_path)
                current_files.add(file_str)
                
                stat = file_path.stat()
                
                if file_str not in self._file_states:
                    # 新文件
                    changes.append(('created', file_str))
                    self._file_states[file_str] = {
                        'mtime': stat.st_mtime,
                        'size': stat.st_size,
                    }
                elif (self._file_states[file_str]['mtime'] != stat.st_mtime or
                      self._file_states[file_str]['size'] != stat.st_size):
                    # 修改的文件
                    changes.append(('modified', file_str))
                    self._file_states[file_str] = {
                        'mtime': stat.st_mtime,
                        'size': stat.st_size,
                    }
            
            # 检查删除的文件
            for file_str in list(self._file_states.keys()):
                if file_str not in current_files:
                    changes.append(('deleted', file_str))
                    del self._file_states[file_str]
        
        except Exception as e:
            self.logger.error(f"检查变化失败 {path}: {e}")
        
        return changes


# 便捷函数
async def backup_files_async(host: str, port: int, client_id: str,
                             file_paths: List[str],
                             max_concurrent: int = 4) -> Dict[str, bool]:
    """
    便捷函数：异步备份多个文件
    
    Args:
        host: 服务端地址
        port: 服务端端口
        client_id: 客户端ID
        file_paths: 文件路径列表
        max_concurrent: 最大并发数
        
    Returns:
        备份结果字典
    """
    client = AsyncBackupClient(host, port, client_id, 
                               max_concurrent=max_concurrent)
    
    try:
        await client.connect()
        results = await client.send_files_batch(file_paths, max_concurrent)
        return results
    finally:
        await client.disconnect()


# 导入json用于消息序列化
import json
