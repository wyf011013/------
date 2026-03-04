"""
TCP服务器模块
企业级局域网文件自动备份系统 - 服务端核心

功能清单:
  1. UDP发现响应服务 (端口8889)
  2. TCP多客户端并发连接管理
  3. 客户端注册与信息持久化
  4. 块级增量备份 (chunk hash对比 + 差异块传输)
  5. 文件存储去重 (SHA256 + ref_count)
  6. 备份配置管理与下发
  7. OTA升级分发
  8. 远程命令中继
  9. 文件列表请求与响应
 10. 备份控制 (启用/禁用)
 11. 客户端卸载/删除
 12. 系统资源统计采集
 13. 连接超时检测与清理
"""

import socket
import struct
import json
import io
import hashlib
import zlib
import zipfile
import logging
import time
import uuid
import os
import threading
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
# 导入共享协议模块（客户端/服务端统一协议）
# ============================================================
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.protocol import (
    Message, MessageType, ProtocolHandler, ProtocolError,
    DISCOVERY_PORT, DISCOVERY_MAGIC, DISCOVERY_RESPONSE_MAGIC,
    create_discovery_response, parse_discovery_response
)

# 导入服务端数据库与存储模块
from database import (
    DatabaseManager, ClientManager, BackupConfigManager,
    FileVersionManager, SystemLogManager
)
from storage_manager import StorageManager


# ============================================================
# psutil 可选依赖（用于服务端系统资源统计）
# ============================================================
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


# ============================================================
# 常量定义
# ============================================================
DEFAULT_TCP_PORT = 8888                # TCP默认端口
DEFAULT_CHUNK_SIZE = 1024 * 1024       # 块大小 1MB - 优化传输效率
CONNECTION_TIMEOUT = 300               # 连接超时 5分钟
RECV_BUFFER_SIZE = 1024 * 1024         # TCP接收缓冲区 1MB
OTA_CHUNK_SIZE = 256 * 1024            # OTA分包大小 256KB
SERVER_VERSION = '1.0.0'               # 服务端版本号


# ============================================================
# ClientConnection - 单客户端连接处理
# ============================================================
class ClientConnection:
    """
    客户端连接对象
    每个TCP连接对应一个实例, 负责:
    - 消息收发与缓冲区管理
    - 消息类型路由到对应处理方法
    - 跟踪客户端身份与文件传输状态
    """

    def __init__(self, client_socket: socket.socket, address: Tuple[str, int],
                 server: 'BackupServer'):
        """
        初始化客户端连接

        Args:
            client_socket: 客户端套接字
            address: (ip, port)元组
            server: 所属的BackupServer实例
        """
        self.socket = client_socket
        self.address = address
        self.server = server
        self.logger = logging.getLogger(f"Client-{address[0]}:{address[1]}")

        # ---- 客户端身份信息 ----
        self.client_id: Optional[str] = None          # 客户端唯一标识（UUID）
        self.computer_name: Optional[str] = None       # 计算机名
        self.db_client_id: Optional[int] = None        # 数据库自增主键
        self.client_version: Optional[str] = None      # 客户端当前版本
        self.os_type: Optional[str] = None             # 操作系统类型
        self.os_user: Optional[str] = None             # 登录用户名
        self.process_name: Optional[str] = None        # 客户端进程名

        # ---- 连接状态 ----
        self.is_connected = True
        self.last_activity = datetime.utcnow()
        self.last_heartbeat = datetime.utcnow()

        # ---- TCP接收缓冲区（处理粘包/拆包） ----
        self.receive_buffer = bytearray()
        self.header_size = Message.HEADER_SIZE  # 10字节头部

        # ---- 文件传输上下文 {file_path -> transfer_info} ----
        self.file_transfers: Dict[str, Dict[str, Any]] = {}

        # ---- 批量传输上下文 {batch_id -> batch_info} ----
        self._batch_transfers: Dict[str, Dict[str, Any]] = {}

        # ---- 发送锁（保证同一连接的消息发送是串行的） ----
        self._send_lock = threading.Lock()

        self.logger.info(f"新客户端连接已建立: {address}")

    # ----------------------------------------------------------
    # 活动时间更新 / 超时判断
    # ----------------------------------------------------------
    def update_activity(self):
        """更新最后活动时间戳"""
        self.last_activity = datetime.utcnow()

    def is_timeout(self, timeout_seconds: int = CONNECTION_TIMEOUT) -> bool:
        """
        判断连接是否超时（默认5分钟无活动）

        Args:
            timeout_seconds: 超时秒数

        Returns:
            True表示已超时
        """
        return (datetime.utcnow() - self.last_activity).total_seconds() > timeout_seconds

    # ----------------------------------------------------------
    # 消息发送
    # ----------------------------------------------------------
    def send_message(self, message: Message) -> bool:
        """
        线程安全地发送一条协议消息

        Args:
            message: Message对象

        Returns:
            是否发送成功
        """
        with self._send_lock:
            try:
                data = message.to_bytes()
                self.socket.sendall(data)
                self.update_activity()
                self.logger.debug(f"已发送消息: {message.msg_type.name} ({len(data)}B)")
                return True
            except Exception as e:
                self.logger.error(f"发送消息失败: {e}")
                self.close()
                return False

    # ----------------------------------------------------------
    # 数据接收与消息拆分
    # ----------------------------------------------------------
    def receive_data(self, data: bytes):
        """
        将从socket读取的原始字节追加到缓冲区并尝试解析完整消息

        处理流程:
          1. 追加数据到缓冲区
          2. 循环检测缓冲区中是否含有完整消息
          3. 对每条完整消息调用 _handle_message

        Args:
            data: 本次recv到的原始字节
        """
        self.receive_buffer.extend(data)
        self.update_activity()

        # 循环尝试从缓冲区解析完整消息
        while len(self.receive_buffer) >= self.header_size:
            # 读取头部以获取body长度
            header_data = bytes(self.receive_buffer[:self.header_size])
            try:
                magic, version, msg_type, length = struct.unpack('<IBBI', header_data)
            except struct.error:
                self.logger.error("头部解析失败，清空缓冲区")
                self.receive_buffer.clear()
                return

            # 魔数校验
            if magic != Message.MAGIC_NUMBER:
                self.logger.error(f"无效魔数: 0x{magic:08X}，尝试跳过1字节重新对齐")
                # 丢弃1字节后重新扫描
                self.receive_buffer = self.receive_buffer[1:]
                continue

            # 数据完整性检查
            total_len = self.header_size + length
            if len(self.receive_buffer) < total_len:
                # 数据尚未完整到达，等待下次recv
                break

            # 提取完整消息字节
            full_message_bytes = bytes(self.receive_buffer[:total_len])
            self.receive_buffer = self.receive_buffer[total_len:]

            # 反序列化并处理
            try:
                message = Message.from_bytes(full_message_bytes)
                self._handle_message(message)
            except ProtocolError as e:
                self.logger.error(f"协议解析错误: {e}")
            except Exception as e:
                self.logger.error(f"消息处理异常: {e}", exc_info=True)

    # ----------------------------------------------------------
    # 消息路由（核心分发器）
    # ----------------------------------------------------------
    def _handle_message(self, message: Message):
        """
        根据消息类型分发到对应的处理方法

        Args:
            message: 已解析的Message对象
        """
        self.logger.debug(f"收到消息: {message.msg_type.name}")

        handlers = {
            MessageType.CLIENT_REGISTER:        self._handle_register,
            MessageType.CLIENT_HEARTBEAT:       self._handle_heartbeat,
            MessageType.FILE_CHANGE:            self._handle_file_change,
            MessageType.FILE_DATA:              self._handle_file_data,
            MessageType.FILE_CHUNK:             self._handle_file_chunk,
            MessageType.CONFIG_REQUEST:         self._handle_config_request,
            MessageType.CHUNK_HASH_RESPONSE:    self._handle_chunk_hash_response,
            MessageType.BATCH_UPLOAD_START:      self._handle_batch_start,
            MessageType.BATCH_UPLOAD_DATA:       self._handle_batch_data,
            MessageType.OTA_CHECK:              self._handle_ota_check,
            MessageType.REMOTE_COMMAND_RESULT:  self._handle_remote_command_result,
            MessageType.FILE_LIST_RESPONSE:     self._handle_file_list_response,
            MessageType.SYSTEM_INFO:            self._handle_system_info,
            MessageType.ERROR:                  self._handle_error,
        }

        handler = handlers.get(message.msg_type)
        if handler:
            try:
                handler(message)
            except Exception as e:
                self.logger.error(f"处理 {message.msg_type.name} 时异常: {e}", exc_info=True)
        else:
            self.logger.warning(f"未实现的消息类型: {message.msg_type.name} (0x{int(message.msg_type):02X})")

    # ===========================================================
    # [1] 客户端注册 CLIENT_REGISTER (0x01)
    # ===========================================================
    def _handle_register(self, message: Message):
        """
        处理客户端注册请求
        - 解析客户端信息（os_type, os_user, process_name, client_version等）
        - 写入/更新数据库clients表
        - 在服务器内存连接字典中注册
        """
        data = message.data
        self.client_id = data.get('client_id')
        self.computer_name = data.get('computer_name', 'unknown')
        self.os_type = data.get('os_type', '')
        self.os_user = data.get('os_user', '')
        self.process_name = data.get('process_name', '')
        self.client_version = data.get('client_version', '1.0.0')
        ip_address = data.get('ip_address') or self.address[0]

        if not self.client_id:
            self.logger.error("注册失败: 缺少client_id")
            error_msg = ProtocolHandler.create_error_message(
                '', 'REGISTER_FAILED', '缺少client_id字段'
            )
            self.send_message(error_msg)
            return

        # 写入数据库（clients表基本字段）
        self.db_client_id = self.server.client_manager.register_client(
            self.client_id, self.computer_name, ip_address
        )

        # 更新扩展字段（os_type, os_user, process_name, client_version）
        try:
            ext_query = """
                UPDATE clients
                SET os_type = %s, os_user = %s, process_name = %s,
                    client_version = %s, protocol_type = 'TCP',
                    last_heartbeat = NOW()
                WHERE client_id = %s
            """
            self.server.db_manager.execute_update(
                ext_query,
                (self.os_type, self.os_user, self.process_name,
                 self.client_version, self.client_id)
            )
        except Exception as e:
            self.logger.warning(f"更新客户端扩展信息失败: {e}")

        # 在服务器连接字典中注册（覆盖旧连接）
        old_conn = self.server.get_connection(self.client_id)
        if old_conn and old_conn is not self:
            self.logger.info(f"客户端 {self.client_id} 重复连接，关闭旧连接")
            old_conn.close()
        self.server.add_connection(self.client_id, self)

        self.logger.info(
            f"客户端已注册: {self.client_id} "
            f"[{self.computer_name}] {self.os_type} v{self.client_version}"
        )

        # 记录系统日志
        self.server.log_manager.add_log(
            'info',
            f"客户端已连接: {self.computer_name} ({ip_address}) "
            f"OS={self.os_type} User={self.os_user} Ver={self.client_version}",
            self.db_client_id
        )

    # ===========================================================
    # [2] 心跳 CLIENT_HEARTBEAT (0x02)
    # ===========================================================
    def _handle_heartbeat(self, message: Message):
        """
        处理客户端心跳消息
        - 更新last_heartbeat时间
        - 解析并存储客户端系统资源数据（CPU/内存/磁盘）
        """
        if not self.client_id:
            self.logger.warning("收到心跳但客户端未注册，忽略")
            return

        self.last_heartbeat = datetime.utcnow()
        data = message.data

        # 更新数据库状态和心跳时间
        try:
            hb_query = """
                UPDATE clients
                SET status = 'online', last_seen = NOW(), last_heartbeat = NOW()
                WHERE client_id = %s
            """
            self.server.db_manager.execute_update(hb_query, (self.client_id,))
        except Exception as e:
            self.logger.warning(f"更新心跳时间失败: {e}")

        # 保存客户端上报的系统资源信息（供仪表盘展示）
        cpu_percent = data.get('cpu_percent', 0)
        memory_percent = data.get('memory_percent', 0)
        disk_usage = data.get('disk_usage', {})

        self.server._update_client_stats(self.client_id, {
            'cpu_percent': cpu_percent,
            'memory_percent': memory_percent,
            'disk_usage': disk_usage,
            'last_heartbeat': self.last_heartbeat.isoformat() + 'Z'
        })

        self.logger.debug(
            f"心跳: {self.client_id} CPU={cpu_percent}% MEM={memory_percent}%"
        )

    # ===========================================================
    # [3] 文件变化通知 FILE_CHANGE (0x04) - 块级增量入口
    # ===========================================================
    def _handle_file_change(self, message: Message):
        """
        处理文件变化通知
        - 如果数据库中有该文件上一版本的块哈希（version_blocks），
          则发送CHUNK_HASH_REQUEST让客户端计算并返回各块哈希用于增量对比
        - 否则客户端将进行全量上传
        """
        if not self.client_id:
            self.logger.warning("收到文件变化通知但客户端未注册")
            return

        data = message.data
        file_path = data.get('file_path', '')
        change_type = data.get('change_type', 'modified')
        file_size = data.get('file_size', 0)
        file_hash = data.get('file_hash', '')

        self.logger.info(f"文件变化: {file_path} ({change_type}) size={file_size}")

        # 记录系统日志
        self.server.log_manager.add_log(
            'info', f"文件{change_type}: {file_path} ({file_size}B)",
            self.db_client_id
        )

        # 删除类型不需要做增量对比
        if change_type == 'deleted':
            return

        # 查找该文件上一个版本的块映射（用于块级增量对比）
        has_previous_blocks = False
        try:
            latest_version = self.server.file_manager.get_latest_version(
                self.db_client_id, file_path
            )
            if latest_version:
                version_id = latest_version['id']
                # 查询version_blocks表获取旧版本的块哈希
                vb_query = """
                    SELECT block_index, block_hash, block_size
                    FROM version_blocks
                    WHERE version_id = %s
                    ORDER BY block_index ASC
                """
                prev_blocks = self.server.db_manager.execute_query(vb_query, (version_id,))
                if prev_blocks:
                    has_previous_blocks = True
                    # 缓存到传输上下文中，等待客户端返回chunk hash后做对比
                    self.file_transfers[file_path] = {
                        'state': 'awaiting_chunk_hashes',
                        'change_type': change_type,
                        'file_size': file_size,
                        'file_hash': file_hash,
                        'prev_version_id': version_id,
                        'prev_version_number': latest_version['version_number'],
                        'prev_blocks': {b['block_index']: b['block_hash'] for b in prev_blocks},
                        'start_time': datetime.utcnow()
                    }
        except Exception as e:
            self.logger.warning(f"查询上一版本块信息失败: {e}")

        if has_previous_blocks:
            # 发送CHUNK_HASH_REQUEST，请求客户端计算并回传每块的SHA256
            chunk_size = self.server.storage_manager.chunk_size
            hash_req = ProtocolHandler.create_chunk_hash_request(
                self.client_id, file_path, chunk_size
            )
            self.send_message(hash_req)
            self.logger.info(f"已请求块哈希: {file_path} (chunk_size={chunk_size})")
        else:
            # 没有历史版本，客户端将自行全量上传（FILE_CHUNK消息）
            self.logger.debug(f"无历史版本，等待全量上传: {file_path}")

    # ===========================================================
    # [4] 块哈希响应 CHUNK_HASH_RESPONSE (0x0D) - 增量对比核心
    # ===========================================================
    def _handle_chunk_hash_response(self, message: Message):
        """
        处理客户端返回的块哈希列表
        - 将客户端新文件的每块哈希与服务端历史版本逐块对比
        - 找出差异块索引列表
        - 通过BACKUP_STATUS消息告知客户端需要上传哪些块
        """
        if not self.client_id:
            return

        data = message.data
        file_path = data.get('file_path', '')
        new_chunk_hashes: List[str] = data.get('chunk_hashes', [])
        file_size = data.get('file_size', 0)
        file_hash = data.get('file_hash', '')

        transfer = self.file_transfers.get(file_path)
        if not transfer or transfer.get('state') != 'awaiting_chunk_hashes':
            self.logger.warning(f"收到块哈希响应但无对应传输上下文: {file_path}")
            return

        prev_blocks = transfer.get('prev_blocks', {})

        # 逐块对比，找出差异块索引
        diff_indices = []
        for idx, new_hash in enumerate(new_chunk_hashes):
            old_hash = prev_blocks.get(idx)
            if old_hash != new_hash:
                diff_indices.append(idx)

        total_chunks = len(new_chunk_hashes)
        diff_count = len(diff_indices)

        self.logger.info(
            f"块级对比完成: {file_path} 总块数={total_chunks}, "
            f"差异块={diff_count}, 节省={(total_chunks - diff_count)}"
        )

        # 更新传输上下文
        transfer['state'] = 'receiving_chunks'
        transfer['file_size'] = file_size
        transfer['file_hash'] = file_hash
        transfer['new_chunk_hashes'] = new_chunk_hashes
        transfer['diff_indices'] = set(diff_indices)
        transfer['total_chunks'] = total_chunks
        transfer['received_chunks'] = 0
        transfer['version'] = transfer.get('prev_version_number', 0) + 1
        transfer['chunk_paths'] = [''] * total_chunks
        # 对于未变化的块，直接引用旧版本的存储路径
        for idx in range(total_chunks):
            if idx not in transfer['diff_indices']:
                # 通过block_hash从data_blocks表查找存储路径
                old_hash = prev_blocks.get(idx)
                if old_hash:
                    try:
                        blk_query = "SELECT storage_path FROM data_blocks WHERE block_hash = %s LIMIT 1"
                        blk_result = self.server.db_manager.execute_query(blk_query, (old_hash,))
                        if blk_result:
                            transfer['chunk_paths'][idx] = blk_result[0]['storage_path']
                    except Exception as e:
                        self.logger.warning(f"查询块存储路径失败: {e}")
                        # 标记为需要重新上传
                        transfer['diff_indices'].add(idx)

        # 告知客户端需要上传哪些块索引
        status_msg = Message(MessageType.BACKUP_STATUS, {
            'client_id': self.client_id,
            'file_path': file_path,
            'action': 'upload_chunks',
            'diff_indices': list(transfer['diff_indices']),
            'chunk_count': total_chunks,
            'version': transfer['version'],
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        })
        self.send_message(status_msg)

    # ===========================================================
    # [5] 文件块传输 FILE_CHUNK (0x06) - 接收块数据并存储
    # ===========================================================
    def _handle_file_chunk(self, message: Message):
        """
        处理客户端发来的文件块数据
        - 存储块数据（含去重: 写入shared_blocks目录, data_blocks表ref_count+1）
        - 跟踪传输进度
        - 所有块接收完成后完成文件版本记录
        """
        if not self.client_id:
            self.logger.warning("收到文件块但客户端未注册")
            return

        data = message.data
        file_path = data.get('file_path', '')
        version = data.get('version', 1)
        chunk_index = data.get('chunk_index', 0)
        chunk_count = data.get('chunk_count', 1)
        chunk_hash = data.get('chunk_hash', '')
        file_hash = data.get('file_hash', '')
        file_size = data.get('file_size', 0)
        is_compressed = data.get('is_compressed', False)

        # 块数据在binary_payload中（高效二进制传输，不经过base64）
        chunk_data = message.binary_payload

        if not chunk_data:
            self.logger.error(f"文件块数据为空: {file_path} chunk#{chunk_index}")
            return

        # 初始化传输上下文（全量上传场景，或首次收到块时）
        if file_path not in self.file_transfers:
            self.file_transfers[file_path] = {
                'state': 'receiving_chunks',
                'version': version,
                'total_chunks': chunk_count,
                'received_chunks': 0,
                'file_hash': file_hash,
                'file_size': file_size,
                'chunk_paths': [''] * chunk_count,
                'change_type': 'modified',
                'diff_indices': None,  # None表示全量
                'start_time': datetime.utcnow(),
                'new_chunk_hashes': [''] * chunk_count
            }

        transfer = self.file_transfers[file_path]

        try:
            # ---- 先解压，确保在原始数据上计算哈希 ----
            raw_data = chunk_data
            if is_compressed:
                try:
                    raw_data = zlib.decompress(chunk_data)
                except zlib.error:
                    raw_data = chunk_data

            # 在原始数据上计算 SHA256（与客户端一致）
            actual_hash = hashlib.sha256(raw_data).hexdigest()
            if chunk_hash and actual_hash != chunk_hash:
                self.logger.warning(
                    f"块哈希不匹配: {file_path} chunk#{chunk_index} "
                    f"期望={chunk_hash} 实际={actual_hash}"
                )
            block_hash = actual_hash

            # 调用 store_block 存储原始数据（内部处理去重和压缩）
            storage_path, stored_size, was_compressed = \
                self.server.storage_manager.store_block(block_hash, raw_data)

            # 更新data_blocks表（维护ref_count）
            self._upsert_data_block(block_hash, len(raw_data), storage_path, was_compressed)

            # 记录块路径和哈希
            transfer['chunk_paths'][chunk_index] = storage_path
            if transfer.get('new_chunk_hashes'):
                if chunk_index < len(transfer['new_chunk_hashes']):
                    transfer['new_chunk_hashes'][chunk_index] = block_hash

            transfer['received_chunks'] += 1

            self.logger.debug(
                f"块已存储: {file_path} chunk {chunk_index + 1}/{chunk_count} "
                f"({len(raw_data)}B hash={block_hash[:12]}...)"
            )

            # 更新进度
            self.server.update_backup_progress(self.client_id, {
                'type': 'single',
                'current_file': os.path.basename(file_path),
                'received_chunks': transfer['received_chunks'],
                'total_chunks': transfer['total_chunks'],
                'percent': round(transfer['received_chunks'] / max(transfer['total_chunks'], 1) * 100, 1),
            })

            # 检查是否所有需要的块都已收齐
            all_received = False
            if transfer.get('diff_indices') is not None:
                # 增量模式: 只需收齐差异块
                received_diff = sum(
                    1 for i in transfer['diff_indices']
                    if transfer['chunk_paths'][i] != ''
                )
                all_received = (received_diff >= len(transfer['diff_indices']))
            else:
                # 全量模式: 收齐所有块
                all_received = (transfer['received_chunks'] >= transfer['total_chunks'])

            if all_received:
                self._complete_file_transfer(file_path, transfer)
                self.server.clear_backup_progress(self.client_id)

        except Exception as e:
            self.logger.error(f"存储文件块失败: {e}", exc_info=True)
            error_msg = ProtocolHandler.create_error_message(
                self.client_id, 'STORAGE_ERROR',
                f'块存储失败: {file_path} chunk#{chunk_index}',
                str(e)
            )
            self.send_message(error_msg)

    def _upsert_data_block(self, block_hash: str, block_size: int,
                           storage_path: str, is_compressed: bool):
        """
        在data_blocks表中插入或更新块记录
        - 新块: 插入记录, ref_count=1
        - 已有块: ref_count += 1

        Args:
            block_hash: 块SHA256哈希（基于原始未压缩数据）
            block_size: 原始块大小（字节）
            storage_path: 存储路径
            is_compressed: 磁盘上是否以压缩形式存储
        """
        try:
            # 先查是否已存在
            check_query = "SELECT id, ref_count FROM data_blocks WHERE block_hash = %s"
            existing = self.server.db_manager.execute_query(check_query, (block_hash,))

            if existing:
                # 引用计数+1
                update_query = "UPDATE data_blocks SET ref_count = ref_count + 1 WHERE block_hash = %s"
                self.server.db_manager.execute_update(update_query, (block_hash,))
            else:
                # 插入新块记录
                insert_query = """
                    INSERT INTO data_blocks (block_hash, block_size, stored_size,
                                             storage_path, is_compressed, ref_count)
                    VALUES (%s, %s, %s, %s, %s, 1)
                """
                self.server.db_manager.execute_insert(
                    insert_query,
                    (block_hash, block_size, block_size, storage_path,
                     1 if is_compressed else 0)
                )
        except Exception as e:
            self.logger.warning(f"更新data_blocks失败: {e}")

    def _complete_file_transfer(self, file_path: str, transfer: Dict[str, Any]):
        """
        文件传输完成：保存版本元数据、写入file_versions和version_blocks表

        Args:
            file_path: 文件路径
            transfer: 传输上下文字典
        """
        try:
            version = transfer.get('version', 1)
            file_hash = transfer.get('file_hash', '')
            file_size = transfer.get('file_size', 0)
            chunk_paths = transfer.get('chunk_paths', [])
            change_type = transfer.get('change_type', 'modified')
            is_full = transfer.get('diff_indices') is None  # 全量备份标记
            ref_version = transfer.get('prev_version_id') if not is_full else None

            # 保存版本元数据文件到磁盘
            metadata_path = self.server.storage_manager.save_file_version_metadata(
                self.client_id, file_path, version, file_size, file_hash,
                chunk_paths, change_type, is_full_backup=is_full,
                reference_version=ref_version
            )

            # 写入file_versions数据库表
            version_id = self.server.file_manager.add_file_version(
                self.db_client_id, file_path, file_path,
                version, file_size, file_hash, metadata_path,
                change_type, len(chunk_paths), is_full, ref_version
            )

            # 写入version_blocks映射表（记录每个块的哈希和索引）
            chunk_hashes = transfer.get('new_chunk_hashes', [])
            prev_blocks = transfer.get('prev_blocks', {})
            chunk_size_val = self.server.storage_manager.chunk_size

            for idx in range(len(chunk_paths)):
                # 获取该块的哈希
                if idx < len(chunk_hashes) and chunk_hashes[idx]:
                    bh = chunk_hashes[idx]
                elif idx in (prev_blocks or {}):
                    bh = prev_blocks[idx]
                else:
                    bh = ''

                if bh:
                    try:
                        vb_insert = """
                            INSERT INTO version_blocks
                                (version_id, block_index, block_hash, block_offset, block_size)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        self.server.db_manager.execute_insert(
                            vb_insert,
                            (version_id, idx, bh, idx * chunk_size_val, chunk_size_val)
                        )
                    except Exception as e:
                        self.logger.warning(f"写入version_blocks失败 idx={idx}: {e}")

            # 清理传输上下文
            if file_path in self.file_transfers:
                del self.file_transfers[file_path]

            # 记录系统日志
            duration = (datetime.utcnow() - transfer.get('start_time', datetime.utcnow())).total_seconds()
            mode_str = "全量" if is_full else "增量"
            self.server.log_manager.add_log(
                'info',
                f"文件备份完成({mode_str}): {file_path} v{version} "
                f"({file_size}B, {len(chunk_paths)}块, {duration:.1f}s)",
                self.db_client_id
            )

            self.logger.info(
                f"文件传输完成({mode_str}): {file_path} v{version} "
                f"({file_size}B, {duration:.1f}s)"
            )

        except Exception as e:
            self.logger.error(f"完成文件传输失败: {e}", exc_info=True)

    # ===========================================================
    # [6] 旧版FILE_DATA处理（兼容v1协议的base64文件传输）
    # ===========================================================
    def _handle_file_data(self, message: Message):
        """
        处理旧版FILE_DATA消息（base64编码的文件数据）
        兼容v1协议客户端
        """
        if not self.client_id:
            return

        data = message.data
        file_path = data.get('file_path', '')
        version = data.get('version', 1)
        chunk_index = data.get('chunk_index', 0)
        chunk_count = data.get('chunk_count', 1)
        file_hash = data.get('file_hash', '')
        is_compressed = data.get('is_compressed', False)

        # 提取base64编码的文件数据
        import base64
        data_b64 = data.get('data', '')
        if not data_b64:
            self.logger.error(f"FILE_DATA数据为空: {file_path}")
            return

        try:
            file_data = base64.b64decode(data_b64)
            if is_compressed:
                file_data = zlib.decompress(file_data)
        except Exception as e:
            self.logger.error(f"解码文件数据失败: {e}")
            return

        # 初始化传输上下文
        if file_path not in self.file_transfers:
            self.file_transfers[file_path] = {
                'state': 'receiving_chunks',
                'version': version,
                'total_chunks': chunk_count,
                'received_chunks': 0,
                'file_hash': file_hash,
                'file_size': 0,
                'chunk_paths': [''] * chunk_count,
                'change_type': 'modified',
                'diff_indices': None,
                'start_time': datetime.utcnow(),
                'new_chunk_hashes': [''] * chunk_count
            }

        transfer = self.file_transfers[file_path]

        # 存储块（file_data 已是解压后的原始数据）
        try:
            block_hash = hashlib.sha256(file_data).hexdigest()
            storage_path, stored_size, was_compressed = \
                self.server.storage_manager.store_block(block_hash, file_data)
            self._upsert_data_block(block_hash, len(file_data), storage_path, was_compressed)

            transfer['chunk_paths'][chunk_index] = storage_path
            transfer['received_chunks'] += 1
            transfer['file_size'] += len(file_data)
            transfer['new_chunk_hashes'][chunk_index] = block_hash
            self._upsert_data_block(block_hash, len(file_data), storage_path, False)

            if transfer['received_chunks'] >= transfer['total_chunks']:
                self._complete_file_transfer(file_path, transfer)

        except Exception as e:
            self.logger.error(f"处理FILE_DATA块失败: {e}")

    # ===========================================================
    # [7] 批量上传 BATCH_UPLOAD (0x40-0x42)
    # ===========================================================
    def _handle_batch_start(self, message: Message):
        """
        处理批量上传清单:
        - 对每个文件与历史版本做块级增量对比
        - 返回 diff_map 告知客户端需要上传哪些块
        """
        if not self.client_id:
            return

        data = message.data
        batch_id = data.get('batch_id', '')
        files = data.get('files', [])

        self.logger.info(f"收到批量上传清单: batch={batch_id}, 文件数={len(files)}")

        diff_map = {}            # {file_path: [需上传的块索引]}
        versions_map = {}        # {file_path: 新版本号}
        prev_chunk_paths = {}    # {file_path: {idx: storage_path}} 未变块的旧路径
        prev_blocks_map = {}     # {file_path: {idx: block_hash}} 用于 version_blocks

        for entry in files:
            fp = entry.get('file_path', '')
            new_hashes = entry.get('chunk_hashes', [])

            # 查找上一版本块映射
            prev_map = {}
            latest_version_num = 0
            try:
                latest = self.server.file_manager.get_latest_version(
                    self.db_client_id, fp)
                if latest:
                    latest_version_num = latest['version_number']
                    vb_query = """SELECT block_index, block_hash
                                  FROM version_blocks
                                  WHERE version_id = %s ORDER BY block_index"""
                    rows = self.server.db_manager.execute_query(
                        vb_query, (latest['id'],))
                    prev_map = {r['block_index']: r['block_hash'] for r in rows}
            except Exception as e:
                self.logger.warning(f"查询历史版本失败 {fp}: {e}")

            # 逐块对比
            diff_indices = []
            reuse_paths = {}
            for idx, new_hash in enumerate(new_hashes):
                old_hash = prev_map.get(idx)
                if old_hash == new_hash:
                    # 未变化块，查找旧存储路径复用
                    try:
                        blk = self.server.db_manager.execute_query(
                            "SELECT storage_path FROM data_blocks WHERE block_hash = %s LIMIT 1",
                            (old_hash,))
                        if blk:
                            reuse_paths[idx] = blk[0]['storage_path']
                            continue
                    except Exception:
                        pass
                diff_indices.append(idx)

            diff_map[fp] = diff_indices
            versions_map[fp] = latest_version_num + 1
            prev_chunk_paths[fp] = reuse_paths
            prev_blocks_map[fp] = prev_map

        total_diff = sum(len(v) for v in diff_map.values())
        total_reuse = sum(len(v) for v in prev_chunk_paths.values())
        self.logger.info(
            f"批量增量对比完成: batch={batch_id}, "
            f"差异块={total_diff}, 复用块={total_reuse}")

        # 保存批次上下文
        self._batch_transfers[batch_id] = {
            'files': files,
            'diff_map': diff_map,
            'versions_map': versions_map,
            'prev_chunk_paths': prev_chunk_paths,
            'prev_blocks_map': prev_blocks_map,
            'received_data': bytearray(),
            'total_data_segs': 0,
            'received_data_segs': 0,
            'start_time': datetime.utcnow(),
        }

        # 返回 diff_map 给客户端
        ack = ProtocolHandler.create_batch_upload_result(
            self.client_id, batch_id, True, diff_map=diff_map)
        self.send_message(ack)

    def _handle_batch_data(self, message: Message):
        """接收批量上传数据段，收齐后处理"""
        if not self.client_id:
            return

        data = message.data
        batch_id = data.get('batch_id', '')
        chunk_index = data.get('chunk_index', 0)
        chunk_count = data.get('chunk_count', 1)

        batch = self._batch_transfers.get(batch_id)
        if not batch:
            self.logger.warning(f"未知批次数据: {batch_id}")
            return

        batch['total_data_segs'] = chunk_count
        batch['received_data'].extend(message.binary_payload)
        batch['received_data_segs'] += 1

        # 更新进度
        self.server.update_backup_progress(self.client_id, {
            'type': 'batch',
            'total_files': len(batch['files']),
            'received_segs': batch['received_data_segs'],
            'total_segs': chunk_count,
            'percent': round(batch['received_data_segs'] / max(chunk_count, 1) * 100, 1),
        })

        self.logger.debug(
            f"批量数据段: batch={batch_id} "
            f"seg {batch['received_data_segs']}/{chunk_count}")

        if batch['received_data_segs'] >= chunk_count:
            self._process_batch(batch_id)

    def _process_batch(self, batch_id: str):
        """解压 zip 包，逐文件存储块数据并创建版本记录"""
        batch = self._batch_transfers.pop(batch_id, None)
        if not batch:
            return

        files = batch['files']
        diff_map = batch['diff_map']
        versions_map = batch['versions_map']
        prev_chunk_paths = batch['prev_chunk_paths']
        prev_blocks_map = batch['prev_blocks_map']
        archive_bytes = bytes(batch['received_data'])

        results = []
        self.logger.info(
            f"开始处理批量包: batch={batch_id}, "
            f"包大小={len(archive_bytes)}")

        try:
            zf = zipfile.ZipFile(io.BytesIO(archive_bytes), 'r')
        except Exception as e:
            self.logger.error(f"批量 zip 解压失败: {e}")
            ack = ProtocolHandler.create_batch_upload_result(
                self.client_id, batch_id, False)
            self.send_message(ack)
            self.server.clear_backup_progress(self.client_id)
            return

        with zf:
            for file_idx, entry in enumerate(files):
                fp = entry.get('file_path', '')
                file_size = entry.get('file_size', 0)
                file_hash = entry.get('file_hash', '')
                change_type = entry.get('change_type', 'modified')
                chunk_hashes = entry.get('chunk_hashes', [])
                version = versions_map.get(fp, 1)
                diff_indices = set(diff_map.get(fp, []))
                reuse_paths = prev_chunk_paths.get(fp, {})
                prev_blocks = prev_blocks_map.get(fp, {})

                chunk_paths = [''] * len(chunk_hashes)
                ok = True

                for idx in range(len(chunk_hashes)):
                    if idx not in diff_indices:
                        # 复用旧块
                        sp = reuse_paths.get(idx, '')
                        if sp:
                            chunk_paths[idx] = sp
                        continue

                    # 从 zip 读差异块
                    entry_name = f"{file_idx}/{idx}"
                    try:
                        raw_data = zf.read(entry_name)
                    except KeyError:
                        self.logger.warning(
                            f"zip 中缺少条目: {entry_name} (batch={batch_id})")
                        ok = False
                        break

                    # 校验哈希
                    actual_hash = hashlib.sha256(raw_data).hexdigest()
                    expected_hash = chunk_hashes[idx] if idx < len(chunk_hashes) else ''
                    if expected_hash and actual_hash != expected_hash:
                        self.logger.warning(
                            f"批量块哈希不匹配: {fp} chunk#{idx}")

                    block_hash = actual_hash
                    storage_path, stored_size, was_compressed = \
                        self.server.storage_manager.store_block(block_hash, raw_data)
                    self._upsert_data_block(
                        block_hash, len(raw_data), storage_path, was_compressed)
                    chunk_paths[idx] = storage_path

                if not ok:
                    results.append({'file_path': fp, 'success': False, 'version': version})
                    continue

                # 判断是否为全量（没有历史版本或所有块都是差异）
                is_full = len(diff_indices) == len(chunk_hashes)

                try:
                    # 保存元数据
                    metadata_path = self.server.storage_manager.save_file_version_metadata(
                        self.client_id, fp, version, file_size, file_hash,
                        chunk_paths, change_type, is_full_backup=is_full)

                    # 写入 file_versions 表
                    version_id = self.server.file_manager.add_file_version(
                        self.db_client_id, fp, fp,
                        version, file_size, file_hash, metadata_path,
                        change_type, len(chunk_paths), is_full, None)

                    # 写入 version_blocks 表
                    chunk_size_val = self.server.storage_manager.chunk_size
                    for idx in range(len(chunk_paths)):
                        bh = chunk_hashes[idx] if idx < len(chunk_hashes) else ''
                        if not bh:
                            bh = prev_blocks.get(idx, '')
                        if bh and version_id:
                            try:
                                self.server.db_manager.execute_insert(
                                    """INSERT INTO version_blocks
                                       (version_id, block_index, block_hash,
                                        block_offset, block_size)
                                       VALUES (%s, %s, %s, %s, %s)""",
                                    (version_id, idx, bh,
                                     idx * chunk_size_val, chunk_size_val))
                            except Exception:
                                pass

                    self.logger.info(f"批量文件版本已记录: {fp} v{version}")
                    results.append({
                        'file_path': fp, 'success': True,
                        'version': version, 'file_hash': file_hash})

                    # 记录系统日志
                    self.server.log_manager.add_log(
                        'info',
                        f"批量备份: {fp} v{version} ({file_size}B)",
                        self.db_client_id)

                except Exception as e:
                    self.logger.error(f"批量文件版本写入失败 {fp}: {e}")
                    results.append({'file_path': fp, 'success': False, 'version': version})

                # 更新进度
                self.server.update_backup_progress(self.client_id, {
                    'type': 'batch',
                    'total_files': len(files),
                    'processed_files': file_idx + 1,
                    'current_file': os.path.basename(fp),
                    'percent': round((file_idx + 1) / max(len(files), 1) * 100, 1),
                })

        # 发送最终结果
        ack = ProtocolHandler.create_batch_upload_result(
            self.client_id, batch_id, True, results=results)
        self.send_message(ack)

        # 清除进度
        self.server.clear_backup_progress(self.client_id)

        ok_count = sum(1 for r in results if r.get('success'))
        elapsed = (datetime.utcnow() - batch.get('start_time', datetime.utcnow())).total_seconds()
        self.logger.info(
            f"批量上传处理完成: batch={batch_id}, "
            f"成功={ok_count}/{len(results)}, 耗时={elapsed:.1f}s")

    # ===========================================================
    # [8] 配置请求 CONFIG_REQUEST (0x09)
    # ===========================================================
    def _handle_config_request(self, message: Message):
        """
        处理客户端的配置请求
        - 从数据库backup_configs表查询该客户端的备份配置
        - 回复BACKUP_CONFIG消息
        """
        if not self.client_id:
            self.logger.warning("收到配置请求但客户端未注册")
            return

        config = None
        try:
            config = self.server.config_manager.get_client_config(self.db_client_id)
        except Exception as e:
            self.logger.error(f"查询客户端配置失败: {e}")

        if config:
            config_data = {
                'client_id': self.client_id,
                'backup_paths': config.get('backup_paths', config.get('backup_path', '')),
                'backup_path': config.get('backup_path', ''),
                'file_pattern': config.get('file_pattern', '*'),
                'exclude_patterns': config.get('exclude_patterns', []),
                'max_file_size_mb': config.get('max_file_size_mb', 100),
                'enable_compression': config.get('enable_compression', True),
                'incremental_only': config.get('incremental_only', True),
                'bandwidth_limit_kbps': config.get('bandwidth_limit_kbps', 0),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
        else:
            # 使用服务器默认配置
            defaults = self.server.backup_defaults
            config_data = {
                'client_id': self.client_id,
                'backup_paths': defaults.get('monitor_paths', []),
                'backup_path': defaults.get('monitor_paths', [''])[0] if defaults.get('monitor_paths') else '',
                'file_pattern': defaults.get('file_pattern', '*'),
                'exclude_patterns': defaults.get('exclude_patterns', []),
                'max_file_size_mb': defaults.get('max_file_size_mb', 100),
                'enable_compression': True,
                'incremental_only': True,
                'bandwidth_limit_kbps': 0,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }

        response = Message(MessageType.BACKUP_CONFIG, config_data)
        self.send_message(response)
        self.logger.info(f"配置已发送给客户端: {self.client_id}")

    # ===========================================================
    # [8] OTA版本检查 OTA_CHECK (0x10)
    # ===========================================================
    def _handle_ota_check(self, message: Message):
        """
        处理客户端OTA版本检查请求
        - 查询ota_versions表中最新的活跃版本
        - 与客户端当前版本对比
        - 若有更新, 返回OTA_RESPONSE并随后发送OTA_DATA分块
        """
        if not self.client_id:
            return

        data = message.data
        current_version = data.get('current_version', '0.0.0')

        # 查询最新活跃的OTA版本
        try:
            ota_query = """
                SELECT id, version, file_path, file_size, file_hash, changelog
                FROM ota_versions
                WHERE is_active = 1
                ORDER BY id DESC
                LIMIT 1
            """
            ota_result = self.server.db_manager.execute_query(ota_query)
        except Exception as e:
            self.logger.error(f"查询OTA版本失败: {e}")
            ota_result = []

        if ota_result:
            latest = ota_result[0]
            latest_version = latest['version']

            # 简单版本对比（字符串比较，适用于语义化版本号）
            has_update = self._compare_versions(current_version, latest_version) < 0

            if has_update:
                # 发送OTA_RESPONSE通知有更新
                ota_resp = ProtocolHandler.create_ota_response_message(
                    self.client_id,
                    has_update=True,
                    new_version=latest_version,
                    file_size=latest.get('file_size', 0),
                    file_hash=latest.get('file_hash', ''),
                    changelog=latest.get('changelog', '')
                )
                self.send_message(ota_resp)
                self.logger.info(
                    f"OTA更新可用: {self.client_id} {current_version} -> {latest_version}"
                )

                # 异步发送OTA数据
                threading.Thread(
                    target=self._send_ota_data,
                    args=(latest_version, latest['file_path'],),
                    daemon=True
                ).start()
            else:
                # 已是最新版本
                ota_resp = ProtocolHandler.create_ota_response_message(
                    self.client_id, has_update=False,
                    new_version=latest_version
                )
                self.send_message(ota_resp)
                self.logger.info(f"客户端已是最新版本: {self.client_id} v{current_version}")
        else:
            # 没有OTA版本信息
            ota_resp = ProtocolHandler.create_ota_response_message(
                self.client_id, has_update=False
            )
            self.send_message(ota_resp)

    def _compare_versions(self, v1: str, v2: str) -> int:
        """
        比较两个语义化版本号

        Args:
            v1: 版本1 (如 '1.0.0')
            v2: 版本2 (如 '1.1.0')

        Returns:
            -1 if v1<v2, 0 if v1==v2, 1 if v1>v2
        """
        try:
            parts1 = [int(x) for x in v1.split('.')]
            parts2 = [int(x) for x in v2.split('.')]
            # 补齐长度
            while len(parts1) < 3:
                parts1.append(0)
            while len(parts2) < 3:
                parts2.append(0)
            for a, b in zip(parts1, parts2):
                if a < b:
                    return -1
                elif a > b:
                    return 1
            return 0
        except (ValueError, AttributeError):
            # 无法解析时做字符串对比
            if v1 < v2:
                return -1
            elif v1 > v2:
                return 1
            return 0

    def _send_ota_data(self, version: str, file_path: str):
        """
        将OTA升级文件分块发送给客户端

        Args:
            version: OTA版本号
            file_path: 升级包文件路径
        """
        try:
            if not os.path.isfile(file_path):
                self.logger.error(f"OTA文件不存在: {file_path}")
                return

            total_size = os.path.getsize(file_path)
            chunk_count = (total_size + OTA_CHUNK_SIZE - 1) // OTA_CHUNK_SIZE

            self.logger.info(
                f"开始发送OTA数据: {file_path} ({total_size}B, {chunk_count}块)"
            )

            with open(file_path, 'rb') as f:
                for idx in range(chunk_count):
                    if not self.is_connected:
                        self.logger.warning("OTA传输中断: 客户端已断开")
                        return

                    chunk = f.read(OTA_CHUNK_SIZE)
                    ota_msg = ProtocolHandler.create_ota_data_message(
                        self.client_id, version, idx, chunk_count, chunk
                    )
                    if not self.send_message(ota_msg):
                        self.logger.error(f"OTA块发送失败: chunk#{idx}")
                        return

            self.logger.info(f"OTA数据发送完成: {version} -> {self.client_id}")

        except Exception as e:
            self.logger.error(f"发送OTA数据异常: {e}", exc_info=True)

    # ===========================================================
    # [9] 远程命令结果 REMOTE_COMMAND_RESULT (0x21)
    # ===========================================================
    def _handle_remote_command_result(self, message: Message):
        """
        处理客户端返回的远程命令执行结果
        - 存储到服务器pending字典中供Web API查询
        - 记录系统日志
        """
        if not self.client_id:
            return

        data = message.data
        command_id = data.get('command_id', '')
        return_code = data.get('return_code', -1)
        stdout = data.get('stdout', '')
        stderr = data.get('stderr', '')

        self.logger.info(
            f"远程命令结果: {self.client_id} cmd_id={command_id} rc={return_code}"
        )

        # 存储到服务器的待取结果字典
        self.server.store_command_result(command_id, {
            'client_id': self.client_id,
            'command_id': command_id,
            'return_code': return_code,
            'stdout': stdout,
            'stderr': stderr,
            'received_at': datetime.utcnow().isoformat() + 'Z'
        })

        # 记录系统日志
        self.server.log_manager.add_log(
            'info',
            f"远程命令执行完成: cmd_id={command_id} rc={return_code}",
            self.db_client_id
        )

    # ===========================================================
    # [10] 文件列表响应 FILE_LIST_RESPONSE (0x0B)
    # ===========================================================
    def _handle_file_list_response(self, message: Message):
        """
        处理客户端返回的文件列表
        - 存储到服务器pending字典供Web API消费
        """
        if not self.client_id:
            return

        data = message.data
        path = data.get('path', '')
        entries = data.get('entries', [])

        # 生成唯一请求ID（使用client_id + path组合）
        request_key = f"{self.client_id}:{path}"

        self.server.store_file_list_result(request_key, {
            'client_id': self.client_id,
            'path': path,
            'entries': entries,
            'received_at': datetime.utcnow().isoformat() + 'Z'
        })

        self.logger.info(
            f"文件列表已收到: {self.client_id} path={path} count={len(entries)}"
        )

    # ===========================================================
    # [11] 系统信息上报 SYSTEM_INFO (0x30)
    # ===========================================================
    def _handle_system_info(self, message: Message):
        """
        处理客户端上报的系统详细信息
        """
        if not self.client_id:
            return

        data = message.data
        self.server._update_client_stats(self.client_id, data)
        self.logger.debug(f"系统信息已更新: {self.client_id}")

    # ===========================================================
    # [12] 错误消息 ERROR (0x08)
    # ===========================================================
    def _handle_error(self, message: Message):
        """处理客户端报告的错误"""
        data = message.data
        error_code = data.get('error_code', 'UNKNOWN')
        error_message = data.get('error_message', '')
        details = data.get('details', '')

        self.logger.error(
            f"客户端错误 [{self.client_id}]: [{error_code}] {error_message} | {details}"
        )

        if self.db_client_id:
            self.server.log_manager.add_log(
                'error',
                f"客户端错误: [{error_code}] {error_message}",
                self.db_client_id,
                details
            )

    # ===========================================================
    # 连接关闭
    # ===========================================================
    def close(self):
        """
        关闭客户端连接
        - 更新数据库状态为offline
        - 从服务器连接字典中移除
        - 关闭socket
        """
        if not self.is_connected:
            return

        self.is_connected = False

        # 关闭socket
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self.socket.close()
        except Exception:
            pass

        # 更新数据库状态
        if self.client_id:
            try:
                self.server.client_manager.update_client_status(self.client_id, 'offline')
            except Exception as e:
                self.logger.warning(f"更新离线状态失败: {e}")

            # 从连接字典中移除
            self.server.remove_connection(self.client_id)

            # 记录日志
            self.server.log_manager.add_log(
                'info', f"客户端已断开: {self.computer_name} ({self.client_id})",
                self.db_client_id
            )

        self.logger.info("客户端连接已关闭")


# ============================================================
# BackupServer - 主服务器类
# ============================================================
class BackupServer:
    """
    备份服务器主类

    职责:
    - TCP监听与客户端接入
    - UDP发现服务
    - 连接管理（线程安全）
    - 向客户端推送指令（配置/命令/控制/卸载）
    - 系统统计采集
    - 定时器：超时检测与清理
    """

    def __init__(self, host: str = '0.0.0.0', port: int = DEFAULT_TCP_PORT,
                 db_config: Dict[str, Any] = None,
                 storage_root: str = './backup_storage',
                 server_version: str = SERVER_VERSION,
                 backup_defaults: Dict[str, Any] = None,
                 chunk_size: int = 262144):
        """
        初始化备份服务器

        Args:
            host: TCP监听地址
            port: TCP监听端口
            db_config: 数据库连接配置字典
            storage_root: 文件存储根目录
            server_version: 服务器版本号
            backup_defaults: 默认备份配置
        """
        self.host = host
        self.port = port
        self.server_version = server_version
        self.logger = logging.getLogger('BackupServer')

        # ---- 默认备份配置（无客户端专属配置时使用） ----
        self.backup_defaults = backup_defaults or {
            'monitor_paths': [],
            'file_pattern': '*',
            'exclude_patterns': ['*.tmp', '*.log', '*.lock'],
            'max_file_size_mb': 100
        }

        # ---- 数据库初始化 ----
        self.db_config = db_config or {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'password',
            'database': 'file_backup_system'
        }
        self.db_manager = DatabaseManager(**self.db_config)

        # ---- 各业务管理器 ----
        self.client_manager = ClientManager(self.db_manager)
        self.config_manager = BackupConfigManager(self.db_manager)
        self.file_manager = FileVersionManager(self.db_manager)
        self.log_manager = SystemLogManager(self.db_manager)

        # ---- 存储管理器 ----
        self.storage_manager = StorageManager(storage_root, chunk_size=chunk_size)

        # ---- 活跃连接字典 {client_id: ClientConnection} ----
        self.connections: Dict[str, ClientConnection] = {}
        self._conn_lock = threading.Lock()

        # ---- 服务器套接字 ----
        self._tcp_socket: Optional[socket.socket] = None
        self._udp_socket: Optional[socket.socket] = None

        # ---- 运行状态 ----
        self.running = False
        self._start_time: Optional[float] = None

        # ---- 待取结果缓存（供Web API查询） ----
        self._command_results: Dict[str, Dict[str, Any]] = {}
        self._command_results_lock = threading.Lock()

        self._file_list_results: Dict[str, Dict[str, Any]] = {}
        self._file_list_lock = threading.Lock()

        # ---- 客户端资源统计缓存 {client_id -> stats_dict} ----
        self._client_stats: Dict[str, Dict[str, Any]] = {}
        self._client_stats_lock = threading.Lock()

        # ---- 备份进度追踪 {client_id -> progress_dict} ----
        self._backup_progress: Dict[str, Dict[str, Any]] = {}
        self._backup_progress_lock = threading.Lock()

        self.logger.info(
            f"BackupServer已初始化: TCP={host}:{port} storage={storage_root}"
        )

    # ===========================================================
    # 服务启动
    # ===========================================================
    def start(self):
        """
        启动服务器
        - 启动UDP发现响应线程
        - 启动TCP监听
        - 启动连接超时检测线程
        - 进入TCP主循环
        """
        if self.running:
            self.logger.warning("服务器已在运行中")
            return

        self.running = True
        self._start_time = time.time()

        # 启动UDP发现响应服务
        udp_thread = threading.Thread(target=self._udp_discovery_loop, daemon=True)
        udp_thread.start()
        self.logger.info(f"UDP发现服务已启动: 端口 {DISCOVERY_PORT}")

        # 启动连接超时检测定时器
        timeout_thread = threading.Thread(target=self._timeout_checker_loop, daemon=True)
        timeout_thread.start()
        self.logger.info("连接超时检测线程已启动")

        # 创建TCP服务器套接字
        try:
            self._tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._tcp_socket.bind((self.host, self.port))
            self._tcp_socket.listen(128)
            self._tcp_socket.settimeout(1.0)  # accept超时1秒，便于优雅退出

            self.logger.info(f"TCP服务器已启动，监听 {self.host}:{self.port}")

            # 记录系统日志
            self.log_manager.add_log('info', f"服务器已启动: {self.host}:{self.port}")

            # 进入TCP接受连接主循环
            self._tcp_accept_loop()

        except Exception as e:
            self.logger.error(f"TCP服务器启动失败: {e}", exc_info=True)
            self.stop()
            raise

    # ===========================================================
    # UDP发现响应服务
    # ===========================================================
    def _udp_discovery_loop(self):
        """
        UDP发现响应线程
        - 监听端口8889上的广播包
        - 收到FBKP_DISC魔数后回复服务器地址信息
        - 通过收包的本地接口地址确定回复中的服务器IP
        """
        try:
            self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # 允许接收广播包
            try:
                self._udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            except Exception:
                pass

            self._udp_socket.bind(('0.0.0.0', DISCOVERY_PORT))
            self._udp_socket.settimeout(1.0)

            self.logger.info(f"UDP发现服务监听中: 0.0.0.0:{DISCOVERY_PORT}")

            while self.running:
                try:
                    data, addr = self._udp_socket.recvfrom(1024)

                    # 检查是否为发现请求
                    if data.startswith(DISCOVERY_MAGIC):
                        self.logger.debug(f"收到发现请求: {addr}")

                        # 确定本机在该网络接口上的IP地址
                        server_ip = self._get_local_ip_for_peer(addr[0])

                        # 构建发现响应
                        response = create_discovery_response(
                            server_ip, self.port, self.server_version
                        )

                        self._udp_socket.sendto(response, addr)
                        self.logger.info(
                            f"已回复发现请求: {addr} -> "
                            f"{{host: {server_ip}, port: {self.port}, version: {self.server_version}}}"
                        )

                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        self.logger.error(f"UDP发现服务异常: {e}")
                        time.sleep(1)

        except Exception as e:
            self.logger.error(f"UDP发现服务启动失败: {e}", exc_info=True)
        finally:
            if self._udp_socket:
                try:
                    self._udp_socket.close()
                except Exception:
                    pass

    def _get_local_ip_for_peer(self, peer_ip: str) -> str:
        """
        通过创建临时UDP连接确定本机面向peer_ip的网络接口地址

        Args:
            peer_ip: 对端IP地址

        Returns:
            本机IP地址字符串
        """
        try:
            tmp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            tmp_sock.settimeout(0)
            # 连接到对端IP（不实际发包，仅用于路由查找）
            tmp_sock.connect((peer_ip, 1))
            local_ip = tmp_sock.getsockname()[0]
            tmp_sock.close()
            return local_ip
        except Exception:
            # 回退到主机名解析
            try:
                return socket.gethostbyname(socket.gethostname())
            except Exception:
                return '127.0.0.1'

    # ===========================================================
    # TCP连接接受主循环
    # ===========================================================
    def _tcp_accept_loop(self):
        """
        TCP连接接受主循环
        每接受一个新连接，创建ClientConnection对象并启动独立接收线程
        """
        while self.running:
            try:
                client_socket, address = self._tcp_socket.accept()

                # 设置TCP keepalive
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

                # 创建连接对象
                connection = ClientConnection(client_socket, address, self)

                # 启动接收线程
                recv_thread = threading.Thread(
                    target=self._client_receive_loop,
                    args=(connection,),
                    daemon=True,
                    name=f"recv-{address[0]}:{address[1]}"
                )
                recv_thread.start()

            except socket.timeout:
                # accept超时，继续循环检查running状态
                continue
            except OSError as e:
                if self.running:
                    self.logger.error(f"接受TCP连接失败: {e}")
                    time.sleep(0.5)

    def _client_receive_loop(self, connection: ClientConnection):
        """
        单客户端的TCP接收循环
        持续从socket读取数据并交给connection处理
        连接断开或异常时清理资源

        Args:
            connection: 客户端连接对象
        """
        try:
            while connection.is_connected and self.running:
                try:
                    data = connection.socket.recv(RECV_BUFFER_SIZE)
                    if not data:
                        # 对端正常关闭
                        self.logger.info(f"客户端断开连接: {connection.address}")
                        break
                    connection.receive_data(data)
                except socket.timeout:
                    continue
                except ConnectionResetError:
                    self.logger.warning(f"连接被重置: {connection.address}")
                    break
                except OSError as e:
                    if connection.is_connected:
                        self.logger.error(f"接收数据异常: {connection.address} - {e}")
                    break
        except Exception as e:
            if connection.is_connected:
                self.logger.error(f"客户端接收线程异常: {e}", exc_info=True)
        finally:
            connection.close()

    # ===========================================================
    # 连接超时检测
    # ===========================================================
    def _timeout_checker_loop(self):
        """
        定期检测所有活跃连接是否超时（5分钟无活动）
        超时的连接将被标记为offline并关闭
        """
        while self.running:
            try:
                time.sleep(30)  # 每30秒检查一次

                now = datetime.utcnow()
                timed_out = []

                with self._conn_lock:
                    for client_id, conn in self.connections.items():
                        if conn.is_timeout(CONNECTION_TIMEOUT):
                            timed_out.append((client_id, conn))

                for client_id, conn in timed_out:
                    self.logger.warning(
                        f"连接超时({CONNECTION_TIMEOUT}s): {client_id} "
                        f"最后活动={conn.last_activity.isoformat()}"
                    )
                    conn.close()

            except Exception as e:
                self.logger.error(f"超时检测异常: {e}")

    # ===========================================================
    # 线程安全的连接管理方法
    # ===========================================================
    def add_connection(self, client_id: str, connection: ClientConnection):
        """
        添加或替换客户端连接（线程安全）

        Args:
            client_id: 客户端唯一标识
            connection: ClientConnection对象
        """
        with self._conn_lock:
            self.connections[client_id] = connection
            self.logger.debug(
                f"连接已注册: {client_id} (当前在线: {len(self.connections)})"
            )

    def remove_connection(self, client_id: str):
        """
        移除客户端连接（线程安全）

        Args:
            client_id: 客户端唯一标识
        """
        with self._conn_lock:
            if client_id in self.connections:
                del self.connections[client_id]
                self.logger.debug(
                    f"连接已移除: {client_id} (当前在线: {len(self.connections)})"
                )

    def get_connection(self, client_id: str) -> Optional[ClientConnection]:
        """
        获取指定客户端的连接对象（线程安全）

        Args:
            client_id: 客户端唯一标识

        Returns:
            ClientConnection或None
        """
        with self._conn_lock:
            return self.connections.get(client_id)

    def get_all_connections(self) -> Dict[str, ClientConnection]:
        """
        获取所有活跃连接的快照（线程安全）

        Returns:
            连接字典副本
        """
        with self._conn_lock:
            return dict(self.connections)

    def broadcast_message(self, message: Message, exclude_client: str = None):
        """
        向所有在线客户端广播消息

        Args:
            message: 要广播的消息
            exclude_client: 排除的客户端ID
        """
        conns = self.get_all_connections()
        for client_id, conn in conns.items():
            if client_id != exclude_client and conn.is_connected:
                conn.send_message(message)

    # ===========================================================
    # 向客户端推送备份配置
    # ===========================================================
    def push_config_to_client(self, client_id: str, config_data: Dict[str, Any]) -> bool:
        """
        主动推送备份配置到指定客户端

        Args:
            client_id: 客户端唯一标识（UUID）
            config_data: 配置字典

        Returns:
            是否发送成功
        """
        conn = self.get_connection(client_id)
        if not conn or not conn.is_connected:
            self.logger.warning(f"推送配置失败: 客户端 {client_id} 不在线")
            return False

        config_msg = ProtocolHandler.create_backup_config_message(client_id, config_data)
        return conn.send_message(config_msg)

    # ===========================================================
    # 远程命令下发
    # ===========================================================
    def send_remote_command(self, client_id: str, command: str,
                           timeout: int = 30) -> Optional[str]:
        """
        向指定客户端发送远程命令

        Args:
            client_id: 客户端唯一标识
            command: 要执行的命令字符串
            timeout: 命令超时时间（秒）

        Returns:
            command_id（用于后续查询结果），或None表示发送失败
        """
        conn = self.get_connection(client_id)
        if not conn or not conn.is_connected:
            self.logger.warning(f"发送命令失败: 客户端 {client_id} 不在线")
            return None

        command_id = str(uuid.uuid4())
        cmd_msg = ProtocolHandler.create_remote_command_message(
            client_id, command_id, command, timeout
        )

        if conn.send_message(cmd_msg):
            self.logger.info(f"远程命令已发送: {client_id} cmd_id={command_id} cmd={command}")
            return command_id
        return None

    def store_command_result(self, command_id: str, result: Dict[str, Any]):
        """
        存储远程命令执行结果（供Web API查询）

        Args:
            command_id: 命令唯一ID
            result: 结果字典
        """
        with self._command_results_lock:
            self._command_results[command_id] = result

    def get_command_result(self, command_id: str) -> Optional[Dict[str, Any]]:
        """
        获取远程命令执行结果

        Args:
            command_id: 命令唯一ID

        Returns:
            结果字典或None
        """
        with self._command_results_lock:
            return self._command_results.get(command_id)

    def pop_command_result(self, command_id: str) -> Optional[Dict[str, Any]]:
        """
        获取并移除远程命令执行结果

        Args:
            command_id: 命令唯一ID

        Returns:
            结果字典或None
        """
        with self._command_results_lock:
            return self._command_results.pop(command_id, None)

    # ===========================================================
    # 文件列表请求
    # ===========================================================
    def send_file_list_request(self, client_id: str, path: str = '') -> bool:
        """
        向指定客户端发送文件列表请求

        Args:
            client_id: 客户端唯一标识
            path: 请求的目录路径

        Returns:
            是否发送成功
        """
        conn = self.get_connection(client_id)
        if not conn or not conn.is_connected:
            self.logger.warning(f"发送文件列表请求失败: 客户端 {client_id} 不在线")
            return False

        fl_msg = ProtocolHandler.create_file_list_request(client_id, path)
        return conn.send_message(fl_msg)

    def store_file_list_result(self, request_key: str, result: Dict[str, Any]):
        """
        存储文件列表响应结果

        Args:
            request_key: 请求标识键
            result: 结果字典
        """
        with self._file_list_lock:
            self._file_list_results[request_key] = result

    def get_file_list_result(self, client_id: str, path: str = '') -> Optional[Dict[str, Any]]:
        """
        获取文件列表结果

        Args:
            client_id: 客户端标识
            path: 请求路径

        Returns:
            结果字典或None
        """
        request_key = f"{client_id}:{path}"
        with self._file_list_lock:
            return self._file_list_results.get(request_key)

    def pop_file_list_result(self, client_id: str, path: str = '') -> Optional[Dict[str, Any]]:
        """
        获取并移除文件列表结果

        Args:
            client_id: 客户端标识
            path: 请求路径

        Returns:
            结果字典或None
        """
        request_key = f"{client_id}:{path}"
        with self._file_list_lock:
            return self._file_list_results.pop(request_key, None)

    # ===========================================================
    # 备份控制（启用/禁用）
    # ===========================================================
    def send_backup_control(self, client_id: str, enable: bool) -> bool:
        """
        向指定客户端发送备份控制指令（启用/禁用备份）

        Args:
            client_id: 客户端唯一标识
            enable: True=启用, False=禁用

        Returns:
            是否发送成功
        """
        conn = self.get_connection(client_id)
        if not conn or not conn.is_connected:
            self.logger.warning(f"发送备份控制失败: 客户端 {client_id} 不在线")
            return False

        # 更新数据库中的backup_enabled字段
        try:
            update_query = """
                UPDATE clients SET backup_enabled = %s WHERE client_id = %s
            """
            self.db_manager.execute_update(update_query, (1 if enable else 0, client_id))
        except Exception as e:
            self.logger.error(f"更新备份状态失败: {e}")

        # 发送控制消息
        ctrl_msg = ProtocolHandler.create_backup_control_message(client_id, enable)
        success = conn.send_message(ctrl_msg)

        if success:
            action = "启用" if enable else "禁用"
            self.logger.info(f"备份已{action}: {client_id}")
            self.log_manager.add_log(
                'info', f"备份已{action}", conn.db_client_id
            )

        return success

    # ===========================================================
    # 客户端卸载/删除
    # ===========================================================
    def send_client_uninstall(self, client_id: str) -> bool:
        """
        向指定客户端发送卸载指令并清理服务端数据

        Args:
            client_id: 客户端唯一标识

        Returns:
            是否成功
        """
        # 先尝试向在线客户端发送卸载指令
        conn = self.get_connection(client_id)
        if conn and conn.is_connected:
            uninstall_msg = ProtocolHandler.create_uninstall_message(client_id)
            conn.send_message(uninstall_msg)
            self.logger.info(f"卸载指令已发送: {client_id}")
            # 关闭连接
            conn.close()

        # 从数据库中删除客户端相关数据
        try:
            # 查询数据库中的客户端记录
            client_info = self.client_manager.get_client_by_id(client_id)
            if client_info:
                db_id = client_info['id']

                # 删除version_blocks（通过file_versions的外键级联可能已处理）
                try:
                    vb_del_query = """
                        DELETE vb FROM version_blocks vb
                        INNER JOIN file_versions fv ON vb.version_id = fv.id
                        WHERE fv.client_id = %s
                    """
                    self.db_manager.execute_update(vb_del_query, (db_id,))
                except Exception as e:
                    self.logger.warning(f"删除version_blocks失败: {e}")

                # data_blocks的ref_count减少（简化处理：此处仅记录日志，
                # 实际生产环境应遍历相关块并递减ref_count）
                self.logger.info(f"注意: 需要清理data_blocks引用计数，client_db_id={db_id}")

                # 删除客户端记录（级联删除file_versions, backup_configs等）
                del_query = "DELETE FROM clients WHERE id = %s"
                self.db_manager.execute_update(del_query, (db_id,))

                self.logger.info(f"客户端数据已从数据库删除: {client_id} (DB ID: {db_id})")

            # 清理磁盘存储（客户端专属目录）
            import shutil
            client_storage_dir = self.storage_manager._get_client_storage_dir(client_id)
            if client_storage_dir.exists():
                shutil.rmtree(str(client_storage_dir), ignore_errors=True)
                self.logger.info(f"客户端存储目录已删除: {client_storage_dir}")

            self.log_manager.add_log('info', f"客户端已删除: {client_id}")
            return True

        except Exception as e:
            self.logger.error(f"删除客户端数据失败: {e}", exc_info=True)
            return False

    # ===========================================================
    # 客户端资源统计更新
    # ===========================================================
    def _update_client_stats(self, client_id: str, stats: Dict[str, Any]):
        """
        更新客户端上报的资源统计（内存缓存）

        Args:
            client_id: 客户端标识
            stats: 统计数据
        """
        with self._client_stats_lock:
            if client_id not in self._client_stats:
                self._client_stats[client_id] = {}
            self._client_stats[client_id].update(stats)

    # ===========================================================
    # 系统统计（服务端+客户端汇总）
    # ===========================================================
    def get_system_stats(self) -> Dict[str, Any]:
        """
        获取系统综合统计信息，供Web仪表盘使用

        包含:
        - 服务器资源: CPU/内存/磁盘/网络
        - 在线客户端数
        - 存储统计
        - 各客户端资源概况

        Returns:
            统计信息字典
        """
        stats: Dict[str, Any] = {
            'server': self._collect_server_stats(),
            'connections': {},
            'storage': {},
            'uptime': 0
        }

        # 服务器运行时间
        if self._start_time:
            stats['uptime'] = time.time() - self._start_time

        # 在线连接信息
        with self._conn_lock:
            stats['active_connections'] = len(self.connections)
            conn_info = {}
            for cid, conn in self.connections.items():
                conn_info[cid] = {
                    'computer_name': conn.computer_name,
                    'address': f"{conn.address[0]}:{conn.address[1]}",
                    'client_version': conn.client_version,
                    'os_type': conn.os_type,
                    'last_activity': conn.last_activity.isoformat() + 'Z',
                    'last_heartbeat': conn.last_heartbeat.isoformat() + 'Z'
                }
            stats['connections'] = conn_info

        # 存储统计
        try:
            stats['storage'] = self.storage_manager.get_storage_stats()
        except Exception as e:
            self.logger.warning(f"获取存储统计失败: {e}")

        # 客户端资源统计快照
        with self._client_stats_lock:
            stats['client_stats'] = dict(self._client_stats)

        return stats

    # ----------------------------------------------------------
    # 备份进度追踪
    # ----------------------------------------------------------
    def update_backup_progress(self, client_id: str, progress: Dict[str, Any]):
        with self._backup_progress_lock:
            self._backup_progress[client_id] = {
                **progress,
                'updated_at': datetime.utcnow().isoformat() + 'Z'
            }

    def get_backup_progress(self, client_id: str = None) -> Dict:
        with self._backup_progress_lock:
            if client_id:
                return self._backup_progress.get(client_id)
            return dict(self._backup_progress)

    def clear_backup_progress(self, client_id: str):
        with self._backup_progress_lock:
            self._backup_progress.pop(client_id, None)

    def _collect_server_stats(self) -> Dict[str, Any]:
        """
        采集服务器自身的系统资源统计
        - 优先使用psutil库
        - 无psutil时回退到基础方案

        Returns:
            服务器资源统计字典
        """
        server_stats: Dict[str, Any] = {
            'cpu_percent': 0.0,
            'memory_percent': 0.0,
            'memory_total_mb': 0,
            'memory_used_mb': 0,
            'disk_usage': {},
            'network_io': {},
            'platform': sys.platform
        }

        if HAS_PSUTIL:
            try:
                # CPU使用率（非阻塞，取上次interval的值）
                server_stats['cpu_percent'] = psutil.cpu_percent(interval=0)
                server_stats['cpu_count'] = psutil.cpu_count()

                # 内存
                mem = psutil.virtual_memory()
                server_stats['memory_percent'] = mem.percent
                server_stats['memory_total_mb'] = round(mem.total / (1024 * 1024), 1)
                server_stats['memory_used_mb'] = round(mem.used / (1024 * 1024), 1)

                # 磁盘使用（存储目录所在分区）
                try:
                    storage_path = str(self.storage_manager.storage_root)
                    disk = psutil.disk_usage(storage_path)
                    server_stats['disk_usage'] = {
                        'total_gb': round(disk.total / (1024 ** 3), 2),
                        'used_gb': round(disk.used / (1024 ** 3), 2),
                        'free_gb': round(disk.free / (1024 ** 3), 2),
                        'percent': disk.percent
                    }
                except Exception:
                    pass

                # 网络IO计数器
                try:
                    net_io = psutil.net_io_counters()
                    server_stats['network_io'] = {
                        'bytes_sent': net_io.bytes_sent,
                        'bytes_recv': net_io.bytes_recv,
                        'packets_sent': net_io.packets_sent,
                        'packets_recv': net_io.packets_recv
                    }
                except Exception:
                    pass

            except Exception as e:
                self.logger.warning(f"psutil采集失败: {e}")

        else:
            # 无psutil时使用/proc文件系统（Linux）
            try:
                # CPU: 读取/proc/loadavg
                if os.path.exists('/proc/loadavg'):
                    with open('/proc/loadavg', 'r') as f:
                        load_avg = f.read().strip().split()
                    server_stats['load_avg_1m'] = float(load_avg[0])
                    server_stats['load_avg_5m'] = float(load_avg[1])

                # 内存: 读取/proc/meminfo
                if os.path.exists('/proc/meminfo'):
                    meminfo = {}
                    with open('/proc/meminfo', 'r') as f:
                        for line in f:
                            parts = line.split(':')
                            if len(parts) == 2:
                                key = parts[0].strip()
                                val = parts[1].strip().split()[0]
                                meminfo[key] = int(val)  # kB

                    total_kb = meminfo.get('MemTotal', 0)
                    available_kb = meminfo.get('MemAvailable', meminfo.get('MemFree', 0))
                    used_kb = total_kb - available_kb

                    server_stats['memory_total_mb'] = round(total_kb / 1024, 1)
                    server_stats['memory_used_mb'] = round(used_kb / 1024, 1)
                    if total_kb > 0:
                        server_stats['memory_percent'] = round(used_kb / total_kb * 100, 1)

                # 磁盘: 使用os.statvfs
                try:
                    storage_path = str(self.storage_manager.storage_root)
                    st = os.statvfs(storage_path)
                    total_bytes = st.f_blocks * st.f_frsize
                    free_bytes = st.f_bavail * st.f_frsize
                    used_bytes = total_bytes - free_bytes
                    server_stats['disk_usage'] = {
                        'total_gb': round(total_bytes / (1024 ** 3), 2),
                        'used_gb': round(used_bytes / (1024 ** 3), 2),
                        'free_gb': round(free_bytes / (1024 ** 3), 2),
                        'percent': round(used_bytes / total_bytes * 100, 1) if total_bytes > 0 else 0
                    }
                except Exception:
                    pass

            except Exception as e:
                self.logger.warning(f"系统资源采集失败(fallback): {e}")

        return server_stats

    # ===========================================================
    # 服务停止
    # ===========================================================
    def stop(self):
        """
        优雅停止服务器
        - 停止接受新连接
        - 关闭所有活跃客户端连接
        - 关闭TCP/UDP套接字
        - 记录停止日志
        """
        if not self.running:
            return

        self.logger.info("正在停止服务器...")
        self.running = False

        # 关闭所有客户端连接
        with self._conn_lock:
            for client_id, conn in list(self.connections.items()):
                try:
                    conn.close()
                except Exception:
                    pass
            self.connections.clear()

        # 关闭TCP套接字
        if self._tcp_socket:
            try:
                self._tcp_socket.close()
            except Exception:
                pass
            self._tcp_socket = None

        # 关闭UDP套接字
        if self._udp_socket:
            try:
                self._udp_socket.close()
            except Exception:
                pass
            self._udp_socket = None

        # 记录日志
        try:
            self.log_manager.add_log('info', '服务器已停止')
        except Exception:
            pass

        self.logger.info("服务器已停止")

    # ===========================================================
    # 服务器统计（简化版，兼容旧接口）
    # ===========================================================
    def get_server_stats(self) -> Dict[str, Any]:
        """
        获取服务器基础统计（兼容旧接口）

        Returns:
            统计字典
        """
        with self._conn_lock:
            active = len(self.connections)

        storage_stats = {}
        try:
            storage_stats = self.storage_manager.get_storage_stats()
        except Exception:
            pass

        return {
            'active_connections': active,
            'storage_stats': storage_stats,
            'uptime': (time.time() - self._start_time) if self._start_time else 0,
            'server_version': self.server_version
        }


# ============================================================
# 模块入口：命令行启动
# ============================================================
def main():
    """命令行入口，解析参数并启动服务器"""
    import argparse

    parser = argparse.ArgumentParser(description='文件备份TCP服务器')
    parser.add_argument('--host', default='0.0.0.0', help='TCP监听地址 (默认: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=DEFAULT_TCP_PORT, help=f'TCP监听端口 (默认: {DEFAULT_TCP_PORT})')
    parser.add_argument('--storage', default='./backup_storage', help='存储根目录 (默认: ./backup_storage)')
    parser.add_argument('--db-host', default='localhost', help='数据库主机')
    parser.add_argument('--db-port', type=int, default=3306, help='数据库端口')
    parser.add_argument('--db-user', default='root', help='数据库用户名')
    parser.add_argument('--db-password', default='password', help='数据库密码')
    parser.add_argument('--db-name', default='file_backup_system', help='数据库名')

    args = parser.parse_args()

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/tcp_server.log', encoding='utf-8')
        ]
    )

    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'user': args.db_user,
        'password': args.db_password,
        'database': args.db_name
    }

    server = BackupServer(
        host=args.host,
        port=args.port,
        db_config=db_config,
        storage_root=args.storage
    )

    try:
        server.start()
    except KeyboardInterrupt:
        logging.info("收到退出信号 (Ctrl+C)")
    except Exception as e:
        logging.error(f"服务器异常: {e}", exc_info=True)
    finally:
        server.stop()


if __name__ == '__main__':
    main()
