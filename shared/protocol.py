"""
统一通信协议定义
文件自动备份系统 - 客户端/服务端共享协议模块
支持: 注册、心跳、文件传输、块级增量、OTA升级、远程命令、文件树请求、系统信息
"""

import struct
import json
import zlib
import hashlib
import base64
from enum import IntEnum
from typing import Dict, Any, Optional, Tuple, List


# ============================================================
# LAN 发现协议 (UDP广播)
# ============================================================
DISCOVERY_PORT = 8889
DISCOVERY_MAGIC = b'FBKP_DISC'  # 9字节魔数
DISCOVERY_RESPONSE_MAGIC = b'FBKP_RESP'


def create_discovery_packet() -> bytes:
    """创建客户端发现广播包"""
    return DISCOVERY_MAGIC


def create_discovery_response(server_host: str, server_port: int, server_version: str = '1.0.0') -> bytes:
    """创建服务端发现响应包"""
    payload = json.dumps({
        'host': server_host,
        'port': server_port,
        'version': server_version,
    }, ensure_ascii=False).encode('utf-8')
    return DISCOVERY_RESPONSE_MAGIC + struct.pack('<H', len(payload)) + payload


def parse_discovery_response(data: bytes) -> Optional[Dict[str, Any]]:
    """解析服务端发现响应"""
    if not data.startswith(DISCOVERY_RESPONSE_MAGIC):
        return None
    offset = len(DISCOVERY_RESPONSE_MAGIC)
    if len(data) < offset + 2:
        return None
    payload_len = struct.unpack('<H', data[offset:offset + 2])[0]
    offset += 2
    if len(data) < offset + payload_len:
        return None
    try:
        return json.loads(data[offset:offset + payload_len].decode('utf-8'))
    except Exception:
        return None


# ============================================================
# TCP 消息协议
# ============================================================

class MessageType(IntEnum):
    """消息类型定义"""
    CLIENT_REGISTER = 0x01      # 客户端注册
    CLIENT_HEARTBEAT = 0x02     # 心跳包
    BACKUP_CONFIG = 0x03        # 备份配置下发
    FILE_CHANGE = 0x04          # 文件变化通知
    FILE_DATA = 0x05            # 文件数据传输
    FILE_CHUNK = 0x06           # 文件分块传输(块级增量)
    BACKUP_STATUS = 0x07        # 备份状态
    ERROR = 0x08                # 错误消息
    CONFIG_REQUEST = 0x09       # 配置请求
    FILE_LIST_REQUEST = 0x0A    # 文件列表请求
    FILE_LIST_RESPONSE = 0x0B   # 文件列表响应
    CHUNK_HASH_REQUEST = 0x0C   # 块哈希校验请求(服务端->客户端)
    CHUNK_HASH_RESPONSE = 0x0D  # 块哈希校验响应(客户端->服务端)
    OTA_CHECK = 0x10            # OTA版本检查
    OTA_RESPONSE = 0x11         # OTA版本响应
    OTA_DATA = 0x12             # OTA升级数据
    REMOTE_COMMAND = 0x20       # 远程命令(服务端->客户端)
    REMOTE_COMMAND_RESULT = 0x21  # 远程命令结果
    SYSTEM_INFO = 0x30          # 系统信息上报
    BACKUP_CONTROL = 0x31       # 备份控制(开启/关闭)
    CLIENT_UNINSTALL = 0x32     # 客户端卸载指令
    BATCH_UPLOAD_START = 0x40   # 批量上传清单(客户端->服务端)
    BATCH_UPLOAD_DATA = 0x41    # 批量上传数据(客户端->服务端)
    BATCH_UPLOAD_RESULT = 0x42  # 批量上传结果(服务端->客户端)


class ProtocolError(Exception):
    """协议错误异常"""
    pass


class Message:
    """消息封装类"""

    MAGIC_NUMBER = 0xDEADBEEF
    PROTOCOL_VERSION = 0x02
    HEADER_SIZE = 10  # 魔数4B + 版本1B + 类型1B + 长度4B

    def __init__(self, msg_type: MessageType, data: Dict[str, Any], binary_payload: bytes = b''):
        self.msg_type = msg_type
        self.data = data
        self.binary_payload = binary_payload  # 用于高效传输二进制数据(文件块等)

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Message':
        if len(data) < cls.HEADER_SIZE:
            raise ProtocolError("数据长度不足")

        magic, version, msg_type, length = struct.unpack('<IBBI', data[:cls.HEADER_SIZE])

        if magic != cls.MAGIC_NUMBER:
            raise ProtocolError(f"无效的魔数: 0x{magic:08X}")

        if version not in (0x01, 0x02):
            raise ProtocolError(f"不支持的协议版本: {version}")

        body = data[cls.HEADER_SIZE:]
        if len(body) != length:
            raise ProtocolError(f"数据长度不匹配: 实际{len(body)}, 期望{length}")

        # 格式: [4B json_len][json_bytes][binary_payload]
        if len(body) >= 4:
            json_len = struct.unpack('<I', body[:4])[0]
            if json_len <= len(body) - 4:
                json_bytes = body[4:4 + json_len]
                binary_payload = body[4 + json_len:]
                try:
                    json_data = json.loads(json_bytes.decode('utf-8'))
                except Exception:
                    raise ProtocolError("JSON解析失败")
                return cls(MessageType(msg_type), json_data, binary_payload)

        # 向后兼容: 纯JSON格式
        try:
            json_data = json.loads(body.decode('utf-8'))
        except Exception:
            raise ProtocolError("JSON解析失败")
        return cls(MessageType(msg_type), json_data)

    def to_bytes(self) -> bytes:
        json_bytes = json.dumps(self.data, ensure_ascii=False).encode('utf-8')
        # 新格式: [4B json_len][json][binary]
        body = struct.pack('<I', len(json_bytes)) + json_bytes + self.binary_payload
        header = struct.pack('<IBBI',
                             self.MAGIC_NUMBER,
                             self.PROTOCOL_VERSION,
                             int(self.msg_type),
                             len(body))
        return header + body

    def __str__(self) -> str:
        bp = f", binary={len(self.binary_payload)}B" if self.binary_payload else ""
        return f"Message(type={self.msg_type.name}{bp})"


class ProtocolHandler:
    """协议消息工厂"""

    @staticmethod
    def _ts() -> str:
        import datetime
        return datetime.datetime.utcnow().isoformat() + "Z"

    # ---- 注册 ----
    @staticmethod
    def create_register_message(client_id: str, computer_name: str,
                                ip_address: str = '', os_type: str = '',
                                os_user: str = '', process_name: str = '',
                                client_version: str = '1.0.0') -> Message:
        return Message(MessageType.CLIENT_REGISTER, {
            "client_id": client_id,
            "computer_name": computer_name,
            "ip_address": ip_address,
            "os_type": os_type,
            "os_user": os_user,
            "process_name": process_name,
            "client_version": client_version,
            "timestamp": ProtocolHandler._ts()
        })

    # ---- 心跳 ----
    @staticmethod
    def create_heartbeat_message(client_id: str, cpu_percent: float = 0,
                                 memory_percent: float = 0,
                                 disk_usage: Dict = None) -> Message:
        return Message(MessageType.CLIENT_HEARTBEAT, {
            "client_id": client_id,
            "cpu_percent": cpu_percent,
            "memory_percent": memory_percent,
            "disk_usage": disk_usage or {},
            "timestamp": ProtocolHandler._ts()
        })

    # ---- 文件变化通知 ----
    @staticmethod
    def create_file_change_message(client_id: str, file_path: str,
                                   change_type: str, file_size: int = 0,
                                   file_hash: str = '') -> Message:
        return Message(MessageType.FILE_CHANGE, {
            "client_id": client_id,
            "file_path": file_path,
            "change_type": change_type,
            "file_size": file_size,
            "file_hash": file_hash,
            "timestamp": ProtocolHandler._ts()
        })

    # ---- 块级文件数据传输 ----
    @staticmethod
    def create_file_chunk_message(client_id: str, file_path: str,
                                  version: int, chunk_index: int,
                                  chunk_count: int, chunk_hash: str,
                                  file_hash: str, file_size: int,
                                  chunk_data: bytes,
                                  is_compressed: bool = False) -> Message:
        """创建文件块消息(二进制payload, 不经过base64)"""
        return Message(MessageType.FILE_CHUNK, {
            "client_id": client_id,
            "file_path": file_path,
            "version": version,
            "chunk_index": chunk_index,
            "chunk_count": chunk_count,
            "chunk_hash": chunk_hash,
            "file_hash": file_hash,
            "file_size": file_size,
            "is_compressed": is_compressed,
            "timestamp": ProtocolHandler._ts()
        }, binary_payload=chunk_data)

    # ---- 块哈希校验(增量备份核心) ----
    @staticmethod
    def create_chunk_hash_request(client_id: str, file_path: str,
                                  chunk_size: int = 65536) -> Message:
        """服务端请求客户端发送文件各块哈希"""
        return Message(MessageType.CHUNK_HASH_REQUEST, {
            "client_id": client_id,
            "file_path": file_path,
            "chunk_size": chunk_size,
            "timestamp": ProtocolHandler._ts()
        })

    @staticmethod
    def create_chunk_hash_response(client_id: str, file_path: str,
                                   chunk_hashes: List[str],
                                   file_size: int, file_hash: str) -> Message:
        """客户端回复文件各块哈希"""
        return Message(MessageType.CHUNK_HASH_RESPONSE, {
            "client_id": client_id,
            "file_path": file_path,
            "chunk_hashes": chunk_hashes,
            "file_size": file_size,
            "file_hash": file_hash,
            "timestamp": ProtocolHandler._ts()
        })

    # ---- 备份配置 ----
    @staticmethod
    def create_backup_config_message(client_id: str, config: Dict[str, Any]) -> Message:
        config['client_id'] = client_id
        config['timestamp'] = ProtocolHandler._ts()
        return Message(MessageType.BACKUP_CONFIG, config)

    @staticmethod
    def create_config_request_message(client_id: str) -> Message:
        return Message(MessageType.CONFIG_REQUEST, {
            "client_id": client_id,
            "timestamp": ProtocolHandler._ts()
        })

    # ---- 文件列表 ----
    @staticmethod
    def create_file_list_request(client_id: str, path: str = '') -> Message:
        return Message(MessageType.FILE_LIST_REQUEST, {
            "client_id": client_id,
            "path": path,
            "timestamp": ProtocolHandler._ts()
        })

    @staticmethod
    def create_file_list_response(client_id: str, path: str,
                                  entries: List[Dict]) -> Message:
        return Message(MessageType.FILE_LIST_RESPONSE, {
            "client_id": client_id,
            "path": path,
            "entries": entries,
            "timestamp": ProtocolHandler._ts()
        })

    # ---- OTA升级 ----
    @staticmethod
    def create_ota_check_message(client_id: str, current_version: str) -> Message:
        return Message(MessageType.OTA_CHECK, {
            "client_id": client_id,
            "current_version": current_version,
            "timestamp": ProtocolHandler._ts()
        })

    @staticmethod
    def create_ota_response_message(client_id: str, has_update: bool,
                                    new_version: str = '', file_size: int = 0,
                                    file_hash: str = '', changelog: str = '') -> Message:
        return Message(MessageType.OTA_RESPONSE, {
            "client_id": client_id,
            "has_update": has_update,
            "new_version": new_version,
            "file_size": file_size,
            "file_hash": file_hash,
            "changelog": changelog,
            "timestamp": ProtocolHandler._ts()
        })

    @staticmethod
    def create_ota_data_message(client_id: str, version: str,
                                chunk_index: int, chunk_count: int,
                                data: bytes) -> Message:
        return Message(MessageType.OTA_DATA, {
            "client_id": client_id,
            "version": version,
            "chunk_index": chunk_index,
            "chunk_count": chunk_count,
            "timestamp": ProtocolHandler._ts()
        }, binary_payload=data)

    # ---- 远程命令 ----
    @staticmethod
    def create_remote_command_message(client_id: str, command_id: str,
                                     command: str, timeout: int = 30) -> Message:
        return Message(MessageType.REMOTE_COMMAND, {
            "client_id": client_id,
            "command_id": command_id,
            "command": command,
            "timeout": timeout,
            "timestamp": ProtocolHandler._ts()
        })

    @staticmethod
    def create_remote_command_result(client_id: str, command_id: str,
                                    return_code: int, stdout: str, stderr: str) -> Message:
        return Message(MessageType.REMOTE_COMMAND_RESULT, {
            "client_id": client_id,
            "command_id": command_id,
            "return_code": return_code,
            "stdout": stdout,
            "stderr": stderr,
            "timestamp": ProtocolHandler._ts()
        })

    # ---- 系统信息上报 ----
    @staticmethod
    def create_system_info_message(client_id: str, info: Dict[str, Any]) -> Message:
        info['client_id'] = client_id
        info['timestamp'] = ProtocolHandler._ts()
        return Message(MessageType.SYSTEM_INFO, info)

    # ---- 备份控制 ----
    @staticmethod
    def create_backup_control_message(client_id: str, enable: bool) -> Message:
        return Message(MessageType.BACKUP_CONTROL, {
            "client_id": client_id,
            "enable": enable,
            "timestamp": ProtocolHandler._ts()
        })

    # ---- 客户端卸载 ----
    @staticmethod
    def create_uninstall_message(client_id: str) -> Message:
        return Message(MessageType.CLIENT_UNINSTALL, {
            "client_id": client_id,
            "timestamp": ProtocolHandler._ts()
        })

    # ---- 错误 ----
    @staticmethod
    def create_error_message(client_id: str, error_code: str,
                             error_message: str, details: str = '') -> Message:
        return Message(MessageType.ERROR, {
            "client_id": client_id,
            "error_code": error_code,
            "error_message": error_message,
            "details": details,
            "timestamp": ProtocolHandler._ts()
        })

    # ---- 批量上传 ----
    @staticmethod
    def create_batch_upload_start(client_id: str, batch_id: str,
                                  files: List[Dict[str, Any]]) -> Message:
        """
        创建批量上传清单消息

        Args:
            files: [{file_path, file_size, file_hash, change_type, chunk_hashes}, ...]
        """
        return Message(MessageType.BATCH_UPLOAD_START, {
            "client_id": client_id,
            "batch_id": batch_id,
            "files": files,
            "timestamp": ProtocolHandler._ts()
        })

    @staticmethod
    def create_batch_upload_data(client_id: str, batch_id: str,
                                 chunk_index: int, chunk_count: int,
                                 data: bytes) -> Message:
        return Message(MessageType.BATCH_UPLOAD_DATA, {
            "client_id": client_id,
            "batch_id": batch_id,
            "chunk_index": chunk_index,
            "chunk_count": chunk_count,
            "timestamp": ProtocolHandler._ts()
        }, binary_payload=data)

    @staticmethod
    def create_batch_upload_result(client_id: str, batch_id: str,
                                   success: bool,
                                   diff_map: Dict = None,
                                   results: List = None) -> Message:
        return Message(MessageType.BATCH_UPLOAD_RESULT, {
            "client_id": client_id,
            "batch_id": batch_id,
            "success": success,
            "diff_map": diff_map,
            "results": results,
            "timestamp": ProtocolHandler._ts()
        })

    # ---- 工具函数 ----
    @staticmethod
    def calculate_block_hashes(file_path: str, chunk_size: int = 262144) -> Tuple[List[str], str, int]:
        """
        计算文件的块级哈希列表, 以及整体文件哈希和大小

        Returns:
            (chunk_hashes, file_hash, file_size)
        """
        import os
        chunk_hashes = []
        file_hasher = hashlib.md5()
        file_size = 0
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                file_hasher.update(chunk)
                chunk_hashes.append(hashlib.sha256(chunk).hexdigest())
                file_size += len(chunk)
        return chunk_hashes, file_hasher.hexdigest(), file_size
