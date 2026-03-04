"""
自定义TCP通信协议定义
文件自动备份系统客户端-服务端通信协议
"""

import struct
import json
import zlib
from enum import IntEnum
from typing import Dict, Any, Optional, Tuple


class MessageType(IntEnum):
    """消息类型定义"""
    CLIENT_REGISTER = 0x01      # 客户端注册
    CLIENT_HEARTBEAT = 0x02     # 心跳包
    BACKUP_CONFIG = 0x03        # 备份配置下发
    FILE_CHANGE = 0x04          # 文件变化通知
    FILE_DATA = 0x05            # 文件数据传输
    FILE_CHUNK = 0x06           # 文件分块传输
    BACKUP_STATUS = 0x07        # 备份状态
    ERROR = 0x08                # 错误消息
    CONFIG_REQUEST = 0x09       # 配置请求
    FILE_LIST_REQUEST = 0x0A    # 文件列表请求


class ProtocolError(Exception):
    """协议错误异常"""
    pass


class Message:
    """消息封装类"""
    
    MAGIC_NUMBER = 0xDEADBEEF  # 魔数
    PROTOCOL_VERSION = 0x01      # 协议版本
    HEADER_SIZE = 10             # 头部大小（魔数4B + 版本1B + 类型1B + 长度4B）
    
    def __init__(self, msg_type: MessageType, data: Dict[str, Any]):
        """
        初始化消息
        
        Args:
            msg_type: 消息类型
            data: 消息数据（字典）
        """
        self.msg_type = msg_type
        self.data = data
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Message':
        """
        从字节数据解析消息
        
        Args:
            data: 原始字节数据
            
        Returns:
            Message对象
            
        Raises:
            ProtocolError: 协议解析错误
        """
        if len(data) < cls.HEADER_SIZE:
            raise ProtocolError("数据长度不足，无法解析头部")
        
        # 解析头部
        magic, version, msg_type, length = struct.unpack('<IBBI', data[:cls.HEADER_SIZE])
        
        # 验证魔数
        if magic != cls.MAGIC_NUMBER:
            raise ProtocolError(f"无效的魔数: 0x{magic:08X}, 期望: 0x{cls.MAGIC_NUMBER:08X}")
        
        # 验证版本
        if version != cls.PROTOCOL_VERSION:
            raise ProtocolError(f"不支持的协议版本: {version}")
        
        # 验证长度
        if len(data) - cls.HEADER_SIZE != length:
            raise ProtocolError(f"数据长度不匹配: 实际{len(data) - cls.HEADER_SIZE}, 期望{length}")
        
        # 解析数据
        try:
            json_data = json.loads(data[cls.HEADER_SIZE:].decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ProtocolError(f"JSON解析失败: {e}")
        
        return cls(MessageType(msg_type), json_data)
    
    def to_bytes(self) -> bytes:
        """
        将消息转换为字节数据
        
        Returns:
            字节数据
        """
        # 序列化JSON数据
        json_bytes = json.dumps(self.data, ensure_ascii=False).encode('utf-8')
        
        # 构建头部
        header = struct.pack('<IBBI', 
                           self.MAGIC_NUMBER,
                           self.PROTOCOL_VERSION,
                           int(self.msg_type),
                           len(json_bytes))
        
        return header + json_bytes
    
    def __str__(self) -> str:
        return f"Message(type={self.msg_type.name}, data={self.data})"
    
    def __repr__(self) -> str:
        return self.__str__()


class ProtocolHandler:
    """协议处理器"""
    
    @staticmethod
    def create_register_message(client_id: str, computer_name: str, 
                               ip_address: str = None) -> Message:
        """创建客户端注册消息"""
        import datetime
        return Message(MessageType.CLIENT_REGISTER, {
            "client_id": client_id,
            "computer_name": computer_name,
            "ip_address": ip_address,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        })
    
    @staticmethod
    def create_heartbeat_message(client_id: str) -> Message:
        """创建心跳消息"""
        import datetime
        return Message(MessageType.CLIENT_HEARTBEAT, {
            "client_id": client_id,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        })
    
    @staticmethod
    def create_file_change_message(client_id: str, file_path: str, 
                                 change_type: str, file_size: int = None,
                                 file_hash: str = None) -> Message:
        """创建文件变化通知消息"""
        import datetime
        data = {
            "client_id": client_id,
            "file_path": file_path,
            "change_type": change_type,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        }
        if file_size is not None:
            data["file_size"] = file_size
        if file_hash is not None:
            data["file_hash"] = file_hash
        return Message(MessageType.FILE_CHANGE, data)
    
    @staticmethod
    def create_file_data_message(client_id: str, file_path: str, 
                                version: int, chunk_index: int, chunk_count: int,
                                chunk_size: int, file_hash: str, data: bytes,
                                is_compressed: bool = False) -> Message:
        """创建文件数据消息"""
        import datetime
        import base64
        
        # 压缩数据
        if not is_compressed and len(data) > 1024:  # 大于1KB才压缩
            compressed_data = zlib.compress(data, level=6)
            if len(compressed_data) < len(data):  # 确保压缩后更小
                data = compressed_data
                is_compressed = True
        
        return Message(MessageType.FILE_DATA, {
            "client_id": client_id,
            "file_path": file_path,
            "version": version,
            "chunk_index": chunk_index,
            "chunk_count": chunk_count,
            "chunk_size": chunk_size,
            "file_hash": file_hash,
            "data": base64.b64encode(data).decode('ascii'),
            "is_compressed": is_compressed,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        })
    
    @staticmethod
    def create_error_message(client_id: str, error_code: str, 
                           error_message: str, details: str = None) -> Message:
        """创建错误消息"""
        import datetime
        data = {
            "client_id": client_id,
            "error_code": error_code,
            "error_message": error_message,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        }
        if details:
            data["details"] = details
        return Message(MessageType.ERROR, data)
    
    @staticmethod
    def create_config_request_message(client_id: str) -> Message:
        """创建配置请求消息"""
        import datetime
        return Message(MessageType.CONFIG_REQUEST, {
            "client_id": client_id,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
        })
    
    @staticmethod
    def parse_message_data(msg: Message) -> Dict[str, Any]:
        """解析消息数据"""
        return msg.data
    
    @staticmethod
    def extract_file_data(msg_data: Dict[str, Any]) -> Tuple[bytes, bool]:
        """
        从文件数据消息中提取文件数据
        
        Args:
            msg_data: 消息数据
            
        Returns:
            (文件数据, 是否压缩)
        """
        import base64
        
        data_b64 = msg_data.get("data", "")
        is_compressed = msg_data.get("is_compressed", False)
        
        if not data_b64:
            return b"", is_compressed
        
        data = base64.b64decode(data_b64)
        
        # 解压数据
        if is_compressed:
            try:
                data = zlib.decompress(data)
            except zlib.error as e:
                raise ProtocolError(f"数据解压失败: {e}")
        
        return data, is_compressed


# 测试代码
if __name__ == "__main__":
    # 测试消息创建和解析
    msg = ProtocolHandler.create_register_message("test-client", "Test-PC", "192.168.1.100")
    print(f"原始消息: {msg}")
    
    # 序列化
    data = msg.to_bytes()
    print(f"序列化后长度: {len(data)} 字节")
    
    # 反序列化
    parsed_msg = Message.from_bytes(data)
    print(f"反序列化后: {parsed_msg}")
    
    # 测试文件数据消息
    test_data = b"This is a test file content." * 100
    file_msg = ProtocolHandler.create_file_data_message(
        "test-client", "C:\\test.txt", 1, 0, 1, len(test_data), 
        "abc123", test_data
    )
    print(f"\n文件数据消息: {file_msg}")
    
    # 测试压缩
    large_data = b"Repeated content." * 1000
    compressed_msg = ProtocolHandler.create_file_data_message(
        "test-client", "C:\\large.txt", 1, 0, 1, len(large_data),
        "def456", large_data
    )
    original_size = len(large_data)
    compressed_size = len(compressed_msg.data.get("data", ""))
    print(f"\n压缩测试:")
    print(f"原始大小: {original_size} 字节")
    print(f"压缩后: {compressed_size} 字节 (Base64编码)")
    print(f"压缩率: {(1 - compressed_size / (original_size * 4/3)) * 100:.1f}%")
