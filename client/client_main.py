"""
文件备份客户端主程序
企业级局域网文件自动备份系统 - 客户端核心

功能清单:
  1. LAN自动发现服务端 (UDP广播)
  2. TCP连接 + 断线自动重连
  3. 块级增量备份 (SHA256分块哈希对比)
  4. OTA远程自动更新
  5. 隐藏运行模式 (无窗口/托盘)
  6. 远程命令执行
  7. 文件列表响应
  8. 系统信息上报
  9. 备份控制 (启用/禁用)
"""

import os
import sys
import io
import json
import time
import select
import socket
import struct
import uuid
import hashlib
import zlib
import zipfile
import subprocess
import threading
import logging
import argparse
import getpass
import configparser
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 添加模块路径（兼容PyInstaller打包后的路径）
if getattr(sys, 'frozen', False):
    # PyInstaller打包后的路径
    _base_dir = os.path.dirname(sys.executable)
    _bundle_dir = getattr(sys, '_MEIPASS', _base_dir)
    sys.path.insert(0, _bundle_dir)
    sys.path.insert(0, _base_dir)
else:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.protocol import (
    Message, MessageType, ProtocolHandler, ProtocolError,
    DISCOVERY_PORT, DISCOVERY_MAGIC, DISCOVERY_RESPONSE_MAGIC,
    create_discovery_packet, parse_discovery_response
)
from file_monitor import FileMonitorService, FileHasher
from registry_manager import RegistryManager

# psutil可选
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

CLIENT_VERSION = '1.0.0'
DEFAULT_CHUNK_SIZE = 256 * 1024  # 256KB - 大块减少协议开销，提升吞吐


class BackupClientConfig:
    """客户端配置管理"""

    DEFAULT_CONFIG = {
        'server': {
            'host': '',
            'port': 8888,
            'timeout': 30,
            'retry_interval': 60,
            'heartbeat_interval': 300
        },
        'client': {
            'client_id': '',
            'computer_name': '',
            'buffer_size': 65536,
            'max_file_size_mb': 100,
            'batch_size': 10,
            'batch_timeout': 5.0
        },
        'backup': {
            'monitor_paths': [],
            'exclude_patterns': ['*.tmp', '*.temp', '*.log', '*.lock', '*.swp', '*.swo', '~*'],
            'include_patterns': ['*'],
            'scan_interval': 3600
        },
        'performance': {
            'network_limit_kbps': 0,
            'cpu_limit_percent': 50,
            'memory_limit_mb': 100,
            'compression_threshold': 1024
        },
        'discovery': {
            'enabled': True,
            'timeout': 5,
            'retries': 3
        }
    }

    def __init__(self, config_file: str = None):
        self.config_file = config_file or self._get_default_config_path()
        self.config = {}
        for section, values in self.DEFAULT_CONFIG.items():
            self.config[section] = dict(values)
        self._load_config()

    def _get_default_config_path(self) -> str:
        # PyInstaller打包后优先查找exe同目录下的config.ini
        if getattr(sys, 'frozen', False):
            exe_dir = os.path.dirname(sys.executable)
            local_config = os.path.join(exe_dir, 'config.ini')
            if os.path.exists(local_config):
                return local_config

        if sys.platform == 'win32':
            app_data = os.environ.get('APPDATA', os.path.expanduser('~'))
            return os.path.join(app_data, 'FileBackupClient', 'config.ini')
        else:
            return os.path.join(os.path.expanduser('~'), '.filebackup', 'config.ini')

    def _load_config(self):
        if not os.path.exists(self.config_file):
            self._create_default_config()
            return

        try:
            parser = configparser.ConfigParser()
            parser.read(self.config_file, encoding='utf-8')

            for section_name, section_config in self.DEFAULT_CONFIG.items():
                if parser.has_section(section_name):
                    for key, default_value in section_config.items():
                        if isinstance(default_value, bool):
                            self.config[section_name][key] = parser.getboolean(section_name, key, fallback=default_value)
                        elif isinstance(default_value, int):
                            self.config[section_name][key] = parser.getint(section_name, key, fallback=default_value)
                        elif isinstance(default_value, float):
                            self.config[section_name][key] = parser.getfloat(section_name, key, fallback=default_value)
                        else:
                            value = parser.get(section_name, key, fallback=str(default_value))
                            if key in ['monitor_paths', 'exclude_patterns', 'include_patterns']:
                                if value.strip():
                                    self.config[section_name][key] = [v.strip() for v in value.split(',')]
                                else:
                                    self.config[section_name][key] = default_value
                            else:
                                self.config[section_name][key] = value

            if not self.config['client']['client_id']:
                self.config['client']['client_id'] = str(uuid.uuid4())
                self.save_config()

        except Exception as e:
            logging.getLogger(__name__).error(f"加载配置文件失败: {e}，使用默认配置")

    def _create_default_config(self):
        os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
        self.config['client']['client_id'] = str(uuid.uuid4())
        self.config['client']['computer_name'] = socket.gethostname()
        if sys.platform == 'win32':
            self.config['backup']['monitor_paths'] = [
                os.path.join(os.environ.get('USERPROFILE', ''), 'Documents'),
                os.path.join(os.environ.get('USERPROFILE', ''), 'Desktop')
            ]
        else:
            self.config['backup']['monitor_paths'] = [
                os.path.join(os.path.expanduser('~'), 'Documents'),
                os.path.join(os.path.expanduser('~'), 'Desktop')
            ]
        self.save_config()

    def save_config(self):
        try:
            parser = configparser.ConfigParser()
            for section_name, section_config in self.config.items():
                parser.add_section(section_name)
                for key, value in section_config.items():
                    if isinstance(value, list):
                        parser.set(section_name, key, ','.join(str(v) for v in value))
                    else:
                        parser.set(section_name, key, str(value))
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                parser.write(f)
        except Exception as e:
            logging.getLogger(__name__).error(f"保存配置文件失败: {e}")

    def get(self, section: str, key: str, default=None):
        return self.config.get(section, {}).get(key, default)

    def set(self, section: str, key: str, value):
        if section not in self.config:
            self.config[section] = {}
        self.config[section][key] = value


class BackupClient:
    """文件备份客户端"""

    def __init__(self, config: BackupClientConfig):
        self.config = config
        self.logger = self._setup_logging()

        self.client_id = config.get('client', 'client_id')
        self.computer_name = config.get('client', 'computer_name') or socket.gethostname()

        # 网络连接
        self.socket: Optional[socket.socket] = None
        self.is_connected = False
        self._send_lock = threading.Lock()

        # 服务组件
        self.file_monitor: Optional[FileMonitorService] = None
        self.registry_manager = RegistryManager()

        # 线程控制
        self.running = False
        self.backup_enabled = True

        # TCP接收缓冲区
        self.receive_buffer = bytearray()

        # 文件版本管理
        self.file_versions: Dict[str, int] = {}
        self.version_file = os.path.join(os.path.dirname(config.config_file), 'versions.json')
        self._load_file_versions()

        # 已备份文件哈希记录（用于扫描时判断文件是否变化）
        self.file_hashes: Dict[str, str] = {}
        self._hashes_file = os.path.join(os.path.dirname(config.config_file), 'file_hashes.json')
        self._load_file_hashes()

        # OTA
        self.ota_buffer: Dict[str, Any] = {}

        # 批量上传缓冲
        self._batch_buffer: List[Dict[str, Any]] = []
        self._batch_lock = threading.Lock()
        self._batch_timer: Optional[threading.Timer] = None
        self._batch_max_size = 50            # 每批最多文件数
        self._batch_timeout = 0.5           # 缓冲超时秒数(快速刷新)
        self._pending_batches: Dict[str, Dict[str, Any]] = {}

        # 并发批量上传控制
        self._batch_upload_semaphore = threading.Semaphore(4)  # 最多4个并发批量上传

        # 待确认的单文件上传（防止全量/增量竞态）
        self._pending_uploads: Dict[str, float] = {}
        self._pending_uploads_lock = threading.Lock()

        self.logger.info(f"备份客户端初始化完成 (ID: {self.client_id})")

    def _setup_logging(self) -> logging.Logger:
        log_dir = os.path.join(os.path.dirname(self.config.config_file), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'client.log')

        handlers = [logging.FileHandler(log_file, encoding='utf-8')]
        if not self._is_service_mode():
            handlers.append(logging.StreamHandler())

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=handlers
        )
        return logging.getLogger(__name__)

    def _is_service_mode(self) -> bool:
        return '--service' in sys.argv

    def _load_file_versions(self):
        if os.path.exists(self.version_file):
            try:
                with open(self.version_file, 'r', encoding='utf-8') as f:
                    self.file_versions = json.load(f)
            except Exception:
                pass

    def _save_file_versions(self):
        try:
            os.makedirs(os.path.dirname(self.version_file), exist_ok=True)
            with open(self.version_file, 'w', encoding='utf-8') as f:
                json.dump(self.file_versions, f, indent=2)
        except Exception:
            pass

    def _load_file_hashes(self):
        if os.path.exists(self._hashes_file):
            try:
                with open(self._hashes_file, 'r', encoding='utf-8') as f:
                    self.file_hashes = json.load(f)
            except Exception:
                pass

    def _save_file_hashes(self):
        try:
            os.makedirs(os.path.dirname(self._hashes_file), exist_ok=True)
            with open(self._hashes_file, 'w', encoding='utf-8') as f:
                json.dump(self.file_hashes, f, indent=2)
        except Exception:
            pass

    def _get_next_version(self, file_path: str) -> int:
        current = self.file_versions.get(file_path, 0)
        next_ver = current + 1
        self.file_versions[file_path] = next_ver
        self._save_file_versions()
        return next_ver

    # ======================================================
    # LAN 发现
    # ======================================================
    def discover_server(self) -> Optional[Tuple[str, int]]:
        """通过UDP广播发现局域网内的服务器"""
        if not self.config.get('discovery', 'enabled'):
            return None

        timeout = self.config.get('discovery', 'timeout') or 5
        retries = self.config.get('discovery', 'retries') or 3

        self.logger.info(f"正在通过UDP广播发现服务器 (端口{DISCOVERY_PORT})...")

        for attempt in range(retries):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                sock.settimeout(timeout)

                packet = create_discovery_packet()
                sock.sendto(packet, ('<broadcast>', DISCOVERY_PORT))
                self.logger.debug(f"发现请求已广播 (尝试 {attempt + 1}/{retries})")

                try:
                    data, addr = sock.recvfrom(1024)
                    result = parse_discovery_response(data)
                    if result:
                        host = result.get('host', addr[0])
                        port = result.get('port', 8888)
                        self.logger.info(f"发现服务器: {host}:{port} (版本: {result.get('version', '?')})")
                        sock.close()
                        return (host, port)
                except socket.timeout:
                    self.logger.debug(f"发现超时 (尝试 {attempt + 1}/{retries})")
                finally:
                    sock.close()

            except Exception as e:
                self.logger.warning(f"发现请求失败: {e}")

        self.logger.info("未发现服务器，将使用配置文件地址")
        return None

    # ======================================================
    # 连接管理
    # ======================================================
    def connect_to_server(self) -> bool:
        if self.is_connected:
            return True

        # 优先尝试LAN发现
        discovered = self.discover_server()
        if discovered:
            host, port = discovered
        else:
            host = self.config.get('server', 'host')
            port = self.config.get('server', 'port')

        if not host:
            self.logger.error("无服务器地址可用（发现失败且未配置地址）")
            return False

        connect_timeout = self.config.get('server', 'timeout') or 30
        try:
            self.logger.info(f"正在连接服务端 {host}:{port}...")
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(connect_timeout)
            # 增大发送缓冲区以提升大块数据吞吐量
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)  # 1MB
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)  # 1MB
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # 禁用Nagle算法
            self.socket.connect((host, int(port)))
            # 连接成功后切换为阻塞模式（send/recv 超时由 select 控制，不用 settimeout）
            self.socket.settimeout(None)
            self.is_connected = True
            self.receive_buffer = bytearray()
            self.logger.info("已连接到服务端")
            self._send_register_message()
            self._request_backup_config()
            return True
        except Exception as e:
            self.logger.error(f"连接服务端失败: {e}")
            self._close_connection()
            return False

    def _close_connection(self):
        self.is_connected = False
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
            self.socket = None

    def _get_local_ip(self) -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return '127.0.0.1'

    # ======================================================
    # 消息发送/接收
    # ======================================================
    def _send_message(self, message: Message) -> bool:
        if not self.is_connected or not self.socket:
            return False
        with self._send_lock:
            try:
                data = message.to_bytes()
                sock = self.socket
                if sock is None:
                    return False
                # 根据数据量计算合理的发送超时（至少30秒，每MB加15秒）
                send_timeout = max(30, 15 * (len(data) / (1024 * 1024)) + 10)
                sock.settimeout(send_timeout)
                sock.sendall(data)
                # 恢复阻塞模式
                sock.settimeout(None)
                return True
            except socket.timeout:
                # 发送超时不立即断连 —— 可能只是大消息发送慢
                self.logger.warning(
                    f"发送消息超时 (size={len(data)}B, type={message.msg_type.name})，"
                    f"不断开连接"
                )
                try:
                    self.socket.settimeout(None)
                except Exception:
                    pass
                return False
            except (OSError, ConnectionError, AttributeError) as e:
                self.logger.error(f"发送消息失败(连接异常): {e}")
                self._close_connection()
                return False
            except Exception as e:
                self.logger.error(f"发送消息失败: {e}")
                self._close_connection()
                return False

    def _receive_loop(self):
        """TCP接收循环线程 - 使用 select 做超时轮询，不干扰 socket timeout"""
        while self.running and self.is_connected:
            try:
                sock = self.socket
                if not sock:
                    break
                # 用 select 等待可读，1秒超时；不修改 socket timeout
                ready, _, _ = select.select([sock], [], [], 1.0)
                if not ready:
                    continue  # 超时，无数据到达，继续循环检查 self.running
                data = sock.recv(256 * 1024)
                if not data:
                    self.logger.info("服务端关闭连接")
                    self._close_connection()
                    break
                self.receive_buffer.extend(data)
                self._process_buffer()
            except (ValueError, OSError) as e:
                # select 或 recv 异常（socket 已关闭等）
                if self.is_connected:
                    self.logger.error(f"接收数据异常: {e}")
                self._close_connection()
                break

    def _process_buffer(self):
        """从缓冲区解析完整消息"""
        header_size = Message.HEADER_SIZE
        while len(self.receive_buffer) >= header_size:
            header_data = bytes(self.receive_buffer[:header_size])
            try:
                magic, version, msg_type, length = struct.unpack('<IBBI', header_data)
            except struct.error:
                self.receive_buffer.clear()
                return

            if magic != Message.MAGIC_NUMBER:
                self.receive_buffer = self.receive_buffer[1:]
                continue

            total_len = header_size + length
            if len(self.receive_buffer) < total_len:
                break

            full_msg = bytes(self.receive_buffer[:total_len])
            self.receive_buffer = self.receive_buffer[total_len:]

            try:
                message = Message.from_bytes(full_msg)
                self._handle_message(message)
            except ProtocolError as e:
                self.logger.error(f"协议解析错误: {e}")
            except Exception as e:
                self.logger.error(f"消息处理异常: {e}")

    # ======================================================
    # 消息路由
    # ======================================================
    def _handle_message(self, message: Message):
        handlers = {
            MessageType.BACKUP_CONFIG: self._handle_backup_config,
            MessageType.CHUNK_HASH_REQUEST: self._handle_chunk_hash_request,
            MessageType.BACKUP_STATUS: self._handle_backup_status,
            MessageType.BATCH_UPLOAD_RESULT: self._handle_batch_upload_result,
            MessageType.OTA_RESPONSE: self._handle_ota_response,
            MessageType.OTA_DATA: self._handle_ota_data,
            MessageType.REMOTE_COMMAND: self._handle_remote_command,
            MessageType.FILE_LIST_REQUEST: self._handle_file_list_request,
            MessageType.BACKUP_CONTROL: self._handle_backup_control,
            MessageType.CLIENT_UNINSTALL: self._handle_uninstall,
            MessageType.ERROR: self._handle_error,
        }
        handler = handlers.get(message.msg_type)
        if handler:
            try:
                handler(message)
            except Exception as e:
                self.logger.error(f"处理 {message.msg_type.name} 异常: {e}")
        else:
            self.logger.debug(f"未处理的消息类型: {message.msg_type.name}")

    # ======================================================
    # 注册 & 心跳
    # ======================================================
    def _send_register_message(self):
        try:
            os_type = f"{sys.platform}"
            try:
                import platform
                os_type = f"{platform.system()} {platform.release()}"
            except Exception:
                pass

            msg = ProtocolHandler.create_register_message(
                self.client_id, self.computer_name, self._get_local_ip(),
                os_type=os_type,
                os_user=getpass.getuser(),
                process_name=os.path.basename(sys.executable),
                client_version=CLIENT_VERSION
            )
            self._send_message(msg)
            self.logger.info("客户端注册消息已发送")
        except Exception as e:
            self.logger.error(f"发送注册消息失败: {e}")

    def _request_backup_config(self):
        try:
            msg = ProtocolHandler.create_config_request_message(self.client_id)
            self._send_message(msg)
        except Exception as e:
            self.logger.error(f"发送配置请求失败: {e}")

    def _get_system_stats(self) -> Tuple[float, float, Dict]:
        cpu = 0.0
        mem = 0.0
        disk = {}
        if HAS_PSUTIL:
            try:
                cpu = psutil.cpu_percent(interval=0)
                mem = psutil.virtual_memory().percent
                d = psutil.disk_usage('/')
                disk = {'total_gb': round(d.total / (1024**3), 2),
                        'used_gb': round(d.used / (1024**3), 2),
                        'percent': d.percent}
            except Exception:
                pass
        return cpu, mem, disk

    def _heartbeat_loop(self):
        interval = self.config.get('server', 'heartbeat_interval') or 300
        ota_check_counter = 0
        while self.running:
            try:
                if self.is_connected:
                    cpu, mem, disk = self._get_system_stats()
                    msg = ProtocolHandler.create_heartbeat_message(
                        self.client_id, cpu, mem, disk)
                    self._send_message(msg)

                    # 每10次心跳检查一次OTA
                    ota_check_counter += 1
                    if ota_check_counter >= 10:
                        ota_check_counter = 0
                        ota_msg = ProtocolHandler.create_ota_check_message(
                            self.client_id, CLIENT_VERSION)
                        self._send_message(ota_msg)

                time.sleep(interval)
            except Exception as e:
                self.logger.error(f"心跳循环异常: {e}")
                time.sleep(5)

    def _reconnect_loop(self):
        retry_interval = self.config.get('server', 'retry_interval') or 60
        while self.running:
            try:
                if not self.is_connected:
                    self.logger.info("尝试重新连接服务端...")
                    if self.connect_to_server():
                        self.logger.info("重连成功")
                        # 启动新的接收线程
                        t = threading.Thread(target=self._receive_loop, daemon=True)
                        t.start()
                    else:
                        self.logger.warning(f"重连失败，{retry_interval}秒后重试")
                time.sleep(retry_interval)
            except Exception as e:
                self.logger.error(f"重连循环异常: {e}")
                time.sleep(5)

    # ======================================================
    # 备份配置处理
    # ======================================================
    def _handle_backup_config(self, message: Message):
        data = message.data
        self.logger.info("收到备份配置")

        paths = data.get('backup_paths', data.get('backup_path', ''))
        if isinstance(paths, str):
            try:
                paths = json.loads(paths)
            except Exception:
                paths = [paths] if paths else []

        if paths:
            self.config.set('backup', 'monitor_paths', paths)
            self.config.save_config()

            # 重启文件监控
            if self.file_monitor:
                self.file_monitor.stop()
            self._start_file_monitor()

    # ======================================================
    # 块级增量备份
    # ======================================================
    def _on_file_change(self, file_info: Dict[str, Any]):
        if not self.backup_enabled:
            return

        file_path = file_info['file_path']
        change_type = file_info['change_type']
        self.logger.info(f"文件变化: {file_path} ({change_type})")

        max_size = (self.config.get('client', 'max_file_size_mb') or 100) * 1024 * 1024
        if file_info.get('file_size', 0) > max_size:
            self.logger.warning(f"文件过大，跳过备份: {file_path}")
            return

        if change_type not in ('created', 'modified'):
            return

        if not os.path.exists(file_path):
            return

        # 加入批量缓冲
        with self._batch_lock:
            # 去重: 同一文件只保留最新的
            self._batch_buffer = [f for f in self._batch_buffer
                                  if f['file_path'] != file_path]
            self._batch_buffer.append(file_info)

            if len(self._batch_buffer) >= self._batch_max_size:
                # 达到批量上限，立即刷新
                self._flush_batch_locked()
            else:
                # 重置超时定时器
                if self._batch_timer is not None:
                    self._batch_timer.cancel()
                self._batch_timer = threading.Timer(
                    self._batch_timeout, self._flush_batch)
                self._batch_timer.start()

    def _flush_batch(self):
        """定时器触发: 刷新批量缓冲"""
        with self._batch_lock:
            self._flush_batch_locked()

    def _flush_batch_locked(self):
        """在持有 _batch_lock 的情况下刷新缓冲"""
        if self._batch_timer is not None:
            self._batch_timer.cancel()
            self._batch_timer = None

        if not self._batch_buffer:
            return

        files_to_process = list(self._batch_buffer)
        self._batch_buffer.clear()

        # 后台线程执行打包上传（支持并发多批）
        def _batch_worker(files):
            self._batch_upload_semaphore.acquire()
            try:
                self._do_batch_upload(files)
            finally:
                self._batch_upload_semaphore.release()

        threading.Thread(
            target=_batch_worker,
            args=(files_to_process,),
            daemon=True
        ).start()

    def _do_batch_upload(self, files_to_process: List[Dict[str, Any]]):
        """在后台线程中执行批量打包上传（并发读取+哈希计算）"""
        if not self.is_connected or not self.backup_enabled:
            return

        batch_id = uuid.uuid4().hex[:16]
        chunk_size = DEFAULT_CHUNK_SIZE
        manifest = []
        file_chunks_map: Dict[str, List[bytes]] = {}  # file_path -> [raw_chunk, ...]

        self.logger.info(f"批量打包开始: batch={batch_id}, 文件数={len(files_to_process)}")

        def _read_one_file(file_info):
            """并发读取单个文件并计算块哈希"""
            fp = file_info['file_path']
            if not os.path.exists(fp):
                return None
            try:
                file_size = os.path.getsize(fp)
                chunk_hashes = []
                chunks = []
                with open(fp, 'rb') as f:
                    while True:
                        block = f.read(chunk_size)
                        if not block:
                            break
                        chunk_hashes.append(hashlib.sha256(block).hexdigest())
                        chunks.append(block)
                file_hash = FileHasher.calculate_md5(fp) or ''
                return {
                    'file_path': fp,
                    'file_size': file_size,
                    'file_hash': file_hash,
                    'change_type': file_info.get('change_type', 'modified'),
                    'chunk_hashes': chunk_hashes,
                    'chunks': chunks,
                }
            except Exception as e:
                self.logger.warning(f"读取文件失败，跳过: {fp} - {e}")
                return None

        # 并发读取所有文件（8线程）
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_read_one_file, fi): fi for fi in files_to_process}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    manifest.append({
                        'file_path': result['file_path'],
                        'file_size': result['file_size'],
                        'file_hash': result['file_hash'],
                        'change_type': result['change_type'],
                        'chunk_hashes': result['chunk_hashes'],
                    })
                    file_chunks_map[result['file_path']] = result['chunks']

        if not manifest:
            return

        # 保存待确认批次
        self._pending_batches[batch_id] = {
            'manifest': manifest,
            'file_chunks_map': file_chunks_map,
        }

        # 发送清单
        msg = ProtocolHandler.create_batch_upload_start(
            self.client_id, batch_id, manifest)
        if not self._send_message(msg):
            self._pending_batches.pop(batch_id, None)
            return

        self.logger.info(f"批量清单已发送: batch={batch_id}, 文件数={len(manifest)}")

    def _handle_batch_upload_result(self, message: Message):
        """处理服务端的批量上传响应"""
        data = message.data
        batch_id = data.get('batch_id', '')
        success = data.get('success', False)
        diff_map = data.get('diff_map')

        if diff_map is not None:
            # 阶段一响应: 服务端返回差异对照表，客户端打包差异块上传
            self._send_batch_data(batch_id, diff_map)
        elif success:
            # 阶段二响应: 服务端确认存储完成
            pending = self._pending_batches.pop(batch_id, None)
            results = data.get('results', [])
            ok_count = sum(1 for r in results if r.get('success'))
            self.logger.info(
                f"批量上传完成: batch={batch_id}, "
                f"成功={ok_count}/{len(results)}")

            # 更新本地版本号和哈希
            if pending:
                for r in results:
                    if r.get('success'):
                        fp = r.get('file_path', '')
                        ver = r.get('version', 0)
                        fh = r.get('file_hash', '')
                        if ver:
                            self.file_versions[fp] = ver
                        if fh:
                            self.file_hashes[fp] = fh
                self._save_file_versions()
                self._save_file_hashes()
        else:
            self._pending_batches.pop(batch_id, None)
            self.logger.error(f"批量上传失败: batch={batch_id}")

    def _send_batch_data(self, batch_id: str, diff_map: Dict):
        """根据服务端返回的 diff_map 打包差异块并发送"""
        pending = self._pending_batches.get(batch_id)
        if not pending:
            self.logger.warning(f"找不到批次上下文: {batch_id}")
            return

        file_chunks_map = pending['file_chunks_map']
        manifest = pending['manifest']

        # 构建 zip 包，只包含差异块
        archive_buf = io.BytesIO()
        total_diff_chunks = 0
        with zipfile.ZipFile(archive_buf, 'w', zipfile.ZIP_STORED) as zf:
            for file_idx, entry in enumerate(manifest):
                fp = entry['file_path']
                diff_indices = diff_map.get(fp, [])
                chunks = file_chunks_map.get(fp, [])
                for idx in diff_indices:
                    if idx < len(chunks):
                        # 条目名: 文件序号/块序号
                        zf.writestr(f"{file_idx}/{idx}", chunks[idx])
                        total_diff_chunks += 1

        archive_bytes = archive_buf.getvalue()

        # 释放内存: 不再需要原始块数据
        pending.pop('file_chunks_map', None)

        self.logger.info(
            f"差异打包完成: batch={batch_id}, "
            f"差异块={total_diff_chunks}, 包大小={len(archive_bytes)}")

        # 分段发送（每段 ≤ 8MB）
        seg_size = 8 * 1024 * 1024
        total_segs = max(1, (len(archive_bytes) + seg_size - 1) // seg_size)

        bw_limit = self.config.get('performance', 'network_limit_kbps') or 0

        for i in range(total_segs):
            start = i * seg_size
            end = min(start + seg_size, len(archive_bytes))
            seg_data = archive_bytes[start:end]

            msg = ProtocolHandler.create_batch_upload_data(
                self.client_id, batch_id, i, total_segs, seg_data)
            if not self._send_message(msg):
                self.logger.error(f"发送批量数据段失败: batch={batch_id} seg={i}")
                self._pending_batches.pop(batch_id, None)
                return

            # 带宽限制
            if bw_limit > 0:
                delay = len(seg_data) / (bw_limit * 1024)
                time.sleep(delay)

        self.logger.info(f"批量数据已发送: batch={batch_id}, 分段={total_segs}")

    def _handle_chunk_hash_request(self, message: Message):
        """处理服务端的块哈希请求 - 增量备份核心"""
        data = message.data
        file_path = data.get('file_path', '')
        chunk_size = data.get('chunk_size', DEFAULT_CHUNK_SIZE)

        if not os.path.exists(file_path):
            self.logger.warning(f"请求哈希的文件不存在: {file_path}")
            return

        self.logger.info(f"计算块哈希: {file_path} (块大小={chunk_size})")

        try:
            chunk_hashes, file_hash, file_size = \
                ProtocolHandler.calculate_block_hashes(file_path, chunk_size)

            response = ProtocolHandler.create_chunk_hash_response(
                self.client_id, file_path, chunk_hashes, file_size, file_hash)
            self._send_message(response)

            self.logger.info(f"块哈希已发送: {file_path} ({len(chunk_hashes)}块)")
        except Exception as e:
            self.logger.error(f"计算块哈希失败: {e}")

    def _handle_backup_status(self, message: Message):
        """处理服务端的增量上传指令"""
        data = message.data
        file_path = data.get('file_path', '')
        diff_indices = data.get('diff_indices', [])
        version = data.get('version', 1)
        chunk_count = data.get('chunk_count', 0)

        if not os.path.exists(file_path):
            self.logger.warning(f"增量上传文件不存在: {file_path}")
            return

        self.logger.info(f"增量上传: {file_path} 差异块={len(diff_indices)}/{chunk_count}")
        self.file_versions[file_path] = version
        self._save_file_versions()

        # 只上传差异块
        self._send_file_chunks(file_path, version=version,
                              diff_indices=set(diff_indices))

    def _send_file_chunks(self, file_path: str, version: int = None,
                          diff_indices: set = None):
        """发送文件块数据"""
        try:
            file_size = os.path.getsize(file_path)
            chunk_size = DEFAULT_CHUNK_SIZE
            chunk_count = (file_size + chunk_size - 1) // chunk_size
            if chunk_count == 0:
                chunk_count = 1

            if version is None:
                version = self._get_next_version(file_path)

            file_hash = FileHasher.calculate_md5(file_path)

            # 带宽限制
            bw_limit = self.config.get('performance', 'network_limit_kbps') or 0

            with open(file_path, 'rb') as f:
                for chunk_index in range(chunk_count):
                    chunk_data = f.read(chunk_size)
                    if not chunk_data:
                        break

                    # 如果指定了差异块，只上传差异块
                    if diff_indices is not None and chunk_index not in diff_indices:
                        continue

                    chunk_hash = hashlib.sha256(chunk_data).hexdigest()

                    # 压缩(如果压缩后更小)
                    is_compressed = False
                    send_data = chunk_data
                    if len(chunk_data) > 1024:
                        compressed = zlib.compress(chunk_data)
                        if len(compressed) < len(chunk_data):
                            send_data = compressed
                            is_compressed = True

                    msg = ProtocolHandler.create_file_chunk_message(
                        self.client_id, file_path, version,
                        chunk_index, chunk_count, chunk_hash,
                        file_hash, file_size, send_data, is_compressed)

                    if not self._send_message(msg):
                        self.logger.error(f"发送文件块失败: {file_path}")
                        return

                    # 带宽限制
                    if bw_limit > 0:
                        delay = len(send_data) / (bw_limit * 1024)
                        time.sleep(delay)

            self.logger.info(f"文件数据发送完成: {file_path} v{version}")

            # 记录已备份文件的哈希，供后续扫描对比
            if file_hash:
                self.file_hashes[file_path] = file_hash
                self._save_file_hashes()

        except Exception as e:
            self.logger.error(f"发送文件数据失败: {e}")

    # ======================================================
    # OTA自动更新
    # ======================================================
    def _handle_ota_response(self, message: Message):
        data = message.data
        if data.get('has_update'):
            new_version = data.get('new_version', '')
            file_size = data.get('file_size', 0)
            self.logger.info(f"OTA更新可用: {CLIENT_VERSION} -> {new_version} ({file_size}B)")
            self.ota_buffer = {
                'version': new_version,
                'file_size': file_size,
                'file_hash': data.get('file_hash', ''),
                'chunks': {},
                'total_chunks': 0
            }
        else:
            self.logger.debug("客户端已是最新版本")

    def _handle_ota_data(self, message: Message):
        data = message.data
        chunk_index = data.get('chunk_index', 0)
        chunk_count = data.get('chunk_count', 1)
        chunk_data = message.binary_payload

        if not self.ota_buffer:
            return

        self.ota_buffer['total_chunks'] = chunk_count
        self.ota_buffer['chunks'][chunk_index] = chunk_data

        self.logger.debug(f"OTA块 {chunk_index + 1}/{chunk_count}")

        if len(self.ota_buffer['chunks']) >= chunk_count:
            self._apply_ota_update()

    def _apply_ota_update(self):
        """应用OTA更新"""
        try:
            version = self.ota_buffer.get('version', '')
            self.logger.info(f"开始应用OTA更新: {version}")

            # 重建更新文件
            temp_dir = os.path.join(os.path.dirname(self.config.config_file), 'ota')
            os.makedirs(temp_dir, exist_ok=True)
            update_path = os.path.join(temp_dir, f'update_{version}.zip')

            with open(update_path, 'wb') as f:
                for i in range(self.ota_buffer['total_chunks']):
                    chunk = self.ota_buffer['chunks'].get(i, b'')
                    f.write(chunk)

            self.logger.info(f"OTA更新包已保存: {update_path}")
            self.ota_buffer = {}

        except Exception as e:
            self.logger.error(f"应用OTA更新失败: {e}")
            self.ota_buffer = {}

    # ======================================================
    # 远程命令
    # ======================================================
    def _handle_remote_command(self, message: Message):
        data = message.data
        command_id = data.get('command_id', '')
        command = data.get('command', '')
        timeout = data.get('timeout', 30)

        self.logger.info(f"收到远程命令: {command} (id={command_id})")

        def execute():
            try:
                client_root = os.path.dirname(self.config.config_file)
                result = subprocess.run(
                    command, shell=True, capture_output=True,
                    text=True, timeout=timeout, cwd=client_root)
                response = ProtocolHandler.create_remote_command_result(
                    self.client_id, command_id,
                    result.returncode, result.stdout, result.stderr)
                self._send_message(response)
            except subprocess.TimeoutExpired:
                response = ProtocolHandler.create_remote_command_result(
                    self.client_id, command_id, -1, '', '命令执行超时')
                self._send_message(response)
            except Exception as e:
                response = ProtocolHandler.create_remote_command_result(
                    self.client_id, command_id, -1, '', str(e))
                self._send_message(response)

        threading.Thread(target=execute, daemon=True).start()

    # ======================================================
    # 文件列表
    # ======================================================
    def _handle_file_list_request(self, message: Message):
        data = message.data
        path = data.get('path', '')

        if not path:
            # 返回驱动器列表(Windows)或根目录
            if sys.platform == 'win32':
                entries = []
                for letter in 'CDEFGHIJKLMNOPQRSTUVWXYZ':
                    drive = f"{letter}:\\"
                    if os.path.exists(drive):
                        entries.append({
                            'name': drive, 'path': drive,
                            'is_dir': True, 'size': 0})
            else:
                entries = [{'name': '/', 'path': '/', 'is_dir': True, 'size': 0}]
        else:
            entries = []
            try:
                for entry in os.scandir(path):
                    try:
                        stat = entry.stat()
                        entries.append({
                            'name': entry.name,
                            'path': entry.path,
                            'is_dir': entry.is_dir(),
                            'size': stat.st_size if not entry.is_dir() else 0,
                            'modified': stat.st_mtime
                        })
                    except (PermissionError, OSError):
                        continue
                entries.sort(key=lambda x: (not x['is_dir'], x['name'].lower()))
            except (PermissionError, OSError) as e:
                self.logger.warning(f"读取目录失败: {path} - {e}")

        response = ProtocolHandler.create_file_list_response(
            self.client_id, path, entries)
        self._send_message(response)

    # ======================================================
    # 备份控制 & 卸载
    # ======================================================
    def _handle_backup_control(self, message: Message):
        data = message.data
        enable = data.get('enable', True)
        self.backup_enabled = enable
        action = "启用" if enable else "禁用"
        self.logger.info(f"备份已{action}")

        if enable:
            if self.file_monitor is None:
                self._start_file_monitor()
            else:
                # 监控已在运行，仍需扫描已有文件
                self._scan_existing_files()
        elif not enable and self.file_monitor:
            self.file_monitor.stop()
            self.file_monitor = None

    def _handle_uninstall(self, message: Message):
        self.logger.warning("收到卸载指令，正在停止客户端...")
        self.running = False
        self._close_connection()

        # 移除开机自启动
        try:
            self.registry_manager.remove_auto_start()
        except Exception:
            pass

    def _handle_error(self, message: Message):
        data = message.data
        self.logger.error(f"服务端错误: [{data.get('error_code')}] {data.get('error_message')}")

    # ======================================================
    # 已有文件扫描
    # ======================================================
    def _scan_existing_files(self):
        """扫描监控路径下的已有文件，对新增或变化的文件触发备份"""
        if not self.backup_enabled or not self.is_connected:
            return

        monitor_paths = self.config.get('backup', 'monitor_paths') or []
        exclude_patterns = set(self.config.get('backup', 'exclude_patterns') or [])
        max_size = (self.config.get('client', 'max_file_size_mb') or 100) * 1024 * 1024

        if not monitor_paths:
            return

        def scan():
            from fnmatch import fnmatch
            files_to_backup = []

            self.logger.info("开始扫描已有文件...")

            for monitor_path in monitor_paths:
                if not os.path.isdir(monitor_path):
                    continue
                for root, _dirs, files in os.walk(monitor_path):
                    for filename in files:
                        file_path = os.path.join(root, filename)

                        # 排除模式检查
                        skip = False
                        for pattern in exclude_patterns:
                            if fnmatch(filename, pattern) or fnmatch(file_path, pattern):
                                skip = True
                                break
                        if skip:
                            continue

                        try:
                            file_size = os.path.getsize(file_path)
                        except OSError:
                            continue

                        if file_size > max_size:
                            continue

                        # 对比已备份哈希
                        stored_hash = self.file_hashes.get(file_path)
                        if stored_hash is not None:
                            current_hash = FileHasher.calculate_md5(file_path)
                            if current_hash and current_hash == stored_hash:
                                continue  # 哈希一致，无需备份

                        files_to_backup.append(file_path)

            self.logger.info(f"扫描完成，发现 {len(files_to_backup)} 个需要备份的文件")

            for file_path in files_to_backup:
                if not self.backup_enabled or not self.running or not self.is_connected:
                    break
                try:
                    file_size = os.path.getsize(file_path)
                    file_hash = FileHasher.calculate_md5(file_path) if file_size < 10 * 1024 * 1024 else ""
                    self._on_file_change({
                        "file_path": file_path,
                        "change_type": "modified",
                        "file_size": file_size,
                        "file_hash": file_hash,
                        "timestamp": datetime.utcnow()
                    })
                    # 极短间隔, 依赖批量缓冲合并请求
                    time.sleep(0.02)
                except Exception as e:
                    self.logger.warning(f"扫描备份文件失败 {file_path}: {e}")

        threading.Thread(target=scan, daemon=True).start()

    # ======================================================
    # 待确认上传巡检（防止单文件 FILE_CHANGE 无响应时丢失）
    # ======================================================
    def _pending_uploads_checker(self):
        """定期检查是否有单文件 FILE_CHANGE 超时未得到服务端响应"""
        while self.running:
            time.sleep(5)
            stale = []
            with self._pending_uploads_lock:
                now = time.time()
                for fp, ts in list(self._pending_uploads.items()):
                    if now - ts > 10.0:
                        stale.append(fp)
                        del self._pending_uploads[fp]
            for fp in stale:
                if os.path.exists(fp) and self.backup_enabled and self.is_connected:
                    self.logger.info(f"单文件上传超时，执行全量上传: {fp}")
                    self._send_file_chunks(fp)

    # ======================================================
    # 文件监控
    # ======================================================
    def _start_file_monitor(self):
        monitor_config = {
            'monitor_paths': self.config.get('backup', 'monitor_paths') or [],
            'exclude_patterns': self.config.get('backup', 'exclude_patterns') or []
        }
        if not monitor_config['monitor_paths']:
            self.logger.warning("无监控路径，文件监控未启动")
            return

        self.file_monitor = FileMonitorService(
            monitor_config, self._on_file_change, self.logger)
        if self.file_monitor.start():
            self.logger.info(f"文件监控已启动: {monitor_config['monitor_paths']}")
            # 启动后扫描已有文件，备份新增或变化的文件
            self._scan_existing_files()
        else:
            self.logger.error("文件监控启动失败")

    # ======================================================
    # 启动 / 停止
    # ======================================================
    def start(self) -> bool:
        self.running = True

        # 隐藏窗口
        if self._is_service_mode():
            self.registry_manager.hide_console_window()

        # 设置进程优先级
        self.registry_manager.set_process_priority("below_normal")

        # 设置开机自启动
        self.registry_manager.set_auto_start()

        # 连接服务端
        if not self.connect_to_server():
            self.logger.warning("初始连接失败，将尝试重连")

        # 启动接收线程
        recv_thread = threading.Thread(target=self._receive_loop, daemon=True)
        recv_thread.start()

        # 启动文件监控
        self._start_file_monitor()

        # 启动心跳线程
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        # 启动重连线程
        threading.Thread(target=self._reconnect_loop, daemon=True).start()

        # 启动待确认上传巡检线程
        threading.Thread(target=self._pending_uploads_checker, daemon=True).start()

        self.logger.info("备份客户端已启动")
        return True

    def stop(self):
        self.logger.info("正在停止备份客户端...")
        self.running = False

        if self.file_monitor:
            self.file_monitor.stop()

        self._close_connection()
        self.logger.info("备份客户端已停止")

    def run_forever(self):
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("收到退出信号")
            self.stop()


def main():
    parser = argparse.ArgumentParser(description='文件自动备份客户端')
    parser.add_argument('--service', action='store_true', help='以服务模式运行（隐藏窗口）')
    parser.add_argument('--config', type=str, help='配置文件路径')
    args = parser.parse_args()

    if args.service:
        registry = RegistryManager()
        registry.hide_console_window()

    config = BackupClientConfig(args.config)
    client = BackupClient(config)

    try:
        if client.start():
            client.logger.info("客户端启动成功，按Ctrl+C退出")
            client.run_forever()
        else:
            client.logger.error("客户端启动失败")
            sys.exit(1)
    except KeyboardInterrupt:
        client.logger.info("收到退出信号")
    except Exception as e:
        client.logger.error(f"客户端异常: {e}")
    finally:
        client.stop()


if __name__ == "__main__":
    main()
