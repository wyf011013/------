"""
服务端配置文件
"""

import os
import json
from typing import Dict, Any


class ServerConfig:
    """服务器配置"""
    
    DEFAULT_CONFIG = {
        # 服务器配置
        'server': {
            'host': '0.0.0.0',
            'port': 8888,
            'max_connections': 100,
            'timeout': 300,
            'heartbeat_interval': 300
        },
        
        # 数据库配置
        'database': {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'password',
            'database': 'file_backup_system',
            'pool_size': 10,
            'charset': 'utf8mb4'
        },
        
        # 存储配置
        'storage': {
            'root_path': './backup_storage',
            'enable_deduplication': True,
            'enable_compression': True,
            'chunk_size': 262144,  # 256KB
            'max_file_size_mb': 1024,  # 1GB
            'keep_versions': 10,  # 保留版本数
            'cleanup_interval': 86400  # 清理间隔（秒）
        },
        
        # 默认备份配置
        'backup_defaults': {
            'monitor_paths': ['C:\\Users\\Documents', 'C:\\Users\\Desktop'],
            'file_pattern': '*',
            'exclude_patterns': [
                '*.tmp', '*.temp', '*.log', '*.lock', '*.swp', '*.swo',
                '~*', '.DS_Store', 'Thumbs.db', '*.bak', '*.cache',
                'node_modules', '.git', '__pycache__', '*.pyc'
            ],
            'max_file_size_mb': 100
        },
        
        # 日志配置
        'logging': {
            'level': 'INFO',
            'file': './logs/server.log',
            'max_size_mb': 100,
            'backup_count': 5,
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        
        # 性能限制
        'performance': {
            'max_bandwidth_mbps': 0,  # 0表示不限速
            'max_concurrent_backups': 5,
            'cpu_limit_percent': 70,
            'memory_limit_mb': 512
        }
    }
    
    def __init__(self, config_file: str = None):
        """
        初始化配置
        
        Args:
            config_file: 配置文件路径
        """
        self.config_file = config_file or self._get_default_config_path()
        self.config = self.DEFAULT_CONFIG.copy()
        self._load_config()
    
    def _get_default_config_path(self) -> str:
        """获取默认配置文件路径"""
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'server_config.json')
    
    def _load_config(self):
        """加载配置文件"""
        if not os.path.exists(self.config_file):
            self._create_default_config()
            return
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                user_config = json.load(f)
            
            # 合并配置
            self._merge_config(self.config, user_config)
            
        except Exception as e:
            print(f"加载配置文件失败: {e}，使用默认配置")
    
    def _merge_config(self, default: Dict[str, Any], user: Dict[str, Any]):
        """递归合并配置"""
        for key, value in user.items():
            if key in default:
                if isinstance(default[key], dict) and isinstance(value, dict):
                    self._merge_config(default[key], value)
                else:
                    default[key] = value
    
    def _create_default_config(self):
        """创建默认配置文件"""
        try:
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"创建配置文件失败: {e}")
    
    def get(self, section: str, key=None, default=None):
        """获取配置值"""
        if key is None:
            # 只传入section，返回整个section配置
            return self.config.get(section, default)
        elif isinstance(key, dict):
            # 如果key是字典，说明是想用默认值的调用方式
            # 这种情况下key应该是default，传入的default应该是None
            return self.config.get(section, key)
        else:
            # 传入了section和key，返回具体配置值
            return self.config.get(section, {}).get(key, default)
    
    def set(self, section: str, key: str, value):
        """设置配置值"""
        if section not in self.config:
            self.config[section] = {}
        self.config[section][key] = value
    
    def save_config(self):
        """保存配置到文件"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"保存配置文件失败: {e}")


# 全局配置实例
config = ServerConfig()
