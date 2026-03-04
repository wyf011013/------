"""
性能优化配置模块
集中管理所有性能优化参数
"""

from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class NetworkConfig:
    """网络优化配置"""
    # 块大小配置
    chunk_size: int = 1024 * 1024  # 1MB
    
    # TCP缓冲区
    tcp_send_buffer: int = 8 * 1024 * 1024  # 8MB
    tcp_recv_buffer: int = 8 * 1024 * 1024  # 8MB
    
    # 连接池
    enable_connection_pool: bool = True
    max_connections: int = 4
    
    # 批量传输
    batch_max_files: int = 200
    batch_timeout: float = 2.0
    batch_segment_size: int = 8 * 1024 * 1024  # 8MB
    
    # 超时配置
    connect_timeout: int = 30
    send_timeout_base: int = 30
    send_timeout_per_mb: int = 15


@dataclass
class CompressionConfig:
    """压缩优化配置"""
    enable_compression: bool = True
    min_compress_size: int = 1024  # 小于1KB不压缩
    max_compress_size: int = 100 * 1024 * 1024  # 大于100MB不压缩
    
    # 压缩算法选择
    default_algorithm: str = 'lz4'  # 'lz4', 'zlib_fast', 'zlib_balanced'
    zlib_level_fast: int = 1
    zlib_level_balanced: int = 6
    
    # 文件类型配置
    skip_compressed_extensions: List[str] = field(default_factory=lambda: [
        '.zip', '.gz', '.bz2', '.7z', '.rar', '.xz',
        '.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp',
        '.mp3', '.mp4', '.avi', '.mov', '.mkv', '.flv',
        '.pdf', '.docx', '.xlsx', '.pptx',
        '.exe', '.dll', '.so', '.dylib',
    ])


@dataclass
class AsyncConfig:
    """异步I/O配置"""
    enable_async: bool = True
    max_concurrent_tasks: int = 8
    io_thread_pool_size: int = 8
    
    # 文件读取
    read_buffer_size: int = 1024 * 1024  # 1MB
    max_open_files: int = 100


@dataclass
class PrefetchConfig:
    """预读取配置"""
    enable_prefetch: bool = True
    prefetch_workers: int = 4
    hash_cache_size: int = 10000
    
    # 目录扫描
    scan_interval: int = 300  # 5分钟
    max_file_size_mb: int = 100


@dataclass
class CacheConfig:
    """缓存配置"""
    # LRU缓存
    lru_cache_size: int = 1000
    lru_max_memory_mb: int = 100
    
    # 块缓存
    block_cache_enabled: bool = True
    block_cache_max_mb: int = 500
    block_cache_max_blocks: int = 10000
    hot_threshold: int = 3
    
    # 去重索引
    dedup_index_size: int = 100000
    bloom_filter_size: int = 1000000


@dataclass
class DatabaseConfig:
    """数据库优化配置"""
    # 批量插入
    batch_insert_size: int = 1000
    batch_flush_interval: float = 5.0
    
    # version_blocks表
    version_blocks_batch_size: int = 500
    
    # data_blocks表
    data_blocks_batch_size: int = 500
    
    # 连接池
    db_pool_size: int = 10
    db_pool_max_overflow: int = 20


@dataclass
class PerformanceConfig:
    """综合性能配置"""
    network: NetworkConfig = field(default_factory=NetworkConfig)
    compression: CompressionConfig = field(default_factory=CompressionConfig)
    async_io: AsyncConfig = field(default_factory=AsyncConfig)
    prefetch: PrefetchConfig = field(default_factory=PrefetchConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    
    # 全局开关
    enable_all_optimizations: bool = True
    debug_mode: bool = False
    
    def get_summary(self) -> dict:
        """获取配置摘要"""
        return {
            'chunk_size_mb': self.network.chunk_size / (1024 * 1024),
            'tcp_buffer_mb': self.network.tcp_send_buffer / (1024 * 1024),
            'max_connections': self.network.max_connections,
            'batch_max_files': self.network.batch_max_files,
            'compression_algorithm': self.compression.default_algorithm,
            'async_enabled': self.async_io.enable_async,
            'prefetch_enabled': self.prefetch.enable_prefetch,
            'block_cache_mb': self.cache.block_cache_max_mb,
            'db_batch_size': self.database.batch_insert_size,
        }


# 全局配置实例
_default_config: Optional[PerformanceConfig] = None


def get_config() -> PerformanceConfig:
    """获取默认性能配置"""
    global _default_config
    if _default_config is None:
        _default_config = PerformanceConfig()
    return _default_config


def set_config(config: PerformanceConfig):
    """设置全局配置"""
    global _default_config
    _default_config = config


# 预设配置
def get_high_performance_config() -> PerformanceConfig:
    """获取高性能配置（适用于千兆以上网络）"""
    return PerformanceConfig(
        network=NetworkConfig(
            chunk_size=4 * 1024 * 1024,  # 4MB
            max_connections=8,
            batch_max_files=500,
        ),
        compression=CompressionConfig(
            default_algorithm='lz4',
        ),
        async_io=AsyncConfig(
            max_concurrent_tasks=16,
        ),
        cache=CacheConfig(
            block_cache_max_mb=1024,
        ),
    )


def get_balanced_config() -> PerformanceConfig:
    """获取平衡配置（默认推荐）"""
    return PerformanceConfig()


def get_low_resource_config() -> PerformanceConfig:
    """获取低资源配置（适用于资源受限环境）"""
    return PerformanceConfig(
        network=NetworkConfig(
            chunk_size=256 * 1024,  # 256KB
            max_connections=2,
            batch_max_files=50,
        ),
        compression=CompressionConfig(
            enable_compression=False,
        ),
        async_io=AsyncConfig(
            enable_async=False,
        ),
        prefetch=PrefetchConfig(
            enable_prefetch=False,
        ),
        cache=CacheConfig(
            block_cache_enabled=False,
        ),
    )
