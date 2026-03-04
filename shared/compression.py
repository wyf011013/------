"""
压缩优化模块
根据文件类型和内容动态选择最佳压缩策略
"""

import zlib
import logging
from enum import IntEnum
from typing import Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

# 尝试导入lz4，如果不存在则标记为不可用
try:
    import lz4.frame
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False
    logger.debug("LZ4模块未安装，使用zlib作为默认压缩算法")


class CompressionAlgorithm(IntEnum):
    """压缩算法枚举"""
    NONE = 0
    ZLIB_FAST = 1      # zlib level 1 - 速度优先
    ZLIB_BALANCED = 2  # zlib level 6 - 平衡
    ZLIB_MAX = 3       # zlib level 9 - 压缩率优先
    LZ4 = 4            # LZ4 - 超快压缩（需要lz4模块）


class CompressionOptimizer:
    """
    压缩优化器
    根据文件类型、大小和内容智能选择压缩策略
    """
    
    # 已压缩文件扩展名（不需要再压缩）
    ALREADY_COMPRESSED_EXTENSIONS = {
        '.zip', '.gz', '.bz2', '.7z', '.rar', '.xz',
        '.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp',
        '.mp3', '.mp4', '.avi', '.mov', '.mkv', '.flv',
        '.pdf', '.docx', '.xlsx', '.pptx',  # Office文档已压缩
        '.exe', '.dll', '.so', '.dylib',  # 二进制
    }
    
    # 文本文件扩展名（适合压缩）
    TEXT_EXTENSIONS = {
        '.txt', '.md', '.json', '.xml', '.yaml', '.yml',
        '.html', '.htm', '.css', '.js', '.py', '.java',
        '.c', '.cpp', '.h', '.hpp', '.go', '.rs',
        '.sql', '.log', '.csv', '.tsv',
    }
    
    def __init__(self, 
                 enable_compression: bool = True,
                 min_compress_size: int = 1024,
                 max_compress_size: int = 100 * 1024 * 1024,
                 default_algorithm: CompressionAlgorithm = None):
        self.enable_compression = enable_compression
        self.min_compress_size = min_compress_size
        self.max_compress_size = max_compress_size
        
        # 如果没有LZ4，使用ZLIB_FAST作为默认
        if default_algorithm is None:
            if HAS_LZ4:
                self.default_algorithm = CompressionAlgorithm.LZ4
            else:
                self.default_algorithm = CompressionAlgorithm.ZLIB_FAST
        else:
            self.default_algorithm = default_algorithm
        
        # 压缩统计
        self.stats = {
            'total_attempts': 0,
            'skipped': 0,
            'compressed': 0,
            'compression_saved': 0,
        }
    
    def should_compress(self, file_path: str, data: bytes) -> bool:
        """
        判断是否应该压缩
        
        Args:
            file_path: 文件路径
            data: 数据内容
            
        Returns:
            是否应该压缩
        """
        if not self.enable_compression:
            return False
        
        data_len = len(data)
        
        # 太小或太大的文件不压缩
        if data_len < self.min_compress_size:
            return False
        if data_len > self.max_compress_size:
            return False
        
        # 检查文件扩展名
        ext = Path(file_path).suffix.lower()
        if ext in self.ALREADY_COMPRESSED_EXTENSIONS:
            return False
        
        # 采样检查内容是否已压缩（随机采样检测熵）
        if data_len > 4096:
            sample = data[:2048] + data[-2048:]
        else:
            sample = data
        
        # 如果样本已经是高熵（可能是已压缩数据），跳过压缩
        if self._is_high_entropy(sample):
            return False
        
        return True
    
    def _is_high_entropy(self, data: bytes, threshold: float = 7.5) -> bool:
        """
        检测数据是否是高熵（已压缩或加密）
        
        Args:
            data: 数据样本
            threshold: 熵阈值（0-8）
            
        Returns:
            是否高熵
        """
        if len(data) == 0:
            return False
        
        # 计算香农熵
        from collections import Counter
        byte_counts = Counter(data)
        length = len(data)
        entropy = 0.0
        
        for count in byte_counts.values():
            if count > 0:
                freq = count / length
                import math
                entropy -= freq * math.log2(freq)
        
        return entropy > threshold
    
    def select_algorithm(self, file_path: str, data: bytes) -> CompressionAlgorithm:
        """
        选择最佳压缩算法
        
        Args:
            file_path: 文件路径
            data: 数据内容
            
        Returns:
            推荐的压缩算法
        """
        ext = Path(file_path).suffix.lower()
        data_len = len(data)
        
        # 如果有LZ4，优先使用
        if HAS_LZ4:
            return CompressionAlgorithm.LZ4
        
        # 文本文件使用快速压缩
        if ext in self.TEXT_EXTENSIONS:
            return CompressionAlgorithm.ZLIB_FAST
        
        # 中等大小文件使用平衡压缩
        if data_len < 10 * 1024 * 1024:  # < 10MB
            return CompressionAlgorithm.ZLIB_FAST
        
        # 大文件使用最快压缩
        return CompressionAlgorithm.ZLIB_FAST
    
    def compress(self, file_path: str, data: bytes,
                 algorithm: Optional[CompressionAlgorithm] = None) -> Tuple[bytes, bool, CompressionAlgorithm]:
        """
        压缩数据
        
        Args:
            file_path: 文件路径
            data: 原始数据
            algorithm: 指定算法，None则自动选择
            
        Returns:
            (压缩后数据, 是否压缩, 使用的算法)
        """
        self.stats['total_attempts'] += 1
        
        if not self.should_compress(file_path, data):
            self.stats['skipped'] += 1
            return data, False, CompressionAlgorithm.NONE
        
        if algorithm is None:
            algorithm = self.select_algorithm(file_path, data)
        
        # 如果没有LZ4，降级到ZLIB_FAST
        if algorithm == CompressionAlgorithm.LZ4 and not HAS_LZ4:
            algorithm = CompressionAlgorithm.ZLIB_FAST
        
        try:
            if algorithm == CompressionAlgorithm.LZ4 and HAS_LZ4:
                compressed = lz4.frame.compress(data)
            elif algorithm == CompressionAlgorithm.ZLIB_FAST:
                compressed = zlib.compress(data, level=1)
            elif algorithm == CompressionAlgorithm.ZLIB_BALANCED:
                compressed = zlib.compress(data, level=6)
            elif algorithm == CompressionAlgorithm.ZLIB_MAX:
                compressed = zlib.compress(data, level=9)
            else:
                return data, False, CompressionAlgorithm.NONE
            
            # 只保留有效压缩（压缩后更小）
            if len(compressed) < len(data):
                self.stats['compressed'] += 1
                self.stats['compression_saved'] += len(data) - len(compressed)
                return compressed, True, algorithm
            else:
                return data, False, CompressionAlgorithm.NONE
                
        except Exception as e:
            logger.warning(f"压缩失败 {file_path}: {e}")
            return data, False, CompressionAlgorithm.NONE
    
    def decompress(self, data: bytes, algorithm: CompressionAlgorithm) -> bytes:
        """
        解压数据
        
        Args:
            data: 压缩数据
            algorithm: 压缩算法
            
        Returns:
            解压后数据
        """
        try:
            if algorithm == CompressionAlgorithm.LZ4:
                if HAS_LZ4:
                    return lz4.frame.decompress(data)
                else:
                    logger.error("无法解压LZ4数据：LZ4模块未安装")
                    return data
            elif algorithm in (CompressionAlgorithm.ZLIB_FAST, 
                              CompressionAlgorithm.ZLIB_BALANCED,
                              CompressionAlgorithm.ZLIB_MAX):
                return zlib.decompress(data)
            else:
                return data
        except Exception as e:
            logger.error(f"解压失败: {e}")
            return data
    
    def get_stats(self) -> dict:
        """获取压缩统计信息"""
        stats = self.stats.copy()
        if stats['total_attempts'] > 0:
            stats['compression_ratio'] = (
                stats['compression_saved'] / 
                max(stats['total_attempts'] * self.min_compress_size, 1)
            )
        stats['has_lz4'] = HAS_LZ4
        return stats


# 全局压缩优化器实例
_default_optimizer: Optional[CompressionOptimizer] = None


def get_optimizer() -> CompressionOptimizer:
    """获取默认压缩优化器实例"""
    global _default_optimizer
    if _default_optimizer is None:
        _default_optimizer = CompressionOptimizer()
    return _default_optimizer


def compress_data(data: bytes, algorithm: CompressionAlgorithm = CompressionAlgorithm.ZLIB_FAST) -> Tuple[bytes, bool]:
    """
    便捷函数：压缩数据
    
    Args:
        data: 原始数据
        algorithm: 压缩算法
        
    Returns:
        (压缩后数据, 是否成功压缩)
    """
    optimizer = get_optimizer()
    compressed, was_compressed, _ = optimizer.compress("", data, algorithm)
    return compressed, was_compressed


def decompress_data(data: bytes, algorithm: CompressionAlgorithm) -> bytes:
    """
    便捷函数：解压数据
    
    Args:
        data: 压缩数据
        algorithm: 压缩算法
        
    Returns:
        解压后数据
    """
    optimizer = get_optimizer()
    return optimizer.decompress(data, algorithm)
