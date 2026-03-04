"""
存储管理模块
负责块级存储、去重、压缩、文件重建和客户端部署包生成。
采用 SHA256 哈希进行块级去重，支持 zlib 压缩，线程安全。
"""

import os
import io
import json
import zlib
import shutil
import hashlib
import logging
import zipfile
import threading
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

# 导入优化模块
try:
    from cache_manager import CacheManager, BlockCache
    from batch_operations import DataBlocksBatchManager
    HAS_OPTIMIZATIONS = True
except ImportError:
    HAS_OPTIMIZATIONS = False


class StorageManager:
    """
    存储管理器

    核心职责:
      - 块级存储与去重（基于 SHA256 哈希）
      - 可选 zlib 压缩（仅压缩后更小时才存储压缩版本）
      - 文件版本重建（按序拼接已存储的块）
      - OTA 更新包管理
      - 客户端部署包生成
      - 存储统计与清理
    """

    # ------------------------------------------------------------------ #
    #  初始化
    # ------------------------------------------------------------------ #

    def __init__(self, storage_root: str, enable_compression: bool = True,
                 chunk_size: int = 262144):
        """
        初始化存储管理器

        Args:
            storage_root: 存储根目录的路径
            enable_compression: 是否启用 zlib 压缩（默认开启）
            chunk_size: 分块大小（字节），默认 256KB
        """
        self.storage_root: Path = Path(storage_root).resolve()
        self.enable_compression: bool = enable_compression
        self.chunk_size: int = chunk_size
        self.logger = logging.getLogger(__name__)

        # ---------- 目录结构 ----------
        # storage_root/
        # ├── shared_blocks/     # 去重块存储
        # ├── temp/              # 临时文件（重建时使用）
        # ├── ota/               # OTA 升级包
        # └── client_packages/   # 客户端部署 zip 包
        self._shared_blocks_dir: Path = self.storage_root / "shared_blocks"
        self._temp_dir: Path = self.storage_root / "temp"
        self._ota_dir: Path = self.storage_root / "ota"
        self._client_packages_dir: Path = self.storage_root / "client_packages"

        self._ensure_directories()

        # ---------- 块索引与引用计数 ----------
        # block_index 结构:
        # {
        #     "<sha256_hash>": {
        #         "storage_path": "shared_blocks/ab/ab12...blk",
        #         "ref_count": 3,
        #         "original_size": 65536,
        #         "stored_size": 41002,
        #         "is_compressed": true,
        #         "created_at": "2025-01-01T00:00:00"
        #     },
        #     ...
        # }
        self._block_index_file: Path = self._shared_blocks_dir / "block_index.json"
        self._block_index_lock = threading.Lock()
        self._block_index: Dict[str, Dict[str, Any]] = self._load_block_index()

        self.logger.info(
            f"存储管理器已初始化: root={self.storage_root}, "
            f"compression={self.enable_compression}, "
            f"已索引块数={len(self._block_index)}"
        )

    # ------------------------------------------------------------------ #
    #  目录管理（私有）
    # ------------------------------------------------------------------ #

    def _ensure_directories(self) -> None:
        """确保所有必需的目录结构存在"""
        for d in (
            self._shared_blocks_dir,
            self._temp_dir,
            self._ota_dir,
            self._client_packages_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #
    #  块索引持久化（私有）
    # ------------------------------------------------------------------ #

    def _load_block_index(self) -> Dict[str, Dict[str, Any]]:
        """
        从磁盘加载块索引文件。

        如果文件不存在或解析失败，返回空字典。
        """
        if not self._block_index_file.exists():
            return {}
        try:
            with open(self._block_index_file, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            self.logger.debug(f"块索引已加载，共 {len(data)} 条记录")
            return data
        except Exception as exc:
            self.logger.warning(f"加载块索引失败，将使用空索引: {exc}")
            return {}

    def _save_block_index(self) -> None:
        """
        将块索引写回磁盘。

        注意: 调用方应已持有 _block_index_lock。
        """
        try:
            # 先写临时文件，再原子替换，降低写入中途掉电导致索引损坏的风险
            tmp_path = self._block_index_file.with_suffix(".json.tmp")
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(self._block_index, fh, indent=2, ensure_ascii=False)
            # 替换（在同一文件系统上相当于原子操作）
            shutil.move(str(tmp_path), str(self._block_index_file))
        except Exception as exc:
            self.logger.error(f"保存块索引失败: {exc}")

    # ------------------------------------------------------------------ #
    #  块路径计算
    # ------------------------------------------------------------------ #

    def get_block_path(self, block_hash: str) -> Path:
        """
        根据块哈希计算其在文件系统中的绝对路径。

        路径规则: shared_blocks/{hash[:2]}/{hash}.blk

        Args:
            block_hash: 块的 SHA256 十六进制字符串

        Returns:
            块文件的 Path 对象（绝对路径）
        """
        prefix = block_hash[:2]
        return self._shared_blocks_dir / prefix / f"{block_hash}.blk"

    def block_exists(self, block_hash: str) -> bool:
        """
        检查某个块是否已存在于存储中。

        同时验证索引记录与物理文件一致。

        Args:
            block_hash: 块的 SHA256 哈希

        Returns:
            存在返回 True，否则返回 False
        """
        with self._block_index_lock:
            if block_hash not in self._block_index:
                return False
        # 索引中存在，再确认物理文件也在
        block_path = self.get_block_path(block_hash)
        return block_path.exists()

    # ------------------------------------------------------------------ #
    #  核心: 存储块（带去重 + 压缩）
    # ------------------------------------------------------------------ #

    def store_block(
        self,
        block_hash: str,
        block_data: bytes,
        compress: bool = True,
    ) -> Tuple[str, int, bool]:
        """
        存储一个数据块，支持去重与可选压缩。

        工作流程:
          1. 如果该哈希已存在 —— 仅增加引用计数，不重复写磁盘。
          2. 如果为新块:
             a. 根据 compress 参数及全局 enable_compression 决定是否压缩。
             b. 仅当压缩后体积更小时才保留压缩版本。
             c. 将数据写入 shared_blocks/{hash[:2]}/{hash}.blk。
             d. 记录到块索引。

        Args:
            block_hash:  块数据的 SHA256 十六进制字符串（由调用方预先计算）
            block_data:  块的原始字节内容
            compress:    本次调用是否尝试压缩（会与全局开关取 AND）

        Returns:
            (storage_path, stored_size, is_compressed)
            - storage_path:  相对于 storage_root 的存储路径
            - stored_size:   实际写入磁盘的字节数
            - is_compressed: 该块是否以压缩形式存储
        """
        with self._block_index_lock:
            # ---- 去重: 块已存在，仅增加引用计数 ----
            if block_hash in self._block_index:
                entry = self._block_index[block_hash]
                entry["ref_count"] = entry.get("ref_count", 1) + 1
                self._save_block_index()

                self.logger.debug(
                    f"块已存在(去重), ref_count={entry['ref_count']}: {block_hash}"
                )
                return (
                    entry["storage_path"],
                    entry["stored_size"],
                    entry["is_compressed"],
                )

            # ---- 新块: 决定是否压缩 ----
            should_compress = compress and self.enable_compression
            is_compressed = False
            data_to_write = block_data

            if should_compress:
                try:
                    compressed = zlib.compress(block_data, level=6)
                    # 仅在压缩后体积更小时采用压缩版本
                    if len(compressed) < len(block_data):
                        data_to_write = compressed
                        is_compressed = True
                except Exception as exc:
                    self.logger.warning(f"压缩块数据失败，将存储原始数据: {exc}")

            # ---- 写入磁盘 ----
            block_path = self.get_block_path(block_hash)
            block_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                with open(block_path, "wb") as fh:
                    fh.write(data_to_write)
            except OSError as exc:
                self.logger.error(f"写入块文件失败 {block_path}: {exc}")
                raise

            stored_size = len(data_to_write)
            relative_path = f"shared_blocks/{block_hash[:2]}/{block_hash}.blk"

            # ---- 更新索引 ----
            self._block_index[block_hash] = {
                "storage_path": relative_path,
                "ref_count": 1,
                "original_size": len(block_data),
                "stored_size": stored_size,
                "is_compressed": is_compressed,
                "created_at": datetime.utcnow().isoformat(),
            }
            self._save_block_index()

            self.logger.debug(
                f"新块已存储: {block_hash}, "
                f"original={len(block_data)}, stored={stored_size}, "
                f"compressed={is_compressed}"
            )
            return relative_path, stored_size, is_compressed

    # ------------------------------------------------------------------ #
    #  加载块
    # ------------------------------------------------------------------ #

    @staticmethod
    def _looks_like_zlib(data: bytes) -> bool:
        """
        检测数据是否看起来像 zlib 压缩格式。

        zlib 压缩数据以特定的头部字节开始:
          第一字节通常为 0x78（CMF: CM=8 deflate, CINFO=7 32K窗口）
          第二字节常见值: 0x01, 0x5E, 0x9C, 0xDA
          且 (CMF*256 + FLG) % 31 == 0
        """
        if len(data) < 2:
            return False
        # zlib CMF 字节: 低4位=CM(通常8), 高4位=CINFO(0-7)
        cmf = data[0]
        flg = data[1]
        if (cmf & 0x0F) != 8:
            return False
        if (cmf * 256 + flg) % 31 != 0:
            return False
        return True

    def load_block(self, storage_path: str, is_compressed: bool = False) -> bytes:
        """
        从存储中加载一个块，并根据需要解压。

        Args:
            storage_path:  相对于 storage_root 的块文件路径
            is_compressed: 该块是否以 zlib 压缩形式存储

        Returns:
            解压后的原始块数据

        Raises:
            FileNotFoundError: 块文件不存在
            zlib.error:        解压失败
        """
        full_path = self.storage_root / storage_path
        if not full_path.exists():
            raise FileNotFoundError(f"块文件不存在: {full_path}")

        try:
            with open(full_path, "rb") as fh:
                data = fh.read()
        except OSError as exc:
            self.logger.error(f"读取块文件失败 {full_path}: {exc}")
            raise

        if is_compressed:
            try:
                data = zlib.decompress(data)
            except zlib.error as exc:
                self.logger.error(f"解压块数据失败 {storage_path}: {exc}")
                raise
        elif self._looks_like_zlib(data):
            # 安全回退: is_compressed 标记为 False 但数据看起来像 zlib 压缩
            # 这可能发生在块索引丢失/损坏时，尝试解压以避免返回损坏数据
            try:
                decompressed = zlib.decompress(data)
                self.logger.warning(
                    f"块 {storage_path} 未标记压缩但检测到 zlib 头部，"
                    f"已自动解压 ({len(data)}B -> {len(decompressed)}B)"
                )
                data = decompressed
            except zlib.error:
                # 误判: 数据恰好以 zlib 头部字节开头但实际不是压缩数据
                pass

        return data

    # ------------------------------------------------------------------ #
    #  文件重建
    # ------------------------------------------------------------------ #

    def reconstruct_file(
        self,
        block_infos: List[Dict[str, Any]],
        output_path: str = None,
    ) -> Optional[bytes]:
        """
        根据有序的块信息列表，重建完整文件。

        每个 block_info 字典应包含:
          - storage_path  (str):  块的相对存储路径
          - is_compressed (bool): 块是否被压缩
          - block_size    (int):  块原始大小（可选，用于日志）

        如果指定了 output_path，将重建结果写入该路径并返回 None；
        否则将完整文件内容作为 bytes 返回。

        Args:
            block_infos: 按顺序排列的块信息字典列表
            output_path: 输出文件路径（可选）

        Returns:
            当 output_path 为 None 时返回重建的文件 bytes；
            否则返回 None（数据已写入文件）。

        Raises:
            FileNotFoundError: 某个块文件缺失
            zlib.error:        某个块解压失败
        """
        if not block_infos:
            self.logger.warning("block_infos 为空，无法重建文件")
            if output_path:
                # 创建空文件
                Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                Path(output_path).touch()
                return None
            return b""

        if output_path is not None:
            # ---- 写入文件模式 ----
            out = Path(output_path)
            out.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(out, "wb") as fh:
                    for idx, info in enumerate(block_infos):
                        chunk = self.load_block(
                            info["storage_path"],
                            info.get("is_compressed", False),
                        )
                        fh.write(chunk)
                        self.logger.debug(
                            f"重建写入块 {idx + 1}/{len(block_infos)}: "
                            f"{info['storage_path']}"
                        )
            except Exception as exc:
                self.logger.error(f"文件重建失败(写文件模式): {exc}")
                raise

            self.logger.info(f"文件重建完成 -> {output_path}")
            return None
        else:
            # ---- 内存模式：返回 bytes ----
            buf = io.BytesIO()
            try:
                for idx, info in enumerate(block_infos):
                    chunk = self.load_block(
                        info["storage_path"],
                        info.get("is_compressed", False),
                    )
                    buf.write(chunk)
                    self.logger.debug(
                        f"重建读取块 {idx + 1}/{len(block_infos)}: "
                        f"{info['storage_path']}"
                    )
            except Exception as exc:
                self.logger.error(f"文件重建失败(内存模式): {exc}")
                raise

            result = buf.getvalue()
            self.logger.info(f"文件重建完成(内存), 总大小={len(result)} 字节")
            return result

    # ------------------------------------------------------------------ #
    #  删除块
    # ------------------------------------------------------------------ #

    def delete_block(self, storage_path: str) -> None:
        """
        从磁盘中删除一个块文件，并清除其索引记录。

        如果块文件不存在，仅记录警告，不抛出异常。

        Args:
            storage_path: 块文件相对于 storage_root 的路径
        """
        full_path = self.storage_root / storage_path
        # 删除物理文件
        if full_path.exists():
            try:
                full_path.unlink()
                self.logger.debug(f"块文件已删除: {storage_path}")
            except OSError as exc:
                self.logger.error(f"删除块文件失败 {storage_path}: {exc}")
                raise
        else:
            self.logger.warning(f"要删除的块文件不存在: {storage_path}")

        # 从索引中移除
        with self._block_index_lock:
            # 根据 storage_path 在索引中查找对应哈希
            hash_to_remove = None
            for bhash, entry in self._block_index.items():
                if entry.get("storage_path") == storage_path:
                    hash_to_remove = bhash
                    break
            if hash_to_remove:
                del self._block_index[hash_to_remove]
                self._save_block_index()
                self.logger.debug(f"块索引已清除: {hash_to_remove}")

        # 尝试清理空的父目录（hash 前缀目录）
        parent = full_path.parent
        try:
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
        except OSError:
            pass  # 非空或权限不足，忽略

    # ------------------------------------------------------------------ #
    #  引用计数递减
    # ------------------------------------------------------------------ #

    def decrement_ref(self, block_hash: str) -> int:
        """
        将指定块的引用计数减 1，并返回减后的引用计数。

        当引用计数降至 0 时，不会自动删除块文件——
        请配合 cleanup_unreferenced_blocks() 统一清理。

        Args:
            block_hash: 块的 SHA256 哈希

        Returns:
            递减后的引用计数；如果块不存在于索引中则返回 -1
        """
        with self._block_index_lock:
            if block_hash not in self._block_index:
                self.logger.warning(f"块不在索引中，无法递减引用: {block_hash}")
                return -1

            entry = self._block_index[block_hash]
            entry["ref_count"] = max(0, entry.get("ref_count", 1) - 1)
            self._save_block_index()

            self.logger.debug(
                f"引用计数已递减: {block_hash}, ref_count={entry['ref_count']}"
            )
            return entry["ref_count"]

    # ------------------------------------------------------------------ #
    #  清理
    # ------------------------------------------------------------------ #

    def cleanup_unreferenced_blocks(self) -> int:
        """
        清理所有引用计数 <= 0 的块文件。

        Returns:
            本次清理删除的块数量
        """
        removed = 0
        with self._block_index_lock:
            hashes_to_remove = [
                bhash
                for bhash, entry in self._block_index.items()
                if entry.get("ref_count", 0) <= 0
            ]

        # 在锁外逐个删除物理文件，降低锁持有时间
        for bhash in hashes_to_remove:
            with self._block_index_lock:
                entry = self._block_index.get(bhash)
                if entry is None:
                    continue
                storage_path = entry["storage_path"]

            full_path = self.storage_root / storage_path
            if full_path.exists():
                try:
                    full_path.unlink()
                except OSError as exc:
                    self.logger.error(f"清理块文件失败 {storage_path}: {exc}")
                    continue

            # 清理空前缀目录
            parent = full_path.parent
            try:
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
            except OSError:
                pass

            with self._block_index_lock:
                self._block_index.pop(bhash, None)

            removed += 1
            self.logger.debug(f"已清理无引用块: {bhash}")

        # 统一保存索引
        if removed > 0:
            with self._block_index_lock:
                self._save_block_index()
            self.logger.info(f"清理完成，共删除 {removed} 个无引用块")

        return removed

    def cleanup_orphaned_files(self) -> int:
        """
        扫描 shared_blocks 目录，删除索引中不存在的孤立 .blk 文件。

        Returns:
            删除的孤立文件数量
        """
        removed = 0

        # 收集索引中所有合法的存储路径
        with self._block_index_lock:
            valid_paths = {
                entry["storage_path"] for entry in self._block_index.values()
            }

        # 遍历 shared_blocks 下的所有 .blk 文件
        for blk_file in self._shared_blocks_dir.rglob("*.blk"):
            try:
                relative = blk_file.relative_to(self.storage_root)
                rel_str = str(relative).replace("\\", "/")  # 统一为正斜杠
            except ValueError:
                continue

            if rel_str not in valid_paths:
                try:
                    blk_file.unlink()
                    removed += 1
                    self.logger.debug(f"已删除孤立文件: {rel_str}")
                except OSError as exc:
                    self.logger.error(f"删除孤立文件失败 {rel_str}: {exc}")

        if removed > 0:
            self.logger.info(f"孤立文件清理完成，共删除 {removed} 个文件")

        return removed

    def cleanup_temp(self) -> None:
        """
        清空临时目录 temp/ 下的所有文件和子目录。

        在文件重建等操作完成后，可调用此方法释放磁盘空间。
        """
        if not self._temp_dir.exists():
            return

        removed_count = 0
        for item in self._temp_dir.iterdir():
            try:
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
                removed_count += 1
            except OSError as exc:
                self.logger.warning(f"清理临时文件失败 {item}: {exc}")

        if removed_count > 0:
            self.logger.info(f"临时目录已清理，删除 {removed_count} 项")

    # ------------------------------------------------------------------ #
    #  存储统计
    # ------------------------------------------------------------------ #

    def get_storage_stats(self) -> Dict[str, Any]:
        """
        获取全局存储统计信息。

        返回字典包含:
          - total_stored_bytes:       磁盘上实际占用的总字节数
          - unique_blocks:            唯一块数量
          - total_references:         所有块的引用计数总和
          - total_original_bytes:     所有引用对应的原始字节总量（反映去重前逻辑大小）
          - dedup_saved_bytes:        去重节省的字节数
          - dedup_ratio:              去重压缩比 (实际存储 / 逻辑原始)，越小越好
          - compression_saved_bytes:  压缩节省的字节数
          - client_usage:             （预留）每客户端存储用量
        """
        stats: Dict[str, Any] = {
            "total_stored_bytes": 0,
            "unique_blocks": 0,
            "total_references": 0,
            "total_original_bytes": 0,
            "dedup_saved_bytes": 0,
            "dedup_ratio": 0.0,
            "compression_saved_bytes": 0,
            "client_usage": {},
        }

        with self._block_index_lock:
            stats["unique_blocks"] = len(self._block_index)

            for _bhash, entry in self._block_index.items():
                ref_count = entry.get("ref_count", 1)
                original_size = entry.get("original_size", 0)
                stored_size = entry.get("stored_size", 0)

                stats["total_stored_bytes"] += stored_size
                stats["total_references"] += ref_count
                # 逻辑大小 = 原始大小 * 引用次数
                stats["total_original_bytes"] += original_size * ref_count
                # 压缩节省 = (原始大小 - 实际存储) * 1（只存了一份）
                if entry.get("is_compressed", False):
                    stats["compression_saved_bytes"] += original_size - stored_size

        # 去重节省 = 逻辑原始大小 - 所有唯一块的原始大小之和
        unique_original = 0
        with self._block_index_lock:
            for entry in self._block_index.values():
                unique_original += entry.get("original_size", 0)

        stats["dedup_saved_bytes"] = (
            stats["total_original_bytes"] - unique_original
        )

        if stats["total_original_bytes"] > 0:
            stats["dedup_ratio"] = round(
                stats["total_stored_bytes"] / stats["total_original_bytes"], 4
            )

        return stats

    # ------------------------------------------------------------------ #
    #  OTA 更新包管理
    # ------------------------------------------------------------------ #

    def store_ota_package(self, version: str, file_data: bytes) -> str:
        """
        将 OTA 升级包写入 ota/ 目录。

        文件名格式: ota/update_v{version}_{timestamp}.zip

        Args:
            version:   版本号字符串，如 "1.2.0"
            file_data: OTA 包的完整字节数据

        Returns:
            相对于 storage_root 的存储路径
        """
        self._ota_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        # 清理版本号中的不安全字符
        safe_version = version.replace("/", "_").replace("\\", "_").replace("..", "_")
        filename = f"update_v{safe_version}_{timestamp}.zip"
        ota_path = self._ota_dir / filename

        try:
            with open(ota_path, "wb") as fh:
                fh.write(file_data)
        except OSError as exc:
            self.logger.error(f"保存 OTA 包失败: {exc}")
            raise

        relative_path = f"ota/{filename}"
        self.logger.info(
            f"OTA 包已保存: {relative_path}, 大小={len(file_data)} 字节"
        )
        return relative_path

    def get_ota_package_path(self, storage_path: str) -> Path:
        """
        获取 OTA 包的绝对路径。

        Args:
            storage_path: 相对于 storage_root 的路径

        Returns:
            OTA 包的绝对 Path 对象

        Raises:
            FileNotFoundError: 文件不存在
        """
        full_path = self.storage_root / storage_path
        if not full_path.exists():
            raise FileNotFoundError(f"OTA 包不存在: {full_path}")
        return full_path

    # ------------------------------------------------------------------ #
    #  客户端部署包生成
    # ------------------------------------------------------------------ #

    def create_client_package(
        self,
        server_host: str,
        server_port: int,
    ) -> str:
        """
        生成客户端部署 zip 包。

        将 client/ 目录下所有 Python 文件打入 zip，并嵌入
        一份 server_config.json 以便客户端自动发现服务端地址。

        Args:
            server_host: 服务端 IP 或主机名
            server_port: 服务端监听端口

        Returns:
            生成的 zip 文件相对于 storage_root 的路径

        Raises:
            FileNotFoundError: 找不到 client 目录
        """
        # 定位客户端源代码目录
        # 项目结构: project_root/server/storage_manager.py
        #           project_root/client/...
        project_root = Path(__file__).resolve().parent.parent
        client_src_dir = project_root / "client"

        if not client_src_dir.exists():
            raise FileNotFoundError(
                f"客户端源代码目录不存在: {client_src_dir}"
            )

        # 同时打包 shared/ 目录（如果存在）
        shared_src_dir = project_root / "shared"

        self._client_packages_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        zip_filename = f"backup_client_{timestamp}.zip"
        zip_path = self._client_packages_dir / zip_filename

        # 生成嵌入的服务端配置
        server_config = {
            "server": {
                "host": server_host,
                "port": server_port,
            },
            "generated_at": datetime.utcnow().isoformat(),
            "note": "此文件由服务端自动生成，请勿手动修改",
        }

        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                # 打包客户端 Python 文件
                for py_file in client_src_dir.rglob("*.py"):
                    # 跳过 __pycache__
                    if "__pycache__" in py_file.parts:
                        continue
                    arcname = str(py_file.relative_to(project_root))
                    zf.write(py_file, arcname)
                    self.logger.debug(f"打包文件: {arcname}")

                # 打包客户端 requirements.txt（如果存在）
                req_file = client_src_dir / "requirements.txt"
                if req_file.exists():
                    arcname = str(req_file.relative_to(project_root))
                    zf.write(req_file, arcname)

                # 打包 shared/ 目录
                if shared_src_dir.exists():
                    for py_file in shared_src_dir.rglob("*.py"):
                        if "__pycache__" in py_file.parts:
                            continue
                        arcname = str(py_file.relative_to(project_root))
                        zf.write(py_file, arcname)
                        self.logger.debug(f"打包共享文件: {arcname}")

                # 写入服务端配置
                config_json = json.dumps(
                    server_config, indent=2, ensure_ascii=False
                )
                zf.writestr("client/server_config.json", config_json)

            relative_path = f"client_packages/{zip_filename}"
            self.logger.info(
                f"客户端部署包已生成: {relative_path}, "
                f"server={server_host}:{server_port}"
            )
            return relative_path

        except Exception as exc:
            self.logger.error(f"生成客户端部署包失败: {exc}")
            # 清理可能已经部分写入的文件
            if zip_path.exists():
                try:
                    zip_path.unlink()
                except OSError:
                    pass
            raise

    # ------------------------------------------------------------------ #
    #  兼容性方法（保持与旧版 tcp_server.py 的调用兼容）
    # ------------------------------------------------------------------ #

    def store_file_chunk(
        self,
        client_id: str,
        file_path: str,
        version: int,
        chunk_index: int,
        chunk_data: bytes,
        is_compressed: bool = False,
    ) -> str:
        """
        存储文件块（兼容旧接口）。

        内部转换为 store_block 调用，使用 SHA256 去重。

        Args:
            client_id:     客户端标识
            file_path:     原始文件路径
            version:       版本号
            chunk_index:   块在文件中的序号
            chunk_data:    块数据（如果 is_compressed=True 则已压缩）
            is_compressed: 数据是否已被客户端压缩

        Returns:
            相对于 storage_root 的块存储路径
        """
        # 如果客户端已压缩，先解压再交由 store_block 统一处理
        raw_data = chunk_data
        if is_compressed:
            try:
                raw_data = zlib.decompress(chunk_data)
            except zlib.error:
                # 解压失败，按原始数据处理
                raw_data = chunk_data

        # 计算 SHA256
        block_hash = hashlib.sha256(raw_data).hexdigest()

        storage_path, _stored_size, _compressed = self.store_block(
            block_hash, raw_data
        )
        return storage_path

    def load_file_chunk(self, storage_path: str) -> bytes:
        """
        加载文件块（兼容旧接口）。

        根据索引自动判断块是否被压缩。如果索引中找不到对应条目，
        load_block 会通过 zlib 头部检测自动尝试解压。

        Args:
            storage_path: 块文件相对路径

        Returns:
            块的原始（解压后）数据
        """
        # 从索引中查找该路径对应的压缩标记
        is_compressed = False
        found_in_index = False
        with self._block_index_lock:
            for entry in self._block_index.values():
                if entry.get("storage_path") == storage_path:
                    is_compressed = entry.get("is_compressed", False)
                    found_in_index = True
                    break

        if not found_in_index:
            self.logger.warning(
                f"块索引中未找到路径 {storage_path}，将依赖自动检测压缩格式"
            )

        return self.load_block(storage_path, is_compressed)

    def save_file_version_metadata(
        self,
        client_id: str,
        file_path: str,
        version: int,
        file_size: int,
        file_hash: str,
        chunk_paths: List[str],
        change_type: str,
        is_full_backup: bool = False,
        reference_version: int = None,
    ) -> str:
        """
        保存文件版本元数据（兼容旧接口）。

        将版本信息写入 clients/{client_id}/versions/ 目录。

        Args:
            client_id:         客户端 ID
            file_path:         原始文件路径
            version:           版本号
            file_size:         文件大小
            file_hash:         文件哈希
            chunk_paths:       块存储路径列表
            change_type:       变更类型
            is_full_backup:    是否为完整备份
            reference_version: 参考版本号

        Returns:
            元数据文件的相对存储路径
        """
        # 使用文件路径哈希作为目录名，避免路径中的特殊字符
        path_hash = hashlib.md5(file_path.encode("utf-8")).hexdigest()
        version_dir = (
            self.storage_root
            / "clients"
            / client_id
            / "files"
            / path_hash[:2]
            / path_hash
        )
        version_dir.mkdir(parents=True, exist_ok=True)

        # 构建块信息列表（包含压缩标记，便于重建）
        block_details = []
        with self._block_index_lock:
            for cp in chunk_paths:
                detail = {"storage_path": cp, "is_compressed": False}
                for entry in self._block_index.values():
                    if entry.get("storage_path") == cp:
                        detail["is_compressed"] = entry.get("is_compressed", False)
                        detail["block_size"] = entry.get("original_size", 0)
                        break
                block_details.append(detail)

        metadata = {
            "client_id": client_id,
            "file_path": file_path,
            "version": version,
            "file_size": file_size,
            "file_hash": file_hash,
            "change_type": change_type,
            "backup_date": datetime.utcnow().isoformat(),
            "chunk_count": len(chunk_paths),
            "chunk_paths": chunk_paths,
            "block_details": block_details,
            "is_full_backup": is_full_backup,
            "reference_version": reference_version,
        }

        metadata_filename = f"v{version}_metadata.json"
        metadata_path = version_dir / metadata_filename

        try:
            with open(metadata_path, "w", encoding="utf-8") as fh:
                json.dump(metadata, fh, indent=2, ensure_ascii=False)
        except OSError as exc:
            self.logger.error(f"保存版本元数据失败: {exc}")
            raise

        relative_path = (
            f"clients/{client_id}/files/"
            f"{path_hash[:2]}/{path_hash}/{metadata_filename}"
        )
        self.logger.info(f"版本元数据已保存: {relative_path}")
        return relative_path

    def load_file_version_metadata(self, storage_path: str) -> Dict[str, Any]:
        """
        加载文件版本元数据（兼容旧接口）。

        Args:
            storage_path: 元数据文件相对路径

        Returns:
            元数据字典

        Raises:
            FileNotFoundError: 文件不存在
        """
        full_path = self.storage_root / storage_path
        if not full_path.exists():
            raise FileNotFoundError(f"元数据文件不存在: {full_path}")

        with open(full_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    # ------------------------------------------------------------------ #
    #  块信息查询
    # ------------------------------------------------------------------ #

    def get_block_info(self, block_hash: str) -> Optional[Dict[str, Any]]:
        """
        获取指定块的索引信息。

        Args:
            block_hash: 块的 SHA256 哈希

        Returns:
            块信息字典的副本；如果块不存在则返回 None
        """
        with self._block_index_lock:
            entry = self._block_index.get(block_hash)
            return dict(entry) if entry else None

    def get_all_block_hashes(self) -> List[str]:
        """
        获取当前索引中所有块的哈希列表。

        Returns:
            SHA256 哈希字符串列表
        """
        with self._block_index_lock:
            return list(self._block_index.keys())


# ====================================================================== #
#  自测入口
# ====================================================================== #

if __name__ == "__main__":
    import tempfile

    # 设置日志
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # 创建临时存储目录
    temp_dir = tempfile.mkdtemp(prefix="storage_test_")
    print(f"测试存储目录: {temp_dir}")

    try:
        # ---- 初始化 ----
        storage = StorageManager(temp_dir, enable_compression=True)

        # ---- 存储块 ----
        test_data = b"Hello, block-level deduplication! " * 2000
        block_hash = hashlib.sha256(test_data).hexdigest()
        path1, size1, comp1 = storage.store_block(block_hash, test_data)
        print(f"[1] 存储路径: {path1}, 大小: {size1}, 压缩: {comp1}")

        # ---- 去重测试 ----
        path2, size2, comp2 = storage.store_block(block_hash, test_data)
        print(f"[2] 去重路径: {path2}, 大小: {size2}, 压缩: {comp2}")
        assert path1 == path2, "去重失败: 路径不一致"
        info = storage.get_block_info(block_hash)
        assert info["ref_count"] == 2, f"引用计数应为 2，实际为 {info['ref_count']}"
        print(f"[2] 引用计数: {info['ref_count']} (正确)")

        # ---- 加载块 ----
        loaded = storage.load_block(path1, comp1)
        assert loaded == test_data, "加载数据与原始数据不一致"
        print(f"[3] 加载验证通过, 大小={len(loaded)}")

        # ---- 文件重建 ----
        block_infos = [
            {"storage_path": path1, "is_compressed": comp1, "block_size": len(test_data)},
            {"storage_path": path1, "is_compressed": comp1, "block_size": len(test_data)},
        ]
        # 内存模式
        result = storage.reconstruct_file(block_infos)
        assert result == test_data * 2, "内存重建结果不正确"
        print(f"[4] 内存重建验证通过, 大小={len(result)}")

        # 文件模式
        out_file = os.path.join(temp_dir, "reconstructed.bin")
        storage.reconstruct_file(block_infos, output_path=out_file)
        assert os.path.exists(out_file), "文件重建失败: 文件不存在"
        with open(out_file, "rb") as f:
            assert f.read() == test_data * 2, "文件重建内容不正确"
        print(f"[5] 文件重建验证通过")

        # ---- 存储统计 ----
        stats = storage.get_storage_stats()
        print(f"[6] 存储统计: {json.dumps(stats, indent=2)}")

        # ---- 引用计数与清理 ----
        storage.decrement_ref(block_hash)
        storage.decrement_ref(block_hash)
        cleaned = storage.cleanup_unreferenced_blocks()
        print(f"[7] 清理无引用块: {cleaned} 个")
        assert not storage.block_exists(block_hash), "清理后块应不存在"
        print(f"[7] 清理验证通过")

        # ---- OTA 包 ----
        ota_data = b"PK\x03\x04fake_ota_content"
        ota_path = storage.store_ota_package("1.0.0", ota_data)
        print(f"[8] OTA 路径: {ota_path}")
        ota_full = storage.get_ota_package_path(ota_path)
        assert ota_full.exists(), "OTA 包文件应存在"
        print(f"[8] OTA 验证通过")

        # ---- 临时目录清理 ----
        (storage._temp_dir / "test_temp_file.txt").write_text("temp")
        storage.cleanup_temp()
        assert not (storage._temp_dir / "test_temp_file.txt").exists()
        print(f"[9] 临时目录清理通过")

        # ---- 兼容接口测试 ----
        compat_data = b"compatibility test data " * 500
        sp = storage.store_file_chunk("client-001", "/home/user/doc.txt", 1, 0, compat_data)
        loaded_compat = storage.load_file_chunk(sp)
        assert loaded_compat == compat_data, "兼容接口数据不一致"
        print(f"[10] 兼容接口验证通过")

        print("\n全部测试通过!")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"测试目录已清理: {temp_dir}")
