"""
数据库管理模块
封装MySQL数据库操作，提供客户端管理、备份配置、文件版本、数据块、
系统日志、OTA升级、用户认证等功能的数据库访问层。
"""

import mysql.connector
from mysql.connector import Error, pooling
import logging
import bcrypt
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import json
import threading


# =============================================================================
# 数据库连接管理器（单例模式）
# =============================================================================

class DatabaseManager:
    """
    数据库管理器 - 单例模式
    维护MySQL连接池，提供通用的SQL执行方法。
    所有数据库操作均通过此管理器获取连接并执行。
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """线程安全的单例模式实现"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host: str = 'localhost', port: int = 3306,
                 user: str = 'root', password: str = '',
                 database: str = 'file_backup_system',
                 pool_size: int = 10, charset: str = 'utf8mb4'):
        """
        初始化数据库管理器

        Args:
            host: 数据库主机地址
            port: 数据库端口
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
            pool_size: 连接池大小
            charset: 字符集
        """
        # 单例模式：仅初始化一次
        if hasattr(self, '_initialized'):
            return

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool_size = pool_size
        self.charset = charset
        self.pool = None
        self.logger = logging.getLogger(__name__)

        self._init_connection_pool()
        self._initialized = True

    def _init_connection_pool(self):
        """初始化MySQL连接池"""
        try:
            self.pool = pooling.MySQLConnectionPool(
                pool_name="backup_pool",
                pool_size=self.pool_size,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset=self.charset,
                autocommit=True,
                time_zone='+00:00'
            )
            self.logger.info(f"数据库连接池已初始化 (size={self.pool_size})")
        except Error as e:
            self.logger.error(f"初始化连接池失败: {e}")
            raise

    def get_connection(self):
        """
        从连接池获取一个数据库连接

        Returns:
            MySQL连接对象
        """
        try:
            return self.pool.get_connection()
        except Error as e:
            self.logger.error(f"获取数据库连接失败: {e}")
            raise

    def execute_query(self, query: str, params: Tuple = None) -> List[Dict[str, Any]]:
        """
        执行SELECT查询语句，返回字典列表

        Args:
            query: SQL查询语句（支持%s占位符）
            params: 查询参数元组

        Returns:
            查询结果列表，每条记录为字典
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            results = cursor.fetchall()
            return results

        except Error as e:
            self.logger.error(f"查询失败: {e}, SQL: {query}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def execute_update(self, query: str, params: Tuple = None) -> int:
        """
        执行UPDATE/DELETE语句

        Args:
            query: SQL更新语句
            params: 参数元组

        Returns:
            受影响的行数
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            conn.commit()
            return cursor.rowcount

        except Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"更新失败: {e}, SQL: {query}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def execute_insert(self, query: str, params: Tuple = None) -> int:
        """
        执行INSERT语句并返回自增主键ID

        Args:
            query: SQL插入语句
            params: 参数元组

        Returns:
            插入记录的自增ID
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            conn.commit()
            return cursor.lastrowid

        except Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"插入失败: {e}, SQL: {query}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def execute_many(self, query: str, params: List[Tuple]) -> int:
        """
        批量执行SQL语句（如批量INSERT）

        Args:
            query: SQL语句模板
            params: 参数列表，每个元素为一组参数元组

        Returns:
            受影响的总行数
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.executemany(query, params)
            conn.commit()

            return cursor.rowcount

        except Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"批量执行失败: {e}, SQL: {query}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


# =============================================================================
# 客户端管理器
# =============================================================================

class ClientManager:
    """
    客户端管理器
    负责客户端的注册、状态更新、心跳维护、查询与删除等操作。
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化客户端管理器

        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def register_client(self, client_id: str, computer_name: str,
                        ip_address: str = None, os_type: str = '',
                        os_user: str = '', process_name: str = '',
                        client_version: str = '1.0.0') -> int:
        """
        注册或更新客户端信息。
        若client_id已存在则更新信息并置为在线状态，否则插入新记录。

        Args:
            client_id: 客户端唯一标识（UUID字符串）
            computer_name: 计算机名称
            ip_address: 客户端IP地址
            os_type: 操作系统类型（如 Windows 10）
            os_user: 操作系统登录用户名
            process_name: 客户端进程名称
            client_version: 客户端版本号

        Returns:
            客户端在数据库中的自增ID
        """
        # 查询客户端是否已存在
        query = "SELECT id FROM clients WHERE client_id = %s"
        result = self.db.execute_query(query, (client_id,))

        if result:
            # 客户端已存在，更新信息
            client_db_id = result[0]['id']
            update_query = """
                UPDATE clients
                SET computer_name = %s, ip_address = %s, os_type = %s,
                    os_user = %s, process_name = %s, client_version = %s,
                    last_heartbeat = NOW(), last_seen = NOW(), status = 'online'
                WHERE id = %s
            """
            self.db.execute_update(update_query, (
                computer_name, ip_address, os_type,
                os_user, process_name, client_version, client_db_id
            ))
        else:
            # 插入新客户端
            insert_query = """
                INSERT INTO clients
                    (client_id, computer_name, ip_address, os_type, os_user,
                     process_name, client_version, last_heartbeat, last_seen, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), 'online')
            """
            client_db_id = self.db.execute_insert(insert_query, (
                client_id, computer_name, ip_address, os_type,
                os_user, process_name, client_version
            ))

        self.logger.info(f"客户端已注册: {client_id} (DB ID: {client_db_id})")
        return client_db_id

    def update_client_status(self, client_id: str, status: str):
        """
        更新客户端在线状态

        Args:
            client_id: 客户端唯一标识（UUID字符串）
            status: 状态值 ('online' 或 'offline')
        """
        query = """
            UPDATE clients
            SET status = %s, last_seen = NOW()
            WHERE client_id = %s
        """
        self.db.execute_update(query, (status, client_id))
        self.logger.debug(f"客户端状态更新: {client_id} -> {status}")

    def update_client_heartbeat(self, client_id: str):
        """
        更新客户端心跳时间，同时刷新最后在线时间并确保状态为online

        Args:
            client_id: 客户端唯一标识（UUID字符串）
        """
        query = """
            UPDATE clients
            SET last_heartbeat = NOW(), last_seen = NOW(), status = 'online'
            WHERE client_id = %s
        """
        self.db.execute_update(query, (client_id,))
        self.logger.debug(f"客户端心跳更新: {client_id}")

    def update_client_remark(self, client_id_str: str, remark: str):
        """
        更新客户端备注信息

        Args:
            client_id_str: 客户端唯一标识（UUID字符串）
            remark: 新的备注内容
        """
        query = """
            UPDATE clients
            SET remark = %s
            WHERE client_id = %s
        """
        self.db.execute_update(query, (remark, client_id_str))
        self.logger.info(f"客户端备注已更新: {client_id_str} -> {remark}")

    def update_backup_enabled(self, client_id_str: str, enabled: bool):
        """
        更新客户端备份启用状态

        Args:
            client_id_str: 客户端唯一标识（UUID字符串）
            enabled: 是否启用备份（True=启用, False=禁用）
        """
        query = """
            UPDATE clients
            SET backup_enabled = %s
            WHERE client_id = %s
        """
        self.db.execute_update(query, (1 if enabled else 0, client_id_str))
        self.logger.info(f"客户端备份状态已更新: {client_id_str} -> {'启用' if enabled else '禁用'}")

    def get_client_by_uuid(self, client_id_str: str) -> Optional[Dict[str, Any]]:
        """
        根据客户端UUID获取客户端完整信息

        Args:
            client_id_str: 客户端唯一标识（UUID字符串）

        Returns:
            客户端信息字典，不存在则返回None
        """
        query = """
            SELECT id, client_id, computer_name, remark, ip_address,
                   os_type, os_user, process_name, client_version,
                   protocol_type, backup_enabled, status,
                   last_heartbeat, last_seen, created_at, updated_at
            FROM clients
            WHERE client_id = %s
        """
        result = self.db.execute_query(query, (client_id_str,))
        return result[0] if result else None

    def get_client_by_db_id(self, db_id: int) -> Optional[Dict[str, Any]]:
        """
        根据数据库自增ID获取客户端完整信息

        Args:
            db_id: 数据库自增主键ID

        Returns:
            客户端信息字典，不存在则返回None
        """
        query = """
            SELECT id, client_id, computer_name, remark, ip_address,
                   os_type, os_user, process_name, client_version,
                   protocol_type, backup_enabled, status,
                   last_heartbeat, last_seen, created_at, updated_at
            FROM clients
            WHERE id = %s
        """
        result = self.db.execute_query(query, (db_id,))
        return result[0] if result else None

    def get_all_clients(self) -> List[Dict[str, Any]]:
        """
        获取所有客户端列表，附带备份统计信息（文件数量、备份总大小、最后备份时间）

        Returns:
            客户端列表，每个元素包含客户端信息和备份统计
        """
        query = """
            SELECT c.id, c.client_id, c.computer_name, c.remark, c.ip_address,
                   c.os_type, c.os_user, c.process_name, c.client_version,
                   c.protocol_type, c.backup_enabled, c.status,
                   c.last_heartbeat, c.last_seen, c.created_at, c.updated_at,
                   COUNT(DISTINCT fv.id) AS backup_count,
                   COALESCE(SUM(fv.file_size), 0) AS total_backup_size,
                   MAX(fv.backup_date) AS last_backup_date
            FROM clients c
            LEFT JOIN file_versions fv ON c.id = fv.client_id
            GROUP BY c.id
            ORDER BY c.created_at DESC
        """
        return self.db.execute_query(query)

    def get_online_clients(self) -> List[Dict[str, Any]]:
        """
        获取当前在线的客户端列表

        Returns:
            在线客户端列表
        """
        query = """
            SELECT id, client_id, computer_name, remark, ip_address,
                   os_type, os_user, process_name, client_version,
                   protocol_type, backup_enabled, status,
                   last_heartbeat, last_seen, created_at, updated_at
            FROM clients
            WHERE status = 'online'
            ORDER BY last_seen DESC
        """
        return self.db.execute_query(query)

    def get_client_count(self) -> Dict[str, int]:
        """
        获取客户端数量统计

        Returns:
            包含total（总数）和online（在线数）的字典
        """
        query = """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'online' THEN 1 ELSE 0 END) AS online
            FROM clients
        """
        result = self.db.execute_query(query)
        if result:
            return {
                'total': result[0]['total'] or 0,
                'online': result[0]['online'] or 0
            }
        return {'total': 0, 'online': 0}

    def delete_client(self, db_id: int) -> bool:
        """
        删除客户端及其所有关联数据（备份配置、文件版本等通过外键级联删除）

        Args:
            db_id: 客户端数据库自增ID

        Returns:
            是否删除成功
        """
        try:
            query = "DELETE FROM clients WHERE id = %s"
            affected = self.db.execute_update(query, (db_id,))
            if affected > 0:
                self.logger.info(f"客户端已删除: DB ID={db_id}")
                return True
            else:
                self.logger.warning(f"删除客户端失败，未找到记录: DB ID={db_id}")
                return False
        except Error as e:
            self.logger.error(f"删除客户端异常: {e}")
            return False


# =============================================================================
# 备份配置管理器
# =============================================================================

class BackupConfigManager:
    """
    备份配置管理器
    负责每个客户端的备份路径、排除规则、压缩策略、版本保留等配置的存取。
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化备份配置管理器

        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def set_client_config(self, client_id: int, backup_paths: list = None,
                          exclude_patterns: list = None,
                          enable_compression: bool = True,
                          compression_algorithm: str = 'zlib',
                          incremental_only: bool = True,
                          max_versions: int = 0,
                          bandwidth_limit_kbps: int = 0,
                          max_capacity_mb: int = 0) -> int:
        """
        设置或更新客户端备份配置。
        若该客户端已有配置则更新，否则插入新记录。

        Args:
            client_id: 客户端数据库ID（clients表的自增ID）
            backup_paths: 备份路径列表（JSON序列化存储）
            exclude_patterns: 排除模式列表（JSON序列化存储）
            enable_compression: 是否启用压缩
            compression_algorithm: 压缩算法（zlib/lz4/zstd）
            incremental_only: 是否仅增量备份
            max_versions: 最大版本数（0=无限制）
            bandwidth_limit_kbps: 带宽限制KB/s（0=不限）
            max_capacity_mb: 最大备份容量MB（0=不限）

        Returns:
            配置记录ID
        """
        # 将列表序列化为JSON字符串存储
        backup_paths_json = json.dumps(backup_paths or [], ensure_ascii=False)
        exclude_patterns_json = json.dumps(exclude_patterns or [], ensure_ascii=False)

        # 检查是否已有该客户端的配置
        query = "SELECT id FROM backup_configs WHERE client_id = %s"
        result = self.db.execute_query(query, (client_id,))

        if result:
            # 更新已有配置
            config_id = result[0]['id']
            update_query = """
                UPDATE backup_configs
                SET backup_paths = %s, exclude_patterns = %s,
                    enable_compression = %s, compression_algorithm = %s,
                    incremental_only = %s, max_versions = %s,
                    bandwidth_limit_kbps = %s, max_capacity_mb = %s,
                    is_active = TRUE, updated_at = NOW()
                WHERE id = %s
            """
            self.db.execute_update(update_query, (
                backup_paths_json, exclude_patterns_json,
                1 if enable_compression else 0, compression_algorithm,
                1 if incremental_only else 0, max_versions,
                bandwidth_limit_kbps, max_capacity_mb, config_id
            ))
        else:
            # 插入新配置
            insert_query = """
                INSERT INTO backup_configs
                    (client_id, backup_paths, exclude_patterns, enable_compression,
                     compression_algorithm, incremental_only, max_versions,
                     bandwidth_limit_kbps, max_capacity_mb)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            config_id = self.db.execute_insert(insert_query, (
                client_id, backup_paths_json, exclude_patterns_json,
                1 if enable_compression else 0, compression_algorithm,
                1 if incremental_only else 0, max_versions,
                bandwidth_limit_kbps, max_capacity_mb
            ))

        self.logger.info(f"客户端备份配置已设置: client_id={client_id}, config_id={config_id}")
        return config_id

    def get_client_config(self, client_id: int) -> Optional[Dict[str, Any]]:
        """
        获取客户端的备份配置，自动解析JSON字段

        Args:
            client_id: 客户端数据库ID

        Returns:
            配置字典（含解析后的列表字段），不存在则返回None
        """
        query = """
            SELECT id, client_id, backup_paths, exclude_patterns,
                   enable_compression, compression_algorithm, incremental_only,
                   max_versions, bandwidth_limit_kbps, max_capacity_mb,
                   is_active, created_at, updated_at
            FROM backup_configs
            WHERE client_id = %s AND is_active = TRUE
            ORDER BY updated_at DESC
            LIMIT 1
        """
        result = self.db.execute_query(query, (client_id,))

        if result:
            config = result[0]
            # 解析JSON字段为Python列表
            try:
                config['backup_paths'] = json.loads(config.get('backup_paths') or '[]')
            except (json.JSONDecodeError, TypeError):
                config['backup_paths'] = []
            try:
                config['exclude_patterns'] = json.loads(config.get('exclude_patterns') or '[]')
            except (json.JSONDecodeError, TypeError):
                config['exclude_patterns'] = []
            return config

        return None

    def get_all_configs(self) -> List[Dict[str, Any]]:
        """
        获取所有客户端的备份配置，关联客户端信息

        Returns:
            配置列表，每条包含客户端UUID和计算机名
        """
        query = """
            SELECT bc.*, c.client_id AS client_uuid, c.computer_name
            FROM backup_configs bc
            JOIN clients c ON bc.client_id = c.id
            WHERE bc.is_active = TRUE
            ORDER BY bc.updated_at DESC
        """
        results = self.db.execute_query(query)

        # 解析JSON字段
        for config in results:
            try:
                config['backup_paths'] = json.loads(config.get('backup_paths') or '[]')
            except (json.JSONDecodeError, TypeError):
                config['backup_paths'] = []
            try:
                config['exclude_patterns'] = json.loads(config.get('exclude_patterns') or '[]')
            except (json.JSONDecodeError, TypeError):
                config['exclude_patterns'] = []

        return results


# =============================================================================
# 文件版本管理器
# =============================================================================

class FileVersionManager:
    """
    文件版本管理器
    负责文件备份版本记录的增删查及统计功能。
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化文件版本管理器

        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def add_file_version(self, client_id: int, file_path: str,
                         relative_path: str, version_number: int,
                         file_size: int, file_hash: str,
                         storage_path: str, change_type: str,
                         chunk_count: int = 1, is_full_backup: bool = False,
                         reference_version: int = None) -> int:
        """
        添加一条文件版本记录

        Args:
            client_id: 客户端数据库ID
            file_path: 文件完整路径
            relative_path: 文件相对路径
            version_number: 版本号
            file_size: 文件大小（字节）
            file_hash: 文件内容哈希（MD5）
            storage_path: 文件在服务端的存储路径
            change_type: 变更类型 ('created', 'modified', 'deleted')
            chunk_count: 文件分块数量
            is_full_backup: 是否为完整备份
            reference_version: 增量备份的参考版本ID

        Returns:
            新版本记录的ID
        """
        query = """
            INSERT INTO file_versions
                (client_id, file_path, relative_path, version_number, file_size,
                 file_hash, backup_date, storage_path, change_type, chunk_count,
                 is_full_backup, reference_version)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s)
        """
        version_id = self.db.execute_insert(query, (
            client_id, file_path, relative_path, version_number,
            file_size, file_hash, storage_path, change_type,
            chunk_count, is_full_backup, reference_version
        ))

        self.logger.info(f"文件版本已记录: {file_path} v{version_number} (ID: {version_id})")
        return version_id

    def get_file_versions(self, client_id: int, file_path: str) -> List[Dict[str, Any]]:
        """
        获取指定客户端某个文件的所有版本，按版本号降序排列

        Args:
            client_id: 客户端数据库ID
            file_path: 文件完整路径

        Returns:
            版本记录列表（最新版本在前）
        """
        query = """
            SELECT id, client_id, file_path, relative_path, version_number,
                   file_size, file_hash, backup_date, storage_path, change_type,
                   chunk_count, is_full_backup, reference_version, created_at
            FROM file_versions
            WHERE client_id = %s AND file_path = %s
            ORDER BY version_number DESC
        """
        return self.db.execute_query(query, (client_id, file_path))

    def get_latest_version(self, client_id: int, file_path: str) -> Optional[Dict[str, Any]]:
        """
        获取指定文件的最新版本信息

        Args:
            client_id: 客户端数据库ID
            file_path: 文件完整路径

        Returns:
            最新版本信息字典，不存在则返回None
        """
        query = """
            SELECT id, client_id, file_path, relative_path, version_number,
                   file_size, file_hash, backup_date, storage_path, change_type,
                   chunk_count, is_full_backup, reference_version, created_at
            FROM file_versions
            WHERE client_id = %s AND file_path = %s
            ORDER BY version_number DESC
            LIMIT 1
        """
        result = self.db.execute_query(query, (client_id, file_path))
        return result[0] if result else None

    def get_client_files(self, client_id: int) -> List[Dict[str, Any]]:
        """
        获取客户端所有已备份文件的列表（每个文件仅返回最新版本信息）

        Args:
            client_id: 客户端数据库ID

        Returns:
            去重后的文件列表，每条包含最新版本号、大小、备份日期等信息
        """
        query = """
            SELECT fv.file_path, fv.relative_path, fv.version_number,
                   fv.file_size, fv.file_hash, fv.backup_date, fv.change_type,
                   fv.id AS version_id
            FROM file_versions fv
            INNER JOIN (
                SELECT file_path, MAX(version_number) AS max_version
                FROM file_versions
                WHERE client_id = %s
                GROUP BY file_path
            ) latest ON fv.file_path = latest.file_path
                    AND fv.version_number = latest.max_version
            WHERE fv.client_id = %s
            ORDER BY fv.backup_date DESC
        """
        return self.db.execute_query(query, (client_id, client_id))

    def get_client_backup_stats(self, client_id: int) -> Dict[str, Any]:
        """
        获取客户端的备份统计信息

        Args:
            client_id: 客户端数据库ID

        Returns:
            统计字典：unique_files（不同文件数）、total_versions（总版本数）、
            total_size（总大小）、today_files（今日备份文件数）
        """
        # 文件数量和版本统计
        file_count_query = """
            SELECT COUNT(DISTINCT file_path) AS unique_files,
                   COUNT(*) AS total_versions
            FROM file_versions
            WHERE client_id = %s
        """
        file_stats = self.db.execute_query(file_count_query, (client_id,))[0]

        # 总存储大小统计
        size_query = """
            SELECT COALESCE(SUM(file_size), 0) AS total_size
            FROM file_versions
            WHERE client_id = %s
        """
        size_stats = self.db.execute_query(size_query, (client_id,))[0]

        # 今日备份统计
        today_query = """
            SELECT COUNT(*) AS today_files
            FROM file_versions
            WHERE client_id = %s AND DATE(backup_date) = CURDATE()
        """
        today_stats = self.db.execute_query(today_query, (client_id,))[0]

        return {
            'unique_files': file_stats['unique_files'] or 0,
            'total_versions': file_stats['total_versions'] or 0,
            'total_size': size_stats['total_size'] or 0,
            'today_files': today_stats['today_files'] or 0
        }

    def get_total_backup_stats(self) -> Dict[str, Any]:
        """
        获取全局备份统计信息（所有客户端合计）

        Returns:
            统计字典：total_files（不同文件总数）、total_versions（版本总数）、
            total_size（总存储大小）
        """
        query = """
            SELECT COUNT(DISTINCT CONCAT(client_id, ':', file_path)) AS total_files,
                   COUNT(*) AS total_versions,
                   COALESCE(SUM(file_size), 0) AS total_size
            FROM file_versions
        """
        result = self.db.execute_query(query)
        if result:
            return {
                'total_files': result[0]['total_files'] or 0,
                'total_versions': result[0]['total_versions'] or 0,
                'total_size': result[0]['total_size'] or 0
            }
        return {'total_files': 0, 'total_versions': 0, 'total_size': 0}

    def get_version_by_id(self, version_id: int) -> Optional[Dict[str, Any]]:
        """
        根据版本记录ID获取完整版本信息

        Args:
            version_id: 文件版本记录ID

        Returns:
            版本信息字典，不存在则返回None
        """
        query = """
            SELECT id, client_id, file_path, relative_path, version_number,
                   file_size, file_hash, backup_date, storage_path, change_type,
                   chunk_count, is_full_backup, reference_version, created_at
            FROM file_versions
            WHERE id = %s
        """
        result = self.db.execute_query(query, (version_id,))
        return result[0] if result else None


# =============================================================================
# 数据块管理器
# =============================================================================

class BlockManager:
    """
    数据块管理器
    负责块级去重存储中的数据块（data_blocks）和版本块映射（version_blocks）的管理。
    通过块哈希实现跨客户端、跨文件的数据去重。
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化数据块管理器

        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def add_block(self, block_hash: str, block_size: int, stored_size: int,
                  storage_path: str, is_compressed: bool = False) -> int:
        """
        添加一个数据块记录。若block_hash已存在则仅增加引用计数。

        Args:
            block_hash: 块内容的SHA256哈希
            block_size: 原始块大小（字节）
            stored_size: 实际存储大小（压缩后大小）
            storage_path: 块文件的存储路径
            is_compressed: 块是否已压缩

        Returns:
            数据块记录ID
        """
        # 先查询是否已存在相同哈希的块
        existing = self.get_block(block_hash)
        if existing:
            # 块已存在，增加引用计数
            self.increment_ref_count(block_hash)
            return existing['id']

        # 插入新数据块
        query = """
            INSERT INTO data_blocks
                (block_hash, block_size, stored_size, storage_path, is_compressed, ref_count)
            VALUES (%s, %s, %s, %s, %s, 1)
        """
        block_id = self.db.execute_insert(query, (
            block_hash, block_size, stored_size, storage_path,
            1 if is_compressed else 0
        ))

        self.logger.debug(f"新数据块已添加: {block_hash[:16]}... (ID: {block_id})")
        return block_id

    def get_block(self, block_hash: str) -> Optional[Dict[str, Any]]:
        """
        根据块哈希获取数据块信息

        Args:
            block_hash: 块哈希值

        Returns:
            数据块信息字典，不存在则返回None
        """
        query = """
            SELECT id, block_hash, block_size, stored_size,
                   storage_path, is_compressed, ref_count, created_at
            FROM data_blocks
            WHERE block_hash = %s
        """
        result = self.db.execute_query(query, (block_hash,))
        return result[0] if result else None

    def increment_ref_count(self, block_hash: str):
        """
        增加数据块的引用计数（当新版本引用已有块时调用）

        Args:
            block_hash: 块哈希值
        """
        query = """
            UPDATE data_blocks
            SET ref_count = ref_count + 1
            WHERE block_hash = %s
        """
        self.db.execute_update(query, (block_hash,))
        self.logger.debug(f"块引用计数+1: {block_hash[:16]}...")

    def decrement_ref_count(self, block_hash: str):
        """
        减少数据块的引用计数（当版本被删除时调用）。
        当引用计数降为0时，块可在后续清理任务中被安全删除。

        Args:
            block_hash: 块哈希值
        """
        query = """
            UPDATE data_blocks
            SET ref_count = ref_count - 1
            WHERE block_hash = %s AND ref_count > 0
        """
        self.db.execute_update(query, (block_hash,))
        self.logger.debug(f"块引用计数-1: {block_hash[:16]}...")

    def add_version_blocks(self, version_id: int, blocks: List[Dict[str, Any]]):
        """
        批量添加版本块映射记录，描述某个文件版本由哪些块组成

        Args:
            version_id: 文件版本ID（file_versions表的ID）
            blocks: 块信息列表，每个元素为字典：
                    {block_index, block_hash, block_offset, block_size}
        """
        if not blocks:
            return

        query = """
            INSERT INTO version_blocks
                (version_id, block_index, block_hash, block_offset, block_size)
            VALUES (%s, %s, %s, %s, %s)
        """
        params = [
            (
                version_id,
                block['block_index'],
                block['block_hash'],
                block.get('block_offset', 0),
                block['block_size']
            )
            for block in blocks
        ]
        self.db.execute_many(query, params)
        self.logger.debug(f"版本块映射已添加: version_id={version_id}, blocks={len(blocks)}")

    def get_version_blocks(self, version_id: int) -> List[Dict[str, Any]]:
        """
        获取某个文件版本的所有块映射，按块序号排序

        Args:
            version_id: 文件版本ID

        Returns:
            块映射列表，按block_index升序排列
        """
        query = """
            SELECT vb.id, vb.version_id, vb.block_index, vb.block_hash,
                   vb.block_offset, vb.block_size, vb.created_at
            FROM version_blocks vb
            WHERE vb.version_id = %s
            ORDER BY vb.block_index ASC
        """
        return self.db.execute_query(query, (version_id,))

    def get_blocks_by_hashes(self, hash_list: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        根据哈希列表批量查询数据块信息

        Args:
            hash_list: 块哈希列表

        Returns:
            字典：{block_hash: block_info_dict}
        """
        if not hash_list:
            return {}

        # 使用IN子句批量查询，构造占位符
        placeholders = ','.join(['%s'] * len(hash_list))
        query = f"""
            SELECT id, block_hash, block_size, stored_size,
                   storage_path, is_compressed, ref_count, created_at
            FROM data_blocks
            WHERE block_hash IN ({placeholders})
        """
        results = self.db.execute_query(query, tuple(hash_list))

        # 转换为字典形式方便按哈希查找
        return {row['block_hash']: row for row in results}


# =============================================================================
# 系统日志管理器
# =============================================================================

class SystemLogManager:
    """
    系统日志管理器
    负责向system_logs表写入和查询系统运行日志。
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化系统日志管理器

        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def add_log(self, level: str, message: str,
                client_id: int = None, details: str = None):
        """
        添加一条系统日志记录

        Args:
            level: 日志级别 ('info', 'warning', 'error')
            message: 日志消息内容
            client_id: 关联的客户端数据库ID（可选）
            details: 详细补充信息（可选）
        """
        try:
            query = """
                INSERT INTO system_logs (client_id, log_level, message, details)
                VALUES (%s, %s, %s, %s)
            """
            self.db.execute_insert(query, (client_id, level, message, details))
        except Error as e:
            # 日志写入失败不应影响主流程，仅记录到Python日志
            self.logger.error(f"写入系统日志失败: {e}")

    def get_logs(self, limit: int = 100, level: str = None,
                 client_id: int = None) -> List[Dict[str, Any]]:
        """
        查询系统日志，支持按级别和客户端过滤

        Args:
            limit: 返回记录上限（默认100条）
            level: 按日志级别过滤（可选）
            client_id: 按客户端ID过滤（可选）

        Returns:
            日志记录列表，按时间倒序排列
        """
        query = """
            SELECT sl.id, sl.client_id, sl.log_level, sl.message,
                   sl.details, sl.created_at,
                   c.client_id AS client_uuid, c.computer_name
            FROM system_logs sl
            LEFT JOIN clients c ON sl.client_id = c.id
            WHERE 1=1
        """
        params = []

        if level:
            query += " AND sl.log_level = %s"
            params.append(level)

        if client_id is not None:
            query += " AND sl.client_id = %s"
            params.append(client_id)

        query += " ORDER BY sl.created_at DESC LIMIT %s"
        params.append(limit)

        return self.db.execute_query(query, tuple(params))


# =============================================================================
# OTA升级管理器
# =============================================================================

class OTAManager:
    """
    OTA升级管理器
    负责客户端OTA版本包的发布、查询等操作。
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化OTA升级管理器

        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def add_version(self, version: str, file_path: str,
                    file_size: int, file_hash: str,
                    changelog: str = '') -> int:
        """
        发布一个新的OTA升级版本。
        新版本会自动成为活跃版本，之前的活跃版本会被标记为非活跃。

        Args:
            version: 版本号字符串（如 '1.2.0'）
            file_path: 升级包文件路径
            file_size: 升级包大小（字节）
            file_hash: 升级包MD5哈希
            changelog: 更新日志/变更说明

        Returns:
            新版本记录ID
        """
        # 将之前所有版本标记为非活跃
        deactivate_query = """
            UPDATE ota_versions SET is_active = 0 WHERE is_active = 1
        """
        self.db.execute_update(deactivate_query)

        # 插入新版本
        insert_query = """
            INSERT INTO ota_versions
                (version, file_path, file_size, file_hash, changelog, is_active)
            VALUES (%s, %s, %s, %s, %s, 1)
        """
        version_id = self.db.execute_insert(insert_query, (
            version, file_path, file_size, file_hash, changelog
        ))

        self.logger.info(f"OTA新版本已发布: {version} (ID: {version_id})")
        return version_id

    def get_latest_version(self) -> Optional[Dict[str, Any]]:
        """
        获取当前最新的活跃OTA版本

        Returns:
            版本信息字典，不存在则返回None
        """
        query = """
            SELECT id, version, file_path, file_size, file_hash,
                   changelog, is_active, created_at
            FROM ota_versions
            WHERE is_active = 1
            ORDER BY created_at DESC
            LIMIT 1
        """
        result = self.db.execute_query(query)
        return result[0] if result else None

    def get_all_versions(self) -> List[Dict[str, Any]]:
        """
        获取所有OTA版本列表，按发布时间倒序排列

        Returns:
            版本记录列表
        """
        query = """
            SELECT id, version, file_path, file_size, file_hash,
                   changelog, is_active, created_at
            FROM ota_versions
            ORDER BY created_at DESC
        """
        return self.db.execute_query(query)


# =============================================================================
# 用户管理器
# =============================================================================

class UserManager:
    """
    用户管理器
    负责Web管理端用户的认证与查询，密码使用bcrypt安全哈希。
    """

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化用户管理器

        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        验证用户登录凭据。
        使用bcrypt对比密码哈希，验证成功后更新最后登录时间。

        Args:
            username: 用户名
            password: 明文密码

        Returns:
            验证成功返回用户信息字典（不含密码哈希），失败返回None
        """
        query = """
            SELECT id, username, password_hash, role, is_active,
                   last_login, created_at
            FROM users
            WHERE username = %s AND is_active = TRUE
        """
        result = self.db.execute_query(query, (username,))

        if not result:
            self.logger.warning(f"用户认证失败，用户不存在或已禁用: {username}")
            return None

        user = result[0]
        stored_hash = user['password_hash']

        # 使用bcrypt验证密码
        try:
            if isinstance(stored_hash, str):
                stored_hash = stored_hash.encode('utf-8')
            if isinstance(password, str):
                password = password.encode('utf-8')

            if bcrypt.checkpw(password, stored_hash):
                # 密码验证通过，更新最后登录时间
                update_query = """
                    UPDATE users SET last_login = NOW() WHERE id = %s
                """
                self.db.execute_update(update_query, (user['id'],))

                self.logger.info(f"用户认证成功: {username}")

                # 返回用户信息（移除密码哈希）
                del user['password_hash']
                return user
            else:
                self.logger.warning(f"用户认证失败，密码错误: {username}")
                return None
        except Exception as e:
            self.logger.error(f"用户认证异常: {e}")
            return None

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        """
        根据用户名获取用户基本信息（不含密码哈希）

        Args:
            username: 用户名

        Returns:
            用户信息字典，不存在则返回None
        """
        query = """
            SELECT id, username, role, is_active, last_login, created_at
            FROM users
            WHERE username = %s
        """
        result = self.db.execute_query(query, (username,))
        return result[0] if result else None


# =============================================================================
# 模块测试入口
# =============================================================================

if __name__ == "__main__":
    import sys

    # 设置控制台日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    try:
        # 创建数据库管理器（请根据实际环境修改连接参数）
        db = DatabaseManager(
            host='localhost',
            port=3306,
            user='root',
            password='password',
            database='file_backup_system'
        )

        # 测试客户端管理
        client_mgr = ClientManager(db)
        test_client_id = "test-client-001"
        client_db_id = client_mgr.register_client(
            test_client_id, "Test-PC", "192.168.1.100",
            os_type="Windows 10", os_user="testuser",
            process_name="backup_client.exe", client_version="1.0.0"
        )
        print(f"注册客户端: {test_client_id} (DB ID: {client_db_id})")

        # 获取客户端信息
        client_info = client_mgr.get_client_by_uuid(test_client_id)
        print(f"客户端信息: {client_info}")

        # 客户端计数
        count = client_mgr.get_client_count()
        print(f"客户端统计: {count}")

        # 测试备份配置管理
        config_mgr = BackupConfigManager(db)
        config_id = config_mgr.set_client_config(
            client_db_id,
            backup_paths=["C:\\Users\\Documents", "C:\\Users\\Desktop"],
            exclude_patterns=["*.tmp", "*.log", "~*"],
            enable_compression=True,
            compression_algorithm='zlib',
            incremental_only=True,
            max_versions=10,
            bandwidth_limit_kbps=0,
            max_capacity_mb=10240
        )
        print(f"设置配置: {config_id}")

        config = config_mgr.get_client_config(client_db_id)
        print(f"配置信息: {config}")

        # 测试文件版本管理
        version_mgr = FileVersionManager(db)
        version_id = version_mgr.add_file_version(
            client_db_id,
            "C:\\Users\\Documents\\test.txt",
            "Documents\\test.txt",
            1, 1024, "abc123def456",
            "/storage/client1/test.txt.v1",
            "created"
        )
        print(f"添加版本: {version_id}")

        # 获取统计信息
        stats = version_mgr.get_client_backup_stats(client_db_id)
        print(f"备份统计: {stats}")

        total_stats = version_mgr.get_total_backup_stats()
        print(f"全局统计: {total_stats}")

        # 测试数据块管理
        block_mgr = BlockManager(db)
        block_id = block_mgr.add_block(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            65536, 32768,
            "shared_blocks/e3/e3b0c44298fc1c14.blk",
            is_compressed=True
        )
        print(f"添加数据块: {block_id}")

        # 测试系统日志
        log_mgr = SystemLogManager(db)
        log_mgr.add_log('info', '系统测试日志', client_db_id, '这是一条测试详情')
        logs = log_mgr.get_logs(limit=5)
        print(f"最新日志: {len(logs)} 条")

        # 测试用户认证
        user_mgr = UserManager(db)
        user = user_mgr.authenticate('admin', 'admin123')
        print(f"用户认证: {'成功' if user else '失败'}")

        print("\n数据库模块测试完成！")

    except Exception as e:
        print(f"数据库测试失败: {e}")
        sys.exit(1)
