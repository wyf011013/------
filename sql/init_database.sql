-- 文件自动备份系统数据库初始化脚本
-- MySQL 8.0+

-- 创建数据库
CREATE DATABASE IF NOT EXISTS file_backup_system
    DEFAULT CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE file_backup_system;

-- 客户端表
CREATE TABLE IF NOT EXISTS clients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id VARCHAR(64) UNIQUE NOT NULL COMMENT '客户端唯一标识',
    computer_name VARCHAR(255) NOT NULL COMMENT '计算机名',
    remark VARCHAR(255) DEFAULT '' COMMENT '备注(可修改)',
    ip_address VARCHAR(45) COMMENT 'IP地址',
    os_type VARCHAR(64) DEFAULT '' COMMENT '系统类型(Windows10/11等)',
    os_user VARCHAR(128) DEFAULT '' COMMENT '登录用户名',
    process_name VARCHAR(128) DEFAULT '' COMMENT '客户端进程名',
    client_version VARCHAR(32) DEFAULT '1.0.0' COMMENT '客户端版本',
    protocol_type VARCHAR(16) DEFAULT 'TCP' COMMENT '连接协议',
    backup_enabled TINYINT(1) DEFAULT 0 COMMENT '是否开启备份(0未开启/1已开启)',
    status ENUM('online', 'offline') DEFAULT 'offline' COMMENT '在线状态',
    last_heartbeat DATETIME COMMENT '最后心跳时间',
    last_seen DATETIME COMMENT '最后在线时间',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_client_id (client_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='客户端表';

-- 备份配置表(每客户端独立配置)
CREATE TABLE IF NOT EXISTS backup_configs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT NOT NULL COMMENT '客户端ID',
    backup_paths TEXT COMMENT '备份路径列表(JSON数组,可多选目录或文件)',
    exclude_patterns TEXT COMMENT '排除模式(JSON数组)',
    enable_compression TINYINT(1) DEFAULT 1 COMMENT '是否压缩',
    compression_algorithm VARCHAR(32) DEFAULT 'zlib' COMMENT '压缩算法(zlib/lz4/zstd)',
    incremental_only TINYINT(1) DEFAULT 1 COMMENT '是否仅备份增量内容',
    max_versions INT DEFAULT 0 COMMENT '最大版本数限制(0=无限制)',
    bandwidth_limit_kbps INT DEFAULT 0 COMMENT '备份传输带宽限制(KB/s, 0=不限)',
    max_capacity_mb BIGINT DEFAULT 0 COMMENT '最大备份容量限制(MB, 0=不限)',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE,
    INDEX idx_client_id (client_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='备份配置表';

-- 文件版本表
CREATE TABLE IF NOT EXISTS file_versions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT NOT NULL COMMENT '客户端ID',
    file_path VARCHAR(500) NOT NULL COMMENT '文件完整路径',
    relative_path VARCHAR(500) NOT NULL COMMENT '相对路径',
    version_number INT NOT NULL COMMENT '版本号',
    file_size BIGINT COMMENT '文件大小',
    file_hash VARCHAR(64) COMMENT '文件哈希(MD5)',
    backup_date DATETIME NOT NULL COMMENT '备份时间',
    storage_path VARCHAR(500) NOT NULL COMMENT '存储路径',
    change_type ENUM('created', 'modified', 'deleted') DEFAULT 'modified' COMMENT '变更类型',
    chunk_count INT DEFAULT 1 COMMENT '分块数量',
    is_full_backup BOOLEAN DEFAULT FALSE COMMENT '是否完整备份',
    reference_version INT COMMENT '引用版本(增量备份)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE,
    INDEX idx_client_path (client_id, file_path(255)),
    INDEX idx_hash (file_hash),
    INDEX idx_backup_date (backup_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='文件版本表';

-- 数据块表(用于块级去重)
CREATE TABLE IF NOT EXISTS data_blocks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    block_hash VARCHAR(64) UNIQUE NOT NULL COMMENT '块哈希(SHA256)',
    block_size INT NOT NULL COMMENT '原始块大小',
    stored_size INT NOT NULL COMMENT '存储后大小(压缩后)',
    storage_path VARCHAR(500) NOT NULL COMMENT '块文件存储路径',
    is_compressed TINYINT(1) DEFAULT 0 COMMENT '是否已压缩',
    ref_count INT DEFAULT 1 COMMENT '引用计数',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_block_hash (block_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='数据块表';

-- 版本块映射表(版本由哪些块组成)
CREATE TABLE IF NOT EXISTS version_blocks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    version_id INT NOT NULL COMMENT '文件版本ID',
    block_index INT NOT NULL COMMENT '块在文件中的序号',
    block_hash VARCHAR(64) NOT NULL COMMENT '块哈希',
    block_offset BIGINT DEFAULT 0 COMMENT '块在文件中的偏移',
    block_size INT NOT NULL COMMENT '块大小',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (version_id) REFERENCES file_versions(id) ON DELETE CASCADE,
    INDEX idx_version_id (version_id),
    INDEX idx_block_hash (block_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='版本块映射表';

-- 用户表
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL COMMENT '用户名',
    password_hash VARCHAR(255) NOT NULL COMMENT '密码哈希',
    role ENUM('admin', 'operator') DEFAULT 'operator' COMMENT '用户角色',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
    last_login DATETIME COMMENT '最后登录时间',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户表';

-- 系统日志表
CREATE TABLE IF NOT EXISTS system_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT COMMENT '客户端ID',
    log_level ENUM('info', 'warning', 'error') DEFAULT 'info' COMMENT '日志级别',
    message TEXT NOT NULL COMMENT '日志消息',
    details TEXT COMMENT '详细信息',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE SET NULL,
    INDEX idx_created_at (created_at),
    INDEX idx_client_id (client_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='系统日志表';

-- OTA升级版本表
CREATE TABLE IF NOT EXISTS ota_versions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    version VARCHAR(32) NOT NULL COMMENT '版本号',
    file_path VARCHAR(500) NOT NULL COMMENT '升级包路径',
    file_size BIGINT DEFAULT 0 COMMENT '文件大小',
    file_hash VARCHAR(64) COMMENT '文件MD5',
    changelog TEXT COMMENT '更新日志',
    is_active TINYINT(1) DEFAULT 1 COMMENT '是否为当前活跃版本',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_version (version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='OTA升级版本表';

-- 插入默认管理员用户 (用户名: admin, 密码: admin123)
INSERT IGNORE INTO users (username, password_hash, role) VALUES
('admin', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/J9eHCOOLa6', 'admin');

-- 创建视图: 客户端状态统计
CREATE OR REPLACE VIEW client_statistics AS
SELECT
    c.id,
    c.client_id,
    c.computer_name,
    c.remark,
    c.ip_address,
    c.os_type,
    c.os_user,
    c.status,
    c.backup_enabled,
    c.last_seen,
    c.last_heartbeat,
    c.client_version,
    COUNT(DISTINCT bc.id) as config_count,
    COUNT(DISTINCT fv.id) as backup_count,
    COALESCE(SUM(fv.file_size), 0) as total_backup_size,
    MAX(fv.backup_date) as last_backup_date
FROM clients c
LEFT JOIN backup_configs bc ON c.id = bc.client_id AND bc.is_active = TRUE
LEFT JOIN file_versions fv ON c.id = fv.client_id
GROUP BY c.id;

-- 创建视图: 存储使用统计
CREATE OR REPLACE VIEW storage_statistics AS
SELECT
    DATE(backup_date) as backup_date,
    COUNT(*) as file_count,
    SUM(file_size) as total_size,
    COUNT(DISTINCT client_id) as client_count
FROM file_versions
GROUP BY DATE(backup_date);

COMMIT;

SELECT '数据库初始化完成!' AS message;
SELECT '默认管理员账号: admin / admin123' AS message;
