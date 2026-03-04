# 文件自动备份系统

一个基于Python的企业级文件自动备份解决方案，支持Windows平台，提供实时监控、增量备份、版本管理等功能。

## 功能特性

### 核心功能
- ✅ **实时监控文件变化** - 使用watchdog库监控指定目录的文件变化
- ✅ **完全隐藏运行** - 客户端无窗口、无托盘图标，后台服务运行
- ✅ **开机自启动** - 通过Windows注册表实现开机自动启动
- ✅ **增量备份** - 只备份变化的文件部分，节省带宽和存储
- ✅ **版本管理** - 保存文件的多个历史版本
- ✅ **数据去重** - 块级去重，相同数据只存储一次
- ✅ **数据压缩** - 支持数据压缩，减少存储占用

### 技术特性
- ✅ **自定义TCP协议** - 实现高效可靠的客户端-服务端通信
- ✅ **MySQL数据库** - 存储客户端信息、配置、版本记录
- ✅ **Web管理界面** - 基于AdminLTE的现代化管理界面
- ✅ **Windows注册表** - 管理开机自启动和系统配置
- ✅ **Windows服务** - 可选的Windows服务部署方式

## 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   客户端 (PC)    │────│   服务端 (Server) │────│  Web管理端      │
│ • 文件监控       │ TCP │ • 文件接收       │    │ • 用户管理      │
│ • 增量备份       │────▶│ • 版本管理       │    │ • 配置管理      │
│ • 隐藏运行       │     │ • 数据库存储     │    │ • 监控面板      │
│ • 自启动         │     │ • TCP服务器      │    │ • 文件浏览      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 项目结构

```
file_backup_system/
├── client/                     # 客户端代码
│   ├── protocol.py            # 自定义TCP协议
│   ├── file_monitor.py        # 文件监控服务
│   ├── registry_manager.py    # 注册表管理
│   ├── client_main.py         # 客户端主程序
│   ├── windows_service.py     # Windows服务包装
│   └── requirements.txt       # 客户端依赖
│
├── server/                     # 服务端代码
│   ├── database.py            # 数据库管理
│   ├── storage_manager.py     # 存储管理
│   ├── tcp_server.py          # TCP服务器
│   ├── server_main.py         # 服务端主程序
│   ├── config.py              # 配置文件
│   └── requirements.txt       # 服务端依赖
│
├── web/                       # Web管理端
│   ├── app.py                 # Flask应用
│   ├── templates/             # HTML模板
│   │   ├── base.html
│   │   ├── login.html
│   │   ├── dashboard.html
│   │   ├── clients.html
│   │   ├── client_detail.html
│   │   ├── files.html
│   │   ├── logs.html
│   │   ├── settings.html
│   │   └── error.html
│   └── requirements.txt       # Web端依赖
│
├── sql/                       # SQL脚本
│   └── init_database.sql      # 数据库初始化脚本
│
├── docs/                      # 文档
│   └── DEPLOYMENT.md          # 部署文档
│
└── README.md                  # 项目说明
```

## 快速开始

### 环境要求

- **Python 3.12.9**
- **MySQL 8.0+**
- **Windows 10/11** (客户端)
- **Windows/Linux** (服务端)

### 1. 初始化数据库

```bash
# 登录MySQL
mysql -u root -p

# 执行初始化脚本
source sql/init_database.sql
```

### 2. 安装服务端

```bash
cd server

# 安装依赖
pip install -r requirements.txt

# 启动服务端
python server_main.py
```

### 3. 安装客户端

```bash
cd client

# 安装依赖
pip install -r requirements.txt

# 运行客户端（前台测试）
python client_main.py

# 安装为Windows服务（生产环境）
python windows_service.py --install-service
net start FileBackupClient
```

### 4. 启动Web管理端

```bash
cd web

# 安装依赖
pip install -r requirements.txt

# 启动Web服务
python app.py --host 0.0.0.0 --port 5000
```

访问 http://localhost:5000 使用默认账号登录：
- 用户名: `admin`
- 密码: `admin123`

## 详细部署

### 服务端部署

#### 1. 数据库配置

编辑 `server/server_config.json`：

```json
{
    "database": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "your_password",
        "database": "file_backup_system"
    }
}
```

#### 2. 初始化数据库

```bash
python server_main.py --init-db
```

#### 3. 启动服务

**Linux (Systemd)**:

```bash
# 创建服务文件
sudo cp docs/file-backup-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable file-backup-server
sudo systemctl start file-backup-server
```

**Windows (服务)**:

```bash
# 安装为Windows服务
python server_main.py --create-win-service
# 运行生成的 install_service.bat 文件
```

### 客户端部署

#### 1. 配置客户端

编辑客户端配置文件（首次运行自动生成）：
`%APPDATA%\FileBackupClient\config.ini`

```ini
[server]
host = your_server_ip
port = 8888

[backup]
monitor_paths = C:\Users\YourName\Documents, C:\Users\YourName\Desktop
exclude_patterns = *.tmp, *.log, *.cache
```

#### 2. 安装为Windows服务

```bash
# 以管理员身份运行
python windows_service.py --install-service
net start FileBackupClient
```

#### 3. 验证运行

- 查看Windows服务：`services.msc`
- 查看日志：`%SYSTEMROOT%\Logs\FileBackupService.log`

### Web管理端部署

#### 1. 生产部署（Nginx + uWSGI）

**安装uWSGI**:
```bash
pip install uwsgi
```

**创建uWSGI配置** `web/uwsgi.ini`:
```ini
[uwsgi]
module = app:app
master = true
processes = 4
socket = 127.0.0.1:5001
vacuum = true
die-on-term = true
```

**Nginx配置**:
```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        include uwsgi_params;
        uwsgi_pass 127.0.0.1:5001;
    }
    
    location /static {
        alias /path/to/your/static/files;
    }
}
```

#### 2. 启动服务

```bash
# 启动uWSGI
uwsgi --ini uwsgi.ini

# 重启Nginx
sudo systemctl restart nginx
```

## 配置说明

### 客户端配置

```ini
[server]
host = 127.0.0.1          # 服务端IP地址
port = 8888               # 服务端端口
timeout = 30              # 连接超时（秒）
retry_interval = 60       # 重连间隔（秒）
heartbeat_interval = 300  # 心跳间隔（秒）

[client]
client_id =               # 客户端唯一ID（自动生成）
computer_name =           # 计算机名（自动获取）
buffer_size = 65536       # 缓冲区大小（字节）
max_file_size_mb = 100    # 最大文件大小（MB）
batch_size = 10           # 批量处理文件数
batch_timeout = 5.0       # 批处理超时（秒）

[backup]
monitor_paths =           # 监控路径列表
exclude_patterns =        # 排除模式列表
include_patterns = *      # 包含模式列表
scan_interval = 3600      # 扫描间隔（秒）

[performance]
network_limit_kbps = 0    # 网络限速（KB/s，0表示不限速）
cpu_limit_percent = 50    # CPU限制（%）
memory_limit_mb = 100     # 内存限制（MB）
compression_threshold = 1024  # 压缩阈值（字节）
```

### 服务端配置

```json
{
    "server": {
        "host": "0.0.0.0",
        "port": 8888,
        "max_connections": 100,
        "timeout": 300,
        "heartbeat_interval": 300
    },
    "database": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "password",
        "database": "file_backup_system",
        "pool_size": 10,
        "charset": "utf8mb4"
    },
    "storage": {
        "root_path": "./backup_storage",
        "enable_deduplication": true,
        "enable_compression": true,
        "chunk_size": 65536,
        "max_file_size_mb": 1024,
        "keep_versions": 10,
        "cleanup_interval": 86400
    },
    "backup_defaults": {
        "monitor_paths": ["C:\\Users\\Documents", "C:\\Users\\Desktop"],
        "file_pattern": "*",
        "exclude_patterns": ["*.tmp", "*.log", "*.cache"],
        "max_file_size_mb": 100
    },
    "performance": {
        "max_bandwidth_mbps": 0,
        "max_concurrent_backups": 5,
        "cpu_limit_percent": 70,
        "memory_limit_mb": 512
    }
}
```

## 使用说明

### Web管理界面

#### 1. 客户端管理
- 查看所有连接的客户端
- 查看客户端状态和统计信息
- 配置客户端备份路径和规则

#### 2. 文件浏览
- 浏览所有备份的文件
- 查看文件版本历史
- 下载/恢复文件（功能待实现）

#### 3. 系统日志
- 查看系统操作日志
- 筛选不同级别的日志
- 监控客户端活动

#### 4. 系统设置
- 查看当前系统配置
- 修改备份策略（管理员）

### 默认账号

- **管理员**: `admin` / `admin123`

## 性能优化

### 1. 存储优化
- **启用去重**: 相同文件块只存储一次
- **启用压缩**: 减少存储占用
- **定期清理**: 自动清理旧版本文件

### 2. 网络优化
- **增量传输**: 只传输变化的文件部分
- **数据压缩**: 减少网络传输量
- **限速传输**: 避免占用全部带宽

### 3. 性能调优
- **调整块大小**: 根据文件类型优化
- **批量处理**: 合并多个小文件
- **异步I/O**: 提高并发处理能力

## 安全建议

### 1. 网络安全
- 使用防火墙限制访问
- 考虑添加TLS/SSL加密
- 限制客户端连接IP

### 2. 数据安全
- 定期备份数据库
- 加密敏感文件
- 设置合理的文件权限

### 3. 访问控制
- 修改默认密码
- 限制管理员访问
- 记录所有操作日志

## 故障排查

### 常见问题

#### 1. 客户端无法连接服务端
- 检查网络连接
- 确认服务端端口开放
- 查看客户端日志

#### 2. 文件未备份
- 检查监控路径配置
- 确认文件未被排除规则过滤
- 查看文件大小限制

#### 3. 存储空间不足
- 清理旧版本文件
- 启用数据压缩
- 增加存储容量

### 日志位置

- **客户端日志**: `%SYSTEMROOT%\Logs\FileBackupService.log`
- **服务端日志**: `./logs/server.log`
- **Web日志**: `./logs/web.log`

## 开发计划

### 已实现功能 ✅
- 文件实时监控
- 增量备份
- 版本管理
- 数据去重
- 数据压缩
- 隐藏运行
- 开机自启动
- Web管理界面

### 待实现功能 📋
- [ ] 文件恢复功能
- [ ] 定时备份策略
- [ ] TLS/SSL加密
- [ ] 多地备份
- [ ] 文件加密存储
- [ ] 邮件通知
- [ ] RESTful API
- [ ] 分布式存储

## 贡献指南

欢迎提交Issue和Pull Request！

## 开源协议

本项目采用 MIT 协议开源

## 联系方式

如有问题，请提交Issue或联系开发团队。

---

**注意**: 本项目仅供学习和内部使用，请勿用于生产环境未经充分测试。
