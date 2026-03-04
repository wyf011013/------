# 文件备份系统 - 部署指南

## 📋 部署前准备

### 1. 系统要求

#### 服务端
- **操作系统**: Windows 10/11 或 Linux (Ubuntu 20.04+)
- **Python**: 3.12.9
- **MySQL**: 8.0+
- **内存**: 最少2GB，推荐4GB+
- **磁盘**: 根据备份数据量确定

#### 客户端
- **操作系统**: Windows 10/11
- **Python**: 3.12.9
- **权限**: 管理员权限（用于安装服务）
- **网络**: 能够连接到服务端

#### Web管理端
- **浏览器**: Chrome 90+, Firefox 88+, Edge 90+
- **分辨率**: 推荐1920x1080+

### 2. 网络要求

| 组件 | 入站端口 | 出站端口 | 说明 |
|------|---------|---------|------|
| 服务端 | 8888 | - | TCP服务器监听端口 |
| Web管理端 | 5000 | - | Web服务端口 |
| MySQL | 3306 | - | 数据库端口（仅内部访问） |
| 客户端 | - | 8888 | 连接服务端 |

---

## 🚀 快速部署

### 方法一：使用快速部署脚本

```bash
# 1. 运行快速部署脚本
python quick_setup.py

# 2. 按提示操作
# 3. 完成部署
```

### 方法二：手动部署

#### 步骤1: 安装Python 3.12.9

**Windows**:
1. 下载 Python 3.12.9: https://www.python.org/downloads/
2. 运行安装程序，勾选"Add Python to PATH"
3. 完成安装

**Linux**:
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3.12 python3.12-venv python3.12-dev

# CentOS/RHEL
sudo yum install python3.12
```

#### 步骤2: 安装MySQL 8.0

**Windows**:
1. 下载 MySQL Installer: https://dev.mysql.com/downloads/mysql/
2. 运行安装程序，选择"Server only"
3. 设置root密码
4. 完成安装

**Linux**:
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install mysql-server-8.0
sudo mysql_secure_installation

# CentOS/RHEL
sudo yum install mysql-server
sudo systemctl start mysqld
sudo systemctl enable mysqld
```

#### 步骤3: 初始化数据库

```bash
# 方法1: 使用SQL脚本
mysql -u root -p < sql/init_database.sql

# 方法2: 使用服务端初始化
python server/server_main.py --init-db
```

#### 步骤4: 安装项目依赖

```bash
# 客户端依赖
cd client
pip install -r requirements.txt
cd ..

# 服务端依赖
cd server
pip install -r requirements.txt
cd ..

# Web管理端依赖
cd web
pip install -r requirements.txt
cd ..
```

#### 步骤5: 配置服务端

编辑 `server/server_config.json`:

```json
{
    "database": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "your_mysql_password",
        "database": "file_backup_system"
    },
    "server": {
        "host": "0.0.0.0",
        "port": 8888
    }
}
```

#### 步骤6: 启动服务

**启动服务端**:
```bash
cd server
python server_main.py
```

**启动Web管理端**:
```bash
cd web
python app.py --host 0.0.0.0 --port 5000
```

---

## 🏢 生产环境部署

### 服务端生产部署

#### Linux (Systemd)

**1. 创建系统用户**
```bash
sudo useradd -m -s /bin/bash backup
sudo mkdir -p /opt/file_backup_system
sudo chown backup:backup /opt/file_backup_system
```

**2. 复制项目文件**
```bash
sudo cp -r file_backup_system/* /opt/file_backup_system/
sudo chown -R backup:backup /opt/file_backup_system
```

**3. 安装依赖**
```bash
sudo -u backup bash -c "
cd /opt/file_backup_system
python3.12 -m venv venv
source venv/bin/activate
pip install -r server/requirements.txt
pip install -r web/requirements.txt
"
```

**4. 配置文件**
```bash
sudo mkdir -p /etc/file_backup
sudo cp server/server_config.json /etc/file_backup/
sudo chmod 600 /etc/file_backup/server_config.json
```

**5. 创建Systemd服务**
```bash
sudo cp docs/file-backup-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable file-backup-server
sudo systemctl start file-backup-server
```

**6. 检查状态**
```bash
sudo systemctl status file-backup-server
sudo journalctl -u file-backup-server -f
```

#### Windows (服务)

**1. 安装为Windows服务**
```bash
cd server
python server_main.py --create-win-service
# 运行生成的 install_service.bat
```

**2. 或使用NSSM**
```bash
# 下载NSSM: https://nssm.cc/download
nssm install FileBackupServer "C:\Python312\python.exe" "C:\path\to\server_main.py"
nssm start FileBackupServer
```

### Web管理端生产部署

#### 使用Nginx + uWSGI (Linux)

**1. 安装uWSGI**
```bash
source /opt/file_backup_system/venv/bin/activate
pip install uwsgi
```

**2. 创建uWSGI配置**
```ini
# /opt/file_backup_system/web/uwsgi.ini
[uwsgi]
module = app:app
master = true
processes = 4
socket = 127.0.0.1:5001
vacuum = true
die-on-term = true
pythonpath = /opt/file_backup_system
```

**3. 创建Systemd服务**
```bash
sudo tee /etc/systemd/system/file-backup-web.service > /dev/null <<EOF
[Unit]
Description=File Backup Web Server
After=network.target

[Service]
User=backup
WorkingDirectory=/opt/file_backup_system/web
ExecStart=/opt/file_backup_system/venv/bin/uwsgi --ini uwsgi.ini
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable file-backup-web
sudo systemctl start file-backup-web
```

**4. Nginx配置**
```nginx
# /etc/nginx/sites-available/file-backup
server {
    listen 80;
    server_name backup.yourdomain.com;
    
    location / {
        include uwsgi_params;
        uwsgi_pass 127.0.0.1:5001;
        uwsgi_read_timeout 300;
    }
    
    location /static {
        alias /opt/file_backup_system/web/static;
        expires 30d;
    }
}

sudo ln -s /etc/nginx/sites-available/file-backup /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

#### 使用IIS (Windows)

**1. 安装IIS和CGI**
```powershell
# 以管理员身份运行
Enable-WindowsOptionalFeature -Online -FeatureName IIS-CGI
```

**2. 配置IIS**
- 打开IIS管理器
- 添加网站
- 绑定到端口5000
- 配置应用程序池

### 客户端批量部署

#### 1. 创建安装包

**使用PyInstaller打包**
```bash
cd client
pyinstaller --onefile --windowed --name FileBackupClient client_main.py
```

**或使用cx_Freeze**
```python
# setup.py
from cx_Freeze import setup, Executable

setup(
    name="FileBackupClient",
    version="1.0",
    description="File Backup Client",
    executables=[Executable("client_main.py", base="Win32GUI")]
)

python setup.py build
```

#### 2. 创建安装脚本

使用提供的 `docs/install_client.bat`:
```batch
# 1. 复制安装脚本到客户端
# 2. 修改脚本中的服务端地址
# 3. 以管理员身份运行
```

#### 3. 使用组策略部署

**1. 创建GPO**
- 打开"组策略管理"
- 创建新的GPO

**2. 配置软件安装**
- 计算机配置 → 软件设置 → 软件安装
- 新建 → 包
- 选择MSI安装包

**3. 配置启动脚本**
- 计算机配置 → Windows设置 → 脚本(启动/关机)
- 添加安装脚本

---

## ⚙️ 配置说明

### 服务端配置

#### 基本配置
```json
{
    "server": {
        "host": "0.0.0.0",           # 监听地址
        "port": 8888,                # 监听端口
        "max_connections": 100,      # 最大连接数
        "timeout": 300,              # 连接超时(秒)
        "heartbeat_interval": 300    # 心跳间隔(秒)
    },
    "database": {
        "host": "localhost",         # 数据库地址
        "port": 3306,                # 数据库端口
        "user": "root",              # 数据库用户
        "password": "password",      # 数据库密码
        "database": "file_backup_system", # 数据库名
        "pool_size": 10              # 连接池大小
    },
    "storage": {
        "root_path": "./backup_storage", # 存储根目录
        "enable_deduplication": true,    # 启用去重
        "enable_compression": true,      # 启用压缩
        "chunk_size": 65536,             # 块大小(字节)
        "max_file_size_mb": 1024,        # 最大文件大小(MB)
        "keep_versions": 10,             # 保留版本数
        "cleanup_interval": 86400        # 清理间隔(秒)
    }
}
```

#### 性能调优
```json
{
    "performance": {
        "max_bandwidth_mbps": 100,   # 最大带宽(Mbps，0=不限速)
        "max_concurrent_backups": 5, # 最大并发备份数
        "cpu_limit_percent": 70,     # CPU限制(%)
        "memory_limit_mb": 512       # 内存限制(MB)
    }
}
```

### 客户端配置

#### 基本配置
```ini
[server]
host = 192.168.1.100        # 服务端IP地址
port = 8888                 # 服务端端口
timeout = 30                # 连接超时(秒)
retry_interval = 60         # 重连间隔(秒)
heartbeat_interval = 300    # 心跳间隔(秒)

[client]
client_id =                 # 客户端ID(自动生成)
computer_name =             # 计算机名(自动获取)
buffer_size = 65536         # 缓冲区大小(字节)
max_file_size_mb = 100      # 最大文件大小(MB)
batch_size = 10             # 批量处理文件数
batch_timeout = 5.0         # 批处理超时(秒)

[backup]
monitor_paths = C:\Users\Documents,C:\Users\Desktop  # 监控路径
exclude_patterns = *.tmp,*.log,*.cache              # 排除模式
include_patterns = *                                 # 包含模式
scan_interval = 3600                                 # 扫描间隔(秒)

[performance]
network_limit_kbps = 0      # 网络限速(KB/s，0=不限速)
cpu_limit_percent = 50      # CPU限制(%)
memory_limit_mb = 100       # 内存限制(MB)
compression_threshold = 1024 # 压缩阈值(字节)
```

---

## 🔧 运维管理

### 监控和日志

#### 查看日志

**服务端日志**:
```bash
# Linux
sudo journalctl -u file-backup-server -f

# 或直接查看日志文件
tail -f /opt/file_backup_system/logs/server.log
```

**客户端日志**:
```bash
# Windows
type %SYSTEMROOT%\Logs\FileBackupService.log

# 或通过事件查看器
eventvwr.msc -> Windows日志 -> 应用程序
```

**Web日志**:
```bash
sudo journalctl -u file-backup-web -f
```

#### 监控指标

**关键指标**:
- 在线客户端数量
- 备份文件总数
- 存储空间使用
- 网络带宽使用
- 错误率

**查看统计**:
```bash
# 通过Web界面查看
# 或通过API
curl http://localhost:5000/api/stats
```

### 备份和恢复

#### 数据库备份

```bash
# 备份数据库
mysqldump -u root -p file_backup_system > backup_$(date +%Y%m%d).sql

# 恢复数据库
mysql -u root -p file_backup_system < backup_20260115.sql
```

#### 存储数据备份

```bash
# 备份存储数据
tar -czf backup_storage_$(date +%Y%m%d).tar.gz /opt/file_backup_system/backup_storage/

# 恢复存储数据
tar -xzf backup_storage_20260115.tar.gz -C /opt/file_backup_system/
```

### 故障排查

#### 常见问题

**1. 客户端无法连接**
- 检查网络连接: `ping 服务器IP`
- 检查端口开放: `telnet 服务器IP 8888`
- 检查防火墙设置
- 查看客户端日志

**2. 文件未备份**
- 检查监控路径配置
- 检查排除规则
- 检查文件大小限制
- 查看客户端日志

**3. 存储空间不足**
- 清理旧版本: 修改keep_versions配置
- 启用压缩: enable_compression=true
- 增加存储容量

**4. 数据库连接失败**
- 检查MySQL服务: `systemctl status mysql`
- 检查连接配置
- 检查用户权限

#### 调试模式

**客户端调试**:
```bash
# 前台运行，查看详细日志
cd client
python client_main.py --debug
```

**服务端调试**:
```bash
cd server
python server_main.py --debug
```

---

## 🛡️ 安全配置

### 1. 修改默认密码

```bash
# MySQL
mysql -u root -p
ALTER USER 'root'@'localhost' IDENTIFIED BY 'new_strong_password';

# Web管理端
访问 http://localhost:5000/settings 修改密码
```

### 2. 防火墙配置

**Linux (iptables)**:
```bash
# 只允许特定IP访问
sudo iptables -A INPUT -p tcp --dport 8888 -s 192.168.1.0/24 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 8888 -j DROP

sudo iptables -A INPUT -p tcp --dport 5000 -s 192.168.1.0/24 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 5000 -j DROP
```

**Linux (ufw)**:
```bash
sudo ufw allow from 192.168.1.0/24 to any port 8888
sudo ufw allow from 192.168.1.0/24 to any port 5000
sudo ufw enable
```

### 3. 使用HTTPS

**申请SSL证书**:
```bash
# 使用Let's Encrypt
sudo apt install certbot
sudo certbot certonly --standalone -d backup.yourdomain.com
```

**Nginx HTTPS配置**:
```nginx
server {
    listen 443 ssl;
    server_name backup.yourdomain.com;
    
    ssl_certificate /etc/letsencrypt/live/backup.yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/backup.yourdomain.com/privkey.pem;
    
    location / {
        include uwsgi_params;
        uwsgi_pass 127.0.0.1:5001;
    }
}
```

### 4. 访问控制

- 限制Web管理端访问IP
- 使用VPN访问
- 启用双因素认证（2FA）
- 定期审计日志

---

## 📊 性能调优

### 1. 数据库优化

```sql
-- 优化表结构
OPTIMIZE TABLE file_versions;
OPTIMIZE TABLE system_logs;

-- 添加索引
CREATE INDEX idx_file_versions_backup_date ON file_versions(backup_date);
CREATE INDEX idx_system_logs_created_at ON system_logs(created_at);
```

### 2. 存储优化

- 使用SSD存储
- 启用压缩
- 定期清理旧数据
- 使用RAID提高可靠性

### 3. 网络优化

- 启用数据压缩
- 调整块大小
- 限制并发连接数
- 使用CDN（大文件分发）

---

## 🔄 升级维护

### 升级步骤

**1. 备份数据**
```bash
mysqldump -u root -p file_backup_system > backup_pre_upgrade.sql
tar -czf backup_storage_pre_upgrade.tar.gz /opt/file_backup_system/backup_storage/
```

**2. 停止服务**
```bash
sudo systemctl stop file-backup-server
sudo systemctl stop file-backup-web
```

**3. 更新代码**
```bash
cd /opt/file_backup_system
git pull origin main  # 或复制新版本的文件
```

**4. 更新依赖**
```bash
source venv/bin/activate
pip install -r server/requirements.txt
pip install -r web/requirements.txt
```

**5. 数据库迁移**
```bash
# 如果有数据库结构变更，执行迁移脚本
python server/migrate.py
```

**6. 启动服务**
```bash
sudo systemctl start file-backup-server
sudo systemctl start file-backup-web
```

---

## 📞 技术支持

### 获取帮助

1. **查看文档**: README.md, DEPLOYMENT_GUIDE.md
2. **检查日志**: 查看服务端和客户端日志
3. **运行测试**: `python test_system.py`
4. **提交Issue**: 在GitHub上提交问题

### 联系方式

如有问题，请参考项目文档或联系开发团队。

---

**部署完成！祝使用愉快！** 🎉
