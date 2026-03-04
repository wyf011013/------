#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件备份系统快速部署脚本
自动安装依赖、初始化配置、启动服务
"""

import os
import sys
import json
import shutil
import subprocess
import argparse
from pathlib import Path


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    RESET = '\033[0m'


def print_banner():
    """打印横幅"""
    print(f"""
{Colors.CYAN}
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║          文件自动备份系统 - 快速部署脚本                     ║
║                                                              ║
║          版本: 1.0.0                                         ║
║          Python: 3.12.9                                      ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
{Colors.RESET}
""")


def run_command(cmd: str, cwd: str = None, capture_output: bool = True) -> tuple:
    """运行命令"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd,
            capture_output=capture_output,
            text=True,
            check=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr


def check_python_version():
    """检查Python版本"""
    print(f"{Colors.YELLOW}[检查] Python版本...{Colors.RESET}")
    
    version = sys.version_info
    if version.major == 3 and version.minor >= 12:
        print(f"{Colors.GREEN}✓ Python {version.major}.{version.minor}.{version.micro}{Colors.RESET}")
        return True
    else:
        print(f"{Colors.RED}✗ 需要Python 3.12+，当前版本: {version.major}.{version.minor}{Colors.RESET}")
        return False


def install_dependencies(component: str):
    """安装依赖"""
    print(f"{Colors.YELLOW}[安装] {component} 依赖...{Colors.RESET}")
    
    req_file = f"{component}/requirements.txt"
    if not os.path.exists(req_file):
        print(f"{Colors.RED}✗ 未找到 {req_file}{Colors.RESET}")
        return False
    
    success, output = run_command(f"pip install -r {req_file}")
    
    if success:
        print(f"{Colors.GREEN}✓ {component} 依赖安装成功{Colors.RESET}")
        return True
    else:
        print(f"{Colors.RED}✗ {component} 依赖安装失败{Colors.RESET}")
        print(f"错误: {output}")
        return False


def create_default_config():
    """创建默认配置文件"""
    print(f"{Colors.YELLOW}[配置] 创建默认配置文件...{Colors.RESET}")
    
    # 服务端配置
    server_config = {
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
            "enable_deduplication": True,
            "enable_compression": True,
            "chunk_size": 65536,
            "max_file_size_mb": 1024,
            "keep_versions": 10,
            "cleanup_interval": 86400
        },
        "backup_defaults": {
            "monitor_paths": ["C:\\Users\\Documents", "C:\\Users\\Desktop"],
            "file_pattern": "*",
            "exclude_patterns": [
                "*.tmp", "*.temp", "*.log", "*.lock", "*.swp", "*.swo",
                "~*", ".DS_Store", "Thumbs.db", "*.bak", "*.cache"
            ],
            "max_file_size_mb": 100
        },
        "logging": {
            "level": "INFO",
            "file": "./logs/server.log",
            "max_size_mb": 100,
            "backup_count": 5,
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        },
        "performance": {
            "max_bandwidth_mbps": 0,
            "max_concurrent_backups": 5,
            "cpu_limit_percent": 70,
            "memory_limit_mb": 512
        }
    }
    
    # 保存服务端配置
    with open("server/server_config.json", "w", encoding="utf-8") as f:
        json.dump(server_config, f, indent=2, ensure_ascii=False)
    
    print(f"{Colors.GREEN}✓ 配置文件创建成功{Colors.RESET}")
    return True


def init_database():
    """初始化数据库"""
    print(f"{Colors.YELLOW}[数据库] 初始化数据库...{Colors.RESET}")
    
    if not os.path.exists("sql/init_database.sql"):
        print(f"{Colors.RED}✗ 未找到数据库初始化脚本{Colors.RESET}")
        return False
    
    # 检查MySQL是否运行
    success, _ = run_command("mysql --version", capture_output=True)
    if not success:
        print(f"{Colors.RED}✗ 未找到MySQL客户端，请确保MySQL已安装并运行{Colors.RESET}")
        return False
    
    print(f"{Colors.CYAN}请手动执行数据库初始化：{Colors.RESET}")
    print(f"  mysql -u root -p < sql/init_database.sql")
    print(f"{Colors.YELLOW}或使用以下命令：{Colors.RESET}")
    print(f"  python server/server_main.py --init-db")
    
    return True


def setup_directories():
    """创建必要的目录"""
    print(f"{Colors.YELLOW}[目录] 创建必要目录...{Colors.RESET}")
    
    directories = [
        "backup_storage",
        "backup_storage/clients",
        "backup_storage/shared_blocks",
        "backup_storage/temp",
        "logs",
        "web/logs"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    
    print(f"{Colors.GREEN}✓ 目录创建完成{Colors.RESET}")
    return True


def test_basic_functionality():
    """测试基本功能"""
    print(f"{Colors.YELLOW}[测试] 运行基本功能测试...{Colors.RESET}")
    
    # 测试协议模块
    try:
        from client.protocol import Message, MessageType, ProtocolHandler
        
        msg = ProtocolHandler.create_register_message("test", "Test-PC")
        data = msg.to_bytes()
        parsed = Message.from_bytes(data)
        
        print(f"{Colors.GREEN}✓ 协议模块测试通过{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}✗ 协议模块测试失败: {e}{Colors.RESET}")
        return False
    
    # 测试文件哈希
    try:
        from client.file_monitor import FileHasher
        
        test_data = b"test content"
        test_hash = FileHasher.calculate_md5_from_data(test_data)
        
        print(f"{Colors.GREEN}✓ 文件哈希测试通过{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}✗ 文件哈希测试失败: {e}{Colors.RESET}")
        return False
    
    return True


def create_run_scripts():
    """创建运行脚本"""
    print(f"{Colors.YELLOW}[脚本] 创建运行脚本...{Colors.RESET}")
    
    # Windows批处理脚本
    if os.name == 'nt':
        # 运行服务端
        with open("run_server.bat", "w") as f:
            f.write("""@echo off
cd server
python server_main.py
pause
""")
        
        # 运行客户端（测试）
        with open("run_client.bat", "w") as f:
            f.write("""@echo off
cd client
python client_main.py
pause
""")
        
        # 运行Web端
        with open("run_web.bat", "w") as f:
            f.write("""@echo off
cd web
python app.py --host 0.0.0.0 --port 5000
pause
""")
        
        # 安装客户端服务
        with open("install_client_service.bat", "w") as f:
            f.write("""@echo off
cd client
python windows_service.py --install-service
pause
""")
    
    # Linux/Mac脚本
    else:
        # 运行服务端
        with open("run_server.sh", "w") as f:
            f.write("""#!/bin/bash
cd server
python3 server_main.py
""")
        
        # 运行Web端
        with open("run_web.sh", "w") as f:
            f.write("""#!/bin/bash
cd web
python3 app.py --host 0.0.0.0 --port 5000
""")
        
        # 添加执行权限
        os.chmod("run_server.sh", 0o755)
        os.chmod("run_web.sh", 0o755)
    
    print(f"{Colors.GREEN}✓ 运行脚本创建完成{Colors.RESET}")
    return True


def print_usage_guide():
    """打印使用指南"""
    print(f"""
{Colors.CYAN}
╔══════════════════════════════════════════════════════════════╗
║                    部署完成 - 使用指南                        ║
╚══════════════════════════════════════════════════════════════╝
{Colors.RESET}

{Colors.YELLOW}1. 初始化数据库：{Colors.RESET}
   mysql -u root -p < sql/init_database.sql

{Colors.YELLOW}2. 启动服务端：{Colors.RESET}
   {Colors.GREEN}Windows:{Colors.RESET} run_server.bat
   {Colors.GREEN}Linux:{Colors.RESET}   ./run_server.sh

{Colors.YELLOW}3. 启动Web管理端：{Colors.RESET}
   {Colors.GREEN}Windows:{Colors.RESET} run_web.bat
   {Colors.GREEN}Linux:{Colors.RESET}   ./run_web.sh

{Colors.YELLOW}4. 访问管理界面：{Colors.RESET}
   浏览器访问: http://localhost:5000
   默认账号: admin / admin123

{Colors.YELLOW}5. 客户端部署：{Colors.RESET}
   - 安装客户端到目标计算机
   - 修改 client/config.ini 配置服务端地址
   - 运行 install_client_service.bat 安装为Windows服务
   - 或使用 run_client.bat 测试运行

{Colors.CYAN}
默认配置信息：
{Colors.RESET}
- 服务端端口: 8888
- Web端口: 5000
- 数据库: file_backup_system
- 存储路径: ./backup_storage

{Colors.CYAN}
重要提示：
{Colors.RESET}
1. 请修改默认数据库密码
2. 生产环境请使用强密码
3. 建议启用防火墙
4. 定期备份数据库

{Colors.GREEN}部署完成！{Colors.RESET}
""")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='文件备份系统快速部署脚本')
    parser.add_argument('--skip-deps', action='store_true', help='跳过依赖安装')
    parser.add_argument('--skip-db', action='store_true', help='跳过数据库初始化')
    parser.add_argument('--test-only', action='store_true', help='仅运行测试')
    
    args = parser.parse_args()
    
    print_banner()
    
    # 检查Python版本
    if not check_python_version():
        sys.exit(1)
    
    if args.test_only:
        # 仅运行测试
        print(f"\n{Colors.CYAN}运行测试...{Colors.RESET}\n")
        os.system("python test_system.py")
        return
    
    # 安装依赖
    if not args.skip_deps:
        components = ['client', 'server', 'web']
        for component in components:
            if not install_dependencies(component):
                print(f"\n{Colors.RED}依赖安装失败，请手动安装依赖后重试{Colors.RESET}")
                sys.exit(1)
    
    # 创建配置
    create_default_config()
    
    # 创建目录
    setup_directories()
    
    # 初始化数据库
    if not args.skip_db:
        init_database()
    
    # 测试基本功能
    if not test_basic_functionality():
        print(f"\n{Colors.RED}基本功能测试失败{Colors.RESET}")
        sys.exit(1)
    
    # 创建运行脚本
    create_run_scripts()
    
    # 打印使用指南
    print_usage_guide()


if __name__ == "__main__":
    main()
