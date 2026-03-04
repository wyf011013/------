#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件备份系统测试脚本
用于验证系统各模块功能是否正常
"""

import os
import sys
import time
import socket
import tempfile
import shutil
import threading
import subprocess
from pathlib import Path

# 添加模块路径
sys.path.insert(0, str(Path(__file__).parent))

# 颜色定义
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

# 测试状态
class TestStatus:
    PASS = f"{Colors.GREEN}✓ PASS{Colors.RESET}"
    FAIL = f"{Colors.RED}✗ FAIL{Colors.RESET}"
    SKIP = f"{Colors.YELLOW}⊘ SKIP{Colors.RESET}"
    INFO = f"{Colors.BLUE}ℹ INFO{Colors.RESET}"


class SystemTester:
    """系统测试器"""
    
    def __init__(self):
        self.test_results = []
        self.server_process = None
        self.temp_dir = None
        self.test_dir = None
        
    def log_result(self, test_name: str, status: str, message: str = ""):
        """记录测试结果"""
        result = {
            'name': test_name,
            'status': status,
            'message': message,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        self.test_results.append(result)
        print(f"{status} {test_name}")
        if message:
            print(f"    {message}")
    
    def test_python_version(self):
        """测试Python版本"""
        try:
            import sys
            version = sys.version_info
            if version.major == 3 and version.minor >= 12:
                self.log_result("Python版本", TestStatus.PASS, 
                              f"Python {version.major}.{version.minor}.{version.micro}")
            else:
                self.log_result("Python版本", TestStatus.FAIL, 
                              f"需要Python 3.12+，当前版本: {version.major}.{version.minor}")
        except Exception as e:
            self.log_result("Python版本", TestStatus.FAIL, str(e))
    
    def test_dependencies(self):
        """测试依赖包"""
        dependencies = {
            'client': [
                'watchdog',
                'pywin32',
            ],
            'server': [
                'mysql.connector',
            ],
            'web': [
                'flask',
                'werkzeug',
            ]
        }
        
        for module_type, modules in dependencies.items():
            for module in modules:
                try:
                    __import__(module)
                    self.log_result(f"依赖检查 - {module}", TestStatus.PASS)
                except ImportError as e:
                    self.log_result(f"依赖检查 - {module}", TestStatus.FAIL, 
                                  f"请安装: pip install {module}")
    
    def test_protocol_module(self):
        """测试协议模块"""
        try:
            from client.protocol import Message, MessageType, ProtocolHandler
            
            # 测试消息创建
            msg = ProtocolHandler.create_register_message("test-client", "Test-PC", "192.168.1.100")
            assert msg.msg_type == MessageType.CLIENT_REGISTER
            assert msg.data['client_id'] == "test-client"
            
            # 测试消息序列化和反序列化
            data = msg.to_bytes()
            parsed_msg = Message.from_bytes(data)
            assert parsed_msg.data['client_id'] == "test-client"
            
            self.log_result("协议模块", TestStatus.PASS)
        except Exception as e:
            self.log_result("协议模块", TestStatus.FAIL, str(e))
    
    def test_file_hasher(self):
        """测试文件哈希模块"""
        try:
            from client.file_monitor import FileHasher
            
            # 创建测试文件
            test_file = self.test_dir / "test.txt"
            test_content = b"This is a test file content."
            test_file.write_bytes(test_content)
            
            # 计算哈希
            file_hash = FileHasher.calculate_md5(str(test_file))
            expected_hash = "098f6bcd4621d373cade4e832627b4f6"  # test字符串的MD5
            
            assert file_hash is not None
            assert len(file_hash) == 32
            
            self.log_result("文件哈希模块", TestStatus.PASS)
        except Exception as e:
            self.log_result("文件哈希模块", TestStatus.FAIL, str(e))
    
    def test_database_connection(self):
        """测试数据库连接"""
        try:
            from server.database import DatabaseManager
            
            # 尝试连接数据库（使用测试配置）
            db = DatabaseManager(
                host='localhost',
                port=3306,
                user='root',
                password='password',  # 默认密码，实际使用需要修改
                database='file_backup_system'
            )
            
            # 测试连接
            conn = db.get_connection()
            conn.close()
            
            self.log_result("数据库连接", TestStatus.PASS)
        except Exception as e:
            self.log_result("数据库连接", TestStatus.FAIL, 
                          f"请检查MySQL服务是否运行，数据库是否存在: {e}")
    
    def test_storage_manager(self):
        """测试存储管理器"""
        try:
            from server.storage_manager import StorageManager
            
            # 创建临时存储目录
            storage_dir = self.temp_dir / "storage"
            storage_dir.mkdir()
            
            # 初始化存储管理器
            storage = StorageManager(str(storage_dir))
            
            # 测试存储块
            test_data = b"This is a test file content."
            storage_path = storage.store_file_chunk(
                client_id="test-client",
                file_path="C:\\Users\\Documents\\test.txt",
                version=1,
                chunk_index=0,
                chunk_data=test_data
            )
            
            # 测试加载块
            loaded_data = storage.load_file_chunk(storage_path)
            assert loaded_data == test_data
            
            self.log_result("存储管理器", TestStatus.PASS)
        except Exception as e:
            self.log_result("存储管理器", TestStatus.FAIL, str(e))
    
    def test_tcp_server(self):
        """测试TCP服务器"""
        try:
            from server.tcp_server import BackupServer
            from server.database import DatabaseManager
            from server.config import ServerConfig
            
            # 创建测试配置
            config = ServerConfig()
            
            # 在临时目录中启动服务器
            storage_dir = self.temp_dir / "server_storage"
            storage_dir.mkdir()
            
            # 尝试启动服务器（使用随机端口）
            server = BackupServer(
                host='127.0.0.1',
                port=0,  # 随机端口
                db_config=config.get('database', {}),
                storage_root=str(storage_dir)
            )
            
            self.log_result("TCP服务器", TestStatus.PASS, "服务器初始化成功")
        except Exception as e:
            self.log_result("TCP服务器", TestStatus.FAIL, str(e))
    
    def test_web_app(self):
        """测试Web应用"""
        try:
            from web.app import app
            
            # 测试Flask应用创建
            with app.test_client() as client:
                # 测试登录页面
                response = client.get('/login')
                assert response.status_code == 200
                
                # 测试API
                response = client.get('/api/stats')
                # 应该重定向到登录页面
                assert response.status_code in [200, 302]
            
            self.log_result("Web应用", TestStatus.PASS)
        except Exception as e:
            self.log_result("Web应用", TestStatus.FAIL, str(e))
    
    def test_registry_manager(self):
        """测试注册表管理器（仅Windows）"""
        if sys.platform != 'win32':
            self.log_result("注册表管理器", TestStatus.SKIP, "仅Windows平台支持")
            return
        
        try:
            from client.registry_manager import RegistryManager
            
            registry = RegistryManager()
            
            # 测试设置优先级
            success = registry.set_process_priority("below_normal")
            assert success
            
            self.log_result("注册表管理器", TestStatus.PASS)
        except Exception as e:
            self.log_result("注册表管理器", TestStatus.FAIL, str(e))
    
    def test_file_monitor(self):
        """测试文件监控器"""
        try:
            from client.file_monitor import FileMonitorService
            
            # 创建监控配置
            monitor_config = {
                'monitor_paths': [str(self.test_dir)],
                'exclude_patterns': ['*.tmp', '*.log']
            }
            
            # 文件变化回调
            file_changes = []
            def on_file_change(file_info):
                file_changes.append(file_info)
            
            # 创建监控服务
            monitor = FileMonitorService(monitor_config, on_file_change)
            
            # 启动监控
            success = monitor.start()
            assert success
            
            # 创建测试文件
            test_file = self.test_dir / "monitored.txt"
            test_file.write_text("Test content")
            
            # 等待文件变化被检测
            time.sleep(1)
            
            # 停止监控
            monitor.stop()
            
            # 验证检测到文件变化
            assert len(file_changes) > 0
            
            self.log_result("文件监控器", TestStatus.PASS)
        except Exception as e:
            self.log_result("文件监控器", TestStatus.FAIL, str(e))
    
    def test_network_connectivity(self):
        """测试网络连接"""
        try:
            # 测试本地端口是否可用
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            
            # 绑定到随机端口
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
            sock.close()
            
            self.log_result("网络连接", TestStatus.PASS, f"可用端口: {port}")
        except Exception as e:
            self.log_result("网络连接", TestStatus.FAIL, str(e))
    
    def test_directory_structure(self):
        """测试目录结构"""
        try:
            # 检查必要的目录和文件
            required_paths = [
                'client/protocol.py',
                'server/database.py',
                'web/app.py',
                'sql/init_database.sql',
                'README.md'
            ]
            
            base_dir = Path(__file__).parent / "file_backup_system"
            
            for path in required_paths:
                full_path = base_dir / path
                if full_path.exists():
                    self.log_result(f"文件检查 - {path}", TestStatus.PASS)
                else:
                    self.log_result(f"文件检查 - {path}", TestStatus.FAIL, "文件不存在")
            
        except Exception as e:
            self.log_result("目录结构", TestStatus.FAIL, str(e))
    
    def run_all_tests(self):
        """运行所有测试"""
        print(f"\n{Colors.BLUE}╔══════════════════════════════════════╗{Colors.RESET}")
        print(f"{Colors.BLUE}║     文件备份系统功能测试              ║{Colors.RESET}")
        print(f"{Colors.BLUE}╚══════════════════════════════════════╝{Colors.RESET}\n")
        
        # 创建临时目录
        self.temp_dir = Path(tempfile.mkdtemp(prefix="backup_test_"))
        self.test_dir = self.temp_dir / "test_files"
        self.test_dir.mkdir()
        
        print(f"测试目录: {self.temp_dir}\n")
        
        try:
            # 运行测试
            tests = [
                ("Python版本检查", self.test_python_version),
                ("依赖包检查", self.test_dependencies),
                ("协议模块测试", self.test_protocol_module),
                ("文件哈希测试", self.test_file_hasher),
                ("数据库连接测试", self.test_database_connection),
                ("存储管理器测试", self.test_storage_manager),
                ("TCP服务器测试", self.test_tcp_server),
                ("Web应用测试", self.test_web_app),
                ("注册表管理器测试", self.test_registry_manager),
                ("文件监控器测试", self.test_file_monitor),
                ("网络连接测试", self.test_network_connectivity),
                ("目录结构检查", self.test_directory_structure),
            ]
            
            for test_name, test_func in tests:
                print(f"\n{Colors.YELLOW}▶ {test_name}...{Colors.RESET}")
                try:
                    test_func()
                except Exception as e:
                    self.log_result(test_name, TestStatus.FAIL, f"测试异常: {e}")
            
            # 打印测试摘要
            self.print_summary()
            
        finally:
            # 清理临时目录
            if self.temp_dir and self.temp_dir.exists():
                shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def print_summary(self):
        """打印测试摘要"""
        pass_count = sum(1 for r in self.test_results if "PASS" in r['status'])
        fail_count = sum(1 for r in self.test_results if "FAIL" in r['status'])
        skip_count = sum(1 for r in self.test_results if "SKIP" in r['status'])
        
        print(f"\n{Colors.BLUE}╔══════════════════════════════════════╗{Colors.RESET}")
        print(f"{Colors.BLUE}║           测试摘要                    ║{Colors.RESET}")
        print(f"{Colors.BLUE}╚══════════════════════════════════════╝{Colors.RESET}")
        print(f"{Colors.GREEN}通过: {pass_count}{Colors.RESET}")
        print(f"{Colors.RED}失败: {fail_count}{Colors.RESET}")
        print(f"{Colors.YELLOW}跳过: {skip_count}{Colors.RESET}")
        print(f"总计: {len(self.test_results)}")
        
        if fail_count > 0:
            print(f"\n{Colors.RED}存在失败的测试，请检查上述输出！{Colors.RESET}")
            sys.exit(1)
        else:
            print(f"\n{Colors.GREEN}所有测试通过！系统功能正常。{Colors.RESET}")


def main():
    """主函数"""
    tester = SystemTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()
