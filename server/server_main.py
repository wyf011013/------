"""
文件备份服务端主程序
集成TCP备份服务器 + Flask Web管理端
"""

import os
import sys
import time
import signal
import logging
import argparse
import threading
from pathlib import Path
from typing import Dict, Any

# 添加模块路径
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from tcp_server import BackupServer
from database import DatabaseManager
from config import ServerConfig


class BackupService:
    """备份服务管理器"""

    def __init__(self, config: ServerConfig):
        self.config = config
        self.logger = self._setup_logging()
        self.server = None
        self.web_thread = None
        self.running = False

        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _setup_logging(self) -> logging.Logger:
        """设置日志"""
        log_config = self.config.get('logging', {})

        log_file = log_config.get('file', './logs/server.log')
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)

        log_level = getattr(logging, log_config.get('level', 'INFO').upper(), logging.INFO)
        log_format = log_config.get('format',
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

        logger = logging.getLogger(__name__)
        logger.info("日志系统已初始化")
        return logger

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.logger.info(f"收到信号 {signum}，正在停止服务...")
        self.stop()

    def initialize_database(self) -> bool:
        """初始化数据库"""
        try:
            self.logger.info("正在初始化数据库...")
            db_config = self.config.get('database', {})
            db_manager = DatabaseManager(**db_config)
            conn = db_manager.get_connection()
            conn.close()
            self.logger.info("数据库初始化成功")
            return True
        except Exception as e:
            self.logger.error(f"数据库初始化失败: {e}")
            return False

    def _start_web_server(self, host: str, port: int, debug: bool = False):
        """启动Flask Web管理端"""
        try:
            from web.app import app, set_server

            # 将BackupServer实例注入到Web应用
            if self.server:
                set_server(self.server)
                self.logger.info("BackupServer实例已注入Web应用")

            self.logger.info(f"Web管理端启动: http://{host}:{port}")
            app.run(host=host, port=port, debug=debug, threaded=True, use_reloader=False)
        except Exception as e:
            self.logger.error(f"Web管理端启动失败: {e}")

    def start(self, web_host: str = '0.0.0.0', web_port: int = 5000,
              web_debug: bool = False) -> bool:
        """启动完整服务(TCP + Web)"""
        if self.running:
            return True

        try:
            self.logger.info("正在启动文件备份服务...")

            if not self.initialize_database():
                return False

            # 创建TCP备份服务器
            server_config = self.config.get('server', {})
            storage_config = self.config.get('storage', {})
            db_config = self.config.get('database', {})

            self.server = BackupServer(
                host=server_config.get('host', '0.0.0.0'),
                port=server_config.get('port', 8888),
                db_config=db_config,
                storage_root=storage_config.get('root_path', './backup_storage'),
                chunk_size=storage_config.get('chunk_size', 65536)
            )

            self.running = True

            # 启动TCP服务器线程
            server_thread = threading.Thread(target=self.server.start, daemon=True)
            server_thread.start()
            time.sleep(1)

            self.logger.info(f"TCP服务已启动: {server_config.get('host', '0.0.0.0')}:{server_config.get('port', 8888)}")
            self.logger.info(f"存储路径: {storage_config.get('root_path', './backup_storage')}")

            # 启动Web管理端线程
            self.web_thread = threading.Thread(
                target=self._start_web_server,
                args=(web_host, web_port, web_debug),
                daemon=True
            )
            self.web_thread.start()

            self.logger.info("文件备份服务已完全启动")
            return True

        except Exception as e:
            self.logger.error(f"服务启动失败: {e}")
            self.stop()
            return False

    def stop(self):
        """停止服务"""
        if not self.running:
            return

        self.logger.info("正在停止文件备份服务...")
        self.running = False

        if self.server:
            self.server.stop()
            self.server = None

        self.logger.info("文件备份服务已停止")

    def run_forever(self, web_host: str = '0.0.0.0', web_port: int = 5000,
                    web_debug: bool = False):
        """永久运行"""
        try:
            if self.start(web_host, web_port, web_debug):
                self.logger.info("服务运行中，按Ctrl+C停止...")
                while self.running:
                    time.sleep(1)
            else:
                self.logger.error("服务启动失败")
        except KeyboardInterrupt:
            self.logger.info("收到键盘中断")
        except Exception as e:
            self.logger.error(f"服务运行异常: {e}")
        finally:
            self.stop()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='文件备份服务端')
    parser.add_argument('--config', type=str, help='配置文件路径')
    parser.add_argument('--init-db', action='store_true', help='初始化数据库')
    parser.add_argument('--web-host', default='0.0.0.0', help='Web管理端监听地址')
    parser.add_argument('--web-port', type=int, default=5000, help='Web管理端端口')
    parser.add_argument('--web-debug', action='store_true', help='Web调试模式')

    args = parser.parse_args()
    config = ServerConfig(args.config)

    if args.init_db:
        service = BackupService(config)
        if service.initialize_database():
            print("数据库初始化成功！")
        else:
            print("数据库初始化失败！")
            sys.exit(1)
    else:
        service = BackupService(config)
        service.run_forever(args.web_host, args.web_port, args.web_debug)


if __name__ == "__main__":
    main()
