"""
Windows服务包装器
将客户端程序包装为Windows服务，实现开机自启动和后台运行
"""

import sys
import os
import win32serviceutil
import win32service
import win32event
import servicemanager
import logging

# 添加模块路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client_main import BackupClient, BackupClientConfig


class FileBackupService(win32serviceutil.ServiceFramework):
    """文件备份Windows服务"""
    
    # 服务信息
    _svc_name_ = "FileBackupClient"
    _svc_display_name_ = "文件备份客户端"
    _svc_description_ = "自动备份指定文件夹到服务器的后台服务"
    
    def __init__(self, args):
        """初始化服务"""
        win32serviceutil.ServiceFramework.__init__(self, args)
        
        # 创建停止事件
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        
        # 客户端实例
        self.client = None
        self.config = None
        
        # 设置服务日志
        self._setup_service_logging()
    
    def _setup_service_logging(self):
        """设置服务日志"""
        # 服务日志文件
        log_dir = os.path.join(os.environ.get('SYSTEMROOT', 'C:\\Windows'), 'Logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, 'FileBackupService.log')
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Windows服务日志系统已初始化")
    
    def SvcStop(self):
        """服务停止"""
        self.logger.info("正在停止文件备份服务...")
        
        # 报告服务状态
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        
        # 触发停止事件
        win32event.SetEvent(self.hWaitStop)
        
        # 停止客户端
        if self.client:
            self.client.stop()
        
        self.logger.info("文件备份服务已停止")
    
    def SvcDoRun(self):
        """服务运行"""
        self.logger.info("正在启动文件备份服务...")
        
        try:
            # 报告服务状态：正在运行
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, '')
            )
            
            # 启动客户端
            self._run_client()
            
        except Exception as e:
            self.logger.error(f"服务运行异常: {e}")
            servicemanager.LogErrorMsg(f"文件备份服务异常: {e}")
    
    def _run_client(self):
        """运行客户端主循环"""
        try:
            # 加载配置
            self.config = BackupClientConfig()
            
            # 创建客户端（使用服务模式）
            self.client = BackupClient(self.config)
            
            # 启动客户端
            if self.client.start():
                self.logger.info("文件备份客户端已在服务模式下启动")
                
                # 等待停止事件
                while True:
                    # 检查停止事件（每5秒检查一次）
                    result = win32event.WaitForSingleObject(self.hWaitStop, 5000)
                    
                    if result == win32event.WAIT_OBJECT_0:
                        # 收到停止信号
                        self.logger.info("收到服务停止信号")
                        break
                    
                    # 检查客户端状态
                    if not self.client.running:
                        self.logger.warning("客户端已停止运行")
                        break
            else:
                self.logger.error("文件备份客户端启动失败")
                
        except Exception as e:
            self.logger.error(f"客户端运行异常: {e}")
            raise
    
    @classmethod
    def parse_command_line(cls):
        """解析命令行参数"""
        win32serviceutil.HandleCommandLine(cls)


def install_service():
    """安装Windows服务"""
    try:
        # 获取当前脚本路径
        script_path = os.path.abspath(__file__)
        python_exe = sys.executable
        
        # 服务安装命令
        service_cmd = f'"{python_exe}" "{script_path}" --startup=auto install'
        
        print("正在安装文件备份服务...")
        print(f"命令: {service_cmd}")
        
        # 执行安装
        os.system(service_cmd)
        
        print("服务安装完成")
        print("启动服务命令: net start FileBackupClient")
        print("停止服务命令: net stop FileBackupClient")
        print("卸载服务命令: filebackup_client.exe --uninstall-service")
        
    except Exception as e:
        print(f"安装服务失败: {e}")


def uninstall_service():
    """卸载Windows服务"""
    try:
        # 停止服务
        print("正在停止服务...")
        os.system('net stop FileBackupClient 2>nul')
        
        # 卸载服务
        script_path = os.path.abspath(__file__)
        python_exe = sys.executable
        
        uninstall_cmd = f'"{python_exe}" "{script_path}" remove'
        
        print("正在卸载文件备份服务...")
        os.system(uninstall_cmd)
        
        print("服务卸载完成")
        
    except Exception as e:
        print(f"卸载服务失败: {e}")


def run_as_service():
    """以服务模式运行"""
    if len(sys.argv) == 1:
        # 如果没有参数，以服务模式启动
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(FileBackupService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        # 处理命令行参数
        if '--install-service' in sys.argv:
            install_service()
        elif '--uninstall-service' in sys.argv:
            uninstall_service()
        else:
            FileBackupService.parse_command_line()


if __name__ == '__main__':
    # 检查是否在Windows上运行
    if sys.platform != 'win32':
        print("错误: Windows服务只能在Windows系统上运行")
        sys.exit(1)
    
    run_as_service()
