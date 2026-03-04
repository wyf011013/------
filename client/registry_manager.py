"""
Windows注册表管理器
用于设置客户端开机自启动和隐藏运行
跨平台兼容：非Windows平台下所有操作静默跳过
"""

import os
import sys
import logging
from pathlib import Path

# Windows注册表模块（仅Windows可用）
if sys.platform == 'win32':
    try:
        import winreg
    except ImportError:
        winreg = None
else:
    winreg = None


class RegistryManager:
    """Windows注册表管理器（非Windows平台下静默跳过）"""

    # 注册表路径
    RUN_KEY_PATH = r"SOFTWARE\Microsoft\Windows\CurrentVersion\Run"
    APP_NAME = "FileBackupClient"

    # 服务注册表路径
    SERVICE_KEY_PATH = r"SYSTEM\CurrentControlSet\Services\FileBackupClient"

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.is_windows = sys.platform == 'win32' and winreg is not None
    
    def set_auto_start(self, executable_path: str = None, arguments: str = "--service") -> bool:
        """
        设置程序开机自启动

        Args:
            executable_path: 可执行文件路径，默认为当前程序
            arguments: 启动参数

        Returns:
            是否成功
        """
        if not self.is_windows:
            return True

        if executable_path is None:
            # 判断是否为PyInstaller打包的exe
            if getattr(sys, 'frozen', False):
                executable_path = sys.executable
                arguments = arguments  # exe直接带参数
            else:
                executable_path = sys.executable
                # 如果是Python脚本，添加脚本路径
                if not executable_path.endswith('.exe'):
                    script_path = os.path.abspath(sys.argv[0])
                    arguments = f'"{script_path}" {arguments}'

        command = f'"{executable_path}" {arguments}'

        try:
            # 打开注册表项
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, self.RUN_KEY_PATH,
                              0, winreg.KEY_SET_VALUE) as key:
                # 设置值
                winreg.SetValueEx(key, self.APP_NAME, 0, winreg.REG_SZ, command)
                self.logger.info(f"已设置开机自启动: {command}")
                return True
        except PermissionError:
            # 如果没有管理员权限，尝试当前用户
            try:
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER, self.RUN_KEY_PATH,
                                  0, winreg.KEY_SET_VALUE) as key:
                    winreg.SetValueEx(key, self.APP_NAME, 0, winreg.REG_SZ, command)
                    self.logger.info(f"已设置当前用户开机自启动: {command}")
                    return True
            except Exception as e:
                self.logger.error(f"设置开机自启动失败: {e}")
                return False
        except Exception as e:
            self.logger.error(f"设置开机自启动失败: {e}")
            return False
    
    def remove_auto_start(self) -> bool:
        """
        移除开机自启动设置

        Returns:
            是否成功
        """
        if not self.is_windows:
            return True
        try:
            # 尝试从HKEY_LOCAL_MACHINE删除
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, self.RUN_KEY_PATH,
                              0, winreg.KEY_SET_VALUE) as key:
                winreg.DeleteValue(key, self.APP_NAME)
                self.logger.info("已从HKEY_LOCAL_MACHINE移除开机自启动")
                return True
        except FileNotFoundError:
            pass  # 值不存在，继续尝试其他位置
        except PermissionError:
            pass  # 没有权限，继续尝试其他位置
        except Exception as e:
            self.logger.warning(f"从HKEY_LOCAL_MACHINE移除失败: {e}")
        
        try:
            # 尝试从HKEY_CURRENT_USER删除
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, self.RUN_KEY_PATH,
                              0, winreg.KEY_SET_VALUE) as key:
                winreg.DeleteValue(key, self.APP_NAME)
                self.logger.info("已从HKEY_CURRENT_USER移除开机自启动")
                return True
        except FileNotFoundError:
            self.logger.info("开机自启动项不存在")
            return True
        except Exception as e:
            self.logger.error(f"移除开机自启动失败: {e}")
            return False
    
    def is_auto_start_set(self) -> bool:
        """
        检查是否已设置开机自启动

        Returns:
            是否已设置
        """
        if not self.is_windows:
            return False
        # 检查HKEY_LOCAL_MACHINE
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, self.RUN_KEY_PATH,
                              0, winreg.KEY_READ) as key:
                value, _ = winreg.QueryValueEx(key, self.APP_NAME)
                return value is not None
        except (FileNotFoundError, OSError):
            pass
        
        # 检查HKEY_CURRENT_USER
        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, self.RUN_KEY_PATH,
                              0, winreg.KEY_READ) as key:
                value, _ = winreg.QueryValueEx(key, self.APP_NAME)
                return value is not None
        except (FileNotFoundError, OSError):
            pass
        
        return False
    
    def get_auto_start_command(self) -> str:
        """
        获取开机自启动命令

        Returns:
            启动命令，如果不存在则返回空字符串
        """
        if not self.is_windows:
            return ""
        # 检查HKEY_LOCAL_MACHINE
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, self.RUN_KEY_PATH,
                              0, winreg.KEY_READ) as key:
                value, _ = winreg.QueryValueEx(key, self.APP_NAME)
                return value
        except (FileNotFoundError, OSError):
            pass
        
        # 检查HKEY_CURRENT_USER
        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, self.RUN_KEY_PATH,
                              0, winreg.KEY_READ) as key:
                value, _ = winreg.QueryValueEx(key, self.APP_NAME)
                return value
        except (FileNotFoundError, OSError):
            pass
        
        return ""
    
    def hide_console_window(self) -> None:
        """
        隐藏控制台窗口（仅在Windows下有效）
        
        注意：此方法应在程序启动时尽早调用
        """
        if sys.platform != 'win32':
            return
        
        try:
            import ctypes
            import win32gui
            import win32con
            
            # 获取控制台窗口句柄
            hwnd = win32gui.GetConsoleWindow()
            if hwnd:
                # 隐藏窗口
                win32gui.ShowWindow(hwnd, win32con.SW_HIDE)
                self.logger.info("控制台窗口已隐藏")
        except Exception as e:
            self.logger.warning(f"隐藏控制台窗口失败: {e}")
    
    def prevent_shutdown_block(self) -> None:
        """
        防止Windows关机时被阻塞
        
        注册程序为关键服务，使Windows在关机时等待程序退出
        """
        if sys.platform != 'win32':
            return
        
        try:
            import ctypes
            from ctypes import wintypes
            
            # 设置关机参数
            ctypes.windll.kernel32.SetProcessShutdownParameters(0x100, 0)
            self.logger.info("已设置关机参数")
        except Exception as e:
            self.logger.warning(f"设置关机参数失败: {e}")
    
    def set_process_priority(self, priority: str = "below_normal") -> bool:
        """
        设置进程优先级，减少对系统性能的影响
        
        Args:
            priority: 优先级 (idle, below_normal, normal, above_normal, high)
            
        Returns:
            是否成功
        """
        if sys.platform != 'win32':
            return True
        
        try:
            import win32api
            import win32process

            # 使用数值常量，避免某些pywin32版本缺少BELOW_NORMAL_PRIORITY_CLASS
            IDLE_PRIORITY_CLASS = 0x00000040
            BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
            NORMAL_PRIORITY_CLASS = 0x00000020
            ABOVE_NORMAL_PRIORITY_CLASS = 0x00008000
            HIGH_PRIORITY_CLASS = 0x00000080

            priority_map = {
                "idle": IDLE_PRIORITY_CLASS,
                "below_normal": BELOW_NORMAL_PRIORITY_CLASS,
                "normal": NORMAL_PRIORITY_CLASS,
                "above_normal": ABOVE_NORMAL_PRIORITY_CLASS,
                "high": HIGH_PRIORITY_CLASS
            }
            
            if priority not in priority_map:
                priority = "below_normal"
            
            # 获取当前进程句柄
            handle = win32api.GetCurrentProcess()
            # 设置优先级
            win32process.SetPriorityClass(handle, priority_map[priority])
            self.logger.info(f"进程优先级已设置为: {priority}")
            return True
            
        except Exception as e:
            self.logger.warning(f"设置进程优先级失败: {e}")
            return False


# 测试代码
if __name__ == "__main__":
    import uuid
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    registry = RegistryManager()
    
    # 测试开机自启动
    print("=== 测试开机自启动 ===")
    print(f"当前是否已设置: {registry.is_auto_start_set()}")
    
    # 设置自启动
    success = registry.set_auto_start()
    print(f"设置自启动: {'成功' if success else '失败'}")
    
    if success:
        print(f"自启动命令: {registry.get_auto_start_command()}")
        
        # 测试移除
        # remove_success = registry.remove_auto_start()
        # print(f"移除自启动: {'成功' if remove_success else '失败'}")
    
    # 隐藏窗口（测试时注释掉，否则看不到输出）
    # registry.hide_console_window()
    
    # 设置进程优先级
    print("\n=== 测试进程优先级 ===")
    priority_success = registry.set_process_priority("below_normal")
    print(f"设置优先级: {'成功' if priority_success else '失败'}")
