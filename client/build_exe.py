"""
PyInstaller 打包脚本
将客户端打包为无需Python环境的独立EXE可执行文件

使用方法:
    python build_exe.py              # 标准打包
    python build_exe.py --onefile    # 单文件打包
    python build_exe.py --hidden     # 隐藏窗口模式(无控制台)
    python build_exe.py --onefile --hidden  # 单文件+隐藏窗口

依赖:
    pip install pyinstaller
"""

import os
import sys
import shutil
import argparse
import subprocess
from pathlib import Path


def get_project_root():
    """获取项目根目录"""
    return Path(__file__).parent.parent.resolve()


def get_client_dir():
    """获取客户端目录"""
    return Path(__file__).parent.resolve()


def build_exe(onefile=True, hidden=False, clean=True, icon_path=None):
    """
    使用PyInstaller打包客户端为EXE

    Args:
        onefile: 打包为单个EXE文件
        hidden: 隐藏控制台窗口(--noconsole)
        clean: 打包前清理旧的构建文件
        icon_path: 自定义图标路径(.ico)
    """
    project_root = get_project_root()
    client_dir = get_client_dir()
    shared_dir = project_root / 'shared'

    # 主入口文件
    entry_point = client_dir / 'client_main.py'
    if not entry_point.exists():
        print(f"[错误] 入口文件不存在: {entry_point}")
        sys.exit(1)

    # 输出目录
    dist_dir = client_dir / 'dist'
    build_dir = client_dir / 'build'

    # 清理旧构建
    if clean:
        for d in [dist_dir, build_dir]:
            if d.exists():
                print(f"[清理] 删除旧构建: {d}")
                shutil.rmtree(d)

    # 构建PyInstaller命令
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--name', 'FileBackupClient',
        '--noconfirm',
        '--clean',
    ]

    # 单文件模式
    if onefile:
        cmd.append('--onefile')
    else:
        cmd.append('--onedir')

    # 隐藏控制台窗口
    if hidden:
        cmd.append('--noconsole')

    # 自定义图标
    if icon_path and os.path.exists(icon_path):
        cmd.extend(['--icon', str(icon_path)])

    # 添加数据文件: shared/protocol.py
    if shared_dir.exists():
        # PyInstaller数据文件格式: source;dest (Windows) 或 source:dest (Linux)
        sep = ';' if sys.platform == 'win32' else ':'
        cmd.extend(['--add-data', f'{shared_dir}{sep}shared'])

    # 隐藏导入（确保所有依赖被包含）
    hidden_imports = [
        'watchdog',
        'watchdog.observers',
        'watchdog.events',
        'psutil',
        'configparser',
        'hashlib',
        'zlib',
        'uuid',
        'json',
        'struct',
        'threading',
        'queue',
        'fnmatch',
        'logging',
        'logging.handlers',
    ]

    # Windows特有依赖
    if sys.platform == 'win32':
        hidden_imports.extend([
            'winreg',
            'win32gui',
            'win32con',
            'win32api',
            'win32process',
            'ctypes',
            'ctypes.wintypes',
        ])

    for imp in hidden_imports:
        cmd.extend(['--hidden-import', imp])

    # 排除不需要的大模块
    excludes = [
        'tkinter', 'matplotlib', 'numpy', 'pandas', 'scipy',
        'PIL', 'cv2', 'django', 'flask', 'sqlalchemy',
        'pytest', 'unittest', 'test', 'tests',
    ]
    for exc in excludes:
        cmd.extend(['--exclude-module', exc])

    # 路径搜索（确保能找到shared模块）
    cmd.extend(['--paths', str(project_root)])
    cmd.extend(['--paths', str(client_dir)])

    # 输出目录
    cmd.extend(['--distpath', str(dist_dir)])
    cmd.extend(['--workpath', str(build_dir)])
    cmd.extend(['--specpath', str(client_dir)])

    # 入口文件
    cmd.append(str(entry_point))

    # 执行打包
    print("=" * 60)
    print("  文件备份客户端 - EXE打包工具")
    print("=" * 60)
    print(f"  入口文件: {entry_point}")
    print(f"  打包模式: {'单文件' if onefile else '目录'}")
    print(f"  控制台:   {'隐藏' if hidden else '显示'}")
    print(f"  输出目录: {dist_dir}")
    print("=" * 60)
    print()
    print(f"[执行] {' '.join(cmd)}")
    print()

    result = subprocess.run(cmd, cwd=str(client_dir))

    if result.returncode == 0:
        print()
        print("=" * 60)
        print("  打包成功!")
        print("=" * 60)

        if onefile:
            exe_path = dist_dir / 'FileBackupClient.exe'
            if exe_path.exists():
                size_mb = exe_path.stat().st_size / (1024 * 1024)
                print(f"  输出文件: {exe_path}")
                print(f"  文件大小: {size_mb:.1f} MB")
            else:
                # Linux下没有.exe后缀
                exe_path = dist_dir / 'FileBackupClient'
                if exe_path.exists():
                    size_mb = exe_path.stat().st_size / (1024 * 1024)
                    print(f"  输出文件: {exe_path}")
                    print(f"  文件大小: {size_mb:.1f} MB")
        else:
            print(f"  输出目录: {dist_dir / 'FileBackupClient'}")

        # 复制默认配置文件到输出目录
        _copy_config_template(dist_dir, onefile)

        print()
        print("  使用方法:")
        print("    FileBackupClient.exe                    # 正常启动")
        print("    FileBackupClient.exe --service          # 服务模式(隐藏窗口)")
        print("    FileBackupClient.exe --config path.ini  # 指定配置文件")
        print("=" * 60)
    else:
        print()
        print("[错误] 打包失败，请检查上方错误信息")
        sys.exit(1)


def _copy_config_template(dist_dir, onefile):
    """复制默认配置文件模板到输出目录"""
    config_content = """[server]
; 服务器地址（留空则通过LAN自动发现）
host =
port = 8888
timeout = 30
retry_interval = 60
heartbeat_interval = 300

[client]
; 客户端ID（首次运行自动生成，请勿手动修改）
client_id =
computer_name =
buffer_size = 65536
max_file_size_mb = 100
batch_size = 10
batch_timeout = 5.0

[backup]
; 监控路径（多个路径用逗号分隔）
monitor_paths =
; 排除文件模式
exclude_patterns = *.tmp,*.temp,*.log,*.lock,*.swp,*.swo,~*,.DS_Store,Thumbs.db
include_patterns = *
scan_interval = 3600

[performance]
; 带宽限制（KB/s，0=不限制）
network_limit_kbps = 0
cpu_limit_percent = 50
memory_limit_mb = 100
compression_threshold = 1024

[discovery]
; LAN自动发现
enabled = True
timeout = 5
retries = 3
"""
    if onefile:
        config_path = dist_dir / 'config.ini'
    else:
        config_path = dist_dir / 'FileBackupClient' / 'config.ini'

    os.makedirs(config_path.parent, exist_ok=True)
    with open(config_path, 'w', encoding='utf-8') as f:
        f.write(config_content)
    print(f"  配置模板: {config_path}")


def main():
    parser = argparse.ArgumentParser(description='文件备份客户端 EXE打包工具')
    parser.add_argument('--onefile', action='store_true', default=True,
                        help='打包为单个EXE文件 (默认)')
    parser.add_argument('--onedir', action='store_true',
                        help='打包为目录形式')
    parser.add_argument('--hidden', action='store_true',
                        help='隐藏控制台窗口 (后台运行模式)')
    parser.add_argument('--no-clean', action='store_true',
                        help='不清理旧的构建文件')
    parser.add_argument('--icon', type=str, default=None,
                        help='自定义图标路径 (.ico)')

    args = parser.parse_args()

    # 检查PyInstaller是否安装
    try:
        import PyInstaller
        print(f"[OK] PyInstaller {PyInstaller.__version__} 已安装")
    except ImportError:
        print("[错误] PyInstaller未安装，正在自动安装...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyinstaller'])
        print("[OK] PyInstaller安装完成")

    onefile = not args.onedir
    build_exe(
        onefile=onefile,
        hidden=args.hidden,
        clean=not args.no_clean,
        icon_path=args.icon,
    )


if __name__ == '__main__':
    main()
