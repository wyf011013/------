"""
简化的 PyInstaller 打包脚本
确保所有模块正确打包
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path


def main():
    client_dir = Path(__file__).parent.resolve()
    project_root = client_dir.parent
    
    print("=" * 60)
    print("  文件备份客户端 - EXE打包工具")
    print("=" * 60)
    
    # 检查 PyInstaller
    try:
        import PyInstaller
        print(f"[OK] PyInstaller {PyInstaller.__version__} 已安装")
    except ImportError:
        print("[安装] PyInstaller...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyinstaller'])
    
    # 检查可选依赖
    optional_deps = ['lz4', 'aiofiles']
    for dep in optional_deps:
        try:
            __import__(dep)
            print(f"[OK] {dep} 已安装")
        except ImportError:
            print(f"[安装] {dep}...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', dep])
    
    # 清理旧构建
    dist_dir = client_dir / 'dist'
    build_dir = client_dir / 'build'
    for d in [dist_dir, build_dir]:
        if d.exists():
            print(f"[清理] {d}")
            shutil.rmtree(d)
    
    # 构建命令
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--name', 'FileBackupClient',
        '--onefile',
        '--noconfirm',
        '--clean',
    ]
    
    # 隐藏导入 - 包括所有本地模块
    hidden_imports = [
        # 第三方库
        'watchdog', 'watchdog.observers', 'watchdog.events', 'psutil',
        'lz4', 'lz4.frame', 'aiofiles',
        # 本地模块 - 关键！
        'connection_pool', 'async_client', 'prefetcher',
        'file_monitor', 'registry_manager', 'windows_service',
        # shared 包
        'shared', 'shared.protocol', 'shared.compression', 'shared.performance_config',
    ]
    
    for imp in hidden_imports:
        cmd.extend(['--hidden-import', imp])
    
    # 添加路径
    cmd.extend(['--paths', str(project_root)])
    cmd.extend(['--paths', str(client_dir)])
    
    # 添加数据文件
    sep = ';' if sys.platform == 'win32' else ':'
    shared_dir = project_root / 'shared'
    cmd.extend(['--add-data', f'{shared_dir}{sep}shared'])
    
    # 添加客户端目录中的 .py 文件作为数据
    for py_file in client_dir.glob('*.py'):
        if py_file.name not in ['build_exe.py', 'build_exe_fixed.py', 'build_exe_simple.py']:
            cmd.extend(['--add-data', f'{py_file}{sep}.'])
    
    # 排除大模块
    excludes = ['tkinter', 'matplotlib', 'numpy', 'pandas', 'scipy', 
                'PIL', 'cv2', 'django', 'flask', 'sqlalchemy',
                'pytest', 'unittest']
    for exc in excludes:
        cmd.extend(['--exclude-module', exc])
    
    # 入口文件
    cmd.append(str(client_dir / 'client_main.py'))
    
    print()
    print("[打包] 开始构建...")
    print(f"[命令] {' '.join(cmd[:10])} ...")  # 只显示部分命令
    print()
    
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        exe_path = dist_dir / 'FileBackupClient.exe'
        if exe_path.exists():
            size_mb = exe_path.stat().st_size / (1024 * 1024)
            print()
            print("=" * 60)
            print("  打包成功!")
            print("=" * 60)
            print(f"  输出: {exe_path}")
            print(f"  大小: {size_mb:.1f} MB")
            print("=" * 60)
            return 0
    
    print("[错误] 打包失败")
    return 1


if __name__ == '__main__':
    sys.exit(main())
