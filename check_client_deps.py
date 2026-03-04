"""
检查客户端依赖脚本
验证客户端是否可以在没有第三方依赖的情况下运行
"""

import sys
import importlib
from pathlib import Path

def check_module(module_name: str, is_optional: bool = False) -> bool:
    """检查模块是否可以导入"""
    try:
        importlib.import_module(module_name)
        status = "✓"
        result = True
    except ImportError as e:
        if is_optional:
            status = "○ (可选)"
            result = True
        else:
            status = f"✗ 错误: {e}"
            result = False
    
    print(f"  {status:20} {module_name}")
    return result

def main():
    print("=" * 60)
    print("客户端依赖检查")
    print("=" * 60)
    
    # 标准库依赖（必须）
    print("\n标准库依赖:")
    stdlib_modules = [
        'os', 'sys', 'io', 'json', 'time', 'socket', 'struct', 'uuid',
        'hashlib', 'zlib', 'zipfile', 'subprocess', 'threading', 'logging',
        'argparse', 'getpass', 'configparser', 'pathlib', 'typing',
        'datetime', 'concurrent.futures', 'queue', 'select',
        'asyncio', 'dataclasses', 'enum', 'collections'
    ]
    
    all_ok = True
    for mod in stdlib_modules:
        if not check_module(mod):
            all_ok = False
    
    # 可选第三方依赖
    print("\n可选第三方依赖:")
    optional_modules = [
        ('lz4', 'LZ4压缩算法支持'),
        ('lz4.frame', 'LZ4压缩算法支持'),
        ('aiofiles', '异步文件操作支持'),
        ('pybloom_live', '布隆过滤器支持'),
    ]
    
    for mod, desc in optional_modules:
        check_module(mod, is_optional=True)
        print(f"    └─ {desc}")
    
    # 检查客户端模块
    print("\n客户端模块:")
    sys.path.insert(0, str(Path(__file__).parent))
    
    client_modules = [
        'shared.compression',
        'shared.protocol',
        'client.connection_pool',
        'client.async_client',
        'client.prefetcher',
    ]
    
    for mod in client_modules:
        try:
            importlib.import_module(mod)
            print(f"  ✓ {mod}")
        except Exception as e:
            print(f"  ✗ {mod}: {e}")
            all_ok = False
    
    print("\n" + "=" * 60)
    if all_ok:
        print("✅ 所有必需依赖已满足，客户端可以正常运行！")
        print("\n注意：可选依赖未安装时会自动降级到标准库实现：")
        print("  • 无lz4: 使用zlib压缩")
        print("  • 无aiofiles: 使用线程池异步I/O")
        print("  • 无pybloom_live: 使用简单集合代替布隆过滤器")
    else:
        print("❌ 存在依赖问题，请检查错误信息")
    print("=" * 60)
    
    return 0 if all_ok else 1

if __name__ == "__main__":
    sys.exit(main())
