# 客户端打包说明

## 打包命令

### 标准打包（推荐）
```bash
python build_exe.py
```
这会：
- 自动检查并安装可选依赖（lz4、aiofiles）
- 打包为单文件 exe
- 包含性能优化模块

### 不安装可选依赖（纯标准库）
```bash
python build_exe.py --without-optional-deps
```

### 隐藏控制台窗口（服务模式）
```bash
python build_exe.py --hidden
```

### 完整选项
```bash
python build_exe.py --onefile --hidden --icon app.ico
```

## 可选依赖说明

| 依赖 | 功能 | 无依赖时的降级方案 |
|------|------|-------------------|
| lz4 | 超快压缩算法 | 使用 zlib level 1 |
| aiofiles | 异步文件I/O | 使用线程池异步I/O |

## 打包输出

打包完成后会在 `client/dist/` 目录生成：
- `FileBackupClient.exe` - 主程序
- `config.ini` - 默认配置文件

## 文件大小参考

- 无可选依赖：约 15-20 MB
- 包含 lz4：约 +1 MB
- 包含 aiofiles：约 +0.5 MB

## 注意事项

1. **首次打包**会自动安装 PyInstaller
2. **默认包含**可选依赖以获得最佳性能
3. **降级机制**确保即使依赖未安装也能正常运行
4. **服务端**的 pybloom_live 不需要打包到客户端
