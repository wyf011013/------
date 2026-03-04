@echo off
REM 文件备份客户端安装脚本
REM 需要以管理员身份运行

echo ================================================
echo   文件备份客户端安装程序
echo ================================================
echo.

REM 检查管理员权限
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo 错误：请以管理员身份运行此脚本！
    echo.
    echo 操作步骤：
    echo 1. 右键点击此脚本
echo 2. 选择"以管理员身份运行"
    echo.
    pause
    exit /b 1
)

echo [1/5] 检查Python环境...
python --version >nul 2>&1
if %errorLevel% neq 0 (
    echo 错误：未找到Python 3.12！
    echo 请先安装 Python 3.12.9
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo 检测到 Python %PYTHON_VERSION%

echo.
echo [2/5] 安装Python依赖...
cd /d "%~dp0..\client"
pip install -r requirements.txt
if %errorLevel% neq 0 (
    echo 警告：部分依赖安装失败，请手动安装
    pause
)

echo.
echo [3/5] 创建配置文件...
set CONFIG_DIR=%APPDATA%\FileBackupClient
if not exist "%CONFIG_DIR%" mkdir "%CONFIG_DIR%"

set CONFIG_FILE=%CONFIG_DIR%\config.ini
if not exist "%CONFIG_FILE%" (
    echo [server] > "%CONFIG_FILE%"
    echo host=127.0.0.1 >> "%CONFIG_FILE%"
    echo port=8888 >> "%CONFIG_FILE%"
    echo. >> "%CONFIG_FILE%"
    echo [client] >> "%CONFIG_FILE%"
    echo client_id= >> "%CONFIG_FILE%"
    echo computer_name= >> "%CONFIG_FILE%"
    echo. >> "%CONFIG_FILE%"
    echo [backup] >> "%CONFIG_FILE%"
    echo monitor_paths=C:\Users\%USERNAME%\Documents,C:\Users\%USERNAME%\Desktop >> "%CONFIG_FILE%"
    echo exclude_patterns=*.tmp,*.temp,*.log,*.lock,*.swp,*.swo,~* >> "%CONFIG_FILE%"
    echo.
    echo 默认配置文件已创建：%CONFIG_FILE%
) else (
    echo 配置文件已存在：%CONFIG_FILE%
)

echo.
echo [4/5] 安装Windows服务...
cd /d "%~dp0..\client"
python windows_service.py --install-service
if %errorLevel% neq 0 (
    echo 警告：服务安装失败，可以手动安装
    echo 手动安装命令：python windows_service.py --install-service
)

echo.
echo [5/5] 启动服务...
net start FileBackupClient
if %errorLevel% neq 0 (
    echo 警告：服务启动失败
    echo 请检查配置文件和服务状态
)

echo.
echo ================================================
echo   安装完成！
echo ================================================
echo.
echo 配置文件位置: %CONFIG_FILE%
echo 日志位置: %SYSTEMROOT%\Logs\FileBackupService.log
echo.
echo 管理命令：
echo   启动服务: net start FileBackupClient
echo   停止服务: net stop FileBackupClient
echo   卸载服务: sc delete FileBackupClient
echo.
echo 测试运行（前台）：
echo   cd client
echo   python client_main.py
echo.
pause
