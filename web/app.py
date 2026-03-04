"""
Web管理端 - 基于Flask + AdminLTE
提供客户端管理、备份配置、文件浏览、客户端生成器、远程命令等功能
企业局域网文件备份系统的核心Web管理应用
"""

import os
import sys
import json
import time
import uuid
import shutil
import zipfile
import tempfile
import threading
import logging
from pathlib import Path
from datetime import datetime
from functools import wraps

# 添加项目根目录到模块搜索路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import (Flask, render_template, request, jsonify, session, redirect,
                   url_for, flash, send_file, Response)
from werkzeug.security import generate_password_hash, check_password_hash

# 导入数据库管理器（BlockManager、OTAManager、UserManager为可选模块）
from server.database import (DatabaseManager, ClientManager, BackupConfigManager,
                              FileVersionManager, SystemLogManager)
from server.config import ServerConfig
from server.storage_manager import StorageManager

# 尝试导入可选的数据库管理器模块
try:
    from server.database import BlockManager
except ImportError:
    BlockManager = None

try:
    from server.database import OTAManager
except ImportError:
    OTAManager = None

try:
    from server.database import UserManager
except ImportError:
    UserManager = None


# ============================================================================
# Flask应用初始化
# ============================================================================

# 创建Flask应用实例
app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'file-backup-system-secret-key-change-in-production')

# 设置日志
logger = logging.getLogger(__name__)

# 加载服务端配置
config = ServerConfig()

# 初始化数据库管理器
db_config = config.get('database', {})
if isinstance(db_config, dict):
    db_manager = DatabaseManager(**db_config)
else:
    db_manager = DatabaseManager()

# 初始化各业务管理器
client_manager = ClientManager(db_manager)
config_manager = BackupConfigManager(db_manager)
file_manager = FileVersionManager(db_manager)
log_manager = SystemLogManager(db_manager)

# 初始化可选管理器
block_manager = BlockManager(db_manager) if BlockManager else None
ota_manager = OTAManager(db_manager) if OTAManager else None
user_manager = UserManager(db_manager) if UserManager else None

# 初始化存储管理器
storage_config = config.get('storage', {})
if isinstance(storage_config, dict):
    storage_root = storage_config.get('root_path', './backup_storage')
else:
    storage_root = './backup_storage'
storage_manager = StorageManager(storage_root)

# BackupServer实例引用，由server_main.py通过set_server()设置
_backup_server = None
_server_lock = threading.Lock()

# 远程命令结果缓存：{command_id: {result: ..., timestamp: ...}}
_command_results = {}
_command_results_lock = threading.Lock()


# ============================================================================
# 系统资源监控后台采集器（解决API阻塞和数据为0的问题）
# ============================================================================

class SystemResourceMonitor:
    """
    后台资源采集线程，定时更新CPU/内存/磁盘/网络数据。
    API读取缓存值，无需阻塞等待。
    """

    def __init__(self, interval=3):
        self.interval = interval
        self._lock = threading.Lock()
        self._data = {
            'cpu_percent': 0.0,
            'memory_percent': 0.0,
            'memory_total': 0,
            'memory_used': 0,
            'disk_total': 0,
            'disk_used': 0,
            'disk_percent': 0.0,
            'net_upload_speed': 0.0,
            'net_download_speed': 0.0,
        }
        self._has_psutil = False
        self._net_last_sent = 0
        self._net_last_recv = 0
        self._net_last_time = 0
        self._started = False

        # 检测psutil
        try:
            import psutil
            self._has_psutil = True
        except ImportError:
            logger.warning("psutil未安装，系统资源监控将使用备用方案")

    def start(self):
        if self._started:
            return
        self._started = True
        # 初始化网络基线
        if self._has_psutil:
            try:
                import psutil
                net = psutil.net_io_counters()
                self._net_last_sent = net.bytes_sent
                self._net_last_recv = net.bytes_recv
                self._net_last_time = time.time()
                # 预热CPU采样（首次调用返回0，需要先调用一次建立基线）
                psutil.cpu_percent(interval=None)
            except Exception:
                pass
        # 同步执行首次采集，确保API立即有数据
        try:
            self._collect_once()
            logger.info(f"系统资源首次采集完成: CPU={self._data.get('cpu_percent')}% "
                        f"MEM={self._data.get('memory_percent')}% "
                        f"DISK={self._data.get('disk_percent')}%")
        except Exception as e:
            logger.warning(f"系统资源首次采集失败: {e}")
        t = threading.Thread(target=self._collect_loop, daemon=True)
        t.start()
        logger.info(f"系统资源监控线程已启动 (间隔={self.interval}s, psutil={self._has_psutil})")

    def get_data(self):
        with self._lock:
            return dict(self._data)

    def _collect_loop(self):
        _first = True
        while True:
            try:
                self._collect_once()
                if _first:
                    logger.info(f"资源监控后台循环首次采集成功: {self._data}")
                    _first = False
            except Exception as e:
                logger.warning(f"资源采集异常: {e}")
            time.sleep(self.interval)

    def _collect_once(self):
        data = {}

        if self._has_psutil:
            try:
                import psutil

                # CPU (non-blocking，使用上次调用的差值)
                data['cpu_percent'] = round(psutil.cpu_percent(interval=1), 1)

                # 内存
                mem = psutil.virtual_memory()
                data['memory_percent'] = round(mem.percent, 1)
                data['memory_total'] = mem.total
                data['memory_used'] = mem.used

                # 磁盘
                storage_path = os.path.abspath(storage_root)
                try:
                    disk = psutil.disk_usage(storage_path)
                    data['disk_total'] = disk.total
                    data['disk_used'] = disk.used
                    data['disk_percent'] = round(disk.percent, 1)
                except Exception:
                    disk = psutil.disk_usage('/')
                    data['disk_total'] = disk.total
                    data['disk_used'] = disk.used
                    data['disk_percent'] = round(disk.percent, 1)

                # 网络速度
                try:
                    net = psutil.net_io_counters()
                    now = time.time()
                    elapsed = now - self._net_last_time
                    if elapsed > 0 and self._net_last_time > 0:
                        data['net_upload_speed'] = round(
                            (net.bytes_sent - self._net_last_sent) / elapsed, 0)
                        data['net_download_speed'] = round(
                            (net.bytes_recv - self._net_last_recv) / elapsed, 0)
                    else:
                        data['net_upload_speed'] = 0.0
                        data['net_download_speed'] = 0.0
                    self._net_last_sent = net.bytes_sent
                    self._net_last_recv = net.bytes_recv
                    self._net_last_time = now
                except Exception:
                    data['net_upload_speed'] = 0.0
                    data['net_download_speed'] = 0.0

            except Exception as e:
                logger.debug(f"psutil采集异常: {e}")
                self._collect_fallback(data)
        else:
            self._collect_fallback(data)

        with self._lock:
            self._data.update(data)

    def _collect_fallback(self, data):
        """psutil不可用时的备用采集方案"""
        # CPU - 从/proc/stat读取
        try:
            with open('/proc/stat', 'r') as f:
                line = f.readline()
            parts = line.split()
            if parts[0] == 'cpu':
                total = sum(int(x) for x in parts[1:])
                idle = int(parts[4])
                # 需要与上次比较
                if hasattr(self, '_cpu_last_total'):
                    dtotal = total - self._cpu_last_total
                    didle = idle - self._cpu_last_idle
                    if dtotal > 0:
                        data['cpu_percent'] = round((1 - didle / dtotal) * 100, 1)
                self._cpu_last_total = total
                self._cpu_last_idle = idle
        except Exception:
            pass

        # 内存 - 从/proc/meminfo读取
        try:
            with open('/proc/meminfo', 'r') as f:
                meminfo = f.read()
            mem_total = 0
            mem_available = 0
            for line in meminfo.splitlines():
                if line.startswith('MemTotal:'):
                    mem_total = int(line.split()[1]) * 1024
                elif line.startswith('MemAvailable:'):
                    mem_available = int(line.split()[1]) * 1024
            if mem_total > 0:
                mem_used = mem_total - mem_available
                data['memory_total'] = mem_total
                data['memory_used'] = mem_used
                data['memory_percent'] = round(mem_used / mem_total * 100, 1)
        except Exception:
            pass

        # 磁盘 - 使用os.statvfs
        try:
            stat = os.statvfs(os.path.abspath(storage_root))
            data['disk_total'] = stat.f_blocks * stat.f_frsize
            disk_used = (stat.f_blocks - stat.f_bfree) * stat.f_frsize
            data['disk_used'] = disk_used
            data['disk_percent'] = round(
                disk_used / data['disk_total'] * 100, 1) if data['disk_total'] > 0 else 0
        except Exception:
            pass


# 创建并启动全局资源监控器
_resource_monitor = SystemResourceMonitor(interval=3)
_resource_monitor.start()


def set_server(server_instance):
    """
    设置BackupServer实例引用
    由server_main.py在启动时调用，将TCP服务器实例传递给Web应用，
    以便Web端可以通过TCP服务器向已连接的客户端发送命令

    Args:
        server_instance: BackupServer实例
    """
    global _backup_server
    with _server_lock:
        _backup_server = server_instance
        logger.info("BackupServer实例已设置到Web应用")


def get_server():
    """
    获取BackupServer实例引用

    Returns:
        BackupServer实例，如果未设置则返回None
    """
    with _server_lock:
        return _backup_server


# ============================================================================
# 模板过滤器
# ============================================================================

@app.template_filter('datetime_format')
def datetime_format_filter(value, fmt='%Y-%m-%d %H:%M:%S'):
    """
    格式化日期时间
    支持datetime对象和ISO格式字符串

    Args:
        value: 日期时间值
        fmt: 格式化字符串
    """
    if value is None:
        return '-'
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return value
    if isinstance(value, datetime):
        return value.strftime(fmt)
    return str(value)


@app.template_filter('file_size_format')
def file_size_format_filter(size_bytes):
    """
    格式化文件大小为人类可读格式
    支持B、KB、MB、GB、TB单位自动转换

    Args:
        size_bytes: 文件大小（字节数）
    """
    if size_bytes is None or size_bytes == 0:
        return "0 B"

    try:
        size_bytes = float(size_bytes)
    except (ValueError, TypeError):
        return "0 B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    size = size_bytes

    while size >= 1024.0 and i < len(size_names) - 1:
        size /= 1024.0
        i += 1

    return f"{size:.2f} {size_names[i]}"


@app.template_filter('status_badge')
def status_badge_filter(status):
    """
    将状态值映射为Bootstrap徽章样式类名

    Args:
        status: 状态字符串
    """
    badge_classes = {
        'online': 'success',
        'offline': 'danger',
        'info': 'info',
        'warning': 'warning',
        'error': 'danger',
        'success': 'success',
        'pending': 'secondary',
        'running': 'primary',
    }
    return badge_classes.get(status, 'secondary')


@app.template_filter('time_ago')
def time_ago_filter(value):
    """
    将日期时间转换为"多久之前"的友好格式
    例如："3分钟前"、"2小时前"、"1天前"

    Args:
        value: 日期时间值
    """
    if value is None:
        return '从未'

    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return str(value)

    if not isinstance(value, datetime):
        return str(value)

    now = datetime.utcnow()
    # 如果value有时区信息，移除以便比较
    if value.tzinfo is not None:
        try:
            value = value.replace(tzinfo=None)
        except Exception:
            pass

    diff = now - value
    seconds = int(diff.total_seconds())

    if seconds < 0:
        return '刚刚'
    elif seconds < 60:
        return f'{seconds}秒前'
    elif seconds < 3600:
        minutes = seconds // 60
        return f'{minutes}分钟前'
    elif seconds < 86400:
        hours = seconds // 3600
        return f'{hours}小时前'
    elif seconds < 2592000:
        days = seconds // 86400
        return f'{days}天前'
    elif seconds < 31536000:
        months = seconds // 2592000
        return f'{months}个月前'
    else:
        years = seconds // 31536000
        return f'{years}年前'


# ============================================================================
# 认证装饰器
# ============================================================================

def login_required(f):
    """
    登录验证装饰器
    检查session中是否存在user_id，未登录则重定向到登录页面
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """
    管理员权限装饰器
    需要与login_required配合使用，检查用户是否为admin角色
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        if session.get('user_role') != 'admin':
            flash('需要管理员权限', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function


# ============================================================================
# 上下文处理器
# ============================================================================

@app.context_processor
def inject_global_context():
    """
    注入全局模板上下文
    所有模板中均可访问user信息和当前时间
    """
    user_info = None
    if 'user_id' in session:
        user_info = {
            'id': session.get('user_id'),
            'username': session.get('username'),
            'role': session.get('user_role'),
            'login_time': session.get('login_time'),
        }

    return {
        'user': user_info,
        'current_time': datetime.utcnow(),
        'app_version': '1.0.0',
    }


# ============================================================================
# 认证路由
# ============================================================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    """
    登录页面和登录验证
    GET: 渲染登录页面
    POST: 验证用户名密码，支持UserManager数据库验证和默认管理员账户
    """
    # 已登录则直接跳转仪表盘
    if 'user_id' in session:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')

        if not username or not password:
            flash('请输入用户名和密码', 'error')
            return render_template('login.html')

        # 优先通过UserManager进行数据库认证（使用bcrypt）
        authenticated = False
        user_info = None

        if user_manager is not None:
            try:
                user_info = user_manager.authenticate(username, password)
                if user_info:
                    authenticated = True
            except Exception as e:
                logger.warning(f"UserManager认证异常: {e}")

        # 若UserManager不可用或认证失败，使用默认管理员账户
        if not authenticated:
            if username == 'admin' and password == 'admin123':
                authenticated = True
                user_info = {
                    'id': 1,
                    'username': 'admin',
                    'role': 'admin',
                }

        if authenticated and user_info:
            # 认证成功，设置session
            session['user_id'] = user_info.get('id', 1)
            session['username'] = user_info.get('username', username)
            session['user_role'] = user_info.get('role', 'admin')
            session['login_time'] = datetime.utcnow().isoformat()
            session.permanent = True

            # 记录登录日志
            try:
                log_manager.add_log(
                    'info',
                    f"用户 {username} 登录系统 (IP: {request.remote_addr})"
                )
            except Exception as e:
                logger.warning(f"记录登录日志失败: {e}")

            logger.info(f"用户 {username} 登录成功 (IP: {request.remote_addr})")
            return redirect(url_for('dashboard'))
        else:
            flash('用户名或密码错误', 'error')
            logger.warning(f"登录失败: {username} (IP: {request.remote_addr})")

    return render_template('login.html')


@app.route('/logout')
def logout():
    """
    用户登出
    清除所有session数据，重定向到登录页面
    """
    username = session.get('username', 'unknown')
    session.clear()

    try:
        log_manager.add_log('info', f"用户 {username} 已登出")
    except Exception:
        pass

    logger.info(f"用户 {username} 已登出")
    return redirect(url_for('login'))


# ============================================================================
# 仪表盘路由
# ============================================================================

@app.route('/', methods=['GET', 'POST'])
@login_required
def dashboard():
    """
    仪表盘页面
    展示系统总览：客户端数、在线数、备份总量、存储量、最近活动日志
    支持GET和POST方法（POST用于兼容某些健康检查工具）
    """
    try:
        # 获取所有客户端列表
        clients = client_manager.get_all_clients()
        online_clients = [c for c in clients if c.get('status') == 'online']

        # 获取最近系统日志
        recent_logs = log_manager.get_logs(limit=10)

        # 统计备份总量和存储量
        total_backups = 0
        total_size = 0
        for client in clients:
            try:
                client_stats = file_manager.get_client_backup_stats(client['id'])
                total_backups += client_stats.get('total_versions', 0)
                total_size += client_stats.get('total_size', 0)
            except Exception as e:
                logger.warning(f"获取客户端 {client.get('id')} 备份统计失败: {e}")

        stats = {
            'total_clients': len(clients),
            'online_clients': len(online_clients),
            'total_backups': total_backups,
            'total_size': total_size,
            'recent_logs': recent_logs,
        }

        return render_template('dashboard.html', stats=stats)

    except Exception as e:
        logger.error(f"加载仪表盘数据失败: {e}")
        stats = {
            'total_clients': 0,
            'online_clients': 0,
            'total_backups': 0,
            'total_size': 0,
            'recent_logs': [],
        }
        return render_template('dashboard.html', stats=stats)


@app.route('/api/system-stats')
@login_required
def api_system_stats():
    """
    实时系统状态API
    从后台资源监控线程读取缓存数据（无阻塞），同时查询业务指标
    资源数据与业务数据分开try/except，确保资源监控始终有效
    """
    # 从后台监控线程获取缓存的系统资源数据（即时返回，无阻塞）
    resource_data = _resource_monitor.get_data()
    result = dict(resource_data)

    # 业务指标（单独try/except，不影响资源数据返回）
    try:
        clients = client_manager.get_all_clients()
        online_clients = [c for c in clients if c.get('status') == 'online']

        total_backups = 0
        total_size = 0
        for client in clients:
            try:
                client_stats = file_manager.get_client_backup_stats(client['id'])
                total_backups += client_stats.get('total_versions', 0)
                total_size += client_stats.get('total_size', 0)
            except Exception:
                pass

        # 备份进度信息
        active_backups = 0
        server = get_server()
        if server:
            try:
                all_progress = server.get_backup_progress()
                active_backups = len(all_progress)
            except Exception:
                pass

        result.update({
            'total_clients': len(clients),
            'online_clients': len(online_clients),
            'total_backups': total_backups,
            'total_size': total_size,
            'active_backups': active_backups,
        })
    except Exception as e:
        logger.warning(f"获取业务指标失败（资源监控正常）: {e}")
        result.update({
            'total_clients': 0,
            'online_clients': 0,
            'total_backups': 0,
            'total_size': 0,
        })

    return jsonify({
        'success': True,
        'data': result
    })


# ============================================================================
# 客户端管理路由
# ============================================================================

@app.route('/clients')
@login_required
def clients():
    """
    客户端管理页面
    展示所有已注册客户端的列表
    """
    try:
        clients_list = client_manager.get_all_clients()
    except Exception as e:
        logger.error(f"获取客户端列表失败: {e}")
        clients_list = []
        flash('获取客户端列表失败', 'error')

    return render_template('clients.html', clients=clients_list)


@app.route('/api/clients')
@login_required
def api_clients():
    """
    客户端列表API
    返回所有客户端信息及其备份统计数据的JSON
    """
    try:
        clients_list = client_manager.get_all_clients()

        # 为每个客户端附加备份统计
        result = []
        for client in clients_list:
            client_data = dict(client)

            # 序列化datetime对象
            for key in ['last_seen', 'created_at']:
                if key in client_data and isinstance(client_data[key], datetime):
                    client_data[key] = client_data[key].isoformat()

            # 获取备份统计
            try:
                stats = file_manager.get_client_backup_stats(client['id'])
                client_data['backup_stats'] = stats
            except Exception:
                client_data['backup_stats'] = {
                    'unique_files': 0,
                    'total_versions': 0,
                    'total_size': 0,
                    'today_files': 0,
                }

            # 获取备份进度
            server = get_server()
            if server:
                try:
                    bp = server.get_backup_progress(client_data.get('client_id'))
                    if bp:
                        client_data['backup_progress'] = bp
                except Exception:
                    pass

            result.append(client_data)

        return jsonify({'success': True, 'data': result})

    except Exception as e:
        logger.error(f"API获取客户端列表失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/client/<int:client_db_id>/remark', methods=['POST'])
@login_required
def api_update_client_remark(client_db_id):
    """
    更新客户端备注信息
    通过直接SQL更新clients表的remark字段

    Args:
        client_db_id: 客户端数据库ID
    """
    try:
        data = request.get_json() or {}
        remark = data.get('remark', '')

        # 使用参数化查询更新备注
        query = "UPDATE clients SET remark = %s WHERE id = %s"
        db_manager.execute_update(query, (remark, client_db_id))

        logger.info(f"客户端 {client_db_id} 备注已更新: {remark}")
        return jsonify({'success': True, 'message': '备注已更新'})

    except Exception as e:
        logger.error(f"更新客户端备注失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/client/<int:client_db_id>/backup-control', methods=['POST'])
@login_required
def api_backup_control(client_db_id):
    """
    切换客户端备份开关
    向已连接的客户端发送启动/停止备份命令

    Args:
        client_db_id: 客户端数据库ID
    """
    try:
        data = request.get_json() or {}
        action = data.get('action', 'toggle')  # 'start', 'stop', 'toggle'

        # 查找客户端信息
        query = "SELECT client_id, computer_name, status FROM clients WHERE id = %s"
        result = db_manager.execute_query(query, (client_db_id,))
        if not result:
            return jsonify({'success': False, 'message': '客户端不存在'}), 404

        client_info = result[0]
        client_uuid = client_info['client_id']

        # 通过BackupServer向客户端发送命令
        server = get_server()
        if server is None:
            return jsonify({'success': False, 'message': '备份服务器未运行'}), 503

        # 转换action为enable布尔值
        if action == 'start':
            enable = True
        elif action == 'stop':
            enable = False
        else:
            # toggle: 查询当前状态取反
            enable = True

        success = server.send_backup_control(client_uuid, enable)

        if success:
            log_manager.add_log(
                'info',
                f"向客户端 {client_info['computer_name']} 发送备份控制命令: {action}",
                client_db_id
            )
            return jsonify({'success': True, 'message': f'备份控制命令已发送: {action}'})
        else:
            return jsonify({'success': False, 'message': '发送命令失败'}), 500

    except Exception as e:
        logger.error(f"备份控制失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/client/<int:client_db_id>/config', methods=['GET'])
@login_required
def api_get_client_config(client_db_id):
    """
    获取客户端备份配置

    Args:
        client_db_id: 客户端数据库ID
    """
    try:
        backup_config = config_manager.get_client_config(client_db_id)

        if backup_config:
            # 序列化datetime
            for key in ['created_at', 'updated_at']:
                if key in backup_config and isinstance(backup_config[key], datetime):
                    backup_config[key] = backup_config[key].isoformat()

            return jsonify({'success': True, 'data': backup_config})
        else:
            # 返回默认配置
            default_cfg = config.get('backup_defaults', {})
            return jsonify({
                'success': True,
                'data': {
                    'backup_paths': default_cfg.get('monitor_paths', []),
                    'exclude_patterns': default_cfg.get('exclude_patterns', []),
                    'enable_compression': True,
                    'compression_algorithm': 'zlib',
                    'incremental_only': False,
                    'max_versions': 10,
                    'bandwidth_limit_kbps': 0,
                    'max_capacity_mb': 0,
                    'is_default': True,
                }
            })

    except Exception as e:
        logger.error(f"获取客户端配置失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/client/<int:client_db_id>/config', methods=['POST'])
@login_required
@admin_required
def api_save_client_config(client_db_id):
    """
    保存客户端备份配置
    支持配置备份路径、排除模式、压缩、增量备份、版本数限制、带宽限制、容量限制

    Args:
        client_db_id: 客户端数据库ID
    """
    try:
        data = request.get_json() or {}

        # 提取配置参数
        backup_paths = data.get('backup_paths', [])
        exclude_patterns = data.get('exclude_patterns', [])
        enable_compression = data.get('enable_compression', True)
        compression_algorithm = data.get('compression_algorithm', 'zlib')
        incremental_only = data.get('incremental_only', False)
        max_versions = data.get('max_versions', 10)
        bandwidth_limit_kbps = data.get('bandwidth_limit_kbps', 0)
        max_capacity_mb = data.get('max_capacity_mb', 0)

        # 排除模式处理
        if isinstance(exclude_patterns, list):
            exclude_patterns_list = exclude_patterns
        elif isinstance(exclude_patterns, str):
            exclude_patterns_list = [p.strip() for p in exclude_patterns.split('\n') if p.strip()]
        else:
            exclude_patterns_list = []

        # 保存到数据库（使用BackupConfigManager）
        config_manager.set_client_config(
            client_id=client_db_id,
            backup_paths=backup_paths if isinstance(backup_paths, list) else [],
            exclude_patterns=exclude_patterns_list,
            enable_compression=enable_compression,
            compression_algorithm=compression_algorithm,
            incremental_only=incremental_only,
            max_versions=max_versions,
            bandwidth_limit_kbps=bandwidth_limit_kbps,
            max_capacity_mb=max_capacity_mb,
        )

        # 尝试将新配置推送给在线客户端
        _push_config_to_client(client_db_id, data)

        log_manager.add_log('info', f"客户端 {client_db_id} 配置已更新", client_db_id)
        return jsonify({'success': True, 'message': '配置已保存'})

    except Exception as e:
        logger.error(f"保存客户端配置失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


def _push_config_to_client(client_db_id, config_data):
    """
    将配置推送给在线客户端
    使用BackupServer.push_config_to_client()确保配置格式正确

    Args:
        client_db_id: 客户端数据库ID
        config_data: 配置数据字典
    """
    try:
        server = get_server()
        if server is None:
            return

        # 查找客户端UUID
        query = "SELECT client_id FROM clients WHERE id = %s"
        result = db_manager.execute_query(query, (client_db_id,))
        if not result:
            return

        client_uuid = result[0]['client_id']

        # 构建客户端能识别的配置格式（backup_paths在顶层）
        push_data = {
            'backup_paths': config_data.get('backup_paths', []),
            'backup_path': config_data.get('backup_paths', [''])[0] if config_data.get('backup_paths') else '',
            'exclude_patterns': config_data.get('exclude_patterns', []),
            'enable_compression': config_data.get('enable_compression', True),
            'incremental_only': config_data.get('incremental_only', False),
            'max_file_size_mb': config_data.get('max_capacity_mb', 100),
            'bandwidth_limit_kbps': config_data.get('bandwidth_limit_kbps', 0),
        }

        success = server.push_config_to_client(client_uuid, push_data)
        if success:
            logger.info(f"配置已推送给客户端 {client_uuid}")
        else:
            logger.warning(f"推送配置失败: 客户端 {client_uuid} 不在线")

    except Exception as e:
        logger.warning(f"推送配置给客户端失败: {e}")


@app.route('/api/client/<int:client_db_id>', methods=['DELETE'])
@login_required
@admin_required
def api_delete_client(client_db_id):
    """
    删除客户端
    1. 向在线客户端发送卸载命令
    2. 从数据库删除客户端及相关数据

    Args:
        client_db_id: 客户端数据库ID
    """
    try:
        # 查找客户端信息
        query = "SELECT client_id, computer_name FROM clients WHERE id = %s"
        result = db_manager.execute_query(query, (client_db_id,))
        if not result:
            return jsonify({'success': False, 'message': '客户端不存在'}), 404

        client_info = result[0]
        client_uuid = client_info['client_id']
        computer_name = client_info['computer_name']

        # 尝试向在线客户端发送卸载命令
        server = get_server()
        if server:
            connection = server.get_connection(client_uuid)
            if connection:
                try:
                    from shared.protocol import ProtocolHandler
                    uninstall_msg = ProtocolHandler.create_uninstall_message(client_uuid)
                    connection.send_message(uninstall_msg)
                except Exception as e:
                    logger.warning(f"发送卸载命令失败: {e}")

        # 从数据库删除相关数据（按依赖顺序）
        try:
            # 删除文件版本记录
            db_manager.execute_update(
                "DELETE FROM file_versions WHERE client_id = %s", (client_db_id,)
            )
        except Exception as e:
            logger.warning(f"删除文件版本记录失败: {e}")

        try:
            # 删除备份配置
            db_manager.execute_update(
                "DELETE FROM backup_configs WHERE client_id = %s", (client_db_id,)
            )
        except Exception as e:
            logger.warning(f"删除备份配置失败: {e}")

        try:
            # 删除系统日志
            db_manager.execute_update(
                "DELETE FROM system_logs WHERE client_id = %s", (client_db_id,)
            )
        except Exception as e:
            logger.warning(f"删除系统日志失败: {e}")

        # 删除客户端记录
        db_manager.execute_update(
            "DELETE FROM clients WHERE id = %s", (client_db_id,)
        )

        log_manager.add_log(
            'warning',
            f"客户端已删除: {computer_name} ({client_uuid})"
        )
        logger.info(f"客户端已删除: {computer_name} (DB ID: {client_db_id})")

        return jsonify({'success': True, 'message': '客户端已删除'})

    except Exception as e:
        logger.error(f"删除客户端失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/client/<int:client_db_id>/command', methods=['POST'])
@login_required
@admin_required
def api_send_command(client_db_id):
    """
    向客户端发送远程命令
    生成唯一command_id，通过BackupServer发送命令到已连接客户端

    Args:
        client_db_id: 客户端数据库ID

    Returns:
        JSON包含command_id，用于后续查询命令结果
    """
    try:
        data = request.get_json() or {}
        command = data.get('command', '')
        params = data.get('params', {})

        if not command:
            return jsonify({'success': False, 'message': '命令不能为空'}), 400

        # 查找客户端信息
        query = "SELECT client_id, computer_name, status FROM clients WHERE id = %s"
        result = db_manager.execute_query(query, (client_db_id,))
        if not result:
            return jsonify({'success': False, 'message': '客户端不存在'}), 404

        client_info = result[0]
        client_uuid = client_info['client_id']

        # 检查服务器和客户端在线状态
        server = get_server()
        if server is None:
            return jsonify({'success': False, 'message': '备份服务器未运行'}), 503

        connection = server.get_connection(client_uuid)
        if connection is None:
            return jsonify({'success': False, 'message': '客户端不在线'}), 503

        # 通过BackupServer发送远程命令（使用正确的REMOTE_COMMAND消息类型）
        command_id = server.send_remote_command(client_uuid, command)

        if command_id:
            log_manager.add_log(
                'info',
                f"向客户端 {client_info['computer_name']} 发送命令: {command}",
                client_db_id
            )
            return jsonify({
                'success': True,
                'command_id': command_id,
                'message': '命令已发送',
            })
        else:
            return jsonify({'success': False, 'message': '发送命令失败'}), 500

    except Exception as e:
        logger.error(f"发送远程命令失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/client/<int:client_db_id>/command/<command_id>/result')
@login_required
def api_get_command_result(client_db_id, command_id):
    """
    获取远程命令执行结果
    从BackupServer的命令结果缓存中查询（服务端存储）

    Args:
        client_db_id: 客户端数据库ID
        command_id: 命令ID
    """
    try:
        server = get_server()
        if server is None:
            return jsonify({'success': False, 'message': '备份服务器未运行'}), 503

        # 从服务端结果缓存查询（不移除，允许多次查询）
        cmd_result = server.get_command_result(command_id)

        if cmd_result is None:
            return jsonify({
                'success': True,
                'data': {
                    'command_id': command_id,
                    'status': 'pending',
                    'result': None,
                }
            })

        return jsonify({
            'success': True,
            'data': {
                'command_id': command_id,
                'status': 'completed',
                'return_code': cmd_result.get('return_code', -1),
                'stdout': cmd_result.get('stdout', ''),
                'stderr': cmd_result.get('stderr', ''),
                'received_at': cmd_result.get('received_at'),
            }
        })

    except Exception as e:
        logger.error(f"获取命令结果失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/client/<int:client_db_id>/file-tree')
@login_required
def api_client_file_tree(client_db_id):
    """
    请求客户端文件目录树
    向在线客户端发送文件列表请求，获取指定目录下的文件和子目录

    Args:
        client_db_id: 客户端数据库ID
        path: 请求的目录路径（URL参数）
    """
    try:
        path = request.args.get('path', '')

        # 查找客户端信息
        query = "SELECT client_id, computer_name FROM clients WHERE id = %s"
        result = db_manager.execute_query(query, (client_db_id,))
        if not result:
            return jsonify({'success': False, 'message': '客户端不存在'}), 404

        client_info = result[0]
        client_uuid = client_info['client_id']

        # 检查服务器和客户端在线状态
        server = get_server()
        if server is None:
            return jsonify({'success': False, 'message': '备份服务器未运行'}), 503

        connection = server.get_connection(client_uuid)
        if connection is None:
            return jsonify({'success': False, 'message': '客户端不在线'}), 503

        # 先清除可能残留的旧结果
        server.pop_file_list_result(client_uuid, path)

        # 通过BackupServer发送文件列表请求（使用正确的FILE_LIST_REQUEST消息类型）
        success = server.send_file_list_request(client_uuid, path)
        if not success:
            return jsonify({'success': False, 'message': '发送文件列表请求失败'}), 500

        # 轮询服务端的file_list_results缓存等待客户端响应（最多10秒）
        entries = []
        for _ in range(20):
            time.sleep(0.5)
            result_data = server.pop_file_list_result(client_uuid, path)
            if result_data is not None:
                entries = result_data.get('entries', [])
                break

        return jsonify({
            'success': True,
            'data': {
                'path': path,
                'entries': entries,
            }
        })

    except Exception as e:
        logger.error(f"获取客户端文件树失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/client/<int:client_db_id>/stats')
@login_required
def api_client_stats(client_db_id):
    """
    获取客户端备份统计信息

    Args:
        client_db_id: 客户端数据库ID
    """
    try:
        stats = file_manager.get_client_backup_stats(client_db_id)
        return jsonify({'success': True, 'data': stats})

    except Exception as e:
        logger.error(f"获取客户端统计失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


# ============================================================================
# 文件管理路由
# ============================================================================

@app.route('/files')
@login_required
def files():
    """
    文件管理页面
    展示备份文件浏览界面
    """
    try:
        clients_list = client_manager.get_all_clients()
    except Exception as e:
        logger.error(f"获取客户端列表失败: {e}")
        clients_list = []

    return render_template('files.html', files=[], clients=clients_list)


@app.route('/api/files/<int:client_db_id>')
@login_required
def api_client_files(client_db_id):
    """
    获取客户端已备份文件列表
    返回每个文件路径的最新版本信息（去重后的唯一路径列表）

    Args:
        client_db_id: 客户端数据库ID
    """
    try:
        # 查询每个文件路径的最新版本
        query = """
            SELECT fv.id, fv.file_path, fv.relative_path, fv.version_number,
                   fv.file_size, fv.file_hash, fv.backup_date, fv.change_type,
                   c.computer_name as client_name
            FROM file_versions fv
            INNER JOIN (
                SELECT file_path, MAX(version_number) as max_version
                FROM file_versions
                WHERE client_id = %s
                GROUP BY file_path
            ) latest ON fv.file_path = latest.file_path
                     AND fv.version_number = latest.max_version
            LEFT JOIN clients c ON fv.client_id = c.id
            WHERE fv.client_id = %s
            ORDER BY fv.backup_date DESC
        """
        files_list = db_manager.execute_query(query, (client_db_id, client_db_id))

        # 序列化datetime对象
        result = []
        for f in files_list:
            file_data = dict(f)
            if 'backup_date' in file_data and isinstance(file_data['backup_date'], datetime):
                file_data['backup_date'] = file_data['backup_date'].isoformat()
            result.append(file_data)

        return jsonify({'success': True, 'data': result})

    except Exception as e:
        logger.error(f"获取客户端文件列表失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/files/<int:client_db_id>/versions')
@login_required
def api_file_versions(client_db_id):
    """
    获取指定文件的所有版本列表

    Args:
        client_db_id: 客户端数据库ID
        path: 文件路径（URL参数）
    """
    try:
        file_path = request.args.get('path', '')
        if not file_path:
            return jsonify({'success': False, 'message': '缺少文件路径参数'}), 400

        versions = file_manager.get_file_versions(client_db_id, file_path)

        # 序列化datetime对象
        result = []
        for v in versions:
            version_data = dict(v)
            if 'backup_date' in version_data and isinstance(version_data['backup_date'], datetime):
                version_data['backup_date'] = version_data['backup_date'].isoformat()
            result.append(version_data)

        return jsonify({'success': True, 'data': result})

    except Exception as e:
        logger.error(f"获取文件版本列表失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/files/preview')
@login_required
def file_preview():
    """
    文件预览页面
    支持多种文件类型的纯前端预览
    """
    return render_template('file_preview.html')


@app.route('/api/files/<int:client_db_id>/download')
@login_required
def api_download_file(client_db_id):
    """
    统一下载API端点
    支持：
    1. 单个文件下载：通过path参数
    2. 批量文件下载：通过paths参数（JSON数组）
    3. 文件夹下载：通过folder参数
    4. 内联预览：通过inline参数设置为true，浏览器会直接显示文件内容

    Args:
        client_db_id: 客户端数据库ID
    """
    try:
        # 获取参数
        file_path = request.args.get('path', '')
        folder_path = request.args.get('folder', '')
        version_number = request.args.get('version', type=int)
        inline = request.args.get('inline', 'false').lower() == 'true'
        
        # 尝试获取批量文件路径（支持JSON数组）
        paths_param = request.args.get('paths', '')
        file_paths = []
        if paths_param:
            try:
                import json
                file_paths = json.loads(paths_param)
                if not isinstance(file_paths, list):
                    file_paths = []
            except Exception:
                pass
        
        # 处理单个文件下载
        if file_path and not folder_path and not file_paths:
            # 重建文件
            temp_path = reconstruct_file_version(client_db_id, file_path, version_number)

            if temp_path is None:
                return jsonify({'success': False, 'message': '文件重建失败'}), 500

            # 提取文件名
            filename = os.path.basename(file_path)
            if version_number:
                name, ext = os.path.splitext(filename)
                filename = f"{name}_v{version_number}{ext}"

            # 根据文件扩展名确定mimetype
            ext = os.path.splitext(filename)[1].lower()
            mimetype_map = {
                '.pdf': 'application/pdf',
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.png': 'image/png',
                '.gif': 'image/gif',
                '.bmp': 'image/bmp',
                '.svg': 'image/svg+xml',
                '.webp': 'image/webp',
                '.mp4': 'video/mp4',
                '.webm': 'video/webm',
                '.ogg': 'video/ogg',
                '.mov': 'video/quicktime',
                '.avi': 'video/x-msvideo',
                '.mkv': 'video/x-matroska',
                '.flv': 'video/x-flv',
                '.mp3': 'audio/mpeg',
                '.wav': 'audio/wav',
                '.flac': 'audio/flac',
                '.aac': 'audio/aac',
                '.wma': 'audio/x-ms-wma',
                '.m4a': 'audio/mp4',
                '.txt': 'text/plain',
                '.html': 'text/html',
                '.htm': 'text/html',
                '.css': 'text/css',
                '.js': 'application/javascript',
                '.json': 'application/json',
                '.xml': 'application/xml',
                '.md': 'text/markdown',
                '.csv': 'text/csv',
            }
            mimetype = mimetype_map.get(ext, 'application/octet-stream')

            # 返回文件响应（内联预览或下载）
            return send_file(
                temp_path,
                as_attachment=not inline,
                download_name=filename,
                mimetype=mimetype,
            )
        
        # 处理批量下载或文件夹下载
        import zipfile
        import tempfile
        
        # 创建临时zip文件
        temp_fd, temp_zip_path = tempfile.mkstemp(suffix='.zip', prefix='backup_download_')
        
        try:
            with zipfile.ZipFile(os.fdopen(temp_fd, 'wb'), 'w', zipfile.ZIP_DEFLATED) as zipf:
                # 处理批量文件下载
                for path in file_paths:
                    if path:
                        temp_file_path = reconstruct_file_version(client_db_id, path)
                        if temp_file_path:
                            filename = os.path.basename(path)
                            zipf.write(temp_file_path, filename)
                            os.unlink(temp_file_path)
                
                # 处理文件夹下载
                if folder_path:
                    # 获取文件夹内的所有文件
                    folder_files = _get_files_in_folder(client_db_id, folder_path)
                    for file_info in folder_files:
                        temp_file_path = reconstruct_file_version(client_db_id, file_info['file_path'])
                        if temp_file_path:
                            # 保持相对路径结构
                            relative_path = os.path.relpath(file_info['file_path'], folder_path)
                            zipf.write(temp_file_path, relative_path)
                            os.unlink(temp_file_path)
        except Exception as e:
            os.close(temp_fd)
            os.unlink(temp_zip_path)
            raise e
        
        # 生成下载文件名
        if folder_path:
            folder_name = os.path.basename(folder_path)
            download_filename = f"{folder_name}.zip"
        elif file_paths:
            if len(file_paths) == 1:
                filename = os.path.basename(file_paths[0])
                download_filename = f"{filename}.zip"
            else:
                download_filename = f"files_{len(file_paths)}.zip"
        else:
            download_filename = "backup_files.zip"
        
        # 返回zip文件下载响应
        return send_file(
            temp_zip_path,
            as_attachment=True,
            download_name=download_filename,
            mimetype='application/zip',
        )

    except Exception as e:
        logger.error(f"文件下载失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

def _get_files_in_folder(client_db_id, folder_path):
    """
    获取文件夹内的所有文件
    
    Args:
        client_db_id: 客户端数据库ID
        folder_path: 文件夹路径
    
    Returns:
        文件信息列表
    """
    # 查询该客户端的所有文件
    query = """
        SELECT file_path
        FROM file_versions
        WHERE client_id = %s
        GROUP BY file_path
    """
    results = db_manager.execute_query(query, (client_db_id,))
    
    files = []
    for result in results:
        file_path = result['file_path']
        # 统一路径分隔符
        normalized_file_path = file_path.replace('\\', '/')
        normalized_folder_path = folder_path.replace('\\', '/')
        
        # 检查文件是否在文件夹内
        if normalized_file_path.startswith(normalized_folder_path + '/'):
            files.append({'file_path': file_path})
    
    return files


def reconstruct_file_version(client_db_id, file_path, version_number=None):
    """
    从存储块中重建指定版本的文件

    工作流程:
    1. 从数据库查询文件版本记录
    2. 获取该版本的存储路径（元数据文件）
    3. 加载元数据，获取chunk_paths列表
    4. 依次从存储管理器加载每个块
    5. 如果块被压缩，自动解压
    6. 将所有块按顺序写入临时文件
    7. 返回临时文件路径

    Args:
        client_db_id: 客户端数据库ID
        file_path: 原始文件路径
        version_number: 版本号（None则取最新版本）

    Returns:
        重建后的临时文件路径，失败返回None
    """
    try:
        # 获取文件版本记录
        if version_number is not None:
            query = """
                SELECT id, file_path, version_number, file_size, file_hash,
                       storage_path, chunk_count, is_full_backup, reference_version
                FROM file_versions
                WHERE client_id = %s AND file_path = %s AND version_number = %s
                LIMIT 1
            """
            result = db_manager.execute_query(query, (client_db_id, file_path, version_number))
        else:
            # 获取最新版本
            query = """
                SELECT id, file_path, version_number, file_size, file_hash,
                       storage_path, chunk_count, is_full_backup, reference_version
                FROM file_versions
                WHERE client_id = %s AND file_path = %s
                ORDER BY version_number DESC
                LIMIT 1
            """
            result = db_manager.execute_query(query, (client_db_id, file_path))

        if not result:
            logger.error(f"文件版本记录不存在: client={client_db_id}, path={file_path}, version={version_number}")
            return None

        version_info = result[0]
        metadata_storage_path = version_info.get('storage_path', '')

        # 方式一：通过存储管理器的元数据文件重建
        if metadata_storage_path and metadata_storage_path.endswith('.json'):
            try:
                metadata = storage_manager.load_file_version_metadata(metadata_storage_path)
                chunk_paths = metadata.get('chunk_paths', [])

                if chunk_paths:
                    # 获取 block_details（包含每个块的 is_compressed 标记）
                    block_details = metadata.get('block_details', [])

                    # 创建临时文件
                    temp_fd, temp_path = tempfile.mkstemp(
                        suffix=os.path.splitext(file_path)[1] or '.dat',
                        prefix='backup_restore_'
                    )

                    with os.fdopen(temp_fd, 'wb') as temp_file:
                        for chunk_index, chunk_storage_path in enumerate(chunk_paths):
                            if not chunk_storage_path:
                                logger.warning(f"块路径为空: index={chunk_index}")
                                continue
                            # 优先使用元数据中的 is_compressed 标记（可靠），
                            # 而非依赖 block_index 内存查找（可能丢失）
                            if chunk_index < len(block_details):
                                is_compressed = block_details[chunk_index].get('is_compressed', False)
                                chunk_data = storage_manager.load_block(chunk_storage_path, is_compressed)
                            else:
                                chunk_data = storage_manager.load_file_chunk(chunk_storage_path)
                            temp_file.write(chunk_data)

                    logger.info(f"文件重建成功: {file_path} v{version_info['version_number']} -> {temp_path}")
                    return temp_path

            except FileNotFoundError as e:
                logger.warning(f"元数据文件不存在，尝试备用方式: {e}")
            except Exception as e:
                logger.warning(f"通过元数据重建失败，尝试备用方式: {e}")

        # 方式二：通过数据库中的version_blocks + data_blocks表重建（如果存在）
        try:
            block_query = """
                SELECT vb.block_hash, vb.block_index,
                       db.storage_path, db.is_compressed
                FROM version_blocks vb
                JOIN data_blocks db ON vb.block_hash = db.block_hash
                WHERE vb.version_id = %s
                ORDER BY vb.block_index ASC
            """
            blocks = db_manager.execute_query(block_query, (version_info['id'],))

            if blocks:
                temp_fd, temp_path = tempfile.mkstemp(
                    suffix=os.path.splitext(file_path)[1] or '.dat',
                    prefix='backup_restore_'
                )

                with os.fdopen(temp_fd, 'wb') as temp_file:
                    for block in blocks:
                        block_storage_path = block.get('storage_path', '')
                        if not block_storage_path:
                            continue
                        is_compressed = bool(block.get('is_compressed', False))
                        chunk_data = storage_manager.load_block(block_storage_path, is_compressed)
                        temp_file.write(chunk_data)

                logger.info(f"文件重建成功（通过version_blocks）: {file_path} v{version_info['version_number']}")
                return temp_path

        except Exception as e:
            logger.debug(f"通过version_blocks重建失败（表可能不存在）: {e}")

        # 方式三：直接通过StorageManager的reconstruct_file方法
        try:
            # 查找客户端UUID
            client_query = "SELECT client_id FROM clients WHERE id = %s"
            client_result = db_manager.execute_query(client_query, (client_db_id,))
            if client_result:
                client_uuid = client_result[0]['client_id']
                temp_fd, temp_path = tempfile.mkstemp(
                    suffix=os.path.splitext(file_path)[1] or '.dat',
                    prefix='backup_restore_'
                )
                os.close(temp_fd)

                success = storage_manager.reconstruct_file(
                    client_uuid, file_path,
                    version_info['version_number'], temp_path
                )
                if success:
                    logger.info(f"文件重建成功（通过reconstruct_file）: {file_path}")
                    return temp_path
                else:
                    # 清理失败的临时文件
                    try:
                        os.unlink(temp_path)
                    except OSError:
                        pass
        except Exception as e:
            logger.warning(f"通过StorageManager重建失败: {e}")

        logger.error(f"所有文件重建方式均失败: {file_path}")
        return None

    except Exception as e:
        logger.error(f"重建文件版本异常: {e}")
        return None


# ============================================================================
# 客户端生成器路由
# ============================================================================

@app.route('/client-generator')
@login_required
@admin_required
def client_generator():
    """
    客户端生成器页面
    提供界面让管理员配置并生成客户端安装包
    """
    # 获取服务器配置用于预填
    server_config = config.get('server', {})
    default_host = server_config.get('host', '0.0.0.0')
    default_port = server_config.get('port', 8888)

    return render_template('client_generator.html',
                           default_host=default_host,
                           default_port=default_port)


@app.route('/api/generate-client', methods=['POST'])
@login_required
@admin_required
def api_generate_client():
    """
    生成客户端安装包
    打包客户端Python文件，嵌入服务器连接配置，生成zip下载包

    请求参数:
        server_host: 服务器地址
        server_port: 服务器端口
    """
    try:
        # 兼容表单和JSON请求
        if request.content_type and 'application/json' in request.content_type:
            data = request.get_json() or {}
        else:
            data = request.form.to_dict()
        
        server_host = data.get('server_host', '').strip()
        server_port = data.get('server_port', 8888)
        
        # 确保端口是整数
        try:
            server_port = int(server_port)
        except (ValueError, TypeError):
            server_port = 8888

        # 客户端源文件目录
        client_src_dir = Path(__file__).parent.parent / 'client'
        shared_src_dir = Path(__file__).parent.parent / 'shared'

        if not client_src_dir.exists():
            return jsonify({'success': False, 'message': '客户端源文件目录不存在'}), 500

        # 创建临时目录构建安装包
        temp_dir = tempfile.mkdtemp(prefix='client_package_')

        try:
            package_dir = os.path.join(temp_dir, 'file_backup_client')
            os.makedirs(package_dir, exist_ok=True)

            # 需要打包的客户端文件
            client_files = [
                'client_main.py',
                'protocol.py',
                'file_monitor.py',
                'registry_manager.py',
                'windows_service.py',
                # 性能优化模块
                'connection_pool.py',
                'async_client.py',
                'prefetcher.py',
            ]

            # 复制客户端文件
            for filename in client_files:
                src_file = client_src_dir / filename
                if src_file.exists():
                    shutil.copy2(str(src_file), os.path.join(package_dir, filename))

            # 复制shared目录（如果存在）
            if shared_src_dir.exists():
                shared_dest = os.path.join(package_dir, 'shared')
                os.makedirs(shared_dest, exist_ok=True)
                for f in shared_src_dir.iterdir():
                    if f.is_file() and f.suffix == '.py':
                        shutil.copy2(str(f), os.path.join(shared_dest, f.name))

            # 复制requirements.txt（如果存在）
            req_file = client_src_dir / 'requirements.txt'
            if req_file.exists():
                shutil.copy2(str(req_file), os.path.join(package_dir, 'requirements.txt'))

            # 生成客户端配置文件（嵌入服务器地址和端口）
            client_config_content = f"""[server]
host = {server_host}
port = {server_port}
timeout = 30
retry_interval = 60
heartbeat_interval = 300

[client]
client_id =
computer_name =
buffer_size = 65536
max_file_size_mb = 100
batch_size = 10
batch_timeout = 5.0

[backup]
monitor_paths =
exclude_patterns = *.tmp,*.temp,*.log,*.lock,*.swp,*.swo,~*,.DS_Store,Thumbs.db
include_patterns = *
scan_interval = 3600

[performance]
network_limit_kbps = 0
cpu_limit_percent = 50
memory_limit_mb = 100
compression_threshold = 1024
"""
            config_path = os.path.join(package_dir, 'config.ini')
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(client_config_content)

            # 生成安装启动脚本
            # Windows启动脚本
            bat_content = f"""@echo off
chcp 65001 >nul
echo ============================================
echo   文件备份客户端安装程序
echo   服务器: {server_host}:{server_port}
echo ============================================
echo.

REM 检查Python环境
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到Python环境，请先安装Python 3.8+
    echo 提示: 可以使用 build_exe.bat 打包为独立EXE后无需Python
    pause
    exit /b 1
)

REM 安装依赖
echo 正在安装依赖...
pip install watchdog psutil -q

REM 安装可选依赖（性能优化）
echo 正在安装可选依赖（LZ4压缩、异步I/O）...
pip install lz4 aiofiles -q 2>nul

echo.
echo [提示] 如需打包为EXE，请运行 build_exe.bat
echo.

REM 启动客户端
echo 正在启动客户端...
python client_main.py --service
"""
            with open(os.path.join(package_dir, 'install.bat'), 'w', encoding='utf-8') as f:
                f.write(bat_content)

            # EXE打包脚本(Windows)
            build_bat_content = f"""@echo off
chcp 65001 >nul
echo ============================================
echo   文件备份客户端 - EXE打包工具
echo   将客户端打包为无需Python的独立EXE
echo ============================================
echo.

REM 检查Python环境
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 打包需要Python环境，请先安装Python 3.8+
    pause
    exit /b 1
)

REM 安装PyInstaller和依赖
echo 正在安装打包工具...
pip install pyinstaller watchdog psutil pywin32 -q

REM 安装可选依赖（用于性能优化）
echo 正在安装可选依赖...
pip install lz4 aiofiles -q

REM 打包为单文件EXE(隐藏窗口)
echo 正在打包为EXE...
python build_exe_fixed.py --onefile --hidden

echo.
echo 打包完成！请查看 dist 目录下的 FileBackupClient.exe
echo 将 FileBackupClient.exe 和 config.ini 复制到目标机器即可运行
echo.
echo 提示：如果打包失败，请尝试运行 build_exe_simple.py
pause
"""
            with open(os.path.join(package_dir, 'build_exe.bat'), 'w', encoding='utf-8') as f:
                f.write(build_bat_content)

            # 复制打包脚本
            build_scripts = [
                'build_exe.py',
                'build_exe_fixed.py',
                'build_exe_simple.py',
                'BUILD_README.md',
            ]
            for script_name in build_scripts:
                script_path = Path(__file__).parent.parent / 'client' / script_name
                if script_path.exists():
                    shutil.copy2(str(script_path), os.path.join(package_dir, script_name))

            # Linux启动脚本
            sh_content = f"""#!/bin/bash
echo "============================================"
echo "  文件备份客户端安装程序"
echo "  服务器: {server_host}:{server_port}"
echo "============================================"
echo ""

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "[错误] 未检测到Python3环境，请先安装Python 3.8+"
    exit 1
fi

# 安装依赖
echo "正在安装依赖..."
pip3 install watchdog psutil -q

# 启动客户端
echo "正在启动客户端..."
nohup python3 client_main.py --service > /dev/null 2>&1 &
echo "客户端已在后台启动 (PID: $!)"
"""
            sh_path = os.path.join(package_dir, 'install.sh')
            with open(sh_path, 'w', encoding='utf-8') as f:
                f.write(sh_content)
            os.chmod(sh_path, 0o755)

            # 创建zip包
            zip_path = os.path.join(temp_dir, 'file_backup_client.zip')
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for root, dirs, zip_files in os.walk(package_dir):
                    for file in zip_files:
                        file_full_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_full_path, temp_dir)
                        zf.write(file_full_path, arcname)

            # 记录日志
            log_manager.add_log(
                'info',
                f"生成客户端安装包: 服务器={server_host}:{server_port}"
            )
            logger.info(f"客户端安装包已生成: {zip_path}")

            # 返回zip文件下载
            return send_file(
                zip_path,
                as_attachment=True,
                download_name=f'file_backup_client_{server_host}_{server_port}.zip',
                mimetype='application/zip',
            )

        except Exception as e:
            # 清理临时目录
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass
            raise e

    except Exception as e:
        logger.error(f"生成客户端安装包失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


# EXE模板存放目录
_exe_template_dir = os.path.join(os.path.dirname(__file__), 'exe_templates')
os.makedirs(_exe_template_dir, exist_ok=True)


@app.route('/api/upload-client-exe', methods=['POST'])
@login_required
@admin_required
def api_upload_client_exe():
    """
    上传预编译的客户端EXE模板
    管理员在Windows机器上编译好EXE后上传到服务器
    """
    try:
        if 'exe_file' not in request.files:
            return jsonify({'success': False, 'message': '请选择EXE文件'}), 400

        exe_file = request.files['exe_file']
        if not exe_file.filename:
            return jsonify({'success': False, 'message': '请选择EXE文件'}), 400

        if not exe_file.filename.lower().endswith('.exe'):
            return jsonify({'success': False, 'message': '请上传.exe文件'}), 400

        # 保存EXE模板
        exe_path = os.path.join(_exe_template_dir, 'FileBackupClient.exe')
        exe_file.save(exe_path)

        file_size = os.path.getsize(exe_path)
        log_manager.add_log('info', f"客户端EXE模板已上传 ({file_size} bytes)")
        logger.info(f"客户端EXE模板已上传: {exe_path} ({file_size} bytes)")

        return jsonify({
            'success': True,
            'message': f'EXE模板已上传 ({file_size // 1024} KB)',
            'file_size': file_size,
        })

    except Exception as e:
        logger.error(f"上传EXE模板失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/check-client-exe')
@login_required
def api_check_client_exe():
    """检查是否存在预编译的客户端EXE模板"""
    exe_path = os.path.join(_exe_template_dir, 'FileBackupClient.exe')
    exists = os.path.isfile(exe_path)
    size = os.path.getsize(exe_path) if exists else 0
    return jsonify({
        'success': True,
        'exists': exists,
        'file_size': size,
    })


@app.route('/api/generate-client-exe', methods=['POST'])
@login_required
@admin_required
def api_generate_client_exe():
    """
    生成可直接运行的客户端EXE安装包
    将预编译的EXE + 嵌入配置的config.ini打包为zip下载
    """
    try:
        # 兼容表单和JSON请求
        if request.content_type and 'application/json' in request.content_type:
            data = request.get_json() or {}
        else:
            data = request.form.to_dict()
        
        server_host = data.get('server_host', '').strip()
        server_port = data.get('server_port', 8888)
        
        # 确保端口是整数
        try:
            server_port = int(server_port)
        except (ValueError, TypeError):
            server_port = 8888

        exe_path = os.path.join(_exe_template_dir, 'FileBackupClient.exe')
        if not os.path.isfile(exe_path):
            return jsonify({'success': False, 'message': '客户端EXE模板不存在，请先上传预编译的EXE'}), 400

        # 创建临时目录
        temp_dir = tempfile.mkdtemp(prefix='client_exe_')

        try:
            # 生成config.ini
            client_config_content = f"""[server]
host = {server_host}
port = {server_port}
timeout = 30
retry_interval = 60
heartbeat_interval = 300

[client]
client_id =
computer_name =
buffer_size = 65536
max_file_size_mb = 100
batch_size = 10
batch_timeout = 5.0

[backup]
monitor_paths =
exclude_patterns = *.tmp,*.temp,*.log,*.lock,*.swp,*.swo,~*,.DS_Store,Thumbs.db
include_patterns = *
scan_interval = 3600

[performance]
network_limit_kbps = 0
cpu_limit_percent = 50
memory_limit_mb = 100
compression_threshold = 1024
"""
            config_path = os.path.join(temp_dir, 'config.ini')
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(client_config_content)

            # 复制EXE到临时目录
            dest_exe = os.path.join(temp_dir, 'FileBackupClient.exe')
            shutil.copy2(exe_path, dest_exe)

            # 生成安装说明
            readme_content = f"""文件备份客户端 - 快速部署
================================
服务器地址: {server_host}:{server_port}

部署步骤:
1. 将 FileBackupClient.exe 和 config.ini 复制到目标机器的同一目录
2. 双击 FileBackupClient.exe 即可运行
3. 客户端会自动连接服务器并注册

注意事项:
- config.ini 必须与 EXE 在同一目录
- 首次运行会自动设置开机自启动
- 客户端在后台静默运行，无窗口
"""
            with open(os.path.join(temp_dir, '部署说明.txt'), 'w', encoding='utf-8') as f:
                f.write(readme_content)

            # 打包为zip
            zip_path = os.path.join(temp_dir, 'FileBackupClient.zip')
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.write(dest_exe, 'FileBackupClient.exe')
                zf.write(config_path, 'config.ini')
                zf.write(os.path.join(temp_dir, '部署说明.txt'), '部署说明.txt')

            log_manager.add_log(
                'info',
                f"生成客户端EXE安装包: 服务器={server_host}:{server_port}"
            )

            return send_file(
                zip_path,
                as_attachment=True,
                download_name=f'FileBackupClient_{server_host}_{server_port}.zip',
                mimetype='application/zip',
            )

        except Exception as e:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass
            raise e

    except Exception as e:
        logger.error(f"生成客户端EXE安装包失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


# ============================================================================
# 客户端日志查看路由
# ============================================================================

@app.route('/api/client/<int:client_db_id>/logs')
@login_required
def api_client_logs(client_db_id):
    """
    获取客户端日志信息
    从客户端实时请求日志（通过远程命令），或返回服务端记录的该客户端相关日志

    Args:
        client_db_id: 客户端数据库ID
    """
    try:
        source = request.args.get('source', 'server')  # 'server' or 'client'
        limit = request.args.get('limit', 100, type=int)
        limit = min(limit, 500)

        if source == 'server':
            # 返回服务端记录的该客户端相关系统日志
            logs_list = log_manager.get_logs(limit=limit, client_id=client_db_id)
            result = []
            for log_entry in logs_list:
                log_data = dict(log_entry)
                if 'created_at' in log_data and isinstance(log_data['created_at'], datetime):
                    log_data['created_at'] = log_data['created_at'].isoformat()
                result.append(log_data)
            return jsonify({'success': True, 'data': result, 'source': 'server'})

        elif source == 'client':
            # 通过远程命令读取客户端日志文件
            query = "SELECT client_id FROM clients WHERE id = %s"
            db_result = db_manager.execute_query(query, (client_db_id,))
            if not db_result:
                return jsonify({'success': False, 'message': '客户端不存在'}), 404

            client_uuid = db_result[0]['client_id']
            server = get_server()
            if server is None:
                return jsonify({'success': False, 'message': '备份服务器未运行'}), 503

            # 发送读取日志的命令（跨平台兼容）
            log_lines = request.args.get('lines', 200, type=int)
            cmd = f'tail -n {log_lines} logs/client.log 2>/dev/null || type logs\\client.log 2>nul'
            command_id = server.send_remote_command(client_uuid, cmd)

            if not command_id:
                return jsonify({'success': False, 'message': '客户端不在线'}), 503

            # 等待结果（最多10秒）
            for _ in range(20):
                time.sleep(0.5)
                cmd_result = server.get_command_result(command_id)
                if cmd_result is not None:
                    # 清理结果
                    server.pop_command_result(command_id)
                    log_text = cmd_result.get('stdout', '')
                    stderr = cmd_result.get('stderr', '')
                    if not log_text and stderr:
                        log_text = f"[读取日志失败] {stderr}"
                    return jsonify({
                        'success': True,
                        'data': log_text,
                        'source': 'client',
                    })

            return jsonify({
                'success': False,
                'message': '获取客户端日志超时',
            }), 504

        else:
            return jsonify({'success': False, 'message': '无效的source参数'}), 400

    except Exception as e:
        logger.error(f"获取客户端日志失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


# ============================================================================
# 系统路由
# ============================================================================

@app.route('/logs')
@login_required
def logs():
    """
    系统日志页面
    支持按级别和客户端过滤
    """
    try:
        level = request.args.get('level')
        client_id = request.args.get('client_id', type=int)
        limit = request.args.get('limit', 200, type=int)

        logs_list = log_manager.get_logs(
            limit=limit,
            level=level,
            client_id=client_id,
        )
    except Exception as e:
        logger.error(f"获取系统日志失败: {e}")
        logs_list = []
        flash('获取系统日志失败', 'error')

    return render_template('logs.html', logs=logs_list)


@app.route('/api/logs')
@login_required
def api_logs():
    """
    系统日志API
    支持按日志级别、客户端ID过滤，以及限制返回数量

    URL参数:
        level: 日志级别过滤（info/warning/error）
        client_id: 客户端数据库ID过滤
        limit: 返回记录数（默认100）
    """
    try:
        level = request.args.get('level')
        client_id = request.args.get('client_id', type=int)
        limit = request.args.get('limit', 100, type=int)

        # 限制最大返回数量防止数据过大
        limit = min(limit, 1000)

        logs_list = log_manager.get_logs(
            limit=limit,
            level=level,
            client_id=client_id,
        )

        # 序列化datetime对象
        result = []
        for log_entry in logs_list:
            log_data = dict(log_entry)
            if 'created_at' in log_data and isinstance(log_data['created_at'], datetime):
                log_data['created_at'] = log_data['created_at'].isoformat()
            result.append(log_data)

        return jsonify({'success': True, 'data': result})

    except Exception as e:
        logger.error(f"API获取系统日志失败: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/settings')
@login_required
@admin_required
def settings():
    """
    系统设置页面
    展示服务器、数据库、存储、性能等配置（只读展示）
    """
    return render_template('settings.html', config=config.config)


# ============================================================================
# 客户端详情路由
# ============================================================================

@app.route('/client/<int:client_id>')
@login_required
def client_detail(client_id):
    """
    客户端详情页面
    展示单个客户端的详细信息、备份配置、备份统计和文件版本列表

    Args:
        client_id: 客户端数据库ID（注意这是数据库自增ID，非UUID）
    """
    try:
        # 通过数据库ID查找客户端
        query = """
            SELECT id, client_id, computer_name, ip_address,
                   last_seen, status, created_at
            FROM clients
            WHERE id = %s
        """
        result = db_manager.execute_query(query, (client_id,))

        if not result:
            flash('客户端不存在', 'error')
            return redirect(url_for('clients'))

        client = result[0]

        # 获取客户端备份配置
        backup_config = config_manager.get_client_config(client_id)

        # 获取备份统计
        backup_stats = file_manager.get_client_backup_stats(client_id)

        # 获取最近的文件版本列表（最近50条）
        file_versions_query = """
            SELECT id, file_path, relative_path, version_number,
                   file_size, file_hash, backup_date, change_type
            FROM file_versions
            WHERE client_id = %s
            ORDER BY backup_date DESC
            LIMIT 50
        """
        file_versions = db_manager.execute_query(file_versions_query, (client_id,))

        return render_template('client_detail.html',
                               client=client,
                               config=backup_config,
                               stats=backup_stats,
                               file_versions=file_versions)

    except Exception as e:
        logger.error(f"加载客户端详情失败: {e}")
        flash('加载客户端详情失败', 'error')
        return redirect(url_for('clients'))


# ============================================================================
# 错误处理
# ============================================================================

@app.errorhandler(404)
def page_not_found(e):
    """404页面未找到"""
    if request.path.startswith('/api/'):
        return jsonify({'success': False, 'message': '接口不存在'}), 404
    return render_template('error.html', error_code=404, error_message='页面未找到'), 404


@app.errorhandler(500)
def internal_error(e):
    """500服务器内部错误"""
    logger.error(f"服务器内部错误: {e}")
    if request.path.startswith('/api/'):
        return jsonify({'success': False, 'message': '服务器内部错误'}), 500
    return render_template('error.html', error_code=500, error_message='服务器内部错误'), 500


@app.errorhandler(403)
def forbidden(e):
    """403禁止访问"""
    if request.path.startswith('/api/'):
        return jsonify({'success': False, 'message': '禁止访问'}), 403
    return render_template('error.html', error_code=403, error_message='禁止访问'), 403


# ============================================================================
# 定时清理任务
# ============================================================================

def _cleanup_expired_command_results():
    """
    定期清理过期的命令结果缓存
    超过5分钟的命令结果将被自动删除
    """
    while True:
        try:
            time.sleep(60)  # 每分钟清理一次
            now = time.time()
            expired_keys = []

            with _command_results_lock:
                for cmd_id, cmd_data in _command_results.items():
                    if now - cmd_data.get('timestamp', 0) > 300:  # 5分钟过期
                        expired_keys.append(cmd_id)
                for key in expired_keys:
                    del _command_results[key]

            if expired_keys:
                logger.debug(f"已清理 {len(expired_keys)} 个过期命令结果")

        except Exception as e:
            logger.warning(f"清理过期命令结果异常: {e}")


# 启动清理线程
_cleanup_thread = threading.Thread(target=_cleanup_expired_command_results, daemon=True)
_cleanup_thread.start()


# ============================================================================
# 主函数
# ============================================================================

def main():
    """
    主函数
    支持命令行参数配置监听地址、端口和调试模式
    """
    import argparse

    parser = argparse.ArgumentParser(description='文件备份系统Web管理端')
    parser.add_argument('--host', default='0.0.0.0', help='监听主机 (默认: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=5000, help='监听端口 (默认: 5000)')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')

    args = parser.parse_args()

    logger.info(f"Web管理端启动: {args.host}:{args.port} (debug={args.debug})")

    # 运行Flask应用
    app.run(
        host=args.host,
        port=args.port,
        debug=args.debug,
        threaded=True,
    )


if __name__ == '__main__':
    main()
