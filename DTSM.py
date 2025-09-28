import cx_Oracle
import requests
import json
import time
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from logging.handlers import RotatingFileHandler
import configparser
import os
import re
import pymysql
import psycopg2
from abc import ABC, abstractmethod

# 初始化配置
def load_config(config_file='config.ini'):
    config = configparser.ConfigParser(interpolation=None)
    config.read(config_file)
    
    def safe_json_loads(json_str, fallback):
        """安全解析JSON字符串"""
        if not json_str:
            return fallback
        try:
            return json.loads(json_str.replace("'", '"'))
        except:
            return fallback
    
    # 全局配置
    global_config = {
        'wechat': {
            'webhook': config.get('wechat', 'webhook', fallback='')
        },
        'monitor': {
            'check_interval': config.getint('monitor', 'check_interval', fallback=3600),
            'ignore_tables': safe_json_loads(config.get('monitor', 'ignore_tables', fallback='[]'), []),
            'ignore_change_types': safe_json_loads(config.get('monitor', 'ignore_change_types', fallback='[]'), []),
            'log_file': config.get('monitor', 'log_file', fallback='/var/log/dbmon.log'),
            'max_log_size': config.getint('monitor', 'max_log_size', fallback=10485760),
            'backup_count': config.getint('monitor', 'backup_count', fallback=5)
        }
    }
    
    # 收集所有数据库实例配置
    db_instances = {}
    for section in config.sections():
        if section.startswith('database:'):
            instance_name = section.split(':', 1)[1]
            db_type = config.get(section, 'type')
            
            # 基本连接配置
            instance_config = {
                'type': db_type,
                'instance_name': instance_name
            }
            
            # 公共连接参数
            connection_keys = ['host', 'port', 'user', 'password', 'database', 'schema', 'dsn', 'service_name']
            for key in connection_keys:
                if config.has_option(section, key):
                    instance_config[key] = config.get(section, key)
            
            # 监控目标配置
            targets = {}
            monitor_schemas = safe_json_loads(config.get(section, 'schemas', fallback='[]'), [])
            for schema in monitor_schemas:
                # 获取该schema下需要监控的表列表
                table_option = f"tables.{schema}"
                if config.has_option(section, table_option):
                    tables = safe_json_loads(config.get(section, table_option), [])
                else:
                    tables = []  # 监控所有表
                targets[schema] = tables
            
            instance_config['targets'] = targets
            db_instances[instance_name] = instance_config
    
    return {
        'global': global_config,
        'databases': db_instances
    }

# 初始化日志
def setup_logging(log_file, max_log_size, backup_count):
    logger = logging.getLogger('dbmon')
    logger.setLevel(logging.INFO)
    
    # 创建日志目录
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 文件日志（带轮转）
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=max_log_size, 
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # 控制台日志
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(file_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

class DatabaseMonitor(ABC):
    """数据库监控基类"""
    def __init__(self, instance_config, global_config):
        self.instance_config = instance_config
        self.global_config = global_config
        self.logger = logging.getLogger('dbmon')
        self.last_schemas = {}  # {schema: {table: [cols]}}
        self.instance_name = instance_config['instance_name']
    
    @abstractmethod
    def get_connection(self):
        """获取数据库连接"""
        pass
    
    @abstractmethod
    def get_current_tables(self, schema):
        """获取当前schema下的所有表名"""
        pass
    
    @abstractmethod
    def get_current_schema(self, schema):
        """获取当前完整的表结构"""
        pass
    
    def is_ignored_change(self, change):
        """检查是否忽略此变更"""
        # 忽略特定表
        for table in self.global_config['monitor']['ignore_tables']:
            if table in change:
                return True
                
        # 忽略特定变更类型
        for change_type in self.global_config['monitor']['ignore_change_types']:
            if change_type.lower() in change.lower():
                return True
                
        return False

    def compare_table_changes(self, schema, old_tables, new_tables):
        """比较表级别的变更"""
        changes = []
        
        # 新增表
        added_tables = new_tables - old_tables
        for table in added_tables:
            change = f"🆕 新增表: {table}"
            if not self.is_ignored_change(change):
                changes.append({
                    'type': 'table',
                    'operation': 'add',
                    'table': table,
                    'description': change
                })
                self.logger.info(f"{self.instance_name} - 检测到新增表: {schema}.{table}")
        
        # 删除表
        dropped_tables = old_tables - new_tables
        for table in dropped_tables:
            change = f"❌ 删除表: {table}"
            if not self.is_ignored_change(change):
                changes.append({
                    'type': 'table',
                    'operation': 'drop',
                    'table': table,
                    'description': change
                })
                self.logger.warning(f"{self.instance_name} - 检测到表被删除: {schema}.{table}")
        
        return changes

    def compare_column_changes(self, schema, old_schema, new_schema):
        """比较列级别的变更"""
        changes = []
        common_tables = set(new_schema.keys()) & set(old_schema.keys())
        
        for table in common_tables:
            # 新增列
            new_cols = {col['column_name'] for col in new_schema[table]}
            old_cols = {col['column_name'] for col in old_schema[table]}
            
            added_cols = new_cols - old_cols
            for col in added_cols:
                col_info = next(c for c in new_schema[table] if c['column_name'] == col)
                change = f"🆕 新增列: {col} ({col_info['data_type']})"
                if not self.is_ignored_change(change):
                    changes.append({
                        'type': 'column',
                        'operation': 'add',
                        'table': table,
                        'column': col,
                        'description': change
                    })
            
            # 删除列
            removed_cols = old_cols - new_cols
            for col in removed_cols:
                change = f"❌ 删除列: {col}"
                if not self.is_ignored_change(change):
                    changes.append({
                        'type': 'column',
                        'operation': 'drop',
                        'table': table,
                        'column': col,
                        'description': change
                    })
            
            # 修改列
            for new_col in new_schema[table]:
                if new_col['column_name'] in old_cols:
                    old_col = next(c for c in old_schema[table] if c['column_name'] == new_col['column_name'])
                    
                    # 数据类型变化
                    if new_col['data_type'] != old_col['data_type']:
                        change = (f"🔄 修改列类型: {new_col['column_name']}\n"
                                 f"  旧: {old_col['data_type']}\n"
                                 f"  新: {new_col['data_type']}")
                        if not self.is_ignored_change(change):
                            changes.append({
                                'type': 'column',
                                'operation': 'modify',
                                'table': table,
                                'column': new_col['column_name'],
                                'description': change
                            })
                    
                    # 长度变化
                    if 'data_length' in new_col and new_col['data_length'] != old_col.get('data_length', 0):
                        change = (f"📏 修改列长度: {new_col['column_name']}\n"
                                 f"  旧: {old_col.get('data_length', 'N/A')}\n"
                                 f"  新: {new_col.get('data_length', 'N/A')}")
                        if not self.is_ignored_change(change):
                            changes.append({
                                'type': 'column',
                                'operation': 'modify',
                                'table': table,
                                'column': new_col['column_name'],
                                'description': change
                            })
                    
                    # 可为空变化
                    if new_col.get('nullable') and new_col['nullable'] != old_col.get('nullable'):
                        change = (f"🔘 修改可为空: {new_col['column_name']}\n"
                                 f"  旧: {'NULL' if old_col.get('nullable') == 'YES' else 'NOT NULL'}\n"
                                 f"  新: {'NULL' if new_col['nullable'] == 'YES' else 'NOT NULL'}")
                        if not self.is_ignored_change(change):
                            changes.append({
                                'type': 'column',
                                'operation': 'modify',
                                'table': table,
                                'column': new_col['column_name'],
                                'description': change
                            })
                    
                    # 默认值变化
                    if new_col.get('default') != old_col.get('default'):
                        old_val = 'NULL' if old_col.get('default') is None else old_col.get('default')
                        new_val = 'NULL' if new_col.get('default') is None else new_col.get('default')
                        change = (f"⚙️ 修改默认值: {new_col['column_name']}\n"
                                 f"  旧: {old_val}\n"
                                 f"  新: {new_val}")
                        if not self.is_ignored_change(change):
                            changes.append({
                                'type': 'column',
                                'operation': 'modify',
                                'table': table,
                                'column': new_col['column_name'],
                                'description': change
                            })
        
        return changes

    def send_wechat_alert(self, changes_by_schema):
        """发送企业微信报警
        changes_by_schema: 字典，键为schema，值为该schema下的变更列表
        """
        try:
            if not changes_by_schema or not self.global_config['wechat']['webhook']:
                return True
                
            total_changes = sum(len(changes) for changes in changes_by_schema.values())
            headers = {'Content-Type': 'application/json'}
            
            # 美化报警排版
            markdown = f"## 🚨 数据库结构变更告警 ({self.instance_name})\n\n"
            markdown += f"**▷ 检测时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            markdown += f"**▷ 变更总数:** {total_changes} 处\n\n\n\n"
            
            # 按schema组织变更信息
            for schema, changes in changes_by_schema.items():
                markdown += f"### Schema: `{schema}`\n"
                
                # 按表分组变更
                changes_by_table = {}
                for change in changes:
                    table = change.get('table', '未知表')
                    if table not in changes_by_table:
                        changes_by_table[table] = []
                    changes_by_table[table].append(change)
                
                # 为每个表添加变更详情
                for table, table_changes in changes_by_table.items():
                    markdown += f"#### 表: `{table}`\n"
                    
                    # 按变更类型分组
                    changes_by_type = {}
                    for change in table_changes:
                        change_type = change.get('type', 'unknown')
                        if change_type not in changes_by_type:
                            changes_by_type[change_type] = []
                        changes_by_type[change_type].append(change)
                    
                    # 输出表级变更
                    if 'table' in changes_by_type:
                        markdown += "**表结构变更:**\n"
                        for change in changes_by_type['table']:
                            markdown += f" {change['description']}\n"
                        markdown += "\n"
                    
                    # 输出列级变更
                    if 'column' in changes_by_type:
                        markdown += "**列变更:**\n"
                        for change in changes_by_type['column']:
                            markdown += f" {change['description']}\n"
                        markdown += "\n"
            
            # 风险提示
            has_drop = any(any(change.get('operation') == 'drop' for change in changes) 
                          for changes in changes_by_schema.values())
            if has_drop:
                markdown += "\n⚠️ **高风险操作:** 检测到表/列删除操作！"
            elif total_changes > 5:
                markdown += "\n⚠️ **注意:** 检测到多处变更，请及时核查！"
            
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "content": markdown
                },
                "at": {
                    "isAtAll": False
                }
            }
            
            response = requests.post(
                self.global_config['wechat']['webhook'],
                headers=headers,
                json=data,
                timeout=10
            )
            
            if response.status_code != 200:
                self.logger.error(f"发送报警失败: {response.text}")
                return False
            return True
        except Exception as e:
            self.logger.error(f"构建报警消息失败: {str(e)}")
            return False

    def check_schema_changes(self):
        """主检查逻辑"""
        try:
            all_changes = {}  # 按schema分组的变更字典: {schema: [change1, change2]}
            targets = self.instance_config['targets']
            
            for schema, monitor_tables in targets.items():
                current_schema = self.get_current_schema(schema)
                current_tables = set(current_schema.keys()) if current_schema else set()
                
                # 初始化基准schema
                if schema not in self.last_schemas:
                    self.last_schemas[schema] = {
                        'schema': current_schema,
                        'tables': current_tables
                    }
                    self.logger.info(f"{self.instance_name} - 初始化schema基准: {schema}")
                    continue
                
                # 获取上次的状态
                last_state = self.last_schemas[schema]
                last_schema = last_state['schema']
                last_tables = last_state['tables']
                
                # 检测变更
                table_changes = self.compare_table_changes(schema, last_tables, current_tables)
                column_changes = self.compare_column_changes(schema, last_schema, current_schema)
                schema_changes = table_changes + column_changes
                
                if schema_changes:
                    self.logger.info(f"{self.instance_name} - [{schema}] 检测到 {len(schema_changes)} 处变更")
                    all_changes[schema] = schema_changes
                    
                    # 更新基准
                    self.last_schemas[schema] = {
                        'schema': current_schema,
                        'tables': current_tables
                    }
                else:
                    self.logger.debug(f"{self.instance_name} - [{schema}] 未检测到变更")
            
            # 发送报警
            if all_changes:
                if self.send_wechat_alert(all_changes):
                    self.logger.info(f"{self.instance_name} - 报警发送成功")
                else:
                    self.logger.warning(f"{self.instance_name} - 报警发送失败")
                
        except Exception as e:
            self.logger.error(f"{self.instance_name} - 检查变更时出错: {str(e)}")
            time.sleep(300)  # 出错后等待5分钟

class OracleMonitor(DatabaseMonitor):
    """Oracle数据库监控"""
    def get_connection(self):
        try:
            # 构建dsn
            dsn = self.instance_config.get('dsn')
            if not dsn:
                host = self.instance_config['host']
                port = self.instance_config.get('port', '1521')
                service_name = self.instance_config['service_name']
                dsn = f"{host}:{port}/{service_name}"
            
            return cx_Oracle.connect(
                user=self.instance_config['user'],
                password=self.instance_config['password'],
                dsn=dsn,
                encoding="UTF-8"
            )
        except cx_Oracle.Error as e:
            self.logger.error(f"{self.instance_name} - Oracle连接失败: {str(e)}")
            raise
    
    def get_current_tables(self, schema):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT table_name 
                FROM all_tables 
                WHERE owner = UPPER(:schema)
            """, {'schema': schema})
            
            tables = {row[0] for row in cursor}
            cursor.close()
            conn.close()
            return tables
        except Exception as e:
            self.logger.error(f"{self.instance_name} - 获取表列表失败: {str(e)}")
            if 'conn' in locals():
                conn.close()
            return set()
    
    def get_current_schema(self, schema):
        schema_info = {}
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 获取该schema下要监控的表
            monitor_tables = self.instance_config['targets'][schema]
            
            if monitor_tables:
                table_filter = f" AND table_name IN ({','.join([f':table{i}' for i in range(len(monitor_tables))])})"
                params = {f'table{i}': table for i, table in enumerate(monitor_tables)}
                params['schema'] = schema
            else:
                table_filter = ""
                params = {'schema': schema}
            
            sql = f"""
                SELECT 
                    table_name, 
                    column_name, 
                    data_type, 
                    data_length, 
                    nullable,
                    data_default
                FROM 
                    all_tab_columns
                WHERE 
                    owner = UPPER(:schema)
                    {table_filter}
                ORDER BY 
                    table_name, column_id
            """
            
            cursor.execute(sql, params)
            
            for table, column, dtype, length, nullable, default in cursor:
                if table not in schema_info:
                    schema_info[table] = []
                schema_info[table].append({
                    'column_name': column,
                    'data_type': dtype,
                    'data_length': length,
                    'nullable': nullable,
                    'default': default
                })
            
            cursor.close()
            conn.close()
            return schema_info
        except Exception as e:
            self.logger.error(f"{self.instance_name} - 获取schema失败: {str(e)}")
            if 'conn' in locals():
                conn.close()
            return {}

class MySQLMonitor(DatabaseMonitor):
    """MySQL数据库监控"""
    def get_connection(self):
        try:
            return pymysql.connect(
                host=self.instance_config['host'],
                port=int(self.instance_config.get('port', 3306)),
                user=self.instance_config['user'],
                password=self.instance_config['password'],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
        except pymysql.Error as e:
            self.logger.error(f"{self.instance_name} - MySQL连接失败: {str(e)}")
            raise
    
    def get_current_tables(self, schema):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT TABLE_NAME
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s
            """, (schema,))
            
            tables = {row['TABLE_NAME'] for row in cursor}
            cursor.close()
            conn.close()
            return tables
        except Exception as e:
            self.logger.error(f"{self.instance_name} - 获取表列表失败: {str(e)}")
            if 'conn' in locals():
                conn.close()
            return set()
    
    def get_current_schema(self, schema):
        schema_info = {}
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 获取该schema下要监控的表
            monitor_tables = self.instance_config['targets'][schema]
            
            if monitor_tables:
                table_filter = f" AND TABLE_NAME IN ({','.join(['%s']*len(monitor_tables))})"
                params = [schema] + monitor_tables
            else:
                table_filter = ""
                params = [schema]
            
            sql = f"""
                SELECT 
                    TABLE_NAME,
                    COLUMN_NAME,
                    COLUMN_TYPE AS DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT
                FROM 
                    information_schema.COLUMNS
                WHERE 
                    TABLE_SCHEMA = %s
                    {table_filter}
                ORDER BY 
                    TABLE_NAME, ORDINAL_POSITION
            """
            
            cursor.execute(sql, params)
            
            for row in cursor:
                table = row['TABLE_NAME']
                if table not in schema_info:
                    schema_info[table] = []
                schema_info[table].append({
                    'column_name': row['COLUMN_NAME'],
                    'data_type': row['DATA_TYPE'],
                    'nullable': row['IS_NULLABLE'],
                    'default': row['COLUMN_DEFAULT']
                })
            
            cursor.close()
            conn.close()
            return schema_info
        except Exception as e:
            self.logger.error(f"{self.instance_name} - 获取schema失败: {str(e)}")
            if 'conn' in locals():
                conn.close()
            return {}

class PostgresMonitor(DatabaseMonitor):
    """PostgreSQL数据库监控"""
    def get_connection(self):
        try:
            return psycopg2.connect(
                host=self.instance_config['host'],
                port=self.instance_config.get('port', 5432),
                user=self.instance_config['user'],
                password=self.instance_config['password'],
                dbname=self.instance_config['database'],
                connect_timeout=5
            )
        except psycopg2.Error as e:
            self.logger.error(f"{self.instance_name} - PostgreSQL连接失败: {str(e)}")
            raise
    
    def get_current_tables(self, schema):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
            """, (schema,))
            
            tables = {row[0] for row in cursor}
            cursor.close()
            conn.close()
            return tables
        except Exception as e:
            self.logger.error(f"{self.instance_name} - 获取表列表失败: {str(e)}")
            if 'conn' in locals():
                conn.close()
            return set()
    
    def get_current_schema(self, schema):
        schema_info = {}
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 获取该schema下要监控的表
            monitor_tables = self.instance_config['targets'][schema]
            
            if monitor_tables:
                table_filter = f" AND table_name IN ({','.join(['%s']*len(monitor_tables))})"
                params = [schema] + monitor_tables
            else:
                table_filter = ""
                params = [schema]
            
            sql = f"""
                SELECT 
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM 
                    information_schema.columns
                WHERE 
                    table_schema = %s
                    {table_filter}
                ORDER BY 
                    table_name, ordinal_position
            """
            
            cursor.execute(sql, params)
            
            for row in cursor:
                table = row[0]
                if table not in schema_info:
                    schema_info[table] = []
                schema_info[table].append({
                    'column_name': row[1],
                    'data_type': row[2],
                    'nullable': row[3],
                    'default': row[4]
                })
            
            cursor.close()
            conn.close()
            return schema_info
        except Exception as e:
            self.logger.error(f"{self.instance_name} - 获取schema失败: {str(e)}")
            if 'conn' in locals():
                conn.close()
            return {}

class DatabaseMonitorManager:
    """数据库监控管理器"""
    def __init__(self, config):
        self.config = config
        self.logger = setup_logging(
            config['global']['monitor']['log_file'],
            config['global']['monitor']['max_log_size'],
            config['global']['monitor']['backup_count']
        )
        self.monitors = self.create_monitors()
        self.scheduler = BackgroundScheduler()
    
    def create_monitors(self):
        """根据配置创建监控器"""
        monitors = {}
        for instance_name, instance_config in self.config['databases'].items():
            db_type = instance_config['type']
            try:
                if db_type == 'oracle':
                    monitors[instance_name] = OracleMonitor(instance_config, self.config['global'])
                elif db_type == 'mysql':
                    monitors[instance_name] = MySQLMonitor(instance_config, self.config['global'])
                elif db_type == 'postgresql':
                    monitors[instance_name] = PostgresMonitor(instance_config, self.config['global'])
                else:
                    self.logger.error(f"不支持的数据库类型: {db_type}")
            except Exception as e:
                self.logger.error(f"创建监控器 {instance_name} 失败: {str(e)}")
        return monitors
    
    def start_monitor(self, monitor):
        """启动单个监控器"""
        instance_name = monitor.instance_name
        try:
            # 初始加载
            self.logger.info(f"开始监控实例: {instance_name}")
            
            # 初始化所有schema的状态
            for schema in monitor.instance_config['targets']:
                monitor.get_current_schema(schema)
            
            # 添加到调度器
            self.scheduler.add_job(
                monitor.check_schema_changes,
                'interval',
                seconds=self.config['global']['monitor']['check_interval'],
                id=f"monitor_{instance_name}",
                name=f"Monitor-{instance_name}",
                next_run_time=datetime.now()  # 立即执行一次
            )
            
        except Exception as e:
            self.logger.error(f"监控器 {instance_name} 初始化失败: {str(e)}")
    
    def start(self):
        """启动所有监控任务"""
        try:
            if not self.monitors:
                self.logger.error("没有可用的数据库监控配置")
                return
                
            # 启动所有监控器
            for monitor in self.monitors.values():
                self.start_monitor(monitor)
            
            # 启动调度器
            self.scheduler.start()
            self.logger.info(f"监控服务已启动，实例数量: {len(self.monitors)}，检查间隔: {self.config['global']['monitor']['check_interval']}秒")
            
            # 保持主线程运行
            while True:
                time.sleep(3600)
                
        except KeyboardInterrupt:
            self.logger.info("正在停止监控服务...")
            self.scheduler.shutdown()
            self.logger.info("监控服务已停止")
        except Exception as e:
            self.logger.error(f"服务启动失败: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        config = load_config()
        manager = DatabaseMonitorManager(config)
        manager.start()
    except Exception as e:
        print(f"程序启动失败: {str(e)}")
        exit(1)
