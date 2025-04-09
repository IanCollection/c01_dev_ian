import time
import os
import sys
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing  # 添加这行来获取CPU核心数

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging
import sshtunnel
import requests
import json
from requests.auth import HTTPBasicAuth

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 全局SSH隧道对象，用于在多个函数之间共享
global_ssh_tunnel = None

##检索德勤数据库----policy（ sc_policy_detail,sc_policy_relation,dq_policy_data）
def connect_to_deloitte_db():
    """
    连接到德勤数据库

    Returns:
        connection: 数据库连接对象
        cursor: 数据库游标对象
    """
    try:
        # 尝试从环境变量加载配置
        load_dotenv()
        host = os.getenv("DELOITTE_DB_HOST", "pgm-uf61ya3k69j587m2.pg.rds.aliyuncs.com")
        port = os.getenv("DELOITTE_DB_PORT", "5432")
        database = os.getenv("DELOITTE_DB_NAME", "deloitte_data")
        user = os.getenv("DELOITTE_DB_USER", "deloitte_data")
        password = os.getenv("DELOITTE_DB_PASSWORD", "R9henc1VDdtuUxBG")

        # 建立连接
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

        # 创建游标
        cursor = connection.cursor()

        # print("成功连接到德勤数据库")
        return connection, cursor

    except Exception as e:
        print(f"连接德勤数据库失败: {str(e)}")
        return None, None

def query_deloitte_data(sql_query, params=None):
    """
    查询德勤数据库并返回结果

    Args:
        sql_query (str): SQL查询语句
        params (tuple, optional): 查询参数

    Returns:
        DataFrame: 查询结果的DataFrame
    """
    connection, cursor = connect_to_deloitte_db()

    if connection is None or cursor is None:
        return pd.DataFrame()

    try:
        # 执行查询
        if params:
            cursor.execute(sql_query, params)
        else:
            cursor.execute(sql_query)

        # 获取列名
        columns = [desc[0] for desc in cursor.description]

        # 获取所有结果
        results = cursor.fetchall()

        # 转换为DataFrame
        df = pd.DataFrame(results, columns=columns)

        return df

    except Exception as e:
        print(f"查询德勤数据库失败: {str(e)}")
        return pd.DataFrame()

    finally:
        # 关闭连接
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def close_deloitte_connection(connection, cursor):
    """
    关闭数据库连接

    Args:
        connection: 数据库连接对象
        cursor: 数据库游标对象
    """
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        # print("已关闭德勤数据库连接")
    except Exception as e:
        print(f"关闭德勤数据库连接失败: {str(e)}")

def search_policy_relation(query_text, size=15, boost=3, field="public_sc_policy_relation_industry"):
    """
    搜索政策关系数据，并返回public_sc_policy_relation_id的值

    Args:
        query_text (str): 搜索关键词
        size (int): 返回结果数量
        boost (int): 搜索权重
        field (str): 搜索字段，默认为"public_sc_policy_relation_industry"

    Returns:
        list: 包含每个结果的public_sc_policy_relation_id值的列表（已去重）
    """
    # 从环境变量加载ES服务器信息
    load_dotenv()
    es_host = os.getenv("ES_HOST", "http://172.31.137.25:9200")
    es_index = os.getenv("ES_INDEX", "search-sc_policy_relation")
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD", "es@c012025")

    # 构建查询
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            field: {
                                "query": query_text,
                                "boost": boost
                            }
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        },
        "size": size
    }

    try:
        # 发送请求
        response = requests.post(
            f"{es_host}/{es_index}/_search",
            auth=HTTPBasicAuth(es_user, es_password),
            headers={"Content-Type": "application/json"},
            data=json.dumps(query)
        )

        # 检查响应状态
        if response.status_code == 200:
            result = response.json()
            # 提取public_sc_policy_relation_id的值并去重
            id_values = []
            for hit in result.get('hits', {}).get('hits', []):
                source = hit.get('_source', {})
                id_value = source.get('public_sc_policy_relation_id')
                if id_value is not None and id_value not in id_values:
                    id_values.append(id_value)
            return id_values
        else:
            print(f"搜索政策关系数据失败: HTTP状态码 {response.status_code}")
            print(f"错误信息: {response.text}")
            return []

    except Exception as e:
        print(f"搜索政策关系数据时发生错误: {str(e)}")
        return []

def create_ssh_tunnel():
    """
    创建SSH隧道并返回

    Returns:
        tunnel: SSH隧道对象
    """
    global global_ssh_tunnel
    
    # 如果已经有一个活跃的隧道，直接返回
    if global_ssh_tunnel and global_ssh_tunnel.is_active:
        logger.info("使用已存在的SSH隧道")
        return global_ssh_tunnel
    
    try:
        # 获取SSH密钥文件的绝对路径
        ssh_key_path = os.path.expanduser("~/.ssh/c01-ssh.pem")
        
        # 创建SSH隧道
        tunnel = sshtunnel.SSHTunnelForwarder(
            ("106.14.88.25", 22),  # 跳板机地址和端口
            ssh_username="root",   # SSH用户名
            ssh_pkey=ssh_key_path, # SSH私钥路径
            remote_bind_address=("172.31.137.25", 9200),  # ES服务器地址和端口
            local_bind_address=("127.0.0.1", 9200)
        )
        
        # 启动隧道
        tunnel.start()
        logger.info(f"SSH隧道已建立，本地端口: {tunnel.local_bind_port}")
        
        # 保存到全局变量
        global_ssh_tunnel = tunnel
        
        return tunnel
    except Exception as e:
        logger.error(f"创建SSH隧道失败: {str(e)}")
        return None

def close_ssh_tunnel(tunnel=None):
    """
    关闭SSH隧道

    Args:
        tunnel: SSH隧道对象，如果为None则使用全局隧道
    """
    global global_ssh_tunnel
    
    try:
        if tunnel:
            if tunnel.is_active:
                tunnel.stop()
                logger.info("已关闭指定的SSH隧道")
        elif global_ssh_tunnel:
            if global_ssh_tunnel.is_active:
                global_ssh_tunnel.stop()
                global_ssh_tunnel = None
                logger.info("已关闭全局SSH隧道")
    except Exception as e:
        logger.error(f"关闭SSH隧道失败: {str(e)}")

def search_policy_relation_with_sshtunnel(query_text, size=15, boost=3, field="public_sc_policy_relation_industry"):
    """
    使用sshtunnel库通过SSH隧道搜索政策关系数据，并返回public_sc_policy_relation_id的值

    Args:
        query_text (str): 搜索关键词
        size (int): 返回结果数量
        boost (int): 搜索权重
        field (str): 搜索字段，默认为"public_sc_policy_relation_industry"

    Returns:
        list: 包含每个结果的public_sc_policy_relation_id值的列表（已去重）
    """
    # 从环境变量加载ES服务器信息
    load_dotenv()
    es_index = os.getenv("ES_INDEX", "search-sc_policy_relation")
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD", "es@c012025")

    # 创建SSH隧道
    tunnel = create_ssh_tunnel()
    if not tunnel:
        logger.error("无法创建SSH隧道，尝试直接连接")
        return search_policy_relation(query_text, size, boost, field)

    try:
        # 构建查询
        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                field: {
                                    "query": query_text,
                                    "boost": boost
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": size
        }

        # 发送请求到本地转发端口
        response = requests.post(
            f"http://127.0.0.1:{tunnel.local_bind_port}/{es_index}/_search",
            auth=HTTPBasicAuth(es_user, es_password),
            headers={"Content-Type": "application/json"},
            data=json.dumps(query),
            timeout=30
        )

        # 检查响应状态
        if response.status_code == 200:
            result = response.json()
            # 提取public_sc_policy_relation_id的值并去重
            id_values = []
            for hit in result.get('hits', {}).get('hits', []):
                source = hit.get('_source', {})
                id_value = source.get('public_sc_policy_relation_id')
                if id_value is not None and id_value not in id_values:
                    id_values.append(id_value)
            return id_values
        else:
            logger.error(f"搜索政策关系数据失败: HTTP状态码 {response.status_code}")
            logger.error(f"错误信息: {response.text}")
            return []

    except Exception as e:
        logger.error(f"搜索政策关系数据时发生错误: {str(e)}")
        return []
    # 注意：不在这里关闭隧道，因为后续操作可能还需要使用

def connect_to_deloitte_db_with_sshtunnel(use_ssh=True):
    """
    连接到德勤数据库，可选择是否使用SSH隧道

    Args:
        use_ssh (bool): 是否使用SSH隧道连接

    Returns:
        connection: 数据库连接对象
        cursor: 数据库游标对象
        tunnel: SSH隧道对象（如果使用SSH）
    """
    try:
        # 尝试从环境变量加载配置
        load_dotenv()
        db_host = os.getenv("DELOITTE_DB_HOST", "pgm-uf61ya3k69j587m2.pg.rds.aliyuncs.com")
        db_port = int(os.getenv("DELOITTE_DB_PORT", "5432"))
        database = os.getenv("DELOITTE_DB_NAME", "deloitte_data")
        user = os.getenv("DELOITTE_DB_USER", "deloitte_data")
        password = os.getenv("DELOITTE_DB_PASSWORD", "R9henc1VDdtuUxBG")

        tunnel = None

        if use_ssh:
            # 使用全局SSH隧道或创建新的
            tunnel = create_ssh_tunnel()
            if not tunnel:
                logger.error("无法创建SSH隧道，尝试直接连接")
                use_ssh = False
            else:
                # 通过隧道连接到数据库
                connection = psycopg2.connect(
                    host="127.0.0.1",
                    port=tunnel.local_bind_port,
                    database=database,
                    user=user,
                    password=password
                )
                logger.info("成功通过SSH隧道连接到德勤数据库")
        
        if not use_ssh:
            # 直接连接到数据库
            connection = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=database,
                user=user,
                password=password
            )
            logger.info("成功直接连接到德勤数据库")

        # 创建游标
        cursor = connection.cursor()

        return connection, cursor, tunnel

    except Exception as e:
        logger.error(f"连接德勤数据库失败: {str(e)}")
        return None, None, None

def query_deloitte_data_with_sshtunnel(sql_query, params=None, use_ssh=True):
    """
    通过SSH隧道查询德勤数据库并返回结果

    Args:
        sql_query (str): SQL查询语句
        params (tuple, optional): 查询参数
        use_ssh (bool): 是否使用SSH隧道

    Returns:
        DataFrame: 查询结果的DataFrame
    """
    connection, cursor, tunnel = connect_to_deloitte_db_with_sshtunnel(use_ssh)

    if connection is None or cursor is None:
        return pd.DataFrame()

    try:
        # 执行查询
        if params:
            cursor.execute(sql_query, params)
        else:
            cursor.execute(sql_query)

        # 获取列名
        columns = [desc[0] for desc in cursor.description]

        # 获取所有结果
        results = cursor.fetchall()

        # 转换为DataFrame
        df = pd.DataFrame(results, columns=columns)

        return df

    except Exception as e:
        logger.error(f"通过SSH隧道查询德勤数据库失败: {str(e)}")
        return pd.DataFrame()

    finally:
        # 关闭连接
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        # 注意：不在这里关闭隧道，因为后续操作可能还需要使用

def close_deloitte_connection_with_sshtunnel(connection, cursor, tunnel=None):
    """
    关闭数据库连接和SSH隧道

    Args:
        connection: 数据库连接对象
        cursor: 数据库游标对象
        tunnel: SSH隧道对象
    """
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        # 不关闭隧道，因为可能还有其他操作需要使用
        # logger.info("已关闭德勤数据库连接")
    except Exception as e:
        logger.error(f"关闭德勤数据库连接失败: {str(e)}")

def get_policy_details_by_ids_with_sshtunnel(policy_ids):
    """
    通过SSH隧道根据政策ID列表批量查询政策详情
    
    Args:
        policy_ids (list): 政策ID列表
        
    Returns:
        list: 包含政策详情的列表
    """
    if not policy_ids:
        return []

    connection, cursor, tunnel = connect_to_deloitte_db_with_sshtunnel()

    if connection is None or cursor is None:
        return []

    try:
        # 构建IN查询
        placeholders = ','.join(['%s'] * len(policy_ids))
        sql = f"""
            SELECT a.id, a.title, a.policy_summary, b.content
            FROM sc_policy_detail a
            LEFT JOIN dq_policy_data b ON a.id = b.id
            WHERE a.id IN ({placeholders})
        """
        cursor.execute(sql, policy_ids)
        results = cursor.fetchall()

        policy_details = []
        for result in results:
            policy_detail = {
                "id": result[0],
                "policy_title": result[1],
                "policy_summary": result[2],
                "content": result[3]
            }
            policy_details.append(policy_detail)

        return policy_details

    except Exception as e:
        logger.error(f"批量查询政策详情失败: {str(e)}")
        return []
    finally:
        # 关闭连接
        close_deloitte_connection_with_sshtunnel(connection, cursor)

def get_policy_detail_by_id(policy_id, use_ssh=True):
    """
    根据政策ID查询政策详情

    Args:
        policy_id (str): 政策ID
        use_ssh (bool): 是否使用SSH隧道

    Returns:
        dict: 包含政策详情的字典
    """
    connection, cursor, tunnel = connect_to_deloitte_db_with_sshtunnel(use_ssh)

    if connection is None or cursor is None:
        return None

    try:
        # 查询政策详情
        sql = """
            SELECT a.id, a.title, a.policy_summary, b.content
            FROM sc_policy_detail a
            LEFT JOIN dq_policy_data b ON a.id = b.id
            WHERE a.id = %s
        """
        cursor.execute(sql, (policy_id,))
        result = cursor.fetchone()

        if result:
            policy_detail = {
                "id": result[0],
                "policy_title": result[1],
                "policy_summary": result[2],
                "content": result[3]
            }
            return policy_detail
        else:
            logger.warning(f"未找到ID为{policy_id}的政策")
            return None

    except Exception as e:
        logger.error(f"查询政策详情失败: {str(e)}")
        return None
    finally:
        # 关闭连接
        close_deloitte_connection_with_sshtunnel(connection, cursor)

def get_policy_details_by_ids(policy_ids, use_ssh=True):
    """
    根据政策ID列表批量查询政策详情

    Args:
        policy_ids (list): 政策ID列表
        use_ssh (bool): 是否使用SSH隧道

    Returns:
        list: 包含政策详情的列表
    """
    if not policy_ids:
        return []

    connection, cursor, tunnel = connect_to_deloitte_db_with_sshtunnel(use_ssh)

    if connection is None or cursor is None:
        return []

    try:
        # 构建IN查询
        placeholders = ','.join(['%s'] * len(policy_ids))
        sql = f"""
            SELECT a.id, a.title, a.policy_summary, b.content
            FROM sc_policy_detail a
            LEFT JOIN dq_policy_data b ON a.id = b.id
            WHERE a.id IN ({placeholders})
        """
        cursor.execute(sql, policy_ids)
        results = cursor.fetchall()

        policy_details = []
        for result in results:
            policy_detail = {
                "id": result[0],
                "policy_title": result[1],
                "policy_summary": result[2],
                "content": result[3]
            }
            policy_details.append(policy_detail)

        return policy_details

    except Exception as e:
        logger.error(f"批量查询政策详情失败: {str(e)}")
        return []
    finally:
        # 关闭连接
        close_deloitte_connection_with_sshtunnel(connection, cursor)

########################################################
#检索产业链数据库
def search_indicators(query_text, size=15, boost=3, use_ssh=True):
    """
    搜索产业链数据库中的指标数据

    Args:
        query_text (str): 搜索关键词
        size (int): 返回结果数量
        boost (int): 搜索权重
        use_ssh (bool): 是否使用SSH隧道

    Returns:
        list: 包含指标ID的列表
    """
    # 从环境变量加载ES服务器信息
    load_dotenv()
    es_index = os.getenv("ES_INDICATOR_INDEX", "search-eco_info_deloitte")
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD", "es@c012025")

    # 构建查询
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            "public_eco_info_deloitte_name_cn": {
                                "query": query_text,
                                "boost": boost
                            }
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        },
        "size": size
    }

    try:
        if use_ssh:
            # 使用SSH隧道连接
            tunnel = create_ssh_tunnel()
            if not tunnel:
                logger.error("SSH隧道连接失败，尝试直接连接")
                return search_indicators_directly(query_text)
            
            es_host = f"http://127.0.0.1:{tunnel.local_bind_port}"
        else:
            # 直接连接
            es_host = "http://172.31.137.25:9200"

        # 增加重试机制
        for attempt in range(3):
            try:
                response = requests.post(
                    f"{es_host}/{es_index}/_search",
                    auth=HTTPBasicAuth(es_user, es_password),
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(query),
                    timeout=30
                )
                break
            except requests.exceptions.ConnectionError as e:
                if attempt == 2:
                    logger.error(f"连接ES失败: {str(e)}")
                    return []

        # 检查响应状态
        if response.status_code == 200:
            result = response.json()
            # 提取指标ID并去重
            id_values = []
            for hit in result.get('hits', {}).get('hits', []):
                source = hit.get('_source', {})
                id_value = source.get('public_eco_info_deloitte_id')
                if id_value is not None and id_value not in id_values:
                    id_values.append(id_value)
            return id_values
        else:
            logger.error(f"搜索指标数据失败: HTTP状态码 {response.status_code}")
            logger.error(f"错误信息: {response.text}")
            return []

    except Exception as e:
        logger.error(f"搜索指标数据时发生错误: {str(e)}")
        return []

def search_indicators_directly(keywords):
    """
    直接连接ES搜索指标数据

    Args:
        keywords (str): 搜索关键词

    Returns:
        list: 包含指标ID的列表
    """
    try:
        # ES连接信息
        es_host = "http://172.31.137.25:9200"
        es_index = "search-eco_info_deloitte"
        es_user = "elastic"
        es_password = "es@c012025"

        # 构建查询
        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "public_eco_info_deloitte_name_cn": {
                                    "query": keywords,
                                    "boost": 3
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": 15
        }

        # 发送请求
        response = requests.post(
            f"{es_host}/{es_index}/_search",
            auth=HTTPBasicAuth(es_user, es_password),
            headers={"Content-Type": "application/json"},
            data=json.dumps(query),
            timeout=30
        )

        # 处理响应
        if response.status_code == 200:
            result = response.json()
            id_values = []
            for hit in result.get('hits', {}).get('hits', []):
                source = hit.get('_source', {})
                id_value = source.get('public_eco_info_deloitte_id')
                if id_value is not None and id_value not in id_values:
                    id_values.append(id_value)
            return id_values
        else:
            logger.error(f"搜索指标数据失败: HTTP状态码 {response.status_code}")
            logger.error(f"错误信息: {response.text}")
            return []

    except Exception as e:
        logger.error(f"搜索指标数据时发生错误: {str(e)}")
        return []

# 示例用法
if __name__ == "__main__":
    try:
        # 设置是否使用SSH隧道
        USE_SSH = True
        
        # 测试搜索政策关系
        print("\n正在搜索政策关系...")
        policy_ids = search_policy_relation_with_sshtunnel("2024年新能源汽车")
        print(f"找到{len(policy_ids)}条相关政策ID")
        print(policy_ids[:5])  # 只显示前5个ID

        # 测试查询政策详情
        print("\n正在查询政策详情...")
        test_policy_id = "1966100"  # 替换为实际的政策ID
        policy_detail = get_policy_detail_by_id(test_policy_id, use_ssh=USE_SSH)
        if policy_detail:
            print(f"政策名称: {policy_detail['policy_title']}")
            print(f"政策摘要: {policy_detail['policy_summary']}")
            print(f"政策内容: {policy_detail['content'][:100]}...")  # 只显示内容的前100个字符
        
        # 测试批量查询
        print("\n正在批量查询政策...")
        test_policy_ids = ["1966100", "1966099", "1962242"]  # 替换为实际的政策ID列表
        policy_details = get_policy_details_by_ids(test_policy_ids, use_ssh=USE_SSH)
        print(f"共找到{len(policy_details)}条政策")
        for detail in policy_details[:3]:  # 只显示前3条
            print(f"ID: {detail['id']}, 名称: {detail['policy_title']}")
            
        # 测试搜索指标数据
        print("\n正在搜索指标数据...")
        indicator_ids = search_indicators("2024年AI市场", use_ssh=USE_SSH)
        print(f"找到{len(indicator_ids)}条相关指标ID")
        print(indicator_ids[:5])  # 只显示前5个ID
        
    except Exception as e:
        logger.error(f"执行过程中发生错误: {str(e)}")
    finally:
        # 确保在程序结束时关闭SSH隧道
        close_ssh_tunnel()
        print("\n程序执行完毕，已关闭所有连接")

    创建数据库连接
