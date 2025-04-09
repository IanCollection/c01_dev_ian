import time
import os
import sys

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging
import requests
import json
from requests.auth import HTTPBasicAuth

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from Agent.Overview_agent import semantic_enhancement_agent

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
        # load_dotenv()
        # host = os.getenv("DELOITTE_DB_HOST", "pgm-uf61ya3k69j587m2.pg.rds.aliyuncs.com")
        # port = os.getenv("DELOITTE_DB_PORT", "5432")
        # database = os.getenv("DELOITTE_DB_NAME", "deloitte_data")
        # user = os.getenv("DELOITTE_DB_USER", "deloitte_data")
        # password = os.getenv("DELOITTE_DB_PASSWORD", "R9henc1VDdtuUxBG")
        #
        # # 建立连接
        # connection = psycopg2.connect(
        #     host=host,
        #     port=port,
        #     database=database,
        #     user=user,
        #     password=password
        # )
        #
        # # 创建游标
        # cursor = connection.cursor()
        connection = psycopg2.connect(
            host="pgm-uf61ya3k69j587m2.pg.rds.aliyuncs.com",
            port=5432,
            database="deloitte_data",
            user="deloitte_data",
            password="R9henc1VDdtuUxBG"
        )
        cursor = connection.cursor()

        #
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

def search_policy(query_text, augement_label=None, size=10, boost=3, field="public_dq_policy_data_title"):
    
    """
    搜索政策数据，使用must限制条件，提高结果精确度

    Args:
        query_text (str): 搜索关键词
        augement_label (str): 可选的增强标签
        size (int): 返回结果数量
        boost (int): 搜索权重
        field (str): 主要搜索字段，默认为"public_dq_policy_data_title"

    Returns:
        list: 包含每个结果的政策ID值的列表（已去重）
    """
    # 从环境变量加载ES服务器信息
    load_dotenv()
    es_host = os.getenv("ES_HOST", "http://172.31.137.25:9200")
    es_index = os.getenv("ES_INDEX", "search-dq_policy_data")
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD", "es@c012025")
    
    # 获取语义增强关键词
    json_content, year = semantic_enhancement_agent(query_text)
    # 从json_content中提取所有关键词
    keywords = []
    if json_content and 'keywords' in json_content:
        for category in ['core_keywords', 'domain_keywords', 'focus_keywords']:
            if category in json_content['keywords']:
                keywords.extend(json_content['keywords'][category])

    # 构建查询 - 使用should来增加匹配灵活性
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            field: {
                                "query": query_text,
                                "operator": "and",  # 强制标题匹配所有词
                                "boost": boost * 2  # 给标题匹配更高权重
                            }
                        }
                    }
                ],
                "should": [
                    {
                        "match": {
                            field: {
                                "query": query_text,
                                "fuzziness": "AUTO",  # 模糊匹配增加召回率
                                "boost": boost      # 普通权重
                            }
                        }
                    },
                    {
                        "match": {
                            "public_dq_policy_content": {
                                "query": query_text,
                                "minimum_should_match": "40%",  # 提高内容匹配要求
                                "boost": boost / 2  # 降低内容匹配的权重
                            }
                        }
                    }
                ]
            }
        },
        "size": size
    }
    
    # 添加关键词匹配条件到should中
    if keywords and len(keywords) > 0:
        for keyword in keywords:
            if keyword and len(keyword) >= 2:  # 忽略过短的关键词
                query["query"]["bool"]["should"].append({
                    "match": {
                        field: {
                            "query": keyword,
                            "boost": boost * 1.5  # 给关键词匹配较高权重
                        }
                    }
                })
                # 同时在内容中搜索关键词
                query["query"]["bool"]["should"].append({
                    "match": {
                        "public_dq_policy_content": {
                            "query": keyword,
                            "boost": boost / 3  # 内容匹配的权重较低
                        }
                    }
                })
    
    # 如果有增强标签，添加到should条件中
    if augement_label:
        query["query"]["bool"]["should"].append({
            "match": {
                "public_dq_policy_data_type": {
                    "query": augement_label,
                    "boost": boost
                }
            }
        })

    try:
        # 发送请求
        response = requests.post(
            f"{es_host}/{es_index}/_search",
            auth=HTTPBasicAuth(es_user, es_password),
            headers={"Content-Type": "application/json"},
            data=json.dumps(query),
            timeout=30
        )

        # 检查响应状态
        if response.status_code == 200:
            result = response.json()
            # 提取政策ID的值并去重
            id_values = []
            for hit in result.get('hits', {}).get('hits', []):
                source = hit.get('_source', {})
                id_value = source.get('public_dq_policy_data_id')
                if id_value is not None and id_value not in id_values:
                    id_values.append(id_value)
            
            logger.info(f"搜索关键词 '{query_text}' 找到 {len(id_values)} 条相关结果")
            
            # 如果没有找到结果，尝试分词搜索
            if len(id_values) == 0:
                # 简单分词，按空格分割
                keywords = query_text.split()
                if len(keywords) > 1:
                    logger.info(f"尝试使用分词搜索: {keywords}")
                    for keyword in keywords:
                        if len(keyword) >= 2:  # 忽略过短的词
                            sub_ids = search_policy(keyword, augement_label, size, boost, field)
                            id_values.extend([id for id in sub_ids if id not in id_values])
                            if len(id_values) >= size:
                                break
            
            return id_values
        else:
            logger.error(f"搜索政策数据失败: HTTP状态码 {response.status_code}")
            logger.error(f"错误信息: {response.text}")
            return []

    except Exception as e:
        logger.error(f"搜索政策数据时发生错误: {str(e)}")
        return []

def search_policy_relation(query_text, augement_label=None, size=10, boost=3, field="public_sc_policy_relation_industry"):
    """
    搜索政策关系数据，使用must限制条件，提高结果精确度

    Args:
        query_text (str): 搜索关键词
        augement_label (str): 可选的增强标签
        size (int): 返回结果数量
        boost (int): 搜索权重
        field (str): 主要搜索字段，默认为"public_sc_policy_relation_industry"

    Returns:
        list: 包含每个结果的public_sc_policy_relation_id值的列表（已去重）
    """
    # 从环境变量加载ES服务器信息
    load_dotenv()
    es_host = os.getenv("ES_HOST", "http://172.31.137.25:9200")
    es_index = os.getenv("ES_INDEX", "search-sc_policy_relation")
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD", "es@c012025")
    
    # 构建查询 - 使用must和should组合
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            field: {
                                "query": query_text,
                                "operator": "or",  # 使用or运算符放宽匹配条件
                                "minimum_should_match": "50%"  # 降低匹配百分比要求
                            }
                        }
                    },
                    {
                        "multi_match": {
                            "query": query_text,
                            "fields": [
                                "public_sc_policy_relation_involved_products^1.5",
                                "public_sc_policy_relation_involved_tech",
                                "public_sc_policy_relation_industry^2"  # 增加行业字段权重
                            ],
                            "type": "best_fields",
                            "fuzziness": "AUTO"
                        }
                    }
                    # 移除了match_phrase查询，因为字段没有位置数据
                ]
            }
        },
        "size": size
    }
    # 如果有增强标签，添加到must条件中
    if augement_label:
        # 确保must字段存在
        if "must" not in query["query"]["bool"]:
            query["query"]["bool"]["must"] = []
            
        query["query"]["bool"]["must"].append({
            "match": {
                "public_sc_policy_relation_type": {
                    "query": augement_label,
                    "boost": boost
                }
            }
        })

    try:
        # 发送请求
        response = requests.post(
            f"{es_host}/{es_index}/_search",
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
            
            logger.info(f"搜索关键词 '{query_text}' 找到 {len(id_values)} 条相关结果")
            return id_values
        else:
            logger.error(f"搜索政策关系数据失败: HTTP状态码 {response.status_code}")
            logger.error(f"错误信息: {response.text}")
            return []

    except Exception as e:
        logger.error(f"搜索政策关系数据时发生错误: {str(e)}")
        return []

def get_policy_details_by_ids(policy_ids,time = None):
    """
    根据政策ID列表批量查询政策详情

    Args:
        policy_ids (list): 政策ID列表

    Returns:
        list: 包含政策详情的列表
    """
    if not policy_ids or len(policy_ids) == 0:
        return []

    # 去重处理
    unique_policy_ids = list(set(policy_ids))

    connection, cursor = connect_to_deloitte_db()

    if connection is None or cursor is None:
        return []

    try:
        # 构建IN查询
        placeholders = ','.join(['%s'] * len(unique_policy_ids))
        sql = f"""
            SELECT DISTINCT a.id, a.title, a.policy_summary, b.content, c.industry, a.policy_start_date,a.policy_end_date, b.org_name,b.publish_at
            FROM sc_policy_detail a
            LEFT JOIN dq_policy_data b ON a.id = b.id
            LEFT JOIN sc_policy_relation c ON a.id = c.id
            WHERE a.id IN ({placeholders})
        """
        cursor.execute(sql, unique_policy_ids)
        results = cursor.fetchall()

        # 使用字典来存储结果，确保唯一性
        policy_details_dict = {}
        for result in results:
            policy_detail = {
                "id": result[0],
                "policy_title": result[1],
                "policy_summary": result[2],
                "content": result[3],
                "industry": result[4],
                "policy_start_date": result[5],
                "policy_end_date": result[6],
                "org_name":result[7],
                "publish_at":result[8]
            }
            policy_details_dict[result[0]] = policy_detail

        # 按照原始顺序返回结果
        policy_details = []
        for policy_id in policy_ids:
            if policy_id in policy_details_dict:
                policy_details.append(policy_details_dict[policy_id])

        return policy_details

    except Exception as e:
        logger.error(f"批量查询政策详情失败: {str(e)}")
        return []
    finally:
        # 关闭连接
        close_deloitte_connection(connection, cursor)



# 示例用法
if __name__ == "__main__":
    try:
        connection, cursor = connect_to_deloitte_db()
        if connection and cursor:
            # 查询数据库中所有表名
            cursor.execute("""
                SELECT COUNT(*) 
                FROM dq_policy_data
            """)
            tables = cursor.fetchall()
            print("当前数据库中的表：")
            for table in tables:
                print(table[0])
        else:
            print("无法连接到数据库")
    except Exception as e:
        print(f"查询失败: {str(e)}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()
    # 测试打印表字段
    # ids = search_policy_relation("新能源汽车")
    # result = get_policy_details_by_ids(ids)
    # print(result)
    # try:
    #     connection, cursor = connect_to_deloitte_db()
    #     if connection and cursor:
    #         # 查询sc_policy_relation表的行数
    #         cursor.execute("""
    #             SELECT COUNT(*) 
    #             FROM sc_policy_relation
    #         """)
    #         row_count = cursor.fetchone()[0]
    #         print(f"sc_policy_relation表共有 {row_count} 行数据")
    #     else:
    #         print("无法连接到数据库")
    # except Exception as e:
    #     print(f"查询失败: {str(e)}")
    # finally:
    #     if 'cursor' in locals() and cursor:
    #         cursor.close()
    #     if 'connection' in locals() and connection:
    #         connection.close()






    # policy_ids = [1964656, 1963254, 1963859, 1964636, 1963360, 1962949, 1962657, 1962242, 1962844, 1961987]
    # result = get_policy_details_by_ids(policy_ids)
    # print(result)


    # query_text = "2024年AI人工智能发展趋势"
    # ids = search_policy_relation(query_text)
    # print(ids)

    # ids = search_policy(query_text)
    # print(ids)
    # results = get_policy_details_by_ids(ids)
    # print(results)



    # 测试查询unified_eco_data_view表结构

    # print(eco_indicators_query_batch([580497, 474683, 339250, 24686, 7261, 307308, 292823, 166147, 166300, 435302, 407749, 119829, 779467, 583531, 806214]))
    
    
    # # 查询public_eco_info_deloitte_name_cn表的所有字段并返回第一行样例
    # def get_eco_info_deloitte_schema_sample():
    #     """
    #     查询public_eco_info_deloitte_name_cn表的所有字段并返回第一行样例数据
        
    #     Returns:
    #         dict: 包含表结构和样例数据的字典
    #     """
    #     try:
    #         connection, cursor = connect_to_deloitte_db()
    #         if not connection or not cursor:
    #             logger.error("无法连接到数据库")
    #             return {}
            
    #         # 查询表结构
    #         cursor.execute("""
    #             SELECT column_name, data_type 
    #             FROM information_schema.columns 
    #             WHERE table_name = 'eco_info_deloitte'
    #             ORDER BY ordinal_position
    #         """)
            
    #         columns_info = cursor.fetchall()
    #         schema = {col[0]: col[1] for col in columns_info}
            
    #         # 查询第一行数据作为样例
    #         cursor.execute("""
    #             SELECT * FROM eco_info_deloitte LIMIT 1
    #         """)
            
    #         sample_row = cursor.fetchone()
            
    #         if not sample_row:
    #             logger.warning("未找到样例数据")
    #             return {"schema": schema, "sample": {}}
            
    #         # 获取列名
    #         column_names = [desc[0] for desc in cursor.description]
            
    #         # 构建样例数据
    #         sample_data = dict(zip(column_names, sample_row))
            
    #         return {
    #             "schema": schema,
    #             "sample": sample_data
    #         }
            
    #     except Exception as e:
    #         logger.error(f"查询表结构和样例数据时发生错误: {str(e)}")
    #         return {}
    #     finally:
    #         if cursor:
    #             cursor.close()
    #         if connection:
    #             connection.close()
    
    # # 测试获取表结构和样例数据
    # print("\n正在查询public_eco_info_deloitte表结构和样例数据...")
    # schema_sample = get_eco_info_deloitte_schema_sample()
    # if schema_sample:
    #     print("\n表结构:")
    #     for col, data_type in schema_sample.get("schema", {}).items():
    #         print(f"{col}: {data_type}")
        
    #     print("\n样例数据:")
    #     sample = schema_sample.get("sample", {})
    #     for key, value in sample.items():
    #         # 对于长文本，只显示前50个字符
    #         if isinstance(value, str) and len(value) > 50:
    #             print(f"{key}: {value[:50]}...")
    #         else:
    #             print(f"{key}: {value}")
    
    
    # print(eco_indicators_query_batch(["474683"]))
    # print(search_indicators("2024年AI市场"))

    # indic_ids = [2020201890]
    # print(eco_indicators_query_batch)
    # # 打印具体指标数值
    # indicator_data = eco_indicators_query_batch(indic_ids)
    # print("\n指标数据详情:")
    # for item in indicator_data:
    #     print(item)
    # print(search_policy_relation("2024年 AI市场"))
    # try:
    #     # 测试查询政策详情
    #     print("\n正在查询政策详情...")
    #     test_policy_id = "1966100"  # 替换为实际的政策ID
    #     policy_detail = get_policy_detail_by_id(test_policy_id)
    #     if policy_detail:
    #         print(f"政策名称: {policy_detail['policy_title']}")
    #         print(f"政策摘要: {policy_detail['policy_summary']}")
    #         print(f"政策内容: {policy_detail['content'][:100]}...")  # 只显示内容的前100个字符
        
    #     # 测试批量查询
    #     print("\n正在批量查询政策...")
    #     test_policy_ids = ["1966100", "1966099", "1962242"]  # 替换为实际的政策ID列表
    #     policy_details = get_policy_details_by_ids(test_policy_ids)
    #     print(f"共找到{len(policy_details)}条政策")
    #     for detail in policy_details[:3]:  # 只显示前3条
    #         print(f"ID: {detail['id']}, 名称: {detail['policy_title']}")
            
    #     # 测试搜索指标数据
    #     print("\n正在搜索指标数据...")
    #     indicator_ids = search_indicators("2024年AI市场")
    #     print(f"找到{len(indicator_ids)}条相关指标ID")
    #     print(indicator_ids[:5])  # 只显示前5个ID
        
    # except Exception as e:
    #     logger.error(f"执行过程中发生错误: {str(e)}")
    # finally:
    #     print("\n程序执行完毕，已关闭所有连接")