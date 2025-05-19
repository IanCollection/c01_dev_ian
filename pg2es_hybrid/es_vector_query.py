import os
import sys
import json
from datetime import date, datetime
# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)


from psycopg2._psycopg import cursor, connection
from scrpit.indicator_query_v4 import connect_to_deloitte_db
from pg2es_hybrid.search import HybridSearch
from Agent.Overview_agent import year_extract_from_title
from psycopg2 import Error as Psycopg2Error # 建议显式导入Error
# 自定义JSON编码器，处理日期类型
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

# 使用绝对路径指定配置文件位置
config_path = os.path.join(current_dir, 'config.yaml')  # 如果配置文件在pg2es_hybrid目录下
# 或者
# config_path = os.path.join(os.path.dirname(current_dir), 'config.yaml')  # 如果配置文件在项目根目录下

searcher = HybridSearch(config_path)

def print_result(hit):
    score = hit['_score']
    source = hit['_source']
    
    print(f"\n得分: {score:.2f}")
    print("文档内容:")
    important_fields = ['title', 'policy_summary']
    for field in important_fields:
        if field in source:
            print(f"{field}: {source[field]}")
    
    print("其他字段:")
    for key, value in source.items():
        if key not in important_fields and not key.endswith('_vector'):
            print(f"{key}: {value}")


def es_vector_query(query_text, table_name="sc_policy_detail", vector_field="title", size=10, min_score=0.7, use_multi_fields=True):
    """
    执行混合搜索查询
    
    Args:
        query_text (str): 查询文本
        table_name (str): 要搜索的表名
        vector_field (str): 用于向量搜索的字段
        size (int): 返回结果数量
        min_score (float): 最低分数阈值，低于此分数的结果将被过滤
        use_multi_fields (bool): 是否使用多字段搜索
        
    Returns:
        list: 搜索结果列表
        list: 所有结果的ID列表
    """
    try:
        # 预处理查询文本，去除停用词和特殊字符
        processed_query = preprocess_query(query_text)
        
        # 如果启用多字段搜索，则同时搜索title和policy_summary
        vector_fields = [vector_field]
        if use_multi_fields and vector_field in ["title", "policy_summary"]:
            vector_fields = ["title", "policy_summary"]
        
        # 对每个字段执行搜索并合并结果
        all_results = []
        for field in vector_fields:
            results = searcher.search(
                table_name=table_name,
                query=processed_query,
                vector_field=field,
                size=size * 2  # 获取更多结果以便后续过滤
            )
            all_results.extend(results)
        
        # 去重并按分数排序
        unique_results = {}
        for hit in all_results:
            doc_id = hit['_id']
            if doc_id not in unique_results or hit['_score'] > unique_results[doc_id]['_score']:
                unique_results[doc_id] = hit
        
        # 过滤低分结果并按分数排序
        filtered_results = [hit for hit in unique_results.values() if hit['_score'] >= min_score]
        sorted_results = sorted(filtered_results, key=lambda x: x['_score'], reverse=True)
        
        # 提取所有ID
        ids = [hit['_id'] for hit in sorted_results[:size]]
        
        # 只返回指定字段
        result_list = []
        for hit in sorted_results[:size]:
            source = hit['_source']
            filtered_hit = {
                'id': hit['_id'],
                'title': source.get('title', ''),
                'involved_industry_chain': source.get('involved_industry_chain', ''),
                'policy_summary': source.get('policy_summary', ''),
                # 'org_name':source.get('release_agency','')
            }
            result_list.append(filtered_hit)
        
        return result_list, ids
    except Exception as e:
        print(f"搜索出错: {str(e)}")
        return []

def es_keyword_query_policy_info(query_text, table_name="sc_policy_detail", field="title", size=10, min_score=0.8):
    """
    执行关键词搜索查询
    
    Args:
        query_text (str): 查询文本
        table_name (str): 要搜索的表名
        field (str): 用于搜索的字段
        size (int): 返回结果数量
        min_score (float): 最低分数阈值，低于此分数的结果将被过滤
        
    Returns:
        list: 搜索结果列表
        list: 所有结果的ID列表
    """
    try:
        # 预处理查询文本，去除停用词和特殊字符
        processed_query = preprocess_query(query_text)
        
        # 直接使用Elasticsearch客户端进行查询，绕过HybridSearch.search方法
        index_name = f"{table_name}_index"
        search_query = {
            "query": {
                "match": {
                    field: {
                        "query": processed_query,
                        "fuzziness": "AUTO"
                    }
                }
            },
            "size": size * 2  # 获取更多结果以便后续过滤
        }
        
        try:
            results = searcher.es.search(index=index_name, body=search_query)
            all_results = results['hits']['hits']
        except Exception as e:
            print(f"Elasticsearch查询错误: {str(e)}")
            return [], []
        
        # 过滤低分结果并按分数排序
        filtered_results = [hit for hit in all_results if hit['_score'] >= min_score]
        sorted_results = sorted(filtered_results, key=lambda x: x['_score'], reverse=True)
        
        # 提取所有ID
        ids = [hit['_id'] for hit in sorted_results[:size]]
        
        # 只返回指定字段
        result_list = []
        
        # 连接数据库
        connection, cursor = connect_to_deloitte_db()
        if connection and cursor:
            try:
                for hit in sorted_results[:size]:
                    source = hit['_source']
                    doc_id = hit['_id']
                    
                    # 查询org_name
                    sql = f"SELECT org_name FROM dq_policy_data WHERE id = '{doc_id}'"
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    org_name = result[0] if result else ''
                    
                    filtered_hit = {
                        'id': doc_id,
                        'title': source.get('title', ''),
                        'involved_industry_chain': source.get('involved_industry_chain', ''),
                        'policy_summary': source.get('policy_summary', ''),
                        'involved_region': source.get('involved_region', ''),
                        'org_name': org_name
                    }
                    result_list.append(filtered_hit)
            except Exception as db_error:
                print(f"数据库查询错误: {str(db_error)}")
            finally:
                # 确保关闭连接
                connection.close()
        else:
            # 如果数据库连接失败，仍然返回基本信息
            for hit in sorted_results[:size]:
                source = hit['_source']
                filtered_hit = {
                    'id': hit['_id'],
                    'title': source.get('title', ''),
                    'involved_industry_chain': source.get('involved_industry_chain', ''),
                    'policy_summary': source.get('policy_summary', ''),
                    'involved_region': source.get('involved_region', ''),
                    'org_name': ''
                }
                result_list.append(filtered_hit)
        
        return result_list, ids
    except Exception as e:
        print(f"搜索出错: {str(e)}")
        return [], []


def es_vector_query_policy_info(query_text, table_name="sc_policy_detail", vector_field="title", size=10, min_score=0.8, use_multi_fields=True):
    """
    执行混合搜索查询
    
    Args:
        query_text (str): 查询文本
        table_name (str): 要搜索的表名
        vector_field (str): 用于向量搜索的字段
        size (int): 返回结果数量
        min_score (float): 最低分数阈值，低于此分数的结果将被过滤
        use_multi_fields (bool): 是否使用多字段搜索
        
    Returns:
        list: 搜索结果列表
        list: 所有结果的ID列表
    """
    try:
        # 预处理查询文本，去除停用词和特殊字符
        processed_query = preprocess_query(query_text)
        
        # 如果启用多字段搜索，则同时搜索title和policy_summary
        # vector_fields = [vector_field]
        # if use_multi_fields and vector_field in ["title", "policy_summary"]:
        #     vector_fields = ["title", "policy_summary"]
        
         # 只检索title字段，忽略use_multi_fields设置
        vector_fields = ["title"]


        # 对每个字段执行搜索并合并结果
        all_results = []
        for field in vector_fields:
            results = searcher.search(
                table_name=table_name,
                query=processed_query,
                vector_field=field,
                size=size * 2  # 获取更多结果以便后续过滤
            )
            all_results.extend(results)
        
        # 去重并按分数排序
        unique_results = {}
        for hit in all_results:
            doc_id = hit['_id']
            if doc_id not in unique_results or hit['_score'] > unique_results[doc_id]['_score']:
                unique_results[doc_id] = hit
        
        # 过滤低分结果并按分数排序
        filtered_results = [hit for hit in unique_results.values() if hit['_score'] >= min_score]
        sorted_results = sorted(filtered_results, key=lambda x: x['_score'], reverse=True)
        
        # 提取所有ID
        ids = [hit['_id'] for hit in sorted_results[:size]]
        
        # 只返回指定字段
        result_list = []
        
        # 连接数据库
        connection, cursor = connect_to_deloitte_db()
        if connection and cursor:
            try:
                for hit in sorted_results[:size]:
                    source = hit['_source']
                    doc_id = hit['_id']
                    
                    # 查询org_name
                    sql = f"SELECT org_name FROM dq_policy_data WHERE id = '{doc_id}'"
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    org_name = result[0] if result else ''
                    
                    filtered_hit = {
                        'id': doc_id,
                        'title': source.get('title', ''),
                        'involved_industry_chain': source.get('involved_industry_chain', ''),
                        'policy_summary': source.get('policy_summary', ''),
                        'involved_region': source.get('involved_region', ''),  # 添加 involved_region 字段
                        'org_name': org_name
                    }
                    # print(f"194_involved_region:{source.get('involved_region', '')}")
                    result_list.append(filtered_hit)
            except Exception as db_error:
                print(f"数据库查询错误: {str(db_error)}")
            finally:
                # 确保关闭连接
                connection.close()
        else:
            # 如果数据库连接失败，仍然返回基本信息
            for hit in sorted_results[:size]:
                source = hit['_source']
                filtered_hit = {
                    'id': hit['_id'],
                    'title': source.get('title', ''),
                    'involved_industry_chain': source.get('involved_industry_chain', ''),
                    'policy_summary': source.get('policy_summary', ''),
                    'involved_region': source.get('involved_region', ''),  # 添加 involved_region 字段
                    'org_name': ''
                }
                result_list.append(filtered_hit)
        
        return result_list, ids
    except Exception as e:
        print(f"搜索出错: {str(e)}")
        return [], []


def es_vector_query_eco_indicators(query_text, size=10, min_score=0.5):
    """
    使用向量搜索查询经济数据
    
    Args:
        query_text (str): 查询文本
        size (int, optional): 返回结果数量，默认为10
        min_score (float, optional): 最小相似度分数，默认为0.5
        
    Returns:
        tuple: (结果列表, INDIC_ID列表)
    """
    try:
        # 预处理查询文本，去除停用词和特殊字符
        processed_query = preprocess_query(query_text)
        
        # 设置表名和向量字段
        table_name = "eco_info_deloitte"
        vector_field = "name_cn"
        
        # 执行搜索
        results = searcher.search(
            table_name=table_name,
            query=processed_query,
            vector_field=vector_field,
            size=size * 2  # 获取更多结果以便后续过滤
        )

        # 过滤低分结果并按分数排序
        filtered_results = [hit for hit in results if hit['_score'] >= min_score]
        sorted_results = sorted(filtered_results, key=lambda x: x['_score'], reverse=True)
        
        # 提取所有INDIC_ID和对应的name_cn
        indic_id_name_map = {
            hit['_source'].get('indic_id', ''): hit['_source'].get('name_cn', '')
            for hit in sorted_results[:size]
            if hit['_source'].get('indic_id', '')
        }
        indic_ids = list(indic_id_name_map.keys())

        # 如果没有获取到INDIC_ID，直接返回空列表
        if not indic_ids:
            return [], []

        connection, cursor = connect_to_deloitte_db()

        if connection is None or cursor is None:
            print("数据库连接失败")
            return [], []  # 返回空结果列表和已获取的indic_ids

        # 查询unified_eco_data_view获取完整数据
        result_list = []
        try:
            # 构建IN查询的参数
            indic_ids_str = ','.join([f"'{id}'" for id in indic_ids])
            # 查询unified_eco_data_view
            sql = f"SELECT * FROM unified_eco_data_view WHERE indic_id IN ({indic_ids_str})"
            # print("执行的SQL查询:", sql)  # 打印SQL查询
            cursor.execute(sql)
            print(f"v1查询结果数据长度: {len(cursor.fetchall())}")
            # 获取列名
            columns = [desc[0] for desc in cursor.description]
            
            # 获取结果
            rows = cursor.fetchall()
            # print("数据库查询结果行数:", len(rows))  # 打印查询结果行数
            
            for row in rows:
                # print(row)
                # 将结果转换为字典
                result_dict = dict(zip(columns, row))
                # 处理特殊类型数据
                if 'publish_date' in result_dict:
                    result_dict['publish_date'] = result_dict['publish_date'].strftime('%Y-%m-%d') if result_dict['publish_date'] else None
                if 'period_date' in result_dict:
                    result_dict['period_date'] = result_dict['period_date'].strftime('%Y-%m-%d') if result_dict['period_date'] else None
                if 'update_time' in result_dict:
                    result_dict['update_time'] = result_dict['update_time'].strftime('%Y-%m-%d %H:%M:%S') if result_dict['update_time'] else None
                if 'data_value' in result_dict:
                    result_dict['data_value'] = float(result_dict['data_value']) if result_dict['data_value'] else None
                
                # 添加name_cn字段
                result_dict['name_cn'] = indic_id_name_map.get(result_dict['indic_id'], '')
                result_list.append(result_dict)

        except Exception as db_error:
            print(f"数据库查询错误: {str(db_error)}")
        finally:
            # 确保关闭连接
            if connection:
                connection.close()
        
        return result_list, indic_ids
    except Exception as e:
        print(f"搜索出错: {str(e)}")
        return [], []
    
def es_vector_query_eco_indicators_v2(query_text, year, size=15, min_score=0.8):
    """
    使用向量搜索查询经济数据

    Args:
        query_text (str): 查询文本
        year (list): 年份列表，如['2023']或['2022','2023','2024']
        size (int, optional): 返回结果数量，默认为15
        min_score (float, optional): 最小相似度分数，默认为0.8

    Returns:
        tuple: (结果列表, INDIC_ID列表)
    """
    try:
        # 预处理查询文本，去除停用词和特殊字符
        processed_query = preprocess_query(query_text)

        # 设置表名和向量字段
        table_name = "eco_info_deloitte"
        vector_field = "name_cn"

        # 执行搜索
        results = searcher.search(
            table_name=table_name,
            query=processed_query,
            vector_field=vector_field,
            size=size * 2  # 获取更多结果以便后续过滤
        )
        # 过滤低分结果并按分数排序
        filtered_results = [hit for hit in results if hit['_score'] >= min_score]
        sorted_results = sorted(filtered_results, key=lambda x: x['_score'], reverse=True)

        # 提取所有INDIC_ID和对应的name_cn以及unit_cn
        indic_id_info_map = {
            hit['_source'].get('indic_id', ''): {
                'name_cn': hit['_source'].get('name_cn', ''),
                'unit_cn': hit['_source'].get('unit_cn', '')
            }
            for hit in sorted_results[:size]
            if hit['_source'].get('indic_id', '')
        }
        indic_ids = list(indic_id_info_map.keys())

        # 如果没有获取到INDIC_ID，直接返回空列表
        if not indic_ids:
            return [], []

        connection, cursor = connect_to_deloitte_db()

        if connection is None or cursor is None:
            print("数据库连接失败")
            return [], indic_ids  # 返回空结果列表和已获取的indic_ids

        # 查询unified_eco_data_view获取完整数据
        result_list = []

        try:
            # 确保year是列表类型
            if not isinstance(year, (list, tuple)):
                year = [str(year)] if year else []

            # 构建IN查询的参数
            indic_ids_str = ','.join([f"'{id}'" for id in indic_ids])
            
            # 构建年份条件
            from datetime import datetime
            current_year = str(datetime.now().year)
            
            # 处理year参数，确保是字符串类型
            year_str = str(year[0]) if year and isinstance(year, (list, tuple)) else current_year

            # 使用to_char函数将period_date转换为字符串进行比较
            year_conditions = f"to_char(period_date, 'YYYY') = '{year_str}'"
            
            # 查询unified_eco_data_view
            sql = (f"SELECT * FROM unified_eco_data_view "
                f"WHERE indic_id IN ({indic_ids_str}) "
                f"AND {year_conditions} "
                f"LIMIT 30"
            )
            cursor.execute(sql)
            
            # 获取结果只调用一次fetchall()
            rows = cursor.fetchall()
            
            # 获取列名
            columns = [desc[0] for desc in cursor.description]
            
            # 获取当前年份
            # current_year = datetime.now().year
            
            # 使用已获取的rows数据
            for row in rows:
                # 将结果转换为字典
                result_dict = dict(zip(columns, row))
                # 处理特殊类型数据
                if 'publish_date' in result_dict:
                    result_dict['publish_date'] = result_dict['publish_date'].strftime('%Y-%m-%d') if result_dict['publish_date'] else None
                if 'period_date' in result_dict:
                    result_dict['period_date'] = result_dict['period_date'].strftime('%Y-%m-%d') if result_dict['period_date'] else None
                if 'update_time' in result_dict:
                    result_dict['update_time'] = result_dict['update_time'].strftime('%Y-%m-%d %H:%M:%S') if result_dict['update_time'] else None
                if 'data_value' in result_dict:
                    try:
                        result_dict['data_value'] = float(result_dict['data_value']) if result_dict['data_value'] else None
                    except (ValueError, TypeError):
                        result_dict['data_value'] = None

                # 添加name_cn和unit_cn字段
                indic_id = result_dict['indic_id']
                result_dict['name_cn'] = indic_id_info_map.get(indic_id, {}).get('name_cn', '')
                result_dict['unit_cn'] = indic_id_info_map.get(indic_id, {}).get('unit_cn', '')
                
                # 如果name_cn包含[停]则跳过该结果
                if '[停]' in result_dict['name_cn']:
                    continue
                    
                # # 检查update_time是否为当前年份
                # if not is_current_year(result_dict.get('update_time')):
                #     continue

                result_list.append(result_dict)

        except Exception as db_error:
            print(f"数据库查询错误: {str(db_error)}")
            return [], indic_ids  # 返回空结果列表和已获取的indic_ids
        finally:
            # 确保关闭连接
            if connection:
                connection.close()

        return result_list, indic_ids
    except Exception as e:
        print(f"搜索出错: {str(e)}")
        return [], []

def preprocess_query(query_text):
    """
    预处理查询文本，去除停用词和特殊字符
    
    Args:
        query_text (str): 原始查询文本
        
    Returns:
        str: 处理后的查询文本
    """
    # 这里可以实现更复杂的预处理逻辑
    # 例如分词、去除停用词等
    return query_text.strip()

def display_results(results):
    """
    显示搜索结果
    
    Args:
        results (list): 搜索结果列表
    """
    print(f"\n找到 {len(results)} 条结果:")
    for result in results:
        print(f"\nid: {result['id']}")
        print(f"title: {result['title']}")
        print(f"involved_industry_chain: {result['involved_industry_chain']}")
        print(f"policy_summary: {result['policy_summary']}")

def process_indicators(result_list):
    """
    处理指标列表，提取每个指标的总体信息
    
    Args:
        result_list (list): 包含指标数据的列表
        
    Returns:
        list: 包含每个指标汇总信息的列表
    """
    # 用字典来存储每个指标的信息
    indicator_info = {}
    
    # 首先按指标ID分组并收集所有日期
    for item in result_list:
        indic_id = item['indic_id']
        period_date = item.get('period_date')
        
        if indic_id not in indicator_info:
            indicator_info[indic_id] = {
                'indicId': indic_id,
                'title': item['name_cn'],
                'unit': item.get('unit_cn', ''),
                'dates': set(),  # 使用集合来存储唯一日期
                'count': 0  # 初始化数据点计数
            }
        
        if period_date:
            indicator_info[indic_id]['dates'].add(period_date)
        
        # 增加数据点计数
        indicator_info[indic_id]['count'] += 1
    
    # 转换为最终格式
    result = []
    for indic_id, info in indicator_info.items():
        if info['dates']:  # 只有当有日期数据时才添加
            result.append({
                'indicId': info['indicId'],
                'title': info['title'],
                'unit': info['unit'],
                'periodDateStart': min(info['dates']),  # 获取最早日期
                'periodDateEnd': max(info['dates']),    # 获取最晚日期
                'num': info['count']  # 添加数据点个数字段
            })
    
    return result

def count_dq_policy_data_rows():
    """
    查询 dq_policy_data 表的总行数。

    Returns:
        int: 表中的总行数。如果查询失败则返回 -1。
    """
    connection = None
    cursor = None
    total_rows = -1  # 默认为 -1 表示查询失败或未执行

    try:
        connection, cursor = connect_to_deloitte_db()
        if connection and cursor:
            # 执行 COUNT(*) 查询
            cursor.execute("SELECT COUNT(*) FROM dq_policy_data")
            # 获取查询结果 (只有一个结果，即行数)
            result = cursor.fetchone()
            if result:
                total_rows = result[0]
                # (可选) 不在这里打印，让主函数处理
                # print(f"dq_policy_data 表的总行数为: {total_rows}")
            else:
                 print("未能从 dq_policy_data 获取行数。")

        else:
            print("数据库连接失败，无法查询 dq_policy_data 行数。")

    except Psycopg2Error as e:
        print(f"查询 dq_policy_data 表总行数时出错: {e}")
    except Exception as e:
        print(f"查询 dq_policy_data 表总行数时发生未知错误: {e}")
    finally:
        # 确保关闭游标和连接
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            # (可选) 不在这里打印
            # print("数据库连接已关闭。")

    return total_rows


def es_vector_query_cics_name(query_text: str, size: int = 10):
    """
    对 data_original_cics_map 表进行向量查询

    Args:
        query_text (str): 查询文本
        size (int, optional): 返回结果数量. 默认为 10.

    Returns:
        list: 查询结果列表
    """
    try:
        # 初始化 HybridSearch
        search = HybridSearch(config_path='config.yaml')
        
        # 执行向量查询
        results = search.search(
            table_name='data_original_cics_map',
            query=query_text,
            vector_field='name',
            size=size
        )
        
        return results
    
    except Exception as e:
        print(f"向量查询失败: {e}")
        return []






def es_query_cics_industry(query_text: str, size: int = 10):
    """
    使用ES全文检索查询cics_industry表
    
    Args:
        query_text (str): 查询文本
        size (int, optional): 返回结果数量. 默认为 10.
        
    Returns:
        list: 包含name和id的字典列表
    """
    try:
        # 执行ES全文检索
        results = searcher.search(
            table_name='cics_industry',
            query=query_text,
            size=size
        )
        
        # 提取name和id
        return [{
            'name': hit['_source'].get('name', ''),
            'id': hit['_id']
        } for hit in results]
    
    except Exception as e:
        print(f"ES全文检索失败: {e}")
        return []

def es_vector_query_cics_industry(query_text: str, size: int = 10):
    """
    使用ES向量检索查询cics_industry表
    
    Args:
        query_text (str): 查询文本
        size (int, optional): 返回结果数量. 默认为 10.
        
    Returns:
        list: 包含name和id的字典列表
    """
    try:
        # 执行ES向量检索
        results = searcher.search(
            table_name='cics_industry',
            query=query_text,
            vector_field='name',
            size=size
        )
        
        # 提取name和id
        return [{
            'name': hit['_source'].get('name', ''),
            'id': hit['_id']
        } for hit in results]
    
    except Exception as e:
        print(f"ES向量检索失败: {e}")
        return []

def es_hybrid_query_cics_industry(query_text: str, size: int = 10):
    """
    使用ES混合检索（全文+向量）查询cics_industry表
    
    Args:
        query_text (str): 查询文本
        size (int, optional): 返回结果数量. 默认为 10.
        
    Returns:
        list: 包含name和id的字典列表
    """
    try:
        # 使用新增的hybrid_search方法
        results = searcher.hybrid_search(
            table_name='cics_industry',
            query=query_text,
            vector_field='name',
            size=size,
            text_boost=0.4,    # 文本匹配权重
            vector_boost=0.6,  # 向量相似度权重
            min_score=0.2      # 最低分数阈值
        )
        
        # 提取name和id
        return [{
            'name': hit['_source'].get('name', ''),
            'id': hit['_id'],
            'score': hit['_score']
        } for hit in results]
    
    except Exception as e:
        print(f"ES混合检索失败: {str(e)}")
        return []


# ===== 新增的主函数入口 =====
if __name__ == "__main__":
    # print(1)
    # 获取当前数据库中的所有表
    # connection = None
    # cursor = None
    #
    # try:
    #     connection, cursor = connect_to_deloitte_db()
    #     if connection and cursor:
    #         # 查询所有表名
    #         cursor.execute("""
    #             SELECT table_name 
    #             FROM information_schema.tables 
    #             WHERE table_schema = 'public'
    #         """)
    #         tables = cursor.fetchall()
            
    #         if tables:
    #             print("当前数据库中的表：")
    #             for table in tables:
    #                 print(f"- {table[0]}")
    #         else:
    #             print("数据库中没有表")
    #     else:
    #         print("数据库连接失败，无法查询表信息")
            
    # except Psycopg2Error as e:
    #     print(f"查询数据库表时出错: {e}")
    # except Exception as e:
    #     print(f"查询数据库表时发生未知错误: {e}")
    # finally:
    #     # 确保关闭游标和连接
    #     if cursor:
    #         cursor.close()
    #     if connection:
    #         connection.close()

    # try:
    #     connection, cursor = connect_to_deloitte_db()
    #     if connection and cursor:
    #         # 查询 data_original_cics_map 表的前5行数据
    #         cursor.execute("SELECT COUNT(*) FROM sc_policy_relation")
    #         rows = cursor.fetchall()
    #
    #         if rows:
    #             # 获取列名
    #             colnames = [desc[0] for desc in cursor.description]
    #             # 创建DataFrame
    #             import pandas as pd
    #             df = pd.DataFrame(rows, columns=colnames)
    #             print("data_original_cics_map 表前5行数据：")
    #             print(df)
    #         else:
    #             print("data_original_cics_map 表为空")
    #     else:
    #         print("数据库连接失败，无法查询表数据")
    #
    # except Psycopg2Error as e:
    #     print(f"查询 data_original_cics_map 表时出错: {e}")
    # except Exception as e:
    #     print(f"查询 data_original_cics_map 表时发生未知错误: {e}")
    # finally:
    #     # 确保关闭游标和连接
    #     if cursor:
    #         cursor.close()
    #     if connection:
    #         connection.close()


    topic = "2024年新能源汽车行业发展趋势"
    current_title = "政策驱动与行业趋势分析/碳中和目标下的政策导向与市场影响/智能驾驶相关政策的技术支持与规范"
    analyze_instruction = "结合碳中和目标，分析智能驾驶政策如何通过技术支持与规范推动市场发展，探讨其对行业趋势的具体影响及潜在机遇。"

    # 获取 current_title 的第一级和第三级标题
    title_parts = current_title.split('/')
    if len(title_parts) >= 3:
        first_level_title = title_parts[0]
        third_level_title = title_parts[2]
        print(f"第一级标题: {first_level_title}")
        print(f"第三级标题: {third_level_title}")
    else:
        first_level_title = title_parts[0] if len(title_parts) >= 1 else ""
        third_level_title = title_parts[2] if len(title_parts) >= 3 else ""
        print("current_title 不包含足够的层级用于提取第一级和第三级标题")

    query_text = f"{third_level_title}-{topic}"
    # # topic = "2023年中国新能源汽车行业全景分析"
    # # current_title = "政策驱动与行业趋势分析/碳中和目标下的政策导向与市场影响/智能驾驶相关政策的技术支持与规范/"
    # # analyze_instruction = "结合碳中和目标，分析智能驾驶政策如何通过技术支持与规范推动市场发展，探讨其对行业趋势的具体影响及潜在机遇。"

    #cics 测试查询v2
    print(f"current_query_text:{query_text}")

    # 调用 es_query_cics_industry（全文检索）
    print("【ES全文检索 cics_industry】")
    cics_fulltext_results = es_query_cics_industry(query_text, size=10)
    if cics_fulltext_results:
        for item in cics_fulltext_results:
            print(f"- id: {item['id']}, name: {item['name']}")
    else:
        print("未检索到匹配的CICS行业（全文检索）")

    print("\n【ES向量检索 cics_industry】")
    cics_vector_results = es_vector_query_cics_industry(query_text, size=10)
    if cics_vector_results:
        for item in cics_vector_results:
            print(f"- id: {item['id']}, name: {item['name']}")
    else:
        print("未检索到匹配的CICS行业（向量检索）")

    print("\n【ES混合检索 cics_industry】")
    cics_hybrid_results = es_hybrid_query_cics_industry(query_text, size=10)
    if cics_hybrid_results:
        for item in cics_hybrid_results:
            print(f"- id: {item['id']}, name: {item['name']}")
    else:
        print("未检索到匹配的CICS行业（混合检索）")



    #cics 测试查询v1
    # # 获取第三级标题
    # title_parts = current_title.split('/')
    # if len(title_parts) >= 3:
    #     third_level_title = title_parts[2]
    #     print(f"第三级标题: {third_level_title}")
    # else:
    #     print("当前标题没有第三级")

    # query_text = f"{third_level_title}-{topic}"
    # # query_text = f"{third_level_title}"
    # # query_text = current_title

    # # 调用es_vector_query_cics_name函数进行查询
    # cics_names = es_vector_query_cics_name(query_text)
    
    # # 打印检索到的cics_id和name
    # if cics_names:
    #     print("检索到的CICS信息：")
    #     for item in cics_names:
    #         cics_id = item['_source']['cics_id']
    #         name = item['_source']['name']
    #         print(f"- cics_id: {cics_id}, name: {name}")
    # else:
    #     print("未检索到匹配的CICS信息")




    #政策信息查询
    # policy_info,policy_ids = es_vector_query_policy_info(query_text,size = 10,min_score=0.7)
    # print(f"基于向量 min_score = 0.7,找到 {len(policy_info)} 条结果:")
    # print("="*50)
    # print("="*50)
    # for policy in policy_info:
    #     print(policy['title'])
    #     print('-'*50)
    #     # print(policy['policy_summary'])
    # # print(policy_info)
    # # print(policy_ids)
    # print("="*50)
    # print("="*50)

    # policy_info, policy_ids = es_vector_query_policy_info(query_text, size=10, min_score=0.7)
    # print(f"基于向量 min_score = 0.8,找到 {len(policy_info)} 条结果:")
    # print("=" * 50)
    # print("=" * 50)
    # for policy in policy_info:
    #     print(policy['title'])
    #     # print(policy['policy_summary'])
    #     print('-'*50)
    # # print(policy_info)
    # # print(policy_ids)
    # print("=" * 50)
    # print("=" * 50)

    # policy_info_keyword,policy_ids_keyword = es_keyword_query_policy_info(query_text,size = 10,min_score=0.6)
    # print(f"基于关键词 min_score = 0.6, 找到 {len(policy_info_keyword)} 条结果:")
    # print("="*50)
    # print("="*50)
    # for policy in policy_info_keyword:
    #     print(policy['title'])
    #     print('-'*50)
    # # print(policy_info_keyword)
    # # print(policy_ids_keyword)
    # print("="*50)
    # print("="*50)


    # policy_info_keyword_v2,policy_ids_keyword_v2 = es_keyword_query_policy_info(query_text,size = 10,min_score=0.7)
    # print(f"基于关键词 min_score = 0.7, 找到 {len(policy_info_keyword_v2)} 条结果:")
    # print("="*50)
    # print("="*50)
    # for policy in policy_info_keyword_v2:
    #     print(policy['title'])
    #     print('-'*50)
    #     # print(policy['policy_summary'])
    # # print(policy_info_keyword_v2)
    # # print(policy_ids_keyword_v2)
    # print("="*50)
    # print("="*50)

    # policy_info_keyword_v3,policy_ids_keyword_v3 = es_keyword_query_policy_info(query_text,size = 10,min_score=0.8)
    # print(f"基于关键词,min_score = 0.8, 找到 {len(policy_info_keyword_v3)} 条结果:")
    # print("="*50)
    # print("="*50)
    # for policy in policy_info_keyword_v3:
    #     print(policy['title'])
    #     print('-'*50)   
    #     # print(policy['policy_summary'])
    # # print(policy_info_keyword_v3)
    # # print(policy_ids_keyword_v3)
    # print("="*50)
    # print("="*50)



    # print("开始查询 dq_policy_data 表的行数...")
    # count = count_dq_policy_data_rows()
    #
    # if count >= 0: # 检查是否成功获取行数 (0 也是有效行数)
    #     print(f"查询结果: dq_policy_data 表共有 {count} 行数据。")
    # else:
    #     print("查询失败，未能获取 dq_policy_data 表的行数。")
    #
    # print("\n主程序执行完毕。如果需要执行其他测试，请取消下面的注释。")

