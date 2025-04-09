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


def es_vector_query(query_text, table_name="sc_policy_detail", vector_field="title", size=10, min_score=0.2, use_multi_fields=True):
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
                        'org_name': org_name  # 添加查询到的org_name
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
                    'org_name': ''  # 数据库连接失败时返回空字符串
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
    
def es_vector_query_eco_indicators_v2(query_text, year, size=10, min_score=0.5):
    """
    使用向量搜索查询经济数据

    Args:
        query_text (str): 查询文本
        year (list): 年份列表，如['2023']或['2022','2023','2024']
        size (int, optional): 返回结果数量，默认为10
        min_score (float, optional): 最小相似度分数，默认为0.5

    Returns:
        tuple: (结果列表, INDIC_ID列表, 指标信息列表)
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
            return [], [], []

        connection, cursor = connect_to_deloitte_db()

        if connection is None or cursor is None:
            print("数据库连接失败")
            return [], indic_ids, []  # 返回空结果列表和已获取的indic_ids

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
            # year_conditions = f"to_char(period_date, 'YYYY') = '{year_str}'"
            
            # 查询unified_eco_data_view
            sql = (f"SELECT * FROM unified_eco_data_view WHERE indic_id IN ({indic_ids_str}) limit 30"
                   # f"AND {year_conditions}"
                   )
            # print("执行的SQL查询:", sql)  # 添加SQL打印
            cursor.execute(sql)
            
            # 获取结果只调用一次fetchall()
            rows = cursor.fetchall()
            # print(f"v2查询结果数据长度: {len(rows)}")
            
            # 获取列名
            columns = [desc[0] for desc in cursor.description]
            
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

                # 添加name_cn字段
                result_dict['name_cn'] = indic_id_name_map.get(result_dict['indic_id'], '')
                result_list.append(result_dict)
            # print(f"result_list : {result_list}")
            # print(len(result_list))
            # 使用process_indicators处理结果列表，获取汇总的指标信息
            indicator_info_summary = process_indicators(result_list)

        except Exception as db_error:
            print(f"数据库查询错误: {str(db_error)}")
            return [], indic_ids, []  # 返回空结果列表和已获取的indic_ids
        finally:
            # 确保关闭连接
            if connection:
                connection.close()

        return result_list, indic_ids, indicator_info_summary
    except Exception as e:
        print(f"搜索出错: {str(e)}")
        return [], [], []

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
                'dates': set()  # 使用集合来存储唯一日期
            }
        
        if period_date:
            indicator_info[indic_id]['dates'].add(period_date)
    
    # 转换为最终格式
    result = []
    for indic_id, info in indicator_info.items():
        if info['dates']:  # 只有当有日期数据时才添加
            result.append({
                'indicId': info['indicId'],
                'title': info['title'],
                'periodDateStart': min(info['dates']),  # 获取最早日期
                'periodDateEnd': max(info['dates'])     # 获取最晚日期
            })
    
    return result



if __name__ == "__main__":
    # 测试查询函数
    query_text = "2023年新能源"
    result_list, indic_ids, indicator_info_summary = es_vector_query_eco_indicators_v2(query_text, 2024)
    print(result_list)
    print(indic_ids)
    print(indicator_info_summary)


    # # 连接数据库
    # connection, cursor = connect_to_deloitte_db()
    # if connection and cursor:
    #     try:
    #         # 构建SQL查询语句，查询指定ID
    #         sql_query = "SELECT * FROM unified_eco_data_view WHERE indic_id = '2180000492'"
    #         cursor.execute(sql_query)
            
    #         # 获取所有匹配的数据
    #         rows = cursor.fetchall()
            
    #         # 获取列名
    #         columns = [desc[0] for desc in cursor.description]
            
    #         # 将结果转换为字典列表
    #         result_list = []
    #         for row in rows:
    #             row_dict = dict(zip(columns, row))
    #             # 处理特殊类型数据
    #             if 'publish_date' in row_dict:
    #                 row_dict['publish_date'] = row_dict['publish_date'].strftime('%Y-%m-%d') if row_dict['publish_date'] else None
    #             if 'period_date' in row_dict:
    #                 row_dict['period_date'] = row_dict['period_date'].strftime('%Y-%m-%d') if row_dict['period_date'] else None
    #             if 'update_time' in row_dict:
    #                 row_dict['update_time'] = row_dict['update_time'].strftime('%Y-%m-%d %H:%M:%S') if row_dict['update_time'] else None
    #             if 'data_value' in row_dict:
    #                 row_dict['data_value'] = float(row_dict['data_value']) if row_dict['data_value'] else None
    #             result_list.append(row_dict)
            
    #         # 打印查询结果
    #         if result_list:
    #             print(f"找到 {len(result_list)} 条匹配记录：")
    #             for result in result_list:
    #                 print(result)
    #         else:
    #             print("未找到ID为2070900170的记录")
                
    #     except Exception as e:
    #         print(f"查询unified_eco_data_view时出错: {str(e)}")
    #     finally:
    #         # 关闭数据库连接
    #         connection.close()
    # else:
    #     print("数据库连接失败")

