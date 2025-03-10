import os
import json
import numpy as np
import faiss
import time
from dotenv import load_dotenv
from neo4j import GraphDatabase
from utils.vector_generator import get_embedding_single_text
from database.neo4j_query import query_file_node, query_header_node, query_content_node,query_file_batch_nodes,query_header_batch_nodes,query_content_batch_nodes

# 加载环境变量
load_dotenv()

# 连接Neo4j数据库
def get_neo4j_driver():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    return GraphDatabase.driver(uri, auth=(username, password))

# 加载FAISS索引
def load_faiss_index(index_type):
    """
    加载指定类型的FAISS索引
    
    Args:
        index_type (str): 索引类型，可选值为 'filename', 'header', 'content'
    
    Returns:
        tuple: (faiss索引, id列表)
    """
    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database", "faiss_index")
    
    if index_type == 'filename':
        index_path = os.path.join(base_path, "filename_index_flat.index")
        ids_path = os.path.join(base_path, "filename_index_flat_ids.json")
    elif index_type in ['header', 'content']:
        index_path = os.path.join(base_path, f"{index_type}_index_IVFPQ.index")
        ids_path = os.path.join(base_path, f"{index_type}_ids.npy")
    else:
        raise ValueError(f"不支持的索引类型: {index_type}")
    
    # 检查文件是否存在
    if not os.path.exists(index_path):
        raise FileNotFoundError(f"索引文件不存在: {index_path}")
    
    if not os.path.exists(ids_path):
        raise FileNotFoundError(f"ID文件不存在: {ids_path}")
    
    # 加载索引
    index = faiss.read_index(index_path)
    
    # 加载ID列表
    if index_type == 'filename':
        with open(ids_path, 'r', encoding='utf-8') as f:
            ids = json.load(f)
    else:
        ids = np.load(ids_path)
    
    return index, ids

# 检索函数
def search_faiss(query, index_type, top_k=1):
    """
    在FAISS索引中搜索最相似的文档
    """
    try:
        # 加载模型和索引
        index, ids = load_faiss_index(index_type)

        # 将查询文本转换为向量
        query_vector = get_embedding_single_text(query)
        query_vector = np.array(query_vector, dtype=np.float32)

        # 如果是一维的 (512,) 则重塑为 (1, 512)
        if len(query_vector.shape) == 1:
            query_vector = query_vector.reshape(1, -1)

        # 执行搜索
        distances, indices = index.search(query_vector, top_k)
        return indices[0]

    except FileNotFoundError as e:
        print(f"错误: {e}")
        return []

# 从Neo4j获取检索结果的详细信息
def get_details_from_neo4j(result_ids, index_type):
    """
    从Neo4j数据库获取检索结果的详细信息
    
    Args:
        result_ids (list): 检索结果ID列表
        index_type (str): 索引类型，可选值为 'filename', 'header', 'content'
    
    Returns:
        list: 包含详细信息的字典列表
    """
    if not result_ids:
        return []
        
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            if index_type == 'filename':
                query = """
                MATCH (f:File) WHERE f.id IN $ids
                RETURN f.id AS id, f.title AS title, f.org_name AS org_name, 
                       f.file_url AS file_url, f.to_char AS date
                """
            elif index_type == 'header':
                query = """
                MATCH (h:Header) WHERE h.id IN $ids
                MATCH (f:File)-[:HAS_HEADER]->(h)
                RETURN h.id AS id, h.content AS content, h.level AS level, 
                       f.id AS file_id, f.title AS file_title
                """
            elif index_type == 'content':
                query = """
                MATCH (c:Content) WHERE c.id IN $ids
                MATCH (f:File)-[:HAS_CONTENT]->(c)
                RETURN c.id AS id, c.content AS content, c.page_idx AS page_idx,
                       f.id AS file_id, f.title AS file_title
                """
            
            result = session.run(query, ids=result_ids)
            return [dict(record) for record in result]
    except Exception as e:
        print(f"从Neo4j获取数据时发生错误: {e}")
        return []
    finally:
        driver.close()

# 综合检索函数
def search_and_query(query, index_type='content', top_k=2, with_details=True):
    """
    综合检索函数，支持检索文件名、标题和内容
    
    Args:
        query (str): 查询文本
        index_type (str): 索引类型，可选值为 'filename', 'header', 'content'
        top_k (int): 返回的结果数量
        with_details (bool): 是否返回详细信息
    
    Returns:
        list: 检索结果列表
    """
    # 执行FAISS检索
    start_time = time.time()
    faiss_results_ids = search_faiss(query, index_type, top_k)
    end_time = time.time()
    print(f'完成检索，耗时: {end_time - start_time:.2f}秒')
    details = []
    if len(faiss_results_ids) == 1:
        result_id = faiss_results_ids[0]
        if index_type == "filename":
            # 如果是文件名检索,直接从Neo4j获取文件节点信息
            file_node = query_file_node(result_id)
            details.append(file_node)
        elif index_type == "header":
            # 如果是标题检索,获取标题节点及其关联的文件信息
            header_node = query_header_node(result_id)
            details.append(header_node)
        else:
            # 如果是内容检索,获取内容节点及其关联的文件信息
            content_node = query_content_node(result_id)
            details.append(content_node)
    else:
        faiss_results_ids = list(faiss_results_ids)
        # 批量查询
        if index_type == "filename":
            details = query_file_batch_nodes(faiss_results_ids)
        elif index_type == "header":
            details = query_header_batch_nodes(faiss_results_ids)
        else:
            details = query_content_batch_nodes(faiss_results_ids)


    # # 从Neo4j获取详细信息
    # details = get_details_from_neo4j(faiss_results, index_type)
    #
    # # 将相似度分数添加到详细信息中
    # id_to_score = {item["id"]: item["score"] for item in faiss_results}
    # for detail in details:
    #     detail["similarity_score"] = id_to_score.get(detail["id"], 0)
    #
    # # 按相似度分数排序
    # details.sort(key=lambda x: x["similarity_score"], reverse=True)
    #
    return details



def search(query, index_type='content', top_k=2, with_details=True):
    """
    综合检索函数，支持检索文件名、标题和内容
    
    Args:
        query (str): 查询文本
        index_type (str): 索引类型，可选值为 'filename', 'header', 'content'
        top_k (int): 返回的结果数量
        with_details (bool): 是否返回详细信息
    
    Returns:
        list: 检索结果列表
    """
    # 执行FAISS检索
    start_time = time.time()
    faiss_results_ids = search_faiss(query, index_type, top_k)
    end_time = time.time()
    return faiss_results_ids



# 示例用法
if __name__ == "__main__":
    # 示例查询
    query_text = "商业地产行业展望"

    print("在文件名中搜索:")
    filename_results = search(query_text, index_type='filename')
    print(filename_results)
    # 在标题中搜索
    print("\n在标题中搜索:")
    header_results = search(query_text, index_type='header')
    print(header_results)
    # 在内容中搜索
    print("\n在内容中搜索:")
    content_results = search(query_text, index_type='content')
    print(content_results)