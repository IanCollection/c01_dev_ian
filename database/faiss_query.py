import os
import sys

# 将项目根目录添加到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import json
import numpy as np
import faiss
import time
from dotenv import load_dotenv
from neo4j import GraphDatabase
from utils.vector_generator import get_embedding_single_text
from database.neo4j_query import query_file_node, query_header_node, query_content_node, query_file_batch_nodes, \
    query_header_batch_nodes, query_content_batch_nodes

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
    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database", "faiss_index_sc")

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

    # 尝试使用GPU
    if faiss.get_num_gpus() > 0 and index_type != 'filename':
        try:
            print("检测到GPU，转移索引到GPU...")
            res = faiss.StandardGpuResources()
            index = faiss.index_cpu_to_gpu(res, 0, index)
            print("成功将索引转移到GPU")
        except Exception as e:
            print(f"GPU加载失败，使用CPU模式: {e}")
    else:
        print("未检测到GPU或为filename索引，使用CPU模式")

    # 加载ID列表
    if index_type == 'filename':
        with open(ids_path, 'r', encoding='utf-8') as f:
            ids = json.load(f)
    else:
        ids = np.load(ids_path)

    return index, ids


# 检索函数
def search_faiss(query, index_type, top_k=10):
    """
    在FAISS索引中搜索最相似的文档，优先使用GPU加速并记录检索时间

    Args:
        query (str): 查询文本
        index_type (str): 索引类型，可选值为 'filename', 'header', 'content'
        top_k (int): 返回的结果数量

    Returns:
        tuple: (检索结果ID数组, 检索时间信息)
    """
    try:
        # 记录总开始时间
        start_time = time.time()

        # 加载索引
        base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database",
                                 "faiss_index_sc")

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

        # 记录索引加载开始时间
        load_start_time = time.time()

        # 加载索引
        index = faiss.read_index(index_path)

        # 记录索引加载时间
        load_time = time.time() - load_start_time
        print(f"索引加载耗时: {load_time:.4f}秒")

        # 加载元数据以获取最优nprobe值
        meta_file = os.path.join(base_path, f"{index_type}_meta.json")
        if os.path.exists(meta_file) and hasattr(index, 'nprobe'):
            try:
                with open(meta_file, 'r') as f:
                    meta_data = json.load(f)

                # 获取nlist和向量总数
                nlist = meta_data.get('parameters', {}).get('nlist', 100)
                total_vectors = meta_data.get('total_vectors', 1000)

                # 设置最优nprobe值
                if hasattr(index, 'nlist'):
                    optimal_nprobe = max(1, min(index.nlist, int(index.nlist * 0.1)))
                    if total_vectors > 100000:
                        optimal_nprobe = max(1, min(index.nlist, int(index.nlist * 0.05)))
                    index.nprobe = optimal_nprobe
                    print(f"设置nprobe={optimal_nprobe}，基于nlist={index.nlist}和向量数={total_vectors}")
            except Exception as e:
                print(f"读取元数据设置nprobe失败: {e}，使用默认值")
                # 默认设置
                if hasattr(index, 'nprobe') and hasattr(index, 'nlist'):
                    index.nprobe = min(index.nlist, 16)

        # 尝试使用GPU加速
        use_gpu = False
        gpu_transfer_time = 0
        gpu_id = None

        if faiss.get_num_gpus() > 0 and index_type != 'filename':
            try:
                print("检测到GPU，尝试使用GPU加速检索...")
                gpu_start_time = time.time()

                # 获取可用GPU
                n_gpus = faiss.get_num_gpus()
                gpu_id = 0  # 默认使用第一个GPU

                # 尝试获取GPU内存信息选择最佳GPU
                try:
                    import torch
                    if torch.cuda.is_available():
                        gpu_mem_free = []
                        for i in range(n_gpus):
                            free_mem = torch.cuda.get_device_properties(i).total_memory - torch.cuda.memory_allocated(i)
                            gpu_mem_free.append((i, free_mem))

                        # 选择内存最大的GPU
                        gpu_id, _ = max(gpu_mem_free, key=lambda x: x[1])
                        print(f"选择GPU {gpu_id} 作为最佳设备")
                except Exception as e:
                    print(f"获取GPU详细信息失败: {e}，使用默认GPU 0")

                # 创建GPU资源
                res = faiss.StandardGpuResources()

                # 创建GPU配置选项
                gpu_options = faiss.GpuClonerOptions()
                gpu_options.useFloat16 = True  # 使用半精度以节省GPU内存

                # 将索引移至GPU
                index = faiss.index_cpu_to_gpu(res, gpu_id, index, gpu_options)
                use_gpu = True

                gpu_transfer_time = time.time() - gpu_start_time
                print(f"成功将索引转移到GPU {gpu_id}，耗时: {gpu_transfer_time:.4f}秒")
            except Exception as e:
                print(f"GPU加速初始化失败: {str(e)}，使用CPU模式")
        else:
            print("未检测到GPU或为filename索引，使用CPU模式")

        # 记录向量生成开始时间
        vector_start_time = time.time()

        # 将查询文本转换为向量
        query_vector = get_embedding_single_text(query)
        query_vector = np.array(query_vector, dtype=np.float32)

        # 记录向量生成时间
        vector_time = time.time() - vector_start_time
        print(f"查询向量生成耗时: {vector_time:.4f}秒")

        # 如果是一维的 (512,) 则重塑为 (1, 512)
        if len(query_vector.shape) == 1:
            query_vector = query_vector.reshape(1, -1)

        # 记录搜索开始时间
        search_start_time = time.time()

        # 执行搜索
        distances, indices = index.search(query_vector, top_k)

        # 记录搜索时间
        search_time = time.time() - search_start_time
        print(f"实际搜索耗时: {search_time:.4f}秒")

        # 记录总检索时间
        total_time = time.time() - start_time
        print(f"检索完成，使用{'GPU' if use_gpu else 'CPU'}，总耗时: {total_time:.4f}秒")

        # 记录检索信息到日志
        log_search_metrics(
            query=query,
            index_type=index_type,
            top_k=top_k,
            use_gpu=use_gpu,
            gpu_id=gpu_id,
            total_time=total_time,
            load_time=load_time,
            vector_time=vector_time,
            search_time=search_time,
            gpu_transfer_time=gpu_transfer_time,
            result_count=len(indices[0])
        )

        # 清理GPU内存
        if use_gpu:
            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                    print("已清理GPU缓存")
            except Exception as e:
                print(f"清理GPU缓存失败: {str(e)}")

        # 返回检索结果和时间信息
        time_info = {
            'total_time': total_time,
            'load_time': load_time,
            'vector_time': vector_time,
            'search_time': search_time,
            'gpu_transfer_time': gpu_transfer_time,
            'use_gpu': use_gpu,
            'gpu_id': gpu_id
        }

        return indices[0], time_info

    except Exception as e:
        print(f"检索过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        end_time = time.time()
        return np.array([], dtype=np.int64), {'total_time': end_time - start_time, 'error': str(e)}


def log_search_metrics(query, index_type, top_k, use_gpu, gpu_id, total_time,
                       load_time, vector_time, search_time, gpu_transfer_time, result_count):
    """
    记录检索指标到日志文件

    Args:
        query (str): 查询文本
        index_type (str): 索引类型
        top_k (int): 检索结果数量
        use_gpu (bool): 是否使用GPU
        gpu_id (int): GPU设备ID
        total_time (float): 总检索时间
        load_time (float): 索引加载时间
        vector_time (float): 向量生成时间
        search_time (float): 实际搜索时间
        gpu_transfer_time (float): GPU转移时间
        result_count (int): 结果数量
    """
    try:
        # 确保日志目录存在
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
        os.makedirs(log_dir, exist_ok=True)

        # 日志文件路径
        log_file = os.path.join(log_dir, "faiss_search_metrics.log")

        # 创建日志条目
        log_entry = {
            'query': query,
            'index_type': index_type,
            'top_k': top_k,
            'use_gpu': use_gpu,
            'gpu_id': gpu_id,
            'total_time': total_time,
            'load_time': load_time,
            'vector_time': vector_time,
            'search_time': search_time,
            'gpu_transfer_time': gpu_transfer_time,
            'result_count': result_count,
            'query_length': len(query)
        }

        # 追加到日志文件
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')

    except Exception as e:
        print(f"记录检索指标失败: {str(e)}")


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
def search_and_query(query, index_type='content', top_k=10, with_details=True):
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
    faiss_results_ids, time_info = search_faiss(query, index_type, top_k)
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


def search(query, index_type='content', top_k=5):
    """
    综合检索函数，支持检索文件名、标题和内容

    Args:
        query (str): 查询文本
        index_type (str): 索引类型，可选值为 'filename', 'header', 'content'
        top_k (int): 返回的结果数量


    Returns:
        list: 检索结果列表
    """
    # 执行FAISS检索
    # start_time = time.time()
    faiss_results_ids, _ = search_faiss(query, index_type, top_k)
    # end_time = time.time()
    # 对faiss_results_ids进行去重
    faiss_results_ids = list(set(faiss_results_ids))
    return faiss_results_ids


# 示例用法
if __name__ == "__main__":
    # print(1)
    # # 加载filename索引并打印维度
    try:
        base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database", "faiss_index_sc")
        
        # 文件名索引
        filename_index_path = os.path.join(base_path, "filename_index_flat.index")
        filename_ids_path = os.path.join(base_path, "filename_index_flat_ids.json")
        
        # 标题索引
        header_index_path = os.path.join(base_path, "header_index_IVFPQ.index")
        header_ids_path = os.path.join(base_path, "header_ids.npy")
        
        # 内容索引
        content_index_path = os.path.join(base_path, "content_index_IVFPQ.index")
        content_ids_path = os.path.join(base_path, "content_ids.npy")

        # 加载文件名索引
        print("正在加载filename索引...")
        filename_index = faiss.read_index(filename_index_path)
        print(f"filename索引维度: {filename_index.d}")
        print(f"filename索引类型: {type(filename_index).__name__}")
        print(f"filename向量总数: {filename_index.ntotal}")
        with open(filename_ids_path, 'r', encoding='utf-8') as f:
            filename_ids = json.load(f)
        print(f"filename IDs数量: {len(filename_ids)}")
        print(f"filename IDs示例: {filename_ids[:5] if len(filename_ids) > 5 else filename_ids}")
        
        # 加载标题索引
        print("\n正在加载header索引...")
        header_index = faiss.read_index(header_index_path)
        print(f"header索引维度: {header_index.d}")
        print(f"header索引类型: {type(header_index).__name__}")
        print(f"header向量总数: {header_index.ntotal}")
        header_ids = np.load(header_ids_path)
        print(f"header IDs数量: {len(header_ids)}")
        print(f"header IDs示例: {header_ids[:5] if len(header_ids) > 5 else header_ids}")
        
        # 加载内容索引
        print("\n正在加载content索引...")
        content_index = faiss.read_index(content_index_path)
        print(f"content索引维度: {content_index.d}")
        print(f"content索引类型: {type(content_index).__name__}")
        print(f"content向量总数: {content_index.ntotal}")
        content_ids = np.load(content_ids_path)
        print(f"content IDs数量: {len(content_ids)}")
        print(f"content IDs示例: {content_ids[:5] if len(content_ids) > 5 else content_ids}")
    except Exception as e:
        print(f"加载索引失败: {e}")



    # 示例查询
    # query_text = "商业地产行业展望"
    # result = search_and_query(query_text)
    # print(result)

    # result = search_and_query(query_text, index_type='header')
    # print(f"header: {result}")


    # reuslt = search_and_query(query_text, index_type='filename')
    # print(f"filename: {result}")




    # print("在文件名中搜索:")
    # filename_results = search(query_text, index_type='filename')
    # print(filename_results)
    # # 在标题中搜索
    # print("\n在标题中搜索:")
    # header_results = search(query_text, index_type='header')
    # print(header_results)
    # # 在内容中搜索
    # print("\n在内容中搜索:")
    # content_results = search(query_text, index_type='content')
    # print(content_results)