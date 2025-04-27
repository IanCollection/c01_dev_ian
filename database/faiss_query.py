import os
import sys
import functools
import threading  # 添加线程模块

from pandas.core.indexers import validate_indices

# 将项目根目录添加到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import json
import numpy as np
import faiss
import time
import subprocess
import logging
from dotenv import load_dotenv
from neo4j import GraphDatabase
from utils.vector_generator import get_embedding_single_text
from database.neo4j_query import query_file_node, query_header_node, query_content_node, query_file_batch_nodes, \
    query_header_batch_nodes, query_content_batch_nodes

# 导入全局 FAISS 资源模块
from database.faiss_globals import get_faiss_resources

# --- 新增：并发控制 ---
MAX_GPU_CONCURRENT_QUERIES = 10
_faiss_gpu_concurrency_semaphore = threading.Semaphore(MAX_GPU_CONCURRENT_QUERIES)
# ---------------------

# 加载环境变量
load_dotenv()

# 连接Neo4j数据库
def get_neo4j_driver():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    return GraphDatabase.driver(uri, auth=(username, password))


# 获取 GPU 状态函数 (可以保留用于监控)
def get_gpu_status():
    """获取当前 GPU 使用状态"""
    try:
        result = subprocess.check_output("nvidia-smi --query-gpu=timestamp,name,memory.used,memory.total,utilization.gpu --format=csv", shell=True)
        return result.decode('utf-8').strip()
    except Exception as e:
        return f"无法获取 GPU 状态: {str(e)}"

# 检索函数 - 重构：动态克隆到GPU并使用信号量控制并发
def search_faiss_dynamic_gpu(query, cpu_index, index_type, ids_mapping, top_k=10):
    """
    在FAISS索引中搜索最相似的文档ID。
    对于 'header' 或 'content' 索引，尝试使用信号量控制并发，
    动态克隆CPU索引到GPU进行查询，查询后释放GPU资源。
    如果GPU不可用或并发数达到上限，则回退到CPU查询。
    对于 'filename' 索引，始终在CPU上查询。

    Args:
        query (str): 查询文本
        cpu_index (faiss.Index): 预加载的 CPU Faiss 索引对象
        index_type (str): 索引类型 ('filename', 'header', 'content')
        ids_mapping (list | np.ndarray): 与 cpu_index 对应的 ID 列表或数组
        top_k (int): 返回的结果数量

    Returns:
        list: 匹配的ID列表
    """
    if cpu_index is None:
        raise ValueError(f"预加载的 CPU 索引 '{index_type}' 不可用。")

    timing_info = {
        "total_time_ms": 0, "load_time_ms": 0, "vector_time_ms": 0,
        "search_time_ms": 0, "gpu_transfer_time_ms": 0,
        "acquire_semaphore_time_ms": 0, "gpu_resource_creation_time_ms": 0,
        "gpu_cleanup_time_ms": 0,
        "used_gpu": False, "gpus_available": faiss.get_num_gpus(),
        "semaphore_acquired": False, "fallback_to_cpu": False
    }
    valid_indices = np.array([])
    total_start_time = time.time()
    thread_id = threading.get_ident()

    try:
        # 1. 生成查询向量 (总是在CPU上)
        vector_start_time = time.time()
        query_vector = get_embedding_single_text(query)
        # 确保 get_embedding_single_text 返回 numpy array
        if not isinstance(query_vector, np.ndarray):
             query_vector = np.array(query_vector)
        query_vector = query_vector.astype(np.float32) # 确保类型
        query_vector = query_vector.reshape(1, -1) # 保证是2D
        query_vector = np.ascontiguousarray(query_vector)
        timing_info["vector_time_ms"] = (time.time() - vector_start_time) * 1000


        # 2. 判断是否尝试使用GPU
        should_try_gpu = index_type in ['header', 'content'] and timing_info["gpus_available"] > 0
        
        distances, indices = None, None
        gpu_index = None
        gpu_resources = None
        acquired_semaphore = False

        if should_try_gpu:
            # 尝试获取信号量 (非阻塞或带超时，避免无限等待)
            semaphore_start_time = time.time()
            # acquired_semaphore = _faiss_gpu_concurrency_semaphore.acquire(blocking=False)
            acquired_semaphore = _faiss_gpu_concurrency_semaphore.acquire(timeout=0.1) # 等待最多0.1秒
            timing_info["acquire_semaphore_time_ms"] = (time.time() - semaphore_start_time) * 1000
            timing_info["semaphore_acquired"] = acquired_semaphore

            if acquired_semaphore:
                # print(f"[Thread {thread_id}] 成功获取GPU信号量 (可用: {_faiss_gpu_concurrency_semaphore._value + 1}/{MAX_GPU_CONCURRENT_QUERIES})。尝试GPU查询...")
                timing_info["used_gpu"] = True
                gpu_id = 0 # 或选择一个可用GPU的逻辑
                try:
                    # a. 创建临时GPU资源
                    res_create_start = time.time()
                    gpu_resources = faiss.StandardGpuResources()
                    # 可选：配置 gpu_resources, 如 setTempMemory
                    # gpu_resources.setTempMemory(1024 * 1024 * 1024) # 例如1GB
                    timing_info["gpu_resource_creation_time_ms"] = (time.time() - res_create_start) * 1000

                    # b. 克隆CPU索引到GPU
                    transfer_start_time = time.time()
                    gpu_index = faiss.index_cpu_to_gpu(gpu_resources, gpu_id, cpu_index)
                    timing_info["gpu_transfer_time_ms"] = (time.time() - transfer_start_time) * 1000
                    # print(f"[Thread {thread_id}] 克隆索引到 GPU {gpu_id} 耗时: {timing_info['gpu_transfer_time_ms']:.2f}ms")

                    # c. 在GPU上执行搜索
                    search_start_time = time.time()
                    distances, indices = gpu_index.search(query_vector, top_k)
                    print(f"indices_136:{indices}")
                    timing_info["search_time_ms"] = (time.time() - search_start_time) * 1000
                    # 如果indices非空，直接返回
                    if indices is not None and len(indices) > 0:
                        print(indices[0])
                        return indices[0]
                        
                except Exception as gpu_exc:
                    print(f"[Thread {thread_id}] GPU查询过程中出错: {gpu_exc}")
                    timing_info["used_gpu"] = False
                    timing_info["fallback_to_cpu"] = True
                    distances, indices = None, None # 重置结果
                finally:
                    # d. 清理临时GPU资源 (非常重要!)
                    # print(f"[Thread {thread_id}] 开始清理GPU资源...")
                    cleanup_start_time = time.time()
                    del gpu_index # 显式删除GPU索引对象
                    del gpu_resources # 显式删除GPU资源对象
                    # print(f"[Thread {thread_id}] 已删除临时GPU对象")
                    
                    # e. 释放信号量
                    _faiss_gpu_concurrency_semaphore.release()
                    # print(f"[Thread {thread_id}] 已释放GPU信号量 (可用: {_faiss_gpu_concurrency_semaphore._value}/{MAX_GPU_CONCURRENT_QUERIES})")
                    timing_info["gpu_cleanup_time_ms"] = (time.time() - cleanup_start_time) * 1000
            else:
                # 获取信号量失败，回退到CPU
                # print(f"[Thread {thread_id}] 获取GPU信号量失败或超时。回退到CPU查询...")
                timing_info["fallback_to_cpu"] = True
                timing_info["used_gpu"] = False

        # 3. 如果未使用GPU或需要回退，则在CPU上执行搜索
        if not timing_info["used_gpu"]:    
            search_start_time = time.time()
            distances, indices = cpu_index.search(query_vector, top_k)
            timing_info["search_time_ms"] = (time.time() - search_start_time) * 1000
            # print(f"[Thread {thread_id}] CPU 搜索耗时: {timing_info['search_time_ms']:.2f}ms")
            # --- 诊断打印 ---
            print(f"[Thread {thread_id}][CPU Search] Raw distances: {distances}")
            print(f"[Thread {thread_id}][CPU Search] Raw indices: {indices}")
            # ---------------
        # 如果indices非空，直接返回
        if indices is not None and len(indices) > 0:
            print(f"176_[Thread {thread_id}] 找到 {len(indices[0])} 个匹配项。")
            print(indices[0])
            return indices[0]
        return []
       
    except Exception as e:
        timing_info["total_time_ms"] = (time.time() - total_start_time) * 1000
        print(f"[Thread {thread_id}] Faiss搜索函数发生严重错误: {e}")
        import traceback
        traceback.print_exc()
        return []

# ---------------------------------------------------------
# 修改 search 和 search_and_query 函数以使用新的搜索逻辑
# ---------------------------------------------------------

def log_search_metrics(query, index_type, top_k, timing_info, result_count, loaded_on_demand=False, error=None):
    """记录搜索指标的辅助函数"""
    log_entry = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "thread_id": threading.get_ident(), # 添加线程ID
        "query": query[:200] + '...' if len(query) > 200 else query, # 截断长查询
        "index_type": index_type,
        "top_k": top_k,
        "use_gpu": timing_info.get("used_gpu", False),
        "gpu_id": 0 if timing_info.get("used_gpu", False) else -1, # 假设只用GPU 0
        "total_time_ms": round(timing_info.get("total_time_ms", 0), 2),
        "load_time_ms": round(timing_info.get("load_time_ms", 0), 2),
        "vector_time_ms": round(timing_info.get("vector_time_ms", 0), 2),
        "search_time_ms": round(timing_info.get("search_time_ms", 0), 2),
        "gpu_transfer_time_ms": round(timing_info.get("gpu_transfer_time_ms", 0), 2),
        "acquire_semaphore_time_ms": round(timing_info.get("acquire_semaphore_time_ms", 0), 2),
        "gpu_resource_creation_time_ms": round(timing_info.get("gpu_resource_creation_time_ms", 0), 2),
        "gpu_cleanup_time_ms": round(timing_info.get("gpu_cleanup_time_ms", 0), 2),
        "neo4j_query_time_ms": round(timing_info.get("neo4j_query_time_ms", 0), 2),
        "result_count": result_count,
        "loaded_on_demand": loaded_on_demand, # 这个可能不再需要
        "fallback_to_cpu": timing_info.get("fallback_to_cpu", False),
        "semaphore_acquired": timing_info.get("semaphore_acquired", "N/A"), # 使用 N/A 而不是 False
        "gpus_available": timing_info.get("gpus_available", 0),
        "error": error # 使用传入的错误信息
    }
    # 打印或记录日志条目
    print(f"Search Log: {json.dumps(log_entry, ensure_ascii=False)}")
    # 可以替换为 logging.info() 或其他日志记录方式


def get_details_from_neo4j(result_ids, index_type):
    """根据索引类型从Neo4j批量查询详细信息"""
    if not result_ids[0]:
        return []
    
    driver = None
    details = []
    try:
        driver = get_neo4j_driver()
        with driver.session() as session:
            if index_type == 'filename':
                details = query_file_batch_nodes(result_ids)
            elif index_type == 'header':
                details = query_header_batch_nodes(result_ids)
            elif index_type == 'content':
                details = query_content_batch_nodes(result_ids)
            else:
                print(f"不支持的索引类型用于Neo4j查询: {index_type}")
    except Exception as e:
        print(f"从Neo4j查询详情时出错: {e}")
    finally:
        if driver:
             driver.close()
    return details


# 移除 monitor_gpu 装饰器
# @monitor_gpu
def search_and_query(query, index_type='content', top_k=10, with_details=True):
    """
    执行FAISS搜索并（可选地）从Neo4j查询详细信息。
    使用动态GPU克隆和信号量并发控制。
    如果预加载资源不可用，将尝试临时使用CPU进行检索。
    """
    total_start_time_sq = time.time()
    
    # 1. 从全局获取预加载的CPU资源
    faiss_resources = get_faiss_resources()
    if not faiss_resources:
        print("警告：无法获取预加载的 Faiss CPU 资源，尝试临时创建CPU索引...")
        
        try:
            # 临时创建CPU索引
            from utils.vector_generator import get_embedding_single_text
            import numpy as np
            
            # 生成查询向量
            vector_start_time = time.time()
            query_vector = get_embedding_single_text(query)
            if not isinstance(query_vector, np.ndarray):
                query_vector = np.array(query_vector)
            query_vector = query_vector.astype(np.float32)
            query_vector = query_vector.reshape(1, -1)
            vector_time_ms = (time.time() - vector_start_time) * 1000

            print(f"query_vector:{query_vector}")
            print(f"query_vector.shape:{query_vector.shape}")
            # 由于没有预加载索引，使用search函数进行检索
            print(f"尝试使用search函数进行检索...")
            result_ids = search(query, index_type=index_type, top_k=top_k)
            print(f"type:{type(result_ids)}")
            print(f"285_result_ids: {result_ids}")

            # 添加类型转换和空值检查
            if isinstance(result_ids, np.ndarray):
                result_ids = result_ids.tolist()  # 转换为Python list
                print('result_ids 转换成功')
                print(f'291_result:{result_ids}')
            result_ids = result_ids if result_ids[0] is not None else []
            print(f"293_result_ids:{result_ids}")
            # 初始化结果变量
            details = []


            print('296 开始查询neo4j 研报细节')
            # 修改判断条件
            if with_details and len(result_ids) > 0:  # 明确检查长度
                neo4j_start_time = time.time()
                details = get_details_from_neo4j(result_ids, index_type)
                print(f"details:{details}")
                neo4j_query_time_ms = (time.time() - neo4j_start_time) * 1000
            else:
                neo4j_query_time_ms = 0
            
            # 记录时间信息
            timing_info = {
                "total_time_ms": (time.time() - total_start_time_sq) * 1000,
                "vector_time_ms": vector_time_ms,
                "search_time_ms": 0,
                "neo4j_query_time_ms": 0,
                "fallback_mode": "no_index",
                "query_vector_available": True
            }

            log_search_metrics(query, index_type, top_k, timing_info, 0,
                               error="使用空结果降级 - 没有预加载索引")
            return details
            
        except Exception as e:
            print(f"临时CPU检索尝试失败: {e}")
            timing_info = {"total_time_ms": (time.time() - total_start_time_sq) * 1000}
            log_search_metrics(query, index_type, top_k, timing_info, 0, 
                               error=f"降级失败 - {str(e)}")
            return []

    # 正常流程继续 - 根据 index_type 选择正确的 CPU 索引和 ID 映射
    cpu_index = None
    ids_mapping = None
    if index_type == 'filename':
        cpu_index = faiss_resources.get('index_filename')
        ids_mapping = faiss_resources.get('ids_filename')
    elif index_type == 'header':
        cpu_index = faiss_resources.get('index_header')
        ids_mapping = faiss_resources.get('ids_header')
    elif index_type == 'content':
        cpu_index = faiss_resources.get('index_content')
        ids_mapping = faiss_resources.get('ids_content')

    if cpu_index is None or ids_mapping is None:
        print(f"错误：索引类型 '{index_type}' 的预加载 CPU 资源不完整！")
        timing_info = {"total_time_ms": (time.time() - total_start_time_sq) * 1000}
        log_search_metrics(query, index_type, top_k, timing_info, 0)
        return []

    # 在执行search之前，打印索引的关键信息
    print(f"索引类型: {type(cpu_index).__name__}")
    print(f"索引中向量数量: {cpu_index.ntotal}")
    print(f"向量维度: {cpu_index.d}")

    # 对于IVFPQ索引，增加nprobe参数
    if hasattr(cpu_index, 'nprobe'):
        old_nprobe = cpu_index.nprobe
        cpu_index.nprobe = 100  # 大幅增加搜索范围
        print(f"将nprobe从 {old_nprobe} 增加到 100")

    # 2. 执行搜索 (使用新的动态GPU函数)
    result_ids = search_faiss_dynamic_gpu(
        query=query,
        cpu_index=cpu_index,
        index_type=index_type,
        ids_mapping=ids_mapping,
        top_k=top_k
    )
    print(f"result_ids_361:{result_ids}")
    # 补充记录 search_and_query 的总时间 (可能与 timing_info['total_time_ms'] 不同)

    details = []
    neo4j_start_time = time.time()
    # if result_ids[0]:
    #     # print(f"开始从 Neo4j 查询 {len(result_ids)} 个 ID 的详细信息...")
    #     print("")
    #     details = get_details_from_neo4j(result_ids, index_type)
    #     neo4j_time_ms = (time.time() - neo4j_start_time) * 1000
    #     # print(f"Neo4j 查询耗时: {neo4j_time_ms:.2f}ms")

    print(f"379_type:{type(result_ids)}")
    print(f"380:{result_ids.tolist()}")
    # 将整个列表传递给 get_details_from_neo4j，而不是仅第一个元素
    details = get_details_from_neo4j(result_ids.tolist(), index_type)


    # 返回结果
    return details


# 移除 monitor_gpu 装饰器
# @monitor_gpu
def search(query, index_type='content', top_k=5):
    """
    仅执行FAISS搜索并返回ID列表。
    若没有预加载资源，会临时从文件加载FAISS索引到CPU。
    """
    total_start_time_s = time.time()
    
    # 从全局获取预加载的CPU资源
    faiss_resources = get_faiss_resources()
    if not faiss_resources:
        print("警告：无法获取预加载的 Faiss CPU 资源，尝试临时用CPU加载索引文件...")
        
        try:
            # 临时加载FAISS索引
            import os
            import faiss
            import numpy as np
            import json
            
            # 确定索引文件路径
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            index_dir = os.path.join(project_root, "database", "faiss_index_sc")
            
            print(f"尝试从 {index_dir} 加载索引文件")
            
            # 根据请求的索引类型加载对应文件
            cpu_index = None
            ids_mapping = None
            
            # 调整文件命名格式以匹配实际文件
            if index_type == 'filename':
                print(f"临时加载文件名索引...")
                index_path = os.path.join(index_dir, "filename_index_flat.index")
                ids_path = os.path.join(index_dir, "filename_index_flat_ids.json")
            elif index_type == 'header':
                print(f"临时加载标题索引...")
                index_path = os.path.join(index_dir, "header_index_IVFPQ.index")
                ids_path = os.path.join(index_dir, "header_ids.npy")
            elif index_type == 'content':
                print(f"临时加载内容索引...")
                index_path = os.path.join(index_dir, "content_index_IVFPQ.index")
                ids_path = os.path.join(index_dir, "content_ids.npy")
            else:
                raise ValueError(f"不支持的索引类型: {index_type}")
            
            # 检查索引文件是否存在
            if not os.path.exists(index_path):
                print(f"错误：索引文件不存在: {index_path}")
                # 尝试查找替代文件
                possible_index_files = [f for f in os.listdir(index_dir) if index_type in f.lower() and f.endswith('.index')]
                if possible_index_files:
                    print(f"找到可能的替代索引文件: {possible_index_files}")
                    index_path = os.path.join(index_dir, possible_index_files[0])
                    print(f"尝试使用替代文件: {index_path}")
            
            if not os.path.exists(ids_path):
                print(f"错误：ID映射文件不存在: {ids_path}")
                # 尝试查找替代文件
                possible_id_files = [f for f in os.listdir(index_dir) if index_type in f.lower() and ('ids' in f.lower() or 'id' in f.lower())]
                if possible_id_files:
                    print(f"找到可能的替代ID文件: {possible_id_files}")
                    ids_path = os.path.join(index_dir, possible_id_files[0])
                    print(f"尝试使用替代文件: {ids_path}")
            
            # 如果文件仍然不存在，则退出
            if not os.path.exists(index_path) or not os.path.exists(ids_path):
                print(f"错误：无法找到必要的索引文件，搜索失败")
                timing_info = {"total_time_ms": (time.time() - total_start_time_s) * 1000}
                log_search_metrics(query, index_type, top_k, timing_info, 0, 
                                  error="临时加载失败 - 找不到合适的索引文件")
                return []
            
            # 加载索引
            load_start_time = time.time()
            print(f"开始加载索引: {index_path}")
            cpu_index = faiss.read_index(index_path)
            print(f"索引加载完成")
            # --- 诊断打印 ---
            print(f"[Fallback Load] Loaded index path: {index_path}")
            print(f"[Fallback Load] Index ntotal: {cpu_index.ntotal}, Index dimension: {cpu_index.d}")
            # ---------------
            
            # 加载ID映射 - 支持多种格式
            print(f"开始加载ID映射: {ids_path}")
            # --- 诊断打印 ---
            print(f"[Fallback Load] Loading IDs path: {ids_path}")
            # ---------------
            file_extension = os.path.splitext(ids_path)[1].lower()
            if file_extension == '.json':
                with open(ids_path, 'r', encoding='utf-8') as f:
                    ids_mapping = json.load(f)
            elif file_extension == '.npy':
                with open(ids_path, 'rb') as f:
                    ids_mapping = np.load(f, allow_pickle=True)
            else:
                print(f"警告：未知的ID文件格式: {file_extension}，尝试作为文本文件读取")
                with open(ids_path, 'r', encoding='utf-8') as f:
                    ids_mapping = [line.strip() for line in f]
            
            load_time_ms = (time.time() - load_start_time) * 1000
            print(f"临时加载完成，耗时: {load_time_ms:.2f}ms")
            # --- 诊断打印 ---
            print(f"[Fallback Load] Loaded IDs type: {type(ids_mapping)}")
            if isinstance(ids_mapping, (list, np.ndarray)):
                 print(f"[Fallback Load] Loaded IDs length: {len(ids_mapping)}")
            # ---------------
            
            # 生成查询向量
            from utils.vector_generator import get_embedding_single_text
            vector_start_time = time.time()
            query_vector = get_embedding_single_text(query)
            print(f"[Fallback Load] Query Vector: {query_vector}")
            if not isinstance(query_vector, np.ndarray):
                query_vector = np.array(query_vector)
            query_vector = query_vector.astype(np.float32)
            query_vector = query_vector.reshape(1, -1)
            query_vector = np.ascontiguousarray(query_vector)
            vector_time_ms = (time.time() - vector_start_time) * 1000
            # --- 诊断打印 ---
            print(f"[Fallback Load] Query Vector Shape: {query_vector.shape}")
            print(f"[Fallback Load] Query Vector Sample: {query_vector[0][:5]}...")
            # ---------------
            
            # 执行搜索
            search_start_time = time.time()
            distances, indices = cpu_index.search(query_vector, top_k)
            search_time_ms = (time.time() - search_start_time) * 1000
            # --- 诊断打印 ---
            print(f"[Fallback Load][CPU Search] Raw distances: {distances}")
            print(f"[Fallback Load][CPU Search] Raw indices: {indices}")
            # ---------------

            # 在临时加载路径的返回部分添加转换
            if indices is not None and indices.size > 0 and indices[0][0] != -1:
                valid_indices = indices[0][indices[0] != -1]
                # 转换为Python list
                return valid_indices.tolist()  # 添加.tolist()
            
            # 确保返回list类型
            return []  # 而不是numpy array

        except Exception as e:
            print(f"临时CPU加载索引失败: {e}")
            import traceback
            traceback.print_exc()
            timing_info = {"total_time_ms": (time.time() - total_start_time_s) * 1000}
            log_search_metrics(query, index_type, top_k, timing_info, 0, 
                              error=f"临时CPU加载索引失败 - {str(e)}")
            return []

    # 以下是原始的预加载资源逻辑
    cpu_index = None
    ids_mapping = None
    if index_type == 'filename':
        cpu_index = faiss_resources.get('index_filename')
        ids_mapping = faiss_resources.get('ids_filename')
    elif index_type == 'header':
        cpu_index = faiss_resources.get('index_header')
        ids_mapping = faiss_resources.get('ids_header')
    elif index_type == 'content':
        cpu_index = faiss_resources.get('index_content')
        ids_mapping = faiss_resources.get('ids_content')

    if cpu_index is None or ids_mapping is None:
        print(f"错误：索引类型 '{index_type}' 的预加载 CPU 资源不完整！")
        timing_info = {"total_time_ms": (time.time() - total_start_time_s) * 1000}
        log_search_metrics(query, index_type, top_k, timing_info, 0)
        return []

    # 执行搜索 (使用新的动态GPU函数)
    result_ids = search_faiss_dynamic_gpu(
        query=query,
        cpu_index=cpu_index,
        index_type=index_type,
        ids_mapping=ids_mapping,
        top_k=top_k
    )
    
    # 更新总时间
    timing_info = {"total_time_ms": (time.time() - total_start_time_s) * 1000}
    
    # 记录指标
    result_count = len(result_ids) if result_ids is not None else 0
    log_search_metrics(query, index_type, top_k, timing_info, result_count)

    return result_ids if result_ids is not None else []


# 示例用法 (如果直接运行此文件)
if __name__ == '__main__':
    text = '2023年新能源汽车行业发展全景'
#    content_result = search(text, index_type='content', top_k=5)
#    print(f"content搜索结果: {content_result}")
#    header_result = search(text, index_type='header', top_k=5)
#    print(f"header搜索结果: {header_result}")
    filename_result = search_and_query(text, index_type='filename', top_k=5)
    print(f"filename搜索结果: {filename_result}")

    # 测试1：空结果
    # print(search("不存在的内容", index_type='content', top_k=5))  # 应返回[]
    #
    # # 测试2：有效结果
    # print(search("人工智能", index_type='content', top_k=5))  # 应返回ID列表
    #
    # # 测试3：类型检查
    # result = search("测试")
    # print(type(result))  # 应显示 <class 'list'>
