import os
import json
import numpy as np
import faiss
from dotenv import load_dotenv
from utils.vector_generator import get_embedding_single_text

# 加载环境变量
load_dotenv()

# 加载FAISS索引
def load_faiss_index(index_type):
    """
    加载指定类型的FAISS索引
    
    Args:
        index_type (str): 索引类型，可选值为 'filename', 'header', 'content'
    
    Returns:
        tuple: (faiss索引, id列表)
    """
    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database/faiss_index")
    
    if index_type == 'filename':
        index_path = os.path.join(base_path, "filename_index_flat.index")
        ids_path = os.path.join(base_path, "filename_index_flat_ids.json")
    elif index_type in ['header', 'content']:
        index_path = os.path.join(base_path, f"{index_type}_index_IVFPQ.index")
        ids_path = os.path.join(base_path, f"{index_type}_ids.json")
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
    with open(ids_path, 'r', encoding='utf-8') as f:
        ids = json.load(f)
    
    return index, ids

# 检索函数
def search_faiss(query, index_type, top_k=5):
    """
    在FAISS索引中搜索最相似的文档
    
    Args:
        query (str): 查询文本
        index_type (str): 索引类型，可选值为 'filename', 'header', 'content'
        top_k (int): 返回的结果数量
    
    Returns:
        list: 包含检索结果的字典列表，每个字典包含id和相似度分数
    """
    try:
        # 加载模型和索引
        index, ids = load_faiss_index(index_type)
        
        # 将查询文本转换为向量
        query_vector = get_embedding_single_text(query)
        
        # 将向量转换为numpy数组并确保形状正确
        # query_vector = np.array(query_vector, dtype=np.float32).reshape(1, -1)
        
        # 确保查询向量不为空并且维度正确
        if query_vector is None or query_vector.size == 0:
            raise ValueError("查询向量为空")
            
        if query_vector.shape[1] != 512:
            raise ValueError(f"查询向量维度错误: {query_vector.shape[1]}, 应为512维")
        
        # 执行搜索
        distances, indices = index.search(query_vector, top_k)
        
        # 整理结果
        results = []
        for i, idx in enumerate(indices[0]):
            if idx != -1 and idx < len(ids):  # 确保索引有效且在范围内
                results.append({
                    "id": str(ids[idx]),  # 确保ID为字符串类型
                    "score": float(1 - distances[0][i])  # 将距离转换为相似度分数
                })
        
        return results
    except FileNotFoundError as e:
        print(f"错误: {e}")
        return []
    except Exception as e:
        print(f"搜索过程中发生错误: {e}")
        return []

# 示例用法
if __name__ == "__main__":
    # 示例查询
    query_text = "商业地产行业展望"
    
    print("在文件名中搜索:")
    filename_results = search_faiss(query_text, index_type='filename')
    for result in filename_results:
        print(f"ID: {result['id']}, 分数: {result['score']:.4f}")
    
    print("\n在标题中搜索:")
    header_results = search_faiss(query_text, index_type='header')
    for result in header_results:
        print(f"ID: {result['id']}, 分数: {result['score']:.4f}")
    
    print("\n在内容中搜索:")
    content_results = search_faiss(query_text, index_type='content')
    for result in content_results:
        print(f"ID: {result['id']}, 分数: {result['score']:.4f}")