import dashscope
from http import HTTPStatus
import os
from dotenv import load_dotenv
import time

# 加载.env文件中的环境变量
load_dotenv()

# 设置API key
dashscope.api_key = os.getenv("DASHSCOPE_API_KEY")

def text_rerank(query, documents, top_n=10, return_documents=True, model="gte-rerank-v2"):
    """
    使用dashscope的TextReRank对文档进行重排序
    
    Args:
        query (str): 查询文本
        documents (list): 候选文档列表
        top_n (int, optional): 返回前n个结果，默认为10
        return_documents (bool, optional): 是否在结果中返回文档内容，默认为True
        model (str, optional): 使用的模型名称，默认为"gte-rerank-v2"
        
    Returns:
        dict: 重排序后的结果
        None: 如果发生错误
    """
    try:
        resp = dashscope.TextReRank.call(
            model=model,
            query=query,
            documents=documents,
            top_n=top_n,
            return_documents=return_documents
        )
        
        if resp.status_code == HTTPStatus.OK:
            return resp
        else:
            print(f"重排序请求失败: {resp.status_code}")
            print(resp)
            return None
    except Exception as e:
        print(f"重排序过程中发生错误: {str(e)}")
        return None

# 示例使用
if __name__ == '__main__':
    # 定义您的查询文本
    query_text = "2024年新能源汽车行业发展趋势/智能驾驶相关政策的技术支持与规范"
    
    # 定义包含ID和名称的候选文档字典
    candidate_docs_with_id = [
        {"id": "name", "text": "新能源技术推广服务"},
        {"id": "1734", "text": "智能车载设备"},
        {"id": "553", "text": "新能源专业技术咨询服务"},
        {"id": "816", "text": "新能源汽车电动机"},
        {"id": "621", "text": "汽车金融"},
        {"id": "1474", "text": "新能源专业技术评估服务"},
        {"id": "1651", "text": "交通安全、管制及类似专用设备"},
        {"id": "69", "text": "汽车"},
        {"id": "220", "text": "汽车零部件及配件"},
        {"id": "1343", "text": "工业控制计算机及系统"}
    ]
    
    # 提取文本字段用于排序
    docs_for_ranking = [doc["text"] for doc in candidate_docs_with_id]
    
    # 开始计时
    start_time = time.time()
    
    # 调用函数进行重排序
    result = text_rerank(query_text, docs_for_ranking)
    
    # 结束计时
    end_time = time.time()
    execution_time = end_time - start_time
    
    # 打印结果 - 根据正确的响应结构修改
    if result:
        print("\n重排序后的结果:")
        
        # 使用正确的响应结构
        for idx, item in enumerate(result.output['results']):
            # 获取文档文本和相关性得分
            doc_text = item['document']['text']
            score = item['relevance_score']
            
            # 找到对应的原始文档
            original_idx = docs_for_ranking.index(doc_text)
            original_doc = candidate_docs_with_id[original_idx]
            
            # 打印结果
            print(f"{idx+1}. id: {original_doc['id']}, name: {original_doc['text']} (分数: {score:.4f})")
        
        # 打印执行时间
        print(f"\n重排序执行时间: {execution_time:.4f} 秒")