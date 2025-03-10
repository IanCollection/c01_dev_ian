import os
from openai import OpenAI
from tqdm import tqdm
from dotenv import load_dotenv
load_dotenv()

def get_embeddings(texts, ids=None, api_key=None, batch_size=10):
    """
    获取文本嵌入向量，支持批处理和ID关联

    Args:
        texts (list): 文本列表
        ids (list, optional): 与文本对应的ID列表，默认为None
        api_key (str, optional): API密钥，默认从环境变量获取
        batch_size (int, optional): 批处理大小，默认20

    Returns:
        dict: 格式为 {id: embedding} 的字典，将ID与嵌入向量关联
    """
    if not texts:
        return {}

    # 确保ids与texts长度相同
    if ids and len(ids) != len(texts):
        raise ValueError("ids 和 texts 的长度必须相同")

    if api_key is None:
        api_key = os.environ.get("DASHSCOPE_API_KEY")

    # 初始化OpenAI客户端
    client = OpenAI(
        api_key=api_key,
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"  # 百炼服务的base_url
    )

    # 分批处理
    result_dict = {}
    total_cost = 0
    for i in tqdm(range(0, len(texts), batch_size), desc="获取嵌入向量"):
        batch_texts = texts[i:i + batch_size]
        batch_ids = ids[i:i + batch_size]

        try:
            # 直接处理批量文本
            completion = client.embeddings.create(
                model="text-embedding-v3",
                input=batch_texts,
                dimensions=512,
                encoding_format="float"
            )

            # 计算token费用
            total_tokens = completion.usage.total_tokens
            batch_cost = (total_tokens / 1000) * 0.0005
            total_cost += batch_cost
            # print(f"本批次使用token数: {total_tokens}, 费用: {batch_cost:.6f}元")

            # 将ID与embedding关联并添加到结果字典
            for j, embedding_obj in enumerate(completion.data):
                result_dict[batch_ids[j]] = embedding_obj.embedding

        except Exception as e:
            print(f"批次 {i // batch_size + 1} 获取嵌入向量失败: {e}")
            # 失败时返回空向量
            for id_val in batch_ids:
                result_dict[id_val] = []
                
    # print(f"总费用: {total_cost:.6f}元")
    return result_dict,total_cost

def get_embedding_single_text(text, api_key=None):
    """
    获取单个文本的嵌入向量

    Args:
        text (str): 需要获取嵌入向量的文本
        api_key (str, optional): API密钥，默认从环境变量获取

    Returns:
        list: 文本的嵌入向量
    """
    if not text:
        return []

    if api_key is None:
        api_key = os.environ.get("DASHSCOPE_API_KEY")

    # 初始化OpenAI客户端
    client = OpenAI(
        api_key=api_key,
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"  # 百炼服务的base_url
    )

    try:
        # 获取单个文本的嵌入向量
        completion = client.embeddings.create(
            model="text-embedding-v3",
            input=text,
            dimensions=512,
            encoding_format="float"
        )
        
        # 计算token费用
        # total_tokens = completion.usage.total_tokens
        # cost = (total_tokens / 1000) * 0.0005
        # print(f"使用token数: {total_tokens}, 费用: {cost:.6f}元")
        
        return completion.data[0].embedding
    except Exception as e:
        print(f"获取嵌入向量失败: {e}")
        return []