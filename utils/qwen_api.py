import json
import os
from openai import OpenAI
from client.client_manager import qwen_client
from qwen_tools.prompt import PROMPTS

client = qwen_client

def get_embedding(text: str, dimensions: int = 512) -> list:
    """
    获取文本的向量表示
    Args:
        text: 输入文本
        dimensions: 向量维度,默认1024
    Returns:
        文本的向量表示
    """
    completion = client.embeddings.create(
        model="text-embedding-v3", 
        input=text,
        dimensions=dimensions,
        encoding_format="float"
    )
    # 记录embedding的cost
    total_tokens = completion.usage.total_tokens
    total_cost = (total_tokens * 0.0007) / 1000
    return completion.data[0].embedding, total_cost

def extract_entities_and_relations(text: str, entity_types: list = None) -> dict:
    """
    从文本中提取实体和关系
    Args:
        text: 输入文本
        entity_types: 实体类型列表,默认使用PROMPTS["DEFAULT_ENTITY_TYPES"]
    Returns:
        包含实体和关系的字典
    """
    total_cost = 0
    
    # 实体抽取阶段
    entity_prompt = PROMPTS["qwen_entity_extraction"].format(input_text=text)

    try:
        entity_completion = client.chat.completions.create(
            model="qwen-turbo",
            messages=[
                {'role': 'user', 'content': entity_prompt},
                {'role': 'assistant', 'content': '请按规范输出实体信息：'}
            ],
            response_format={"type": "json_object"}
        )
        # 记录实体抽取的cost
        prompt_tokens = entity_completion.usage.prompt_tokens
        completion_tokens = entity_completion.usage.completion_tokens
        total_cost += (prompt_tokens * 0.0003 + completion_tokens * 0.0006) / 1000
        entity_data = json.loads(entity_completion.choices[0].message.content)
    except (KeyError, json.JSONDecodeError) as e:
        raise ValueError("实体抽取结果解析失败") from e

    # 关系优化阶段
    relation_prompt = PROMPTS["qwen_relation_refinement"].format(
        raw_relations=json.dumps(entity_data.get("relations", []), ensure_ascii=False),
        context=text
    )

    try:
        relation_completion = client.chat.completions.create(
            model="qwen-turbo",
            messages=[
                {'role': 'user', 'content': relation_prompt},
                {'role': 'assistant', 'content': '请优化并输出关系信息：'}
            ],
            response_format={"type": "json_object"}
        )
        # 记录关系优化的cost
        prompt_tokens = relation_completion.usage.prompt_tokens
        completion_tokens = relation_completion.usage.completion_tokens
        total_cost += (prompt_tokens * 0.0003 + completion_tokens * 0.0006) / 1000
        relation_data = json.loads(relation_completion.choices[0].message.content)
    except (KeyError, json.JSONDecodeError) as e:
        raise ValueError("关系优化结果解析失败") from e

    # 数据结构标准化处理
    entities = []
    for item in entity_data.get("entities", []):
        try:
            # 加强属性解析的异常处理
            properties = item.get("properties", {})
            if isinstance(properties, str):
                try:
                    properties = json.loads(properties)
                except json.JSONDecodeError as e:
                    print(f"属性解析失败（实体ID: {item.get('id', '未知')}）: {str(e)}")
                    properties = {}

            # 确保必要字段存在
            entity_info = {
                "entity_name": item.get("id", "未知实体"),
                "entity_type": properties.get("category", "未知类型"),
                "description": json.dumps(properties, ensure_ascii=False),
                "metadata": {
                    "source": "qwen_extraction",
                    "confidence": item.get("confidence", 0.9),
                    "aliases": item.get("aliases", [])
                }
            }
            entities.append(entity_info)
            
        except (KeyError, TypeError) as e:
            print(f"实体解析异常（原始数据: {json.dumps(item, ensure_ascii=False)}）: {str(e)}")
            continue

    relations = []
    for rel in relation_data.get("relations", []):
        try:
            # 处理可能为字符串的properties字段
            rel_properties = rel.get("properties", {})
            if isinstance(rel_properties, str):
                try:
                    rel_properties = json.loads(rel_properties)
                except json.JSONDecodeError:
                    rel_properties = {}

            relations.append({
                "source": rel.get("from"),
                "target": rel.get("to"),
                "relation_type": rel_properties.get("relation_type", "关联"),
                "description": rel_properties.get("description", ""),
                "weight": float(rel_properties.get("strength", 1.0)),
                "properties": {
                    "evidence": rel_properties.get("evidence", ""),
                    "timestamp": rel_properties.get("timestamp", ""),
                    "metrics": rel_properties.get("metrics", {}),
                    "confidence": relation_data.get("metadata", {}).get("confidence", 0.9)
                }
            })
        except (KeyError, ValueError) as e:
            print(f"关系解析异常: {str(e)}")
            continue

    print(f"总花费: ${total_cost:.4f}")
    
    return {
        "entities": entities,
        "relations": relations,
        "cost": total_cost
     }
#
# def build_heterogeneous_graph_from_doc(doc_data):
#     """
#     从文档JSON构建异构图,支持文本节点和图片节点
#
#     Args:
#         doc_data: 包含text和text_level的文档JSON数据
#
#     Returns:
#         dict: 包含实体和关系的异构图数据
#     """
#     entities = []
#     relations = []
#
#     # 用于记录已处理的实体,避免重复
#     processed_texts = set()
#
#     # 遍历文档数据构建实体
#     for item in doc_data:
#         text = item.get("text", "").strip()
#         if not text or text in processed_texts:
#             continue
#
#         level = item.get("text_level")
#         page = item.get("page_idx")
#         item_type = item.get("type", "text")
#
#         # 构建实体
#         entity = {
#             "id": f"{item_type}_{len(entities)}",
#             "name": text,
#             "entity_type": item_type,
#             "properties": {
#                 "text_level": level,
#                 "page": page
#             }
#         }
#
#         # 对于图片类型,添加额外属性
#         if item_type == "image":
#             entity["properties"].update({
#                 "img_path": item.get("img_path"),
#                 "img_caption": item.get("img_caption", []),
#                 "img_footnote": item.get("img_footnote", [])
#             })
#
#         entities.append(entity)
#         processed_texts.add(text)
#
#         # 如果有text_level,构建层级关系
#         if level:
#             # 查找同页面的上一级标题
#             for prev_item in reversed(doc_data[:doc_data.index(item)]):
#                 prev_text = prev_item.get("text", "").strip()
#                 if prev_text and \
#                         prev_item.get("text_level", 0) == level - 1 and \
#                         prev_item.get("page_idx") == page:
#                     relations.append({
#                         "source": f"{item_type}_{len(entities) - 1}",
#                         "target": f"{prev_item.get('type', 'text')}_{len(entities) - 2}",
#                         "relation_type": "belongs_to",
#                         "properties": {
#                             "level_diff": 1
#                         }
#                     })
#                     break
#
#         # 对于图片,与相邻的文本建立关联
#         if item_type == "image":
#             # 向前查找最近的文本
#             for prev_item in reversed(doc_data[:doc_data.index(item)]):
#                 if prev_item.get("text", "").strip() and prev_item.get("page_idx") == page:
#                     relations.append({
#                         "source": f"image_{len(entities) - 1}",
#                         "target": f"{prev_item.get('type', 'text')}_{len(entities) - 2}",
#                         "relation_type": "illustrates",
#                         "properties": {
#                             "distance": 1
#                         }
#                     })
#                     break
#
#     return {
#         "entities": entities,
#         "relations": relations
#     }


# def test_build_graph():
#     """测试异构图构建"""
#     sample_doc = [
#         {
#             "type": "text",
#             "text": "新能源汽车发展",
#             "text_level": 1,
#             "page_idx": 1
#         },
#         {
#             "type": "image",
#             "text": "2023年新能源汽车销量达950万辆",
#             "img_path": "image1.jpg",
#             "img_caption": ["销量统计图"],
#             "page_idx": 1
#         }
#     ]
#
#     graph = build_heterogeneous_graph_from_doc(sample_doc)
#     print(json.dumps(graph, ensure_ascii=False, indent=2))

# if __name__ == "__main__":
#     test_build_graph()
    # text = "2024年，中国新能源汽车产量达到1200万辆，同比增长50%。其中，纯电动汽车产量为800万辆，同比增长45%，插电式混合动力汽车产量为400万辆，同比增长60%。"
    # print(extract_entities_and_relations(text))
