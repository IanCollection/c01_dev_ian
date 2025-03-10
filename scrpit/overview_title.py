from typing import Any

from Agent.Overview_agent import match_focus_points, generate_toc_from_focus_points
import json

def match_focus_points_from_file(text: str) -> tuple[dict[Any, dict[Any, Any]], dict[Any, list[Any]], str]:
    """
    根据输入文本匹配关注点,并返回一级和二级关注点的对应关系
    
    Args:
        text (str): 输入的文本内容
        
    Returns:
        tuple: 包含两个字典的元组
            - result (dict): 包含完整的一级和二级关注点及其详细信息的字典
            - focus_points_mapping (dict): 一级关注点到其对应二级关注点列表的映射字典
    """
    # 获取二级关注点匹配结果
    second_level_focus_points_dict, cost = match_focus_points(text)
    second_level_focus_points = second_level_focus_points_dict['二级关注点']
    
    # 读取 JSON 文件
    import os
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    json_path = os.path.join(root_dir, 'data', 'optimized_output.json')
    with open(json_path, 'r', encoding='utf-8') as f:
        focus_points_data = json.load(f)
    
    # 创建结果字典
    result = {}
    
    # 遍历 JSON 数据的顶层键（一级关注点）
    for first_level, content in focus_points_data.items():
        # 遍历该一级关注点下的所有二级关注点
        for second_level, details in content.items():
            # 如果这个二级关注点在匹配列表中
            if second_level in second_level_focus_points:
                # 如果这个一级关注点还没有添加到结果中
                if first_level not in result:
                    result[first_level] = {}
                
                # 添加二级关注点及其详细信息
                result[first_level][second_level] = details
    
    # 创建新的字典来存储一级和二级关注点的对应关系
    focus_points_mapping = {}
    
    # 遍历result字典,提取一级关注点和对应的二级关注点
    for first_level, second_levels in result.items():
        focus_points_mapping[first_level] = list(second_levels.keys())
    
    # 将映射关系转换为文字描述
    text_mapping = {}
    for first_level, second_levels in focus_points_mapping.items():
        # 将二级关注点列表转换为逗号分隔的字符串
        second_levels_text = "、".join([f"二级关注点:{point}" for point in second_levels])
        text_mapping[first_level] = f"一级关注点:{first_level}({second_levels_text})"
    
    # 将所有一级关注点的文字描述合并
    final_text = "，".join(text_mapping.values())

    return result,focus_points_mapping,final_text

def generate_comprehensive_toc_with_focus_points(title: str) -> tuple[str, float]:
    """
    根据标题和关注点生成综合目录结构

    Args:
        title: 研报标题
        focus_points: 关注点字典

    Returns:
        tuple: 包含生成的Markdown格式目录结构(str)和API调用成本(float)
    """
    result, focus_points_mapping, focus_points = match_focus_points_from_file(title)
    overview_from_focus_points = generate_toc_from_focus_points(title, focus_points)
    return overview_from_focus_points




if __name__ == "__main__":
    title = "AI芯片市场分析和行业趋势"
    result, focus_points_mapping, final_text= match_focus_points_from_file(title)

    # print(result)
    # print('-'*20)
    # print(focus_points_mapping)
    # print('-'*20)
    # print(final_text)