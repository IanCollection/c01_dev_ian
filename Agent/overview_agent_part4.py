import time
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)
from scrpit.milestone_4 import process_ic_trends
from scrpit.query_report_policy_ic_indicator import query_relative_data_v2
from Agent.client_manager import qwen_client, silicon_client
import json
import os
from openai import OpenAI
import time
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Tuple, Generator

# 假设这些客户端已经在其他地方初始化
from Agent.client_manager import qwen_client, silicon_client
from Agent.Overview_agent import year_extract_from_title, generate_ana_instruction

client = silicon_client
qwen_client = qwen_client
from Agent.tool_agents import json_format_agent


def generate_second_level_titles(second_level_title: str, title_code: str, analysis_instruction: str) -> Dict:
    """
    基于二级标题、标题代码和分析思路生成三级标题

    参数:
        second_level_title (str): 一级标题文本
        title_code (str): 一级标题代码 (如 "5")
        analysis_instruction (str): 二级标题的分析思路

    返回:
        Dict: 包含生成的二级标题及其代码的JSON结构
    """
    try:
        # 构建提示词
        prompt = f"""
        请基于以下一级标题及其分析思路，生成3-5个合适的二级标题：

        一级标题: {second_level_title}
        标题代码: {title_code}
        分析思路: {analysis_instruction}

        要求:
        1. 二级标题应该逻辑清晰，相互之间有关联性，共同支撑一级标题
        2. 二级标题应该具体、明确，能够指导后续内容撰写
        3. 二级标题应该覆盖分析思路中提到的关键点
        4. 每个二级标题都应有对应的标题代码，格式为"5.1"、"5.2"等

        请以JSON格式返回结果，格式如下:
        {{
            "second_level_titles": [
                {{
                    "title_code": "标题代码",
                    "title": "二级标题内容"
                }},
                ...
            ]
        }}
        """
        # 调用大模型生成二级标题
        response = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system",
                 "content": "你是一个专业的研究报告结构规划专家，擅长根据主题和分析思路设计合理的标题结构。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            response_format={"type": "json_object"},
        )

        # 提取模型返回的内容并直接解析为JSON
        result_text = response.choices[0].message.content
        try:
            result = json.loads(result_text)
            # 确保返回结果包含必要字段
            if "second_level_titles" not in result:
                result["second_level_titles"] = []
            return result
        except json.JSONDecodeError:
            # 如果解析失败，返回默认结构
            return {"second_level_titles": []}

    except Exception as e:
        print(f"生成二级标题时发生错误: {str(e)}")
        # 返回一个空的结果结构
        return {"second_level_titles": []}


def generate_third_level_titles(second_level_title: str, title_code: str, analysis_instruction: str) -> Dict:
    """
    基于二级标题、标题代码和分析思路生成三级标题

    参数:
        second_level_title (str): 二级标题文本
        title_code (str): 二级标题代码 (如 "5.1")
        analysis_instruction (str): 二级标题的分析思路

    返回:
        Dict: 包含生成的三级标题及其代码的JSON结构
    """
    try:
        # 构建提示词
        prompt = f"""
        请基于以下二级标题及其分析思路，生成3-5个合适的三级标题：

        二级标题: {second_level_title}
        标题代码: {title_code}
        分析思路: {analysis_instruction}

        要求:
        1. 三级标题应该逻辑清晰，相互之间有关联性，共同支撑二级标题
        2. 三级标题应该具体、明确，能够指导后续内容撰写
        3. 三级标题应该覆盖分析思路中提到的关键点
        4. 每个三级标题都应有对应的标题代码，格式为"{title_code}.1"、"{title_code}.2"等

        请以JSON格式返回结果，格式如下:
        {{
            "third_level_titles": [
                {{
                    "title_code": "标题代码",
                    "title": "三级标题内容"
                }},
                ...
            ]
        }}
        """
        # 调用大模型生成三级标题
        response = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system",
                 "content": "你是一个专业的研究报告结构规划专家，擅长根据主题和分析思路设计合理的标题结构。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            response_format={"type": "json_object"},
        )

        # 提取模型返回的内容并直接解析为JSON
        result_text = response.choices[0].message.content
        try:
            result = json.loads(result_text)
            # 确保返回结果包含必要字段
            if "third_level_titles" not in result:
                result["third_level_titles"] = []
            return result
        except json.JSONDecodeError:
            # 如果解析失败，返回默认结构
            return {"third_level_titles": []}

    except Exception as e:
        print(f"生成三级标题时发生错误: {str(e)}")
        # 返回一个空的结果结构
        return {"third_level_titles": []}


def format_third_level_result_to_json(title: str, title_code: str, ana_instruction: str, result: Dict) -> Dict:
    """
    将生成的三级标题结果格式化为指定的JSON结构

    参数:
        title (str): 二级标题
        title_code (str): 标题代码
        ana_instruction (str): 分析思路
        result (Dict): 包含三级标题的原始结果

    返回:
        Dict: 格式化后的JSON结构
    """
    formatted_result = {
        "title": title,
        "subsections": [
            {
                "title": second_title["title"],
                "subsections": [],
                "title_code": second_title["title_code"],
                "ana_instruction": ""
            }
            for second_title in result.get("second_level_titles", [])
        ],
        "previous_title": "",
        "title_code": title_code,
        "ana_instruction": ana_instruction
    }
    return formatted_result


def format_third_level_result_to_json_v2(title: str, title_code: str, ana_instruction: str, result: Dict) -> Dict:
    """
    将生成的三级标题结果格式化为指定的JSON结构

    参数:
        title (str): 二级标题
        title_code (str): 标题代码
        ana_instruction (str): 分析思路
        result (Dict): 包含三级标题的原始结果

    返回:
        Dict: 格式化后的JSON结构
    """
    formatted_result = {
        "title": title,
        "subsections": [
            {
                "title": third_title["title"],
                "subsections": [],
                "title_code": third_title["title_code"],
                "ana_instruction": ""
            }
            for third_title in result.get("third_level_titles", [])
        ],
        "previous_title": "",
        "title_code": title_code,
        "ana_instruction": ana_instruction
    }
    return formatted_result


def format_second_level_result_to_json(title: str, title_code: str, ana_instruction: str, result: Dict) -> Dict:
    """
    将生成的二级标题结果格式化为指定的JSON结构

    参数:
        title (str): 一级标题
        title_code (str): 标题代码
        ana_instruction (str): 分析思路
        result (Dict): 包含二级标题的原始结果

    返回:
        Dict: 格式化后的JSON结构
    """
    formatted_result = {
        "title": title,
        "subsections": [
            {
                "title": second_title["title"],
                "subsections": [],
                "title_code": f"{title_code}.{i + 1}",
                "ana_instruction": second_title.get("ana_instruction", "")
            }
            for i, second_title in enumerate(result.get("second_level_titles", []))
        ],
        "previous_title": "",
        "title_code": title_code,
        "ana_instruction": ana_instruction
    }
    return formatted_result
