import time
import os
import sys

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)

from Agent.client_manager import qwen_client, silicon_client
import json
import os
from openai import OpenAI
import time
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Tuple, Generator
import os
import sys
import json
from decimal import Decimal
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)

# 假设这些客户端已经在其他地方初始化
from Agent.client_manager import qwen_client, silicon_client

client = silicon_client
qwen_client = qwen_client
from Agent.tool_agents import json_format_agent



def judge_title_relevance(current_title: str, header_content: str) -> bool:
    """
    判断当前标题与内容是否相关
    
    参数:
        current_title (str): 当前标题
        header_content (str): 内容文本
        
    返回:
        bool: 是否相关
    """
    try:
        # 构建提示词
        prompt = f"""
        请判断以下标题与内容是否相关:
        
        标题: {current_title}
        内容: {header_content}
        
        要求:
        1. 如果标题与内容在主题、关键词或核心内容上相关，返回1
        2. 如果标题与内容完全不相关，返回0
        3. 请严格按照JSON格式返回结果：{{"result": 1}}或{{"result": 0}}
        4. 不要包含任何其他解释或文字
        """
        
        # 调用Qwen-turbo模型进行判断
        completion = qwen_client.chat.completions.create(
            model="qwen-turbo",
            messages=[
                {"role": "system", "content": "你是一个专业的文本分析助手。请始终以JSON格式返回结果，格式为{\"result\": 1}或{\"result\": 0}。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}  # 强制要求JSON格式响应
        )

        # 获取原始响应并打印用于调试
        raw_response = completion.choices[0].message.content
        print(f"API原始响应: {raw_response}")

        try:
            # 尝试解析JSON响应
            result = json.loads(raw_response)
            return result.get("result", 0) == 1
            
        except json.JSONDecodeError as je:
            print(f"JSON解析错误，原始响应: {raw_response}")
            print(f"解析错误详情: {je}")
            return False
    
    except Exception as e:
        print(f"判断标题相关性时出错: {e}")
        return False




def industry_indicator_relevance(industry_list: List[str], current_title: str) -> bool:
    """
    判断行业指标列表与当前标题是否相关
    
    参数:
        industry_list (List[str]): 行业指标列表
        current_title (str): 当前标题
        
    返回:
        bool: 是否相关
    """
    try:
        # 构建提示词
        prompt = f"""
        请判断以下行业指标与标题是否相关:
        
        行业指标: {', '.join(industry_list)}
        标题: {current_title}
    
        要求:
        1. 如果行业指标与标题在主题、关键词或核心内容上相关，返回1
        2. 如果行业指标与标题完全不相关，返回0
        3. 请严格按照JSON格式返回结果：{{"result": 1}}或{{"result": 0}}
        4. 不要包含任何其他解释或文字
        """
        
        # 调用Qwen-turbo模型进行判断
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的文本分析助手。请始终以JSON格式返回结果，格式为{\"result\": 1}或{\"result\": 0}。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            response_format={"type": "json_object"}  # 强制要求JSON格式响应
        )

        # 获取原始响应并打印用于调试
        raw_response = completion.choices[0].message.content
        print(f"API原始响应: {raw_response}")

        try:
            # 尝试解析JSON响应
            result = json.loads(raw_response)
            return result.get("result", 0) == 1
            
        except json.JSONDecodeError as je:
            print(f"JSON解析错误，原始响应: {raw_response}")
            print(f"解析错误详情: {je}")
            return False
    
    except Exception as e:
        print(f"判断行业指标相关性时出错: {e}")
        return False
