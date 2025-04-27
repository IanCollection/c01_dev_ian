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
            model="qwen-plus-latest",
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


def judge_topic_relevance(topic: str, header_content: str) -> bool:
    """
    判断当前标题与内容是否相关

    参数:
        topic (str): 当前大标题
        header_content (str): 内容文本

    返回:
        bool: 是否相关
    """
    try:
        # 构建提示词
        prompt = f"""
        请判断以下标题与内容是否相关:

        大标题: {topic}
        内容: {header_content}

        要求:
        1. 如果标题与内容在主题、关键词或核心内容上相关，或者当前的内容符合大标题相关的行业或者上下游行业，返回1
        2. 如果大标题与内容完全不相关，返回0
        3. 请严格按照JSON格式返回结果：{{"result": 1}}或{{"result": 0}}
        4. 不要包含任何其他解释或文字
        """

        # 调用Qwen-turbo模型进行判断
        # print(f"当前标题: {topic}")
        # print(f"当前内容: {header_content}")
        completion = qwen_client.chat.completions.create(
            model="qwen-plus-latest",
            messages=[
                {"role": "system",
                 "content": "你是一个专业的文本分析助手。请始终以JSON格式返回结果，格式为{\"result\": 1}或{\"result\": 0}。"},
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
            model="qwen-plus-latest",
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
        print(f"判断行业指标相关性时出错: {e}")
        return False


def eco_indicator_relevance(eco_indicator_name, topic):
    """
    判断经济指标与当前标题是否相关
    
    参数:
        eco_indicator_name (str): 经济指标名称
        topic (str): 当前大标题
        
    返回:
        bool: 是否相关
    """
    try:
        # 构建提示词
        prompt = f"""
        请判断以下经济指标与标题是否相关:
        
        经济指标，每个指标可能涉及不同行业或领域: {eco_indicator_name}
        标题，包含行业信息: {topic}
    
        要求:
        1. 请根据标题中的关键词和行业背景理解其核心内容，判断每个指标是否与标题的行业相关。如果经济指标与标题涉及的行业相关，返回1；
        2. 如果经济指标与标题的行业相关性极低,或者完全无关，返回0；
        3. 请严格按照JSON格式返回结果：{{"result": 1}}或{{"result": 0}}
        4. 不要包含任何其他解释或文字
        """

        # 调用Qwen-long模型进行判断
        completion = qwen_client.chat.completions.create(
            model="qwen-plus-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的文本分析助手，擅长根据行业筛选指标。请始终以JSON格式返回结果，格式为{\"result\": 1}或{\"result\": 0}。"},
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
        print(f"判断经济指标相关性时出错: {e}")
        return False


def judge_area_topic_relevance(topic: str, title: str, involved_region: str) -> bool:
    """
    判断当前标题与内容是否相关

    参数:
        topic (str): 当前大标题
        header_content (str): 内容文本

    返回:
        bool: 是否相关
    """
    try:
        # 构建提示词
        prompt = f"""
        请判断以下标题与内容是否相关:

        大标题: {topic}
        政策标题: {title}
        涉及地区: {involved_region}

        要求:
        1. 判断地域相关性：
           - 如果topic提及特定地区（如北京市），则保留该地区的政策、该地区所在省（如北京市的上级为全国）的政策以及全国性政策，返回1
           - 如果topic提及特定省份（如浙江省），则保留该省的政策、该省内所有市的政策以及全国性政策，返回1
           - 如果topic是全国范围的，则只保留全国性政策（国家级单位发布的政策），返回1
           - 如果政策涉及的地区与topic提及的地区无关，也不是其上级行政区域，返回0
        2. 判断内容相关性：
           - 如果标题与内容在主题、关键词或核心内容上相关，或者当前的内容符合大标题相关的行业或者上下游行业，返回1
           - 如果大标题与内容完全不相关，返回0
        3. 请严格按照JSON格式返回结果：{{"result": 1}}或{{"result": 0}}
        4. 不要包含任何其他解释或文字
        """

        # 调用Qwen-turbo模型进行判断
        print(f"当前主题: {topic}")
        print(f"当前政策标题: {title}")
        print(f"当前机构名称: {involved_region}")
        completion = qwen_client.chat.completions.create(
            model="qwen-plus-latest",
            messages=[
                {"role": "system",
                 "content": "你是一个专业的文本分析助手。请始终以JSON格式返回结果，格式为{\"result\": 1}或{\"result\": 0}。"},
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
def cics_name_relavance(cics_name: str, topic: str) -> bool:
    """
    判断CICS名称与当前标题是否相关
    
    参数:
        cics_name (str): CICS名称
        topic (str): 当前标题
        
    返回:
        bool: 是否相关
    """
    try:
        # 构建提示词
        prompt = f"""
        行业名称: {cics_name}
        当前标题: {topic}

        要求:
        1. 判断行业名称与当前标题是否相关：
           - 如果行业名称与当前标题在主题、关键词或核心内容上相关，或者行业是当前标题相关的上下游行业，返回1
           - 如果行业名称与当前标题完全不相关，返回0
        2. 请严格按照JSON格式返回结果：{{"result": 1}}或{{"result": 0}}
        3. 不要包含任何其他解释或文字
        """

        completion = qwen_client.chat.completions.create(
            model="qwen-turbo-latest",
            messages=[
                {"role": "system",
                 "content": "你是一个专业的行业分析助手。请始终以JSON格式返回结果，格式为{\"result\": 1}或{\"result\": 0}。"},
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
        print(f"判断行业相关性时出错: {e}")
        return False
    


def filter_ic_trend_scores_by_relevance(ic_trend_scores, current_title):
    """
    根据cics_name与当前标题的相关性筛选ic_trend_scores
    
    参数:
        ic_trend_scores (List[dict]): 原始的ic_trend_scores数据
        current_title (str): 当前标题
        
    返回:
        List[dict]: 筛选后的ic_trend_scores数据
    """
    # 如果ic_trend_scores为空，直接返回
    if not ic_trend_scores:
        print("ic_trend_scores为空，无需筛选")
        return []
    
    # 统计所有不同的cics_name
    cics_names = set()
    for item in ic_trend_scores:
        if 'cics_name' in item and item['cics_name']:
            cics_names.add(item['cics_name'])
    
    print(f"找到{len(cics_names)}个不同的cics_name: {', '.join(cics_names)}")
    
    # 使用并行处理判断每个cics_name的相关性
    relevant_cics_names = set()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 提交所有判断任务
        futures = {executor.submit(cics_name_relavance, cics_name, current_title): cics_name 
                  for cics_name in cics_names}
        
        # 收集相关的cics_name
        for future in concurrent.futures.as_completed(futures):
            cics_name = futures[future]
            try:
                if future.result():  # 如果返回True则保留
                    relevant_cics_names.add(cics_name)
                    print(f"cics_name '{cics_name}' 与当前标题相关")
                else:
                    print(f"cics_name '{cics_name}' 与当前标题不相关")
            except Exception as e:
                print(f"判断cics_name '{cics_name}' 相关性时出错: {e}")
    
    print(f"筛选出{len(relevant_cics_names)}个相关的cics_name: {', '.join(relevant_cics_names)}")
    
    # 筛选保留相关cics_name的记录
    filtered_scores = [score for score in ic_trend_scores 
                      if 'cics_name' in score and score['cics_name'] in relevant_cics_names]
    
    print(f"原始数据有{len(ic_trend_scores)}条记录，筛选后保留{len(filtered_scores)}条记录")
    
    return filtered_scores

if __name__ == "__main__":
    eco_indicators = [{


                                            "id": 2140920,
                                            "indic_id": 2100604878,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2016-12-31",
                                            "data_value": 3326430.0,
                                            "update_time": "2019-01-23 16:07:48",
                                            "name_cn": "软件产业:软件产品收入:应用软件:行业应用软件:交通运输行业软件"
                                        },
                                        {
                                            "id": 2230313,
                                            "indic_id": 2100604878,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2017-12-31",
                                            "data_value": 4740360.0,
                                            "update_time": "2019-05-22 14:01:47",
                                            "name_cn": "软件产业:软件产品收入:应用软件:行业应用软件:交通运输行业软件"
                                        },
                                        {
                                            "id": 2458352,
                                            "indic_id": 2100604878,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2018-12-31",
                                            "data_value": 5336217.0,
                                            "update_time": "2020-05-28 13:40:53",
                                            "name_cn": "软件产业:软件产品收入:应用软件:行业应用软件:交通运输行业软件"
                                        },
                                        {
                                            "id": 4604924,
                                            "indic_id": 2100604878,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2019-12-31",
                                            "data_value": 7605947.0,
                                            "update_time": "2021-07-06 13:35:43",
                                            "name_cn": "软件产业:软件产品收入:应用软件:行业应用软件:交通运输行业软件"
                                        },
                                        {
                                            "id": 2140927,
                                            "indic_id": 2100604879,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2016-12-31",
                                            "data_value": 3546738.0,
                                            "update_time": "2019-01-23 16:07:48",
                                            "name_cn": "软件产业:软件产品收入:应用软件:行业应用软件:能源控制软件"
                                        },
                                        {
                                            "id": 2230314,
                                            "indic_id": 2100604879,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2017-12-31",
                                            "data_value": 4762151.0,
                                            "update_time": "2019-05-22 14:01:47",
                                            "name_cn": "软件产业:软件产品收入:应用软件:行业应用软件:能源控制软件"
                                        },
                                        {
                                            "id": 2458353,
                                            "indic_id": 2100604879,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2018-12-31",
                                            "data_value": 4651863.0,
                                            "update_time": "2020-05-28 13:40:53",
                                            "name_cn": "软件产业:软件产品收入:应用软件:行业应用软件:能源控制软件"
                                        },
                                        {
                                            "id": 4604943,
                                            "indic_id": 2100604879,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2019-12-31",
                                            "data_value": 6015119.0,
                                            "update_time": "2021-07-06 13:35:43",
                                            "name_cn": "软件产业:软件产品收入:应用软件:行业应用软件:能源控制软件"
                                        },
                                        {
                                            "id": 2141439,
                                            "indic_id": 2100604964,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2014-12-31",
                                            "data_value": 448161.0,
                                            "update_time": "2019-01-23 16:07:58",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:汽车电子:行驶系控制系统"
                                        },
                                        {
                                            "id": 2141438,
                                            "indic_id": 2100604964,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2015-12-31",
                                            "data_value": 398623.0,
                                            "update_time": "2019-01-23 16:07:58",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:汽车电子:行驶系控制系统"
                                        },
                                        {
                                            "id": 2141437,
                                            "indic_id": 2100604964,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2016-12-31",
                                            "data_value": 488522.0,
                                            "update_time": "2019-01-23 16:07:58",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:汽车电子:行驶系控制系统"
                                        },
                                        {
                                            "id": 2230399,
                                            "indic_id": 2100604964,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2017-12-31",
                                            "data_value": 130057.0,
                                            "update_time": "2019-05-22 14:01:49",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:汽车电子:行驶系控制系统"
                                        },
                                        {
                                            "id": 2141445,
                                            "indic_id": 2100604965,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2014-12-31",
                                            "data_value": 2903299.0,
                                            "update_time": "2019-01-23 16:07:59",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:汽车电子:车身控制系统"
                                        },
                                        {
                                            "id": 2141444,
                                            "indic_id": 2100604965,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2015-12-31",
                                            "data_value": 3344438.0,
                                            "update_time": "2019-01-23 16:07:59",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:汽车电子:车身控制系统"
                                        },
                                        {
                                            "id": 2141443,
                                            "indic_id": 2100604965,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2016-12-31",
                                            "data_value": 4451979.0,
                                            "update_time": "2019-01-23 16:07:59",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:汽车电子:车身控制系统"
                                        },
                                        {
                                            "id": 2230400,
                                            "indic_id": 2100604965,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2017-12-31",
                                            "data_value": 2821736.0,
                                            "update_time": "2019-05-22 14:01:49",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:汽车电子:车身控制系统"
                                        },
                                        {
                                            "id": 2141457,
                                            "indic_id": 2100604967,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2014-12-31",
                                            "data_value": 426911.0,
                                            "update_time": "2019-01-23 16:07:59",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:智能交通"
                                        },
                                        {
                                            "id": 2141456,
                                            "indic_id": 2100604967,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2015-12-31",
                                            "data_value": 376984.0,
                                            "update_time": "2019-01-23 16:07:59",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:智能交通"
                                        },
                                        {
                                            "id": 2141455,
                                            "indic_id": 2100604967,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2016-12-31",
                                            "data_value": 145530.0,
                                            "update_time": "2019-01-23 16:07:59",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:智能交通"
                                        },
                                        {
                                            "id": 2230402,
                                            "indic_id": 2100604967,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2017-12-31",
                                            "data_value": 112078.0,
                                            "update_time": "2019-05-22 14:01:49",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:智能交通"
                                        },
                                        {
                                            "id": 2141463,
                                            "indic_id": 2100604968,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2014-12-31",
                                            "data_value": 426911.0,
                                            "update_time": "2019-01-23 16:07:59",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:智能交通:交通信号控制机"
                                        },
                                        {
                                            "id": 2141462,
                                            "indic_id": 2100604968,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2015-12-31",
                                            "data_value": 376984.0,
                                            "update_time": "2019-01-23 16:07:59",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:智能交通:交通信号控制机"
                                        },
                                        {
                                            "id": 2141461,
                                            "indic_id": 2100604968,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2016-12-31",
                                            "data_value": 145530.0,
                                            "update_time": "2019-01-23 16:07:59",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:智能交通:交通信号控制机"
                                        },
                                        {
                                            "id": 2230403,
                                            "indic_id": 2100604968,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2017-12-31",
                                            "data_value": 112078.0,
                                            "update_time": "2019-05-22 14:01:49",
                                            "name_cn": "软件产业:嵌入式系统软件收入:计算机应用产品:智能交通:交通信号控制机"
                                        },
                                        {
                                            "id": 2263082,
                                            "indic_id": 2100616512,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2010-12-31",
                                            "data_value": 1899805.0,
                                            "update_time": "2019-07-10 16:03:12",
                                            "name_cn": "软件产业:内资企业:软件产品收入:应用软件:行业应用软件:交通运输行业软件"
                                        },
                                        {
                                            "id": 2262166,
                                            "indic_id": 2100616512,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2011-12-31",
                                            "data_value": 1698866.0,
                                            "update_time": "2019-07-09 14:52:17",
                                            "name_cn": "软件产业:内资企业:软件产品收入:应用软件:行业应用软件:交通运输行业软件"
                                        },
                                        {
                                            "id": 2261208,
                                            "indic_id": 2100616512,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2012-12-31",
                                            "data_value": 1642228.0,
                                            "update_time": "2019-07-09 13:59:21",
                                            "name_cn": "软件产业:内资企业:软件产品收入:应用软件:行业应用软件:交通运输行业软件"
                                        },
                                        {
                                            "id": 2260237,
                                            "indic_id": 2100616512,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2013-12-31",
                                            "data_value": 1755841.0,
                                            "update_time": "2019-07-09 11:42:38",
                                            "name_cn": "软件产业:内资企业:软件产品收入:应用软件:行业应用软件:交通运输行业软件"
                                        },
                                        {
                                            "id": 2256607,
                                            "indic_id": 2100616512,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2016-12-31",
                                            "data_value": 3059836.0,
                                            "update_time": "2019-07-08 15:42:25",
                                            "name_cn": "软件产业:内资企业:软件产品收入:应用软件:行业应用软件:交通运输行业软件"
                                        },
                                        {
                                            "id": 2255315,
                                            "indic_id": 2100616512,
                                            "publish_date": "2025-03-29",
                                            "period_date": "2017-12-31",
                                            "data_value": 4203859.0,
                                            "update_time": "2019-07-08 15:03:17",
                                            "name_cn": "软件产业:内资企业:软件产品收入:应用软件:行业应用软件:交通运输行业软件"
                                        }
                                    ]
    

    topic = '2023年新能源汽车发展全景'
    print(f"eco_indicators筛选前的长度: {len(eco_indicators)}")
        # 使用并行方式筛选相关的经济指标
    if eco_indicators:  # 检查列表是否为空
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 创建future到indicator的映射
            future_to_indicator = {
                executor.submit(eco_indicator_relevance, indicator.get('name_cn', ''), topic): indicator
                for indicator in eco_indicators
            }
            
            # 收集相关指标
            relevant_eco_indicators = []
            for future in concurrent.futures.as_completed(future_to_indicator):
                indicator = future_to_indicator[future] 
                try:
                    if future.result():  # 如果相关
                        relevant_eco_indicators.append(indicator)
                except Exception as e:
                    print(f"处理经济指标 {indicator.get('name_cn', '')} 时出错: {e}")   
        
        eco_indicators = relevant_eco_indicators  # 重新赋值
        print(f"筛选后的eco_indicators: {eco_indicators}")
        print(f"筛选后的eco_indicators的长度: {len(eco_indicators)}")   