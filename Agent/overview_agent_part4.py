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


# def batch_generate_third_level_titles(second_level_sections: List[Dict]) -> List[Dict]:
#     """
#     批量处理多个二级标题，为每个二级标题生成对应的三级标题

#     参数:
#         second_level_sections (List[Dict]): 包含多个二级标题信息的列表

#     返回:
#         List[Dict]: 添加了三级标题的二级标题列表
#     """
#     results = []

#     for section in second_level_sections:
#         second_level_title = section.get("title", "")
#         title_code = section.get("title_code", "")
#         analysis_instruction = section.get("ana_instruction", "")

#         # 生成三级标题
#         third_level_result = generate_third_level_titles(
#             second_level_title,
#             title_code,
#             analysis_instruction
#         )

#         # 将三级标题添加到二级标题结构中
#         section_copy = section.copy()
#         section_copy["subsections"] = third_level_result.get("third_level_titles", [])
#         results.append(section_copy)

#     return results


if __name__ == "__main__":

    input_json = {
        "title": "消费者行为与需求洞察：趋势与痛点分析",
        "subsections": [
            {
                "title": "用户画像与消费趋势分析",
                "subsections": [],
                "previous_title": "5.1 用户画像分析",
                "title_code": "5.1",
                "ana_instruction": "二级标题聚焦用户画像与消费趋势，三级标题分别从家长角色和移动游戏玩家两个群体切入。建议：1）通过调研数据展示家长决策权重及变化趋势；2）结合玩家行为数据，分析轻氪金模式的驱动因素。确保两群体特征与整体消费趋势关联，强化支撑二级主题。"
            },
            {
                "title": "消费者需求与市场增长痛点分析",
                "subsections": [],
                "previous_title": "5.2 消费者需求特征与痛点分析",
                "title_code": "5.2",
                "ana_instruction": "二级标题聚焦消费者需求与市场痛点，三级标题分别从用户反馈驱动改进和功能情感平衡角度支撑。建议结合供需指数、技术突破案例及消费趋势数据，论证需求变化与增长阻碍，强化逻辑关联。"
            }
        ],
        "previous_title": "5. 消费者行为与需求洞察",
        "title_code": "5.",
        "ana_instruction": "一级标题聚焦消费者行为与需求，二级标题从用户画像和需求痛点两方面支撑。建议：1) 用户画像结合人口统计与消费习惯数据，揭示趋势；2) 需求痛点通过市场调研与反馈，挖掘增长障碍。数据支持包括消费行为统计、满意度调查及竞品分析，强化逻辑关联。"
    }

    title_code = input_json.get("title_code", "")
    title = input_json.get("title", "")
    ana_instruction = input_json.get("ana_instruction", "")

    result = generate_second_level_titles(title, title_code, ana_instruction)
    # 处理返回结果，确保是字典类型
    # print(result)
    # 将结果封装为指定格式的JSON
    formatted_result = format_third_level_result_to_json(title, title_code, ana_instruction, result)
    print(json.dumps(formatted_result, indent=4, ensure_ascii=False))

    # 将结果封装为指定格式的JSON
    # 为每个二级标题生成三级标题并添加到结果中
    if "subsections" in formatted_result:
        for section in formatted_result["subsections"]:
            second_level_title = section.get("title", "")
            section_title_code = section.get("title_code", "")
            section_ana_instruction = section.get("ana_instruction", "")

            # 生成三级标题
            third_level_result = generate_third_level_titles(
                second_level_title,
                section_title_code,
                section_ana_instruction
            )
            # print(f"third_level_result: {third_level_result}")
            # 将三级标题添加到二级标题结构中

            formatted_third_level = format_third_level_result_to_json_v2(
                second_level_title,
                section_title_code,
                section_ana_instruction,
                third_level_result
            )

            print(f"formatted_third_level: {json.dumps(formatted_third_level, indent=4, ensure_ascii=False)}")
            # 确保formatted_third_level是一个列表，其中包含字典对象
            if isinstance(formatted_third_level, str):
                try:
                    # 尝试将字符串解析为JSON
                    formatted_third_level = json.loads(formatted_third_level)
                except json.JSONDecodeError:
                    print(f"无法将formatted_third_level解析为JSON: {formatted_third_level}")
                    formatted_third_level = []

            # 如果formatted_third_level是字典，将其转换为包含该字典的列表
            if isinstance(formatted_third_level, dict):
                if "subsections" in formatted_third_level:
                    formatted_third_level = formatted_third_level.get("subsections", [])
                else:
                    formatted_third_level = [formatted_third_level]

            # 确保formatted_third_level是列表类型
            if not isinstance(formatted_third_level, list):
                print(f"formatted_third_level不是列表类型: {type(formatted_third_level)}")
                formatted_third_level = []
            # 遍历所有的三级标题
            for index, third_level_section in enumerate(formatted_third_level):
                print(f"third_level_section: {third_level_section}")
                # 获取三级标题的文本
                instruction = third_level_section.get("ana_instruction", None)
                title_code = third_level_section.get("title_code", "")
                third_title = third_level_section.get("title", "")
                combined_title = formatted_result.get("title", "") + " - " + third_title
                year = year_extract_from_title(combined_title)
                # print(year)

                try:
                    # 查询所有相关数据
                    reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_relative_data_v2(
                        year, combined_title, instruction)
                except Exception as e:
                    print(f"错误：调用 query_relative_data_v2 时发生异常: {e}")
                    # 设置默认值以避免程序崩溃
                    reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = [], [], [], [], "", [], {}, [], {}, {}

                ic_trends_analysis = process_ic_trends(ic_trends)
                instruction = instruction or "无具体分析思路"
                print(f"current_instruction:{instruction}")
                reference = {
                    "report_source": reports if isinstance(reports, list) else [],
                    "policy_source": policy if isinstance(policy, list) else [],
                    "industry_indicator_part_1": ic_trends_analysis,
                    "industry_indicator_part_1_analysis": analysis_results_ictrend_v2,
                    "industry_indicator_part_2": ic_current,
                    "industry_indicator_part_2_analysis": filtered_result_ic_current_rating if isinstance(
                        filtered_result_ic_current_rating, dict) else {},
                    "indicators": eco_indicators,
                    "indicators_sum": eco_indicators_sum,
                    "indicators_report": eco_indicators_report
                }
                # 将reference和instruction赋值给当前三级标题的relative_data
                third_level_section["relative_data"] = {
                    "reference": reference,
                    "writing_instruction": instruction
                }
                # 将third_level_section的relative_data赋值给formatted_third_level的relative_data
                formatted_third_level[index]["relative_data"] = third_level_section["relative_data"]
            
            # 收集所有三级标题，生成整体分析思路
            all_third_titles = [section.get("title", "") for section in formatted_third_level if section.get("title")]
            print(f"all_third_titles: {all_third_titles}")
            if all_third_titles:
                combined_titles = "、".join(all_third_titles)
                ana_instruction = generate_ana_instruction(combined_titles)
                print(f"ana_instruction: {ana_instruction}")


            section["subsections"] = formatted_third_level
            section["ana_instruction"] = ana_instruction
            # 将formatted_third_level保存为JSON文件
            # # 获取当前时间作为文件名的一部分，确保唯一性
            # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # json_filename = f"formatted_third_level_{timestamp}.json"
            # try:
            #     with open(json_filename, 'w', encoding='utf-8') as f:
            #         json.dump(formatted_third_level, f, ensure_ascii=False, indent=4)
            #     print(f"已成功将formatted_third_level保存为JSON文件: {json_filename}")
            # except Exception as e:
            #     print(f"保存formatted_third_level为JSON文件时发生错误: {e}")
            # print(second_level_list)
            # print(f"second_level_list: {second_level_list}")

    # 将formatted_result保存为JSON文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_filename = f"final_formatted_result_{timestamp}.json"

    try:
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(formatted_result, f, ensure_ascii=False, indent=4)
        print(f"已成功将formatted_result保存为JSON文件: {json_filename}")
    except Exception as e:
        print(f"保存formatted_result为JSON文件时发生错误: {e}")

    # if "subsections" in formatted_result and len(formatted_result["subsections"]) > 0 and "subsections" in formatted_result["subsections"][0]:
    #     # 获取包含三级标题字典的列表
    #     third_level_list = formatted_result["subsections"][0].get("subsections", [])
    #     # 遍历所有的三级标题
    #     for index, third_level_section in enumerate(third_level_list):
    #         # 获取三级标题的文本
    #         instruction = third_level_section.get("ana_instruction", None)
    #         print(instruction)
    #         title_code = third_level_section.get("title_code", "")
    #         third_title = third_level_section.get("title", "")
    #         combined_title = formatted_result.get("title", "") + " - " + third_title
    #         year = year_extract_from_title(combined_title)
    #         # print(year)

    #         try:
    #             # 查询所有相关数据
    #             reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_relative_data_v2(year, combined_title, instruction)
    #         except Exception as e:
    #             print(f"错误：调用 query_relative_data_v2 时发生异常: {e}")
    #             # 设置默认值以避免程序崩溃
    #             reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = [], [], [], [], "", [], {}, [], {}, {}

    #         ic_trends_analysis = process_ic_trends(ic_trends)
    #         instruction = instruction or "无具体分析思路"

    #         reference = {
    #             "report_source": reports if isinstance(reports, list) else [],
    #             "policy_source": policy if isinstance(policy, list) else [],
    #             "industry_indicator_part_1": ic_trends_analysis,
    #             "industry_indicator_part_1_analysis": analysis_results_ictrend_v2,
    #             "industry_indicator_part_2": ic_current,
    #             "industry_indicator_part_2_analysis": filtered_result_ic_current_rating if isinstance(filtered_result_ic_current_rating, dict) else {},
    #             "indicators": eco_indicators,
    #             "indicators_sum": eco_indicators_sum,
    #             "indicators_report": eco_indicators_report
    #         }
    #         # 将reference和instruction赋值给当前三级标题的relative_data
    #         third_level_section["relative_data"] = {
    #             "reference": reference,
    #             "writing_instruction": instruction
    #         }
    #     # 打印更新后的formatted_result
    #     print(json.dumps(formatted_result, indent=4, ensure_ascii=False))
    # # 将formatted_result保存为JSON文件
    # try:
    #     # 获取当前时间作为文件名的一部分，确保唯一性
    #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #     # 构建文件名
    #     filename = f"third_level_titles_{timestamp}.json"
    #     # 构建完整的文件路径（保存在当前文件夹）
    #     file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    #     # 将JSON数据写入文件
    #     with open(file_path, 'w', encoding='utf-8') as f:
    #         json.dump(formatted_result, f, ensure_ascii=False, indent=4)

    #     print(f"已成功将结果保存为JSON文件: {file_path}")
    # except Exception as e:
    #     print(f"保存JSON文件时出错: {e}")
    # else:
    #     print("错误：'formatted_result' 结构不符合预期，无法找到三级标题列表。")

    # 一级标题修改
    # {
    #   "title": "消费者行为与需求洞察：趋势与痛点分析",
    #   "subsections": [
    #     {
    #       "title": "用户画像与消费趋势分析",
    #       "subsections": [
    #       ],
    #       "previous_title": "5.1 用户画像分析",
    #       "title_code": "5.1",
    #       "ana_instruction": "二级标题聚焦用户画像与消费趋势，三级标题分别从家长角色和移动游戏玩家两个群体切入。建议：1）通过调研数据展示家长决策权重及变化趋势；2）结合玩家行为数据，分析轻氪金模式的驱动因素。确保两群体特征与整体消费趋势关联，强化支撑二级主题。"
    #     },
    #     {
    #       "title": "消费者需求与市场增长痛点分析",
    #       "subsections": [
    #       ],
    #       "previous_title": "5.2 消费者需求特征与痛点分析",
    #       "title_code": "5.2",
    #       "ana_instruction": "二级标题聚焦消费者需求与市场痛点，三级标题分别从用户反馈驱动改进和功能情感平衡角度支撑。建议结合供需指数、技术突破案例及消费趋势数据，论证需求变化与增长阻碍，强化逻辑关联。"
    #     }
    #   ],
    #   "previous_title": "5. 消费者行为与需求洞察",
    #   "title_code": "5.",
    #   "ana_instruction": "一级标题聚焦消费者行为与需求，二级标题从用户画像和需求痛点两方面支撑。建议：1) 用户画像结合人口统计与消费习惯数据，揭示趋势；2) 需求痛点通过市场调研与反馈，挖掘增长障碍。数据支持包括消费行为统计、满意度调查及竞品分析，强化逻辑关联。"
    # }

    # 二级标题修改
    # title_code = "5.1"
    # title = "新能源汽车"
    # ana_instruction = "二级标题聚焦用户画像与消费趋势，三级标题分别从家长角色和移动游戏玩家两个群体切入。建议：1）通过调研数据展示家长决策权重及变化趋势；2）结合玩家行为数据，分析轻氪金模式的驱动因素。确保两群体特征与整体消费趋势关联，强化支撑二级主题。"
    # result = generate_third_level_titles(title, title_code, ana_instruction)
    # # 处理返回结果，确保是字典类型
    # print(result)
    # # 将结果封装为指定格式的JSON
    # formatted_result = format_third_level_result_to_json(title, title_code, ana_instruction, result)
    # print(json.dumps(formatted_result, indent=4, ensure_ascii=False))
    #
    # if "subsections" in formatted_result and len(formatted_result["subsections"]) > 0 and "subsections" in formatted_result["subsections"][0]:
    #     # 获取包含三级标题字典的列表
    #     third_level_list = formatted_result["subsections"][0].get("subsections", [])
    #     # 遍历所有的三级标题
    #     for index, third_level_section in enumerate(third_level_list):
    #         # 获取三级标题的文本
    #         instruction = third_level_section.get("ana_instruction", None)
    #         print(instruction)
    #         title_code = third_level_section.get("title_code", "")
    #         third_title = third_level_section.get("title", "")
    #         combined_title = formatted_result.get("title", "") + " - " + third_title
    #         year = year_extract_from_title(combined_title)
    #         # print(year)
    #
    #         try:
    #             # 查询所有相关数据
    #             reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_relative_data_v2(year, combined_title, instruction)
    #         except Exception as e:
    #             print(f"错误：调用 query_relative_data_v2 时发生异常: {e}")
    #             # 设置默认值以避免程序崩溃
    #             reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = [], [], [], [], "", [], {}, [], {}, {}
    #
    #         ic_trends_analysis = process_ic_trends(ic_trends)
    #         instruction = instruction or "无具体分析思路"
    #
    #         reference = {
    #             "report_source": reports if isinstance(reports, list) else [],
    #             "policy_source": policy if isinstance(policy, list) else [],
    #             "industry_indicator_part_1": ic_trends_analysis,
    #             "industry_indicator_part_1_analysis": analysis_results_ictrend_v2,
    #             "industry_indicator_part_2": ic_current,
    #             "industry_indicator_part_2_analysis": filtered_result_ic_current_rating if isinstance(filtered_result_ic_current_rating, dict) else {},
    #             "indicators": eco_indicators,
    #             "indicators_sum": eco_indicators_sum,
    #             "indicators_report": eco_indicators_report
    #         }
    #         # 将reference和instruction赋值给当前三级标题的relative_data
    #         third_level_section["relative_data"] = {
    #             "reference": reference,
    #             "writing_instruction": instruction
    #         }
    #     # 打印更新后的formatted_result
    #     print(json.dumps(formatted_result, indent=4, ensure_ascii=False))
    # # 将formatted_result保存为JSON文件
    # try:
    #     # 获取当前时间作为文件名的一部分，确保唯一性
    #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #     # 构建文件名
    #     filename = f"third_level_titles_{timestamp}.json"
    #     # 构建完整的文件路径（保存在当前文件夹）
    #     file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    #
    #     # 将JSON数据写入文件
    #     with open(file_path, 'w', encoding='utf-8') as f:
    #         json.dump(formatted_result, f, ensure_ascii=False, indent=4)
    #
    #     print(f"已成功将结果保存为JSON文件: {file_path}")
    # except Exception as e:
    #     print(f"保存JSON文件时出错: {e}")
    # else:
    #     print("错误：'formatted_result' 结构不符合预期，无法找到三级标题列表。")
    #
    #