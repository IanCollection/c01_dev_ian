import time
import os
import sys
import json
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from decimal import Decimal
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 本地模块导入
from Agent.tool_agents import code_title_spliter
from scrpit.tune_second_level_headers import modify_second_level_headers, modify_first_level_headers, \
    modify_first_level_headers_stream, modify_second_level_headers_stream
from database.neo4j_query import query_file_batch_nodes
from palyground import extract_headlines, generate_section_list
from scrpit.overview_report import build_overview_with_report, generate_comprehensive_toc_v2, \
    generate_comprehensive_toc_v2_stream
from scrpit.overview_title import generate_comprehensive_toc_with_focus_points
from Agent.Overview_agent import overview_conclusion, tuning_third_heading, year_extract_from_title
from scrpit.query_report_policy_ic_indicator import query_relative_data, query_relative_data_v3


# 自定义JSON编码器
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)



# 一级标题处理函数
def process_first_level_title(first_level_section, index, topic = None):
    start_time = time.time()
    print(f"正在调用process_first_level_title,当前处理的主题是: {topic}")
    first_level_title = first_level_section["title"]
    print(f"开始处理第 {index + 1} 个一级标题: {first_level_title}")

    new_first_level = {
        "title": first_level_title,
        "subsections": []
    }

    # 使用线程池处理二级标题
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_second_level_title, first_level_title, sec, topic)
                  for sec in first_level_section["subsections"]]
        
        for future in as_completed(futures):
            try:
                if (new_second_level := future.result()):
                    new_first_level["subsections"].append(new_second_level)
            except Exception as e:
                print(f"处理二级标题时出错: {str(e)}")

    print(f"完成第 {index + 1} 个一级标题: {first_level_title} (耗时: {time.time() - start_time:.2f}秒)")
    return index, new_first_level

# 二级标题处理函数
def process_second_level_title(first_level_title, second_level_section,topic = None):
    print(f"正在调用process_second_level_title,当前处理的主题是: {topic}")
    new_second_level = {
        "title": second_level_section["title"],
        "subsections": []
    }

    if not second_level_section.get("subsections"):
        print(f"警告：{first_level_title} - {second_level_section['title']} 没有三级标题，将创建默认节点")
        new_second_level["subsections"].append(
            process_third_level_title(first_level_title, second_level_section["title"], {"title": "默认内容"}, instruction=None, topic=topic)
        )
    else:
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_third_level_title, first_level_title, second_level_section["title"], sec, None, topic)
                      for sec in second_level_section["subsections"]]
            
            for future in as_completed(futures):
                try:
                    if (new_third_level := future.result()):
                        new_second_level["subsections"].append(new_third_level)
                except Exception as e:
                    print(f"处理三级标题时出错: {str(e)}")

    return new_second_level


def process_first_level_title_no_refine(first_level_section, index, topic = None):
    start_time = time.time()
    print(f"正在调用process_first_level_title,当前处理的主题是: {topic}")
    first_level_title = first_level_section["title"]
    print(f"开始处理第 {index + 1} 个一级标题: {first_level_title}")

    new_first_level = {
        "title": first_level_title,
        "subsections": []
    }

    # 使用线程池处理二级标题
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_second_level_title_norefine, first_level_title, sec, topic)
                  for sec in first_level_section["subsections"]]
        
        for future in as_completed(futures):
            try:
                if (new_second_level := future.result()):
                    new_first_level["subsections"].append(new_second_level)
            except Exception as e:
                print(f"处理二级标题时出错: {str(e)}")

    print(f"完成第 {index + 1} 个一级标题: {first_level_title} (耗时: {time.time() - start_time:.2f}秒)")
    return index, new_first_level

# 二级标题处理函数
def process_second_level_title_norefine(first_level_title, second_level_section,topic = None):
    print(f"正在调用process_second_level_title,当前处理的主题是: {topic}")
    new_second_level = {
        "title": second_level_section["title"],
        "subsections": []
    }

    if not second_level_section.get("subsections"):
        print(f"警告：{first_level_title} - {second_level_section['title']} 没有三级标题，将创建默认节点")
        new_second_level["subsections"].append(
            process_third_level_title_norefine(first_level_title, second_level_section["title"], {"title": "默认内容"}, instruction=None, topic=topic)
        )
    else:
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_third_level_title_norefine, first_level_title, second_level_section["title"], sec, None, topic)
                      for sec in second_level_section["subsections"]]
            
            for future in as_completed(futures):
                try:
                    if (new_third_level := future.result()):
                        new_second_level["subsections"].append(new_third_level)
                except Exception as e:
                    print(f"处理三级标题时出错: {str(e)}")

    return new_second_level


# 三级标题处理函数
def process_third_level_title_norefine(first_level_title, second_level_title, third_level_section,instruction = None,topic = None):
    try:
        # 修改日志标签以便区分
        print(f"[process_third_level_title ENTRY] Received topic: {topic}") 
        if not isinstance(third_level_section, dict) or 'title' not in third_level_section:
            print(f"无效的三级标题数据格式: {third_level_section}")
            return create_default_third_level()

        combined_title = f"{first_level_title} - {second_level_title} - {third_level_section['title']}"
        # year = 2023
        year = year_extract_from_title(combined_title)
        try:
            # 添加调用前日志
            print(f"[process_third_level_title PRE-CALL] Calling query_relative_data_v3 with topic: {topic}, instruction: {instruction}") 
            reports, policy, ic_trends, ic_current, writing_instruction, eco_indicators,eco_indicators_sum,eco_indicators_report, analysis_results_ictrend_v2,filtered_result_ic_current_rating= query_relative_data_v3(year, combined_title,instruction,topic)
            print(f"writing_instruction: {writing_instruction}")

        except Exception as query_error:
            print(f"查询数据时出错 {combined_title}: {str(query_error)}")
            return create_error_third_level(third_level_section)
        print(f'ic_trends_mile_stone4:{ic_trends}')
        instruction = writing_instruction or "无具体写作指导"
        reference = {
            "report_source": reports if isinstance(reports, list) else [],
            "policy_source": policy if isinstance(policy, list) else [],
            "industry_indicator_part_1": ic_trends if ic_trends else "",
            "industry_indicator_part_1_analysis":analysis_results_ictrend_v2,
            "industry_indicator_part_2": ic_current,
            "industry_indicator_part_2_analysis":filtered_result_ic_current_rating,
            "indicators": eco_indicators,
            "indicators_sum": eco_indicators_sum,
            "indicators_report":eco_indicators_report
        }
        try:
            print(f"181_third_level_section['title']:{third_level_section['title']}")
            current_new_title_code, current_new_pure_title = code_title_spliter(third_level_section['title'])
            print(f'current_new_title_code: {current_new_title_code}')
        except Exception as tuning_error:
            print(f"调整标题时出错 {third_level_section['title']}: {str(tuning_error)}")
            current_new_title = third_level_section['title']

        return {
            'title_code':current_new_title_code,
            "title": current_new_pure_title,
            "previous_title": third_level_section['title'],
            "relative_data": {
                "writing_instructions": instruction,
                "reference": reference
            }
        }
    except Exception as e:
        print(f"处理 {first_level_title} - {second_level_title} - {str(third_level_section)} 时出错: {str(e)}")
        return create_error_third_level(third_level_section)



def process_second_level_title_for_edit(first_level_title, second_level_section, topic = None):
    new_second_level = {
        "title": second_level_section["title"],
        "subsections": [],
    }

    if not second_level_section.get("subsections"):
        print(f"警告：{first_level_title} - {second_level_section['title']} 没有三级标题，将创建默认节点")
        new_second_level["subsections"].append(
            process_third_level_title(first_level_title, second_level_section["title"], {"title": "默认内容"}, None, topic)
        )
    else:
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_third_level_title, first_level_title, second_level_section["title"], sec, None, topic)
                       for sec in second_level_section["subsections"]]

            for future in as_completed(futures):
                try:
                    if (new_third_level := future.result()):
                        new_second_level["subsections"].append(new_third_level)
                except Exception as e:
                    print(f"处理三级标题时出错: {str(e)}")

    return new_second_level

# 三级标题处理函数
def process_third_level_title(first_level_title, second_level_title, third_level_section,instruction = None,topic = None):
    try:
        # 修改日志标签以便区分
        print(f"[process_third_level_title ENTRY] Received topic: {topic}") 
        if not isinstance(third_level_section, dict) or 'title' not in third_level_section:
            print(f"无效的三级标题数据格式: {third_level_section}")
            return create_default_third_level()

        combined_title = f"{first_level_title}/{second_level_title}/{third_level_section['title']}"
        # year = 2023
        year = year_extract_from_title(combined_title)
        try:

            # 添加调用前日志
            print(f"[process_third_level_title PRE-CALL] Calling query_relative_data_v3 with topic: {topic}, instruction: {instruction}") 
            reports, policy, ic_trends, ic_current, writing_instruction, eco_indicators,eco_indicators_sum,eco_indicators_report, analysis_results_ictrend_v2,filtered_result_ic_current_rating= query_relative_data_v3(year, combined_title,instruction,topic)
            print(f"writing_instruction: {writing_instruction}")

        except Exception as query_error:
            print(f"查询数据时出错 {combined_title}: {str(query_error)}")
            return create_error_third_level(third_level_section)

        print(f'ic_trends_mile_stone4:{ic_trends}')
        # 处理数据
        # ic_trends_analysis = process_ic_trends(ic_trends)
        # ic_current = ic_current if isinstance(ic_current, str) else "无相关数据"
        instruction = writing_instruction or "无具体写作指导"

        reference = {
            "report_source": reports if isinstance(reports, list) else [],
            "policy_source": policy if isinstance(policy, list) else [],
            "industry_indicator_part_1": ic_trends if ic_trends else "",
            "industry_indicator_part_1_analysis":analysis_results_ictrend_v2,
            "industry_indicator_part_2": ic_current,
            "industry_indicator_part_2_analysis":filtered_result_ic_current_rating,
            "indicators": eco_indicators,
            "indicators_sum": eco_indicators_sum,
            "indicators_report":eco_indicators_report
        }
        tuning_reference = {
            "report_source": reports if isinstance(reports, list) else [],
            "policy_source": policy if isinstance(policy, list) else [],
            # "industry_indicator_part_1": ic_trends_analysis,
            "industry_indicator_part_1_analysis":analysis_results_ictrend_v2,
            # "industry_indicator_part_2": ic_current,
            "industry_indicator_part_2_analysis":filtered_result_ic_current_rating,
            # "indicators": eco_indicators,
            # "indicators_sum": eco_indicators_sum,
            "indicators_report":eco_indicators_report
        }

        try:
            current_new_title, _, _ = tuning_third_heading(tuning_reference, instruction, third_level_section['title'],topic)

            # 分析思路也需要refine
            # currrent_ana_instruntion = generate_ana_instruction_for_third_headers_after_query(third_level_section['title'], reference)

            current_new_title_code, current_new_pure_title = code_title_spliter(current_new_title)
            print(f'current_new_title_code: {current_new_title_code}')
        except Exception as tuning_error:
            print(f"调整标题时出错 {third_level_section['title']}: {str(tuning_error)}")
            current_new_title = third_level_section['title']

        return {
            'title_code':current_new_title_code,
            "title": current_new_pure_title,
            "previous_title": third_level_section['title'],
            "relative_data": {
                "writing_instructions": instruction,
                "reference": reference
            }
        }
    except Exception as e:
        print(f"处理 {first_level_title} - {second_level_title} - {str(third_level_section)} 时出错: {str(e)}")
        return create_error_third_level(third_level_section)

# 辅助函数
def create_default_third_level():
    return {
        "title": "未知标题",
        "previous_title": "未知标题",
        "relative_data": {
            "writing_instructions": "无法获取分析思路",
            "reference": create_default_reference()
        }
    }

def create_error_third_level(section):
    return {
        "title": section.get('title', '未知标题') if isinstance(section, dict) else '未知标题',
        "previous_title": section.get('title', '未知标题') if isinstance(section, dict) else '未知标题',
        "relative_data": {
            "writing_instructions": "无法获取指导",
            "reference": create_default_reference()
        }
    }

def create_default_reference():
    return {
        "report_source": [],
        "policy_source": [],
        "industry_indicator_part_1": "无相关数据",
        "industry_indicator_part_2": "无相关数据",
        "indicators": []
    }

def process_ic_trends(ic_trends):
    if not ic_trends:
        return "无相关数据"
    if isinstance(ic_trends, dict) and "overall_analysis" in ic_trends:
        return ic_trends["overall_analysis"]
    return ic_trends if isinstance(ic_trends, str) else "无相关数据"

def process_first_level_title_serial(first_level_section, index, topic=None):
    """串行版本的一级标题处理函数，顺序处理所有二级标题，不使用线程池"""
    start_time = time.time()
    print(f"[Serial] 正在处理第 {index + 1} 个一级标题: {first_level_section['title']}，主题: {topic}")

    first_level_title = first_level_section["title"]
    new_first_level = {
        "title": first_level_title,
        "subsections": []
    }

    # 顺序处理二级标题，不使用线程池
    if "subsections" in first_level_section:
        for sec in first_level_section["subsections"]:
            try:
                new_second_level = process_second_level_title_serial(first_level_title, sec, topic)
                if new_second_level:
                    new_first_level["subsections"].append(new_second_level)
            except Exception as e:
                print(f"[Serial] 处理二级标题时出错: {str(e)}")

    print(f"[Serial] 完成第 {index + 1} 个一级标题: {first_level_title} (耗时: {time.time() - start_time:.2f}秒)")
    return index, new_first_level


def process_second_level_title_serial(first_level_title, second_level_section, topic=None):
    """串行版本的二级标题处理函数，顺序处理所有三级标题，不使用线程池"""
    start_time = time.time()
    second_level_title = second_level_section["title"]
    print(f"[Serial] 正在处理二级标题: {second_level_title}，主题: {topic}")

    new_second_level = {
        "title": second_level_title,
        "subsections": []
    }

    if not second_level_section.get("subsections"):
        print(f"[Serial] 警告：{first_level_title} - {second_level_title} 没有三级标题，将创建默认节点")
        new_second_level["subsections"].append(
            process_third_level_title(first_level_title, second_level_title, {"title": "默认内容"}, instruction=None, topic=topic)
        )
    else:
        # 顺序处理三级标题，不使用线程池
        for sec in second_level_section["subsections"]:
            try:
                new_third_level = process_third_level_title(first_level_title, second_level_title, sec, None, topic)
                if new_third_level:
                    new_second_level["subsections"].append(new_third_level)
            except Exception as e:
                print(f"[Serial] 处理三级标题时出错: {str(e)}")

    print(f"[Serial] 完成二级标题: {second_level_title} (耗时: {time.time() - start_time:.2f}秒)")
    return new_second_level


def process_second_level_title_for_edit_serial(first_level_title, second_level_section, topic=None):
    """串行版本的二级标题编辑函数，用于编辑模式下的二级标题处理"""
    start_time = time.time()
    second_level_title = second_level_section["title"]
    print(f"[Serial-Edit] 正在处理二级标题: {second_level_title}，主题: {topic}")

    new_second_level = {
        "title": second_level_title,
        "subsections": [],
    }

    if not second_level_section.get("subsections"):
        print(f"[Serial-Edit] 警告：{first_level_title} - {second_level_title} 没有三级标题，将创建默认节点")
        new_second_level["subsections"].append(
            process_third_level_title(first_level_title, second_level_title, {"title": "默认内容"}, None, topic)
        )
    else:
        # 顺序处理三级标题
        for sec in second_level_section["subsections"]:
            try:
                new_third_level = process_third_level_title(first_level_title, second_level_title, sec, None, topic)
                if new_third_level:
                    new_second_level["subsections"].append(new_third_level)
            except Exception as e:
                print(f"[Serial-Edit] 处理三级标题时出错: {str(e)}")

    print(f"[Serial-Edit] 完成二级标题: {second_level_title} (耗时: {time.time() - start_time:.2f}秒)")
    return new_second_level


# 示例：使用串行处理流程
def process_section_tree_serial(section_list, topic=None):
    """串行处理整个章节树结构，包括一级、二级和三级标题"""
    start_time = time.time()
    print(f"[Serial] 开始串行处理章节，共 {len(section_list)} 个一级标题，主题: {topic}")
    
    full_section_list = []
    
    for i, section in enumerate(section_list):
        try:
            print(f"[Serial] 处理一级标题 {i+1}/{len(section_list)}: {section.get('title', 'N/A')}")
            index, processed_first_level = process_first_level_title_serial(section, i, topic)
            
            # 调整二级标题
            print(f"[Serial] 调整二级标题 {processed_first_level.get('title', 'N/A')}")
            modified_content_second_headings = modify_second_level_headers_stream(processed_first_level, topic)
            
            # 调整一级标题
            print(f"[Serial] 调整一级标题 {processed_first_level.get('title', 'N/A')}")
            modified_content = modify_first_level_headers_stream(modified_content_second_headings, topic)
            
            full_section_list.append(modified_content)
            print(f"[Serial] 完成一级标题 {i+1}: {section.get('title', 'N/A')}")
        except Exception as e:
            print(f"[Serial] 处理一级标题时出错: {str(e)}")
    
    print(f"[Serial] 完成串行处理所有章节，共 {len(full_section_list)} 个结果 (总耗时: {time.time() - start_time:.2f}秒)")
    return full_section_list

if __name__ == "__main__":
    input_title = "中国新能源汽车产业可持续发展报告2023"
    
    # 获取研报数据
    title, reports_node, keywords, time_cost = build_overview_with_report(input_title)
    relative_reports = query_file_batch_nodes(reports_node)
    # print(relative_reports)
    # 生成目录
    reports_overview, all_reports, reports_cost = generate_comprehensive_toc_v2(input_title, relative_reports, keywords)

    #已经上线接口（v2）
    # reports_overview, all_reports, reports_cost = generate_comprehensive_toc_v2_stream(input_title, relative_reports, keywords)

    #已经上线接口（v1）
    general_overview, focus_points,second_levels = generate_comprehensive_toc_with_focus_points(input_title, keywords)


    if len(general_overview)>=2:
        final_overview, cost = overview_conclusion(reports_overview, general_overview[0], input_title)
    else:
        final_overview, cost = overview_conclusion(reports_overview, general_overview, input_title)

    with open('final_overview.json', 'w', encoding='utf-8') as f:
        json.dump(final_overview, f, indent=4, ensure_ascii=False, cls=DateTimeEncoder)
    # 处理章节
    content_json = extract_headlines(final_overview)
    print(f"content_json: {content_json}")
    section_list = generate_section_list(content_json)
    print(f"section_list: {section_list}")

    full_section_list = []
    print(f"总共 {len(section_list)} 个一级标题需要处理")

    for i, section in enumerate(section_list):
        try:
            index, processed_first_level = process_first_level_title(section, i, input_title)
            # print(processed_first_level)
            # print('开始对当前的一级和二级标题进行调整。')

            modified_content_second_headings = modify_second_level_headers_stream(processed_first_level, input_title)
            # print(modified_content_second_headings)
            modified_content = modify_first_level_headers_stream(
                modified_content_second_headings
            )
            # print(modified_content)
            full_section_list.append(modified_content)
            
            # print(full_section_list)
            # print(f"\n--- 一级标题 #{index+1}: {processed_first_level['title']} ---")
            # print(json.dumps(processed_first_level, indent=2, ensure_ascii=False, cls=DateTimeEncoder))
            print(f"--- 一级标题 #{index+1} 结束 ---\n")
        except Exception as e:
            print(f"处理一级标题时出错: {str(e)}")

    # 保存结果
    with open('section_list_stream_trunks.json', 'w', encoding='utf-8') as f:
        json.dump(full_section_list, f, indent=4, ensure_ascii=False, cls=DateTimeEncoder)
    print("结果已保存到 section_list_stream_trunks.json")