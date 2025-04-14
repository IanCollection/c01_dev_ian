import time
import os
import sys

from Agent.tool_agents import code_title_spliter

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)

from Agent.Overview_agent import tuning_second_heading, tuning_first_heading, get_ana_instruction_for_first_level, \
    get_ana_instruction_for_second_level
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# 自定义JSON编码器处理日期时间
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

# 加载报告内容
def load_report_content(file_path='section_list.json'):
    if not os.path.exists(file_path):
        print(f"错误：找不到文件 {file_path}")
        return []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载报告内容时出错: {str(e)}")
        return []

# 打印层次化标题
def print_hierarchical_titles(report_content):
    print("\n开始打印层次化标题...\n")
    
    # 遍历一级标题
    for i, first_level in enumerate(report_content):
        first_title = first_level.get('title', '无标题')
        print(f"第{i+1}章 {first_title}")
        
        # 遍历二级标题
        second_level_sections = first_level.get('subsections', [])
        for j, second_level in enumerate(second_level_sections):
            second_title = second_level.get('title', '无标题')
            print(f"  {i+1}.{j+1} {second_title}")
            
            # 遍历三级标题
            third_level_sections = second_level.get('subsections', [])
            for k, third_level in enumerate(third_level_sections):
                third_title = third_level.get('title', '无标题')
                print(f"    {i+1}.{j+1}.{k+1} {third_title}")
                
    print("\n层次化标题打印完成\n")
    return report_content

# def modify_second_title(second_level):
#     # 获取当前二级标题下的所有三级标题
#     third_level_titles = [sec.get('title', '无标题') for sec in second_level.get('subsections', [])]
#
#     # 这里可以调用大模型来修改标题，暂时用简单的字符串处理作为示例
#     original_title = second_level.get('title', '无标题')
#     modified_title = tuning_second_heading(third_level_titles, original_title)
#     ana_instruction = get_ana_instruction_for_second_level(third_level_titles,modified_title)
#     print(f"优化后的二级标题: {modified_title}")
#     # 更新二级标题，并保留原始标题
#     second_level['previous_title'] = original_title
#     second_level['title'] = modified_title
#     second_level['ana_instruction'] = ana_instruction
#     return second_level

def modify_second_title(second_level,topic = None):
    # 获取当前二级标题下的所有三级标题
    third_level_titles = [sec.get('title', '无标题') for sec in second_level.get('subsections', [])]

    # 这里可以调用大模型来修改标题，暂时用简单的字符串处理作为示例
    original_title = second_level.get('title', '无标题')
    modified_title = tuning_second_heading(third_level_titles, original_title,topic)
    ana_instruction = get_ana_instruction_for_second_level(third_level_titles, modified_title)
    print(f"ana_instruction_modify_second_title:{ana_instruction}")
    # print(f"优化后的二级标题: {modified_title}")

    title_code,modified_pure_title = code_title_spliter(modified_title)
    print(f"title_code: {title_code}")
    # 更新二级标题，并保留原始标题
    second_level['previous_title'] = original_title
    second_level['title_code'] = title_code
    print()
    second_level['title'] = modified_pure_title
    second_level['ana_instruction'] = ana_instruction
    return second_level


def modify_second_level_headers(report_content,topic = None):
    # 使用线程池并行处理二级标题
    with ThreadPoolExecutor() as executor:
        futures = []
        for first_level in report_content:
            second_level_sections = first_level.get('subsections', [])
            futures.extend(executor.submit(modify_second_title, second_level,topic) for second_level in second_level_sections)
        
        # 等待所有任务完成
        for future in as_completed(futures):
            future.result()
    return report_content


def modify_second_level_headers_stream(report_content,topic=None):
    # 使用线程池并行处理二级标题
    with ThreadPoolExecutor() as executor:
        futures = []
        for first_level in [report_content]:
            second_level_sections = first_level.get('subsections', [])
            futures.extend(executor.submit(modify_second_title, second_level,topic) for second_level in second_level_sections)

        # 等待所有任务完成
        for future in as_completed(futures):
            future.result()
    return report_content


# def modify_first_level_title(first_level):
#     # 获取当前一级标题下的所有二级标题
#     second_level_titles = [sec.get('title', '无标题') for sec in first_level.get('subsections', [])]
#     # 这里可以调用大模型来修改标题，暂时用简单的字符串处理作为示例
#     original_title = first_level.get('title', '无标题')
#     modified_title = tuning_first_heading(second_level_titles, original_title)
#     ana_instruction = get_ana_instruction_for_first_level(second_level_titles, original_title)
#     # 更新一级标题，并保留原始标题
#     first_level['previous_title'] = original_title
#     first_level['title'] = modified_title
#     first_level['ana_instruction'] = ana_instruction
#     return first_level

def modify_first_level_title(first_level,topic=None):
    # 获取当前一级标题下的所有二级标题
    second_level_titles = [sec.get('title', '无标题') for sec in first_level.get('subsections', [])]
    # 这里可以调用大模型来修改标题，暂时用简单的字符串处理作为示例
    original_title = first_level.get('title', '无标题')
    modified_title = tuning_first_heading(second_level_titles, original_title,topic)
    ana_instruction = get_ana_instruction_for_first_level(second_level_titles, original_title)
    print(f"ana_instruction_for_1st_level:{ana_instruction}")
    title_code,modified_pure_title = code_title_spliter(modified_title)


    # 更新一级标题，并保留原始标题
    first_level['previous_title'] = original_title
    first_level['title'] = modified_pure_title
    first_level['title_code'] = title_code
    first_level['ana_instruction'] = ana_instruction
    return first_level




def modify_first_level_headers(report_content,topic=None):
    # 使用线程池并行处理一级标题
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(modify_first_level_title, first_level,topic) for first_level in report_content]
        
        # 等待所有任务完成
        for future in as_completed(futures):
            future.result()
    return report_content


def modify_first_level_headers_stream(report_content,topic=None):
    # 使用线程池并行处理一级标题
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(modify_first_level_title, first_level,topic) for first_level in [report_content]]

        # 等待所有任务完成
        for future in as_completed(futures):
            future.result()
    return report_content

if __name__=="__main__":
    # 加载报告内容
    report_content = load_report_content()
    
    if not report_content:
        print("无法处理报告内容，程序退出")
    else:
        # 先处理二级标题
        modified_content = modify_second_level_headers(report_content)
        # 再处理一级标题
        modified_content = modify_first_level_headers(modified_content)
        # 打印修改后的层次化标题
        new_report = print_hierarchical_titles(modified_content)
        # 将修改后的报告内容保存为JSON文件
        with open('modified_report.json', 'w', encoding='utf-8') as f:
            json.dump(new_report, f, indent=4, ensure_ascii=False)
        print("修改后的报告已保存到 modified_report.json")
