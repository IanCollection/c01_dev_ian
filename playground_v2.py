import datetime
import os
import sys
import json
from decimal import Decimal

from database.query_ic_indicators import get_cics_id_by_name, query_ic_trend_score, query_ic_current_rating
from scrpit.analyze_ic_trend_score import analyze_industry_trends, get_analysis_summary
from scrpit.query_report_policy_ic_indicator import query_relative_data

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)
# import pandas as pd
import json

from Agent.Overview_agent import generate_analysis_methods, conclude_from_ic_trend_score, get_potential_ic_trend_labels, \
    filter_ic_current_rating, conclude_from_cat_analysis
from database.faiss_query import search_and_query
from database.neo4j_query import query_file_node_by_header
from scrpit.indicator_query_v4 import search_policy_relation, get_policy_details_by_ids


def extract_headlines(md_content):
    # 初始化数据结构
    sections = []
    current_section = None
    current_subsection = None

    for line in md_content.split('\n'):
        line = line.strip()
        if not line.startswith('#'):
            continue  # 跳过非标题行

        # 计算标题级别
        level = line.count('#', 0, line.find(' '))
        title = line[line.find(' ') + 1:].strip()

        if level == 1:  # 一级标题
            if current_section:  # 保存前一个章节
                sections.append(current_section)
            current_section = {
                'title': title,
                'subsections': []
            }
        elif level == 2:  # 二级标题
            if current_section:  # 确保已存在一级标题
                current_subsection = {
                    'title': title,
                    'subsections': []
                }
                current_section['subsections'].append(current_subsection)
        elif level == 3:  # 三级标题
            if current_subsection:  # 确保已存在二级标题
                current_subsection['subsections'].append({
                    'title': title
                })
        elif level == 4:  # 四级标题
            if current_subsection and current_subsection['subsections']:  # 确保已存在三级标题
                current_subsection['subsections'][-1]['subsections'] = current_subsection['subsections'][-1].get(
                    'subsections', [])
                current_subsection['subsections'][-1]['subsections'].append({
                    'title': title
                })

    # 添加最后一个章节
    if current_section:
        sections.append(current_section)

    return {"chapters": sections}


def generate_section_list(content_json):
    """
    将提取的章节信息转换为二级标题及其子标题的列表

    Args:
        content_json (dict): 包含章节信息的字典

    Returns:
        list: 包含所有二级标题及其子标题的列表
    """
    section_list = []

    # 遍历所有章节
    for chapter in content_json["chapters"]:
        # 遍历所有二级标题
        for subsection in chapter['subsections']:
            # 创建二级标题数据结构
            section_data = {
                'title': subsection['title'],
                'subsections': []
            }

            # 遍历所有三级标题
            for subsubsection in subsection['subsections']:
                subsubsection_data = {
                    'title': subsubsection['title']
                }

                # 如果有四级标题，添加到三级标题下
                if 'subsections' in subsubsection:
                    subsubsection_data['subsections'] = [
                        {'title': sss['title']} for sss in subsubsection['subsections']
                    ]

                section_data['subsections'].append(subsubsection_data)

            # 将二级标题及其子标题添加到列表中
            section_list.append(section_data)

    return section_list


def combine_titles(section_list):
    results = []

    # 遍历每个二级标题
    for section in section_list:
        main_title = section["title"]

        # 遍历每个三级标题
        for subsection in section["subsections"]:
            section_title = subsection["title"]

            # 遍历最次级标题
            for leaf_section in subsection["subsections"]:
                combined_title = f"{main_title} - {section_title} - {leaf_section['title']}"
                results.append(combined_title)

    return results


if __name__ == "__main__":
    content_md = '''
    # 中国新能源汽车产业可持续发展报告2023

    ## 第一章 宏观环境与政策分析
    ### 1.1 全球与中国新能源汽车政策对比
    #### 1.1.1 国际政策趋势与中国政策演变
    #### 1.1.2 双积分政策与补贴政策实施效果
    ### 1.2 政策驱动下的市场环境
    #### 1.2.1 当前政策对产业发展的推动作用
    #### 1.2.2 未来政策方向与行业展望

    ## 第二章 市场现状与发展潜力
    ### 2.1 市场规模与结构特征
    #### 2.1.1 新能源汽车销量增长驱动因素
    #### 2.1.2 区域市场分布与消费偏好差异
    ### 2.2 市场发展潜力分析
    #### 2.2.1 消费者需求变化与市场驱动力
    #### 2.2.2 基础设施与产业链协同效应


    '''
    content_json = extract_headlines(content_md)
    section_list = generate_section_list(content_json)
    print(json.dumps(section_list, indent=4, ensure_ascii=False))
    # combined_titles = combine_titles(section_list)
    # for title in combined_titles:

    # 初始化完整结果结构
    full_section_list = []

    # 遍历每个二级标题
    for section in section_list:
        main_title = section["title"]
        new_section = {
            "title": main_title,
            "subsections": []
        }

        # 遍历每个三级标题
        for subsection in section["subsections"]:
            section_title = subsection["title"]
            new_subsection = {
                "title": section_title,
                "subsections": []
            }

            # 遍历最次级标题
            for leaf_section in subsection["subsections"]:
                combined_title = f"{main_title} - {section_title} - {leaf_section['title']}"
                year = 2023
                reports, policy, ic_trends, ic_current, instruction = query_relative_data(year, combined_title)
                
                # 更新leaf_section
                new_leaf = {
                    "title": leaf_section['title'],
                    "relative_data": {
                        "writing_instructions": instruction,
                        "reference": {
                            "report_source": reports,
                            "policy_source": policy,
                            "industry_indicator_part_1": ic_trends["overall_analysis"],
                            "industry_indicator_part_2": ic_current
                        }
                    }
                }
                new_subsection["subsections"].append(new_leaf)
            
            new_section["subsections"].append(new_subsection)
        
        full_section_list.append(new_section)

    print(json.dumps(full_section_list, indent=4, ensure_ascii=False))
    # 将section_list保存为JSON文件
    with open('section_list.json', 'w', encoding='utf-8') as f:
        json.dump(full_section_list, f, indent=4, ensure_ascii=False)