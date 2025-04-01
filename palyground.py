import datetime
import os
import sys
import json
from decimal import Decimal

from database.query_ic_indicators import get_cics_id_by_name, query_ic_trend_score, query_ic_current_rating
from scrpit.analyze_ic_trend_score import analyze_industry_trends, get_analysis_summary, \
    analyze_flexible_industry_trends, get_flexible_summary

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
        title = line[line.find(' ')+1:].strip()
        
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
                current_subsection['subsections'][-1]['subsections'] = current_subsection['subsections'][-1].get('subsections', [])
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

def combine_titles(section_dict):
    results = []
    main_title = section_dict["title"]
    
    for subsection in section_dict["subsections"]:
        section_title = subsection["title"]
        
        # 遍历最次级标题
        for leaf_section in subsection["subsections"]:
            combined_title = f"{main_title} - {section_title} - {leaf_section['title']}"
            results.append(combined_title)
    
    return results

