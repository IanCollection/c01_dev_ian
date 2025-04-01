import datetime
import os
import sys
import json
from decimal import Decimal

from database.query_ic_indicators import get_cics_id_by_name, query_ic_trend_score, query_ic_current_rating
from scrpit.analyze_ic_trend_score import analyze_industry_trends, get_analysis_summary

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

    ## 第三章 技术创新与产品竞争力
    ### 3.1 核心技术创新进展
    #### 3.1.1 动力电池技术突破与研发路径
    #### 3.1.2 智能网联与能源管理技术应用
    ### 3.2 产品特点与品牌竞争力
    #### 3.2.1 主流产品差异化策略分析
    #### 3.2.2 用户体验优化与品牌影响力提升

    ## 第四章 产业链协同发展与供应链安全
    ### 4.1 产业链关键环节分析
    #### 4.1.1 关键原材料供应现状与挑战
    #### 4.1.2 上下游企业合作模式优化
    ### 4.2 全球化布局与国际竞争力
    #### 4.2.1 海外市场表现与出口战略
    #### 4.2.2 技术标准国际化与海外生产基地建设

    ## 第五章 环境效益与可持续发展路径
    ### 5.1 环保效益评估
    #### 5.1.1 全生命周期碳排放与减排贡献
    #### 5.1.2 废旧电池回收与资源循环利用
    ### 5.2 可持续发展建议
    #### 5.2.1 政策支持与企业战略调整方向
    #### 5.2.2 技术研发重点与能源转型路径
    '''
    content_json = extract_headlines(content_md)
    section_list = generate_section_list(content_json)
    # print(json.dumps(section_list, indent=4, ensure_ascii=False))
    combined_titles = combine_titles(section_list)

    for title in combined_titles:
        print(title)
    # 定义当前分析的标题
#

    year = 2023
    current_title = '第一章 宏观环境与政策分析 - 1.1 全球与中国新能源汽车政策对比 - 1.1.1 国际政策趋势与中国政策演变'


    # #
    # #     # # # 生成分析方法
    analysis_response, cost = generate_analysis_methods(current_title)
    analysis_dict = json.loads(analysis_response)
#     # print(analysis_dict)
#     # # # 将标题与分析方法结合作为查询文本
    query_text = f"{current_title}\n 分析思路：{analysis_dict['analysis']}"
#
    #
    potential_ic_trend_labels = get_potential_ic_trend_labels(query_text)

    #     print(potential_ic_trend_labels)
    #
    #     # #查询研报的policy节点
    report_query_response = search_and_query(query_text, index_type='header')
    # 为每个header_id查询对应的file_node_id并添加到字典中
    for item in report_query_response:
        file_node_id = query_file_node_by_header(item['header_id'])
        if file_node_id:
            item['file_node_id'] = file_node_id
        else:
            item['file_node_id'] = None
    #输出查询结果
    # print(report_query_response)
#     #query 政策： 通过cics 匹配行业来获得所有的政策id，从而返回政策
    policy_ids = search_policy_relation(query_text)
    policy_details = get_policy_details_by_ids(policy_ids)
    # #     # print(policy_details)
    # #     # 创建一个新的列表来存储简化后的政策信息
    simplified_policies = []
    # #     # # 遍历每个政策详情
    all_cics_label = []
    for policy in policy_details:
    #     # 创建一个新的字典，只包含需要的字段
        simplified_policy = {
            'id': policy.get('id'),
            'policy_title': policy.get('policy_title'),
            'policy_summary': policy.get('policy_summary'),
            'industry': policy.get('industry'),
            'policy_start_time':policy.get('policy_start_date'),
            'policy_end_time':policy.get('policy_end_date')
        }
        all_cics_label.append(policy.get('industry'))
    #     #     # 将简化后的政策信息添加到列表中
        simplified_policies.append(simplified_policy)

    # # query 景气度
    cics_ids = get_cics_id_by_name(all_cics_label)
    ic_trend_scores = query_ic_trend_score(cics_ids, year)

    if potential_ic_trend_labels:
        # 定义需要保留的基础字段
        base_fields = ['id', 'date', 'cics_id', 'cics_name', 'updated_at']
        # 根据labels生成需要保留的score和grade字段
        keep_fields = base_fields.copy()
        for label in potential_ic_trend_labels:
            keep_fields.append(f'{label}_score')
            keep_fields.append(f'{label}_grade')

        # 过滤ic_trend_scores，只保留需要的字段
        filtered_scores = []
        for score in ic_trend_scores:
            filtered_score = {k: v for k, v in score.items() if k in keep_fields}
            filtered_scores.append(filtered_score)

        ic_trend_scores = filtered_scores

    analysis_results_ictrend = analyze_industry_trends(ic_trend_scores)
    analysis_results_ictrend_v2 = get_analysis_summary(analysis_results_ictrend)
    industry_analysis = conclude_from_ic_trend_score(analysis_results_ictrend_v2)


    ic_current_rating = query_ic_current_rating(cics_ids, year)
    potential_ic_trend_labels = ['profitability_cat', 'supply_demand']
    filtered_result = filter_ic_current_rating(ic_current_rating, potential_ic_trend_labels)
    cat_indicators, cost = conclude_from_cat_analysis(filtered_result)

    #研报
    print('研报结果:\n')
    print(report_query_response)
    #政策
    print('政策结果:\n')
    print(policy_details)
    #景气度
    print('景气度指标:\n')
    print(industry_analysis)
    print(cat_indicators)
    #指标待定
