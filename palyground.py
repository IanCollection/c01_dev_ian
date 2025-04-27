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
    # 添加一个标志，表示是否已经遇到了第一个标题（无论级别）
    first_header_found = False
    
    for line in md_content.split('\n'):
        line = line.strip()
        if not line.startswith('#'):
            continue  # 跳过非标题行
            
        # 计算标题级别
        # 健壮性：如果找不到空格，可能是无效格式，跳过
        space_index = line.find(' ')
        if space_index == -1:
            print(f"警告: 跳过无效的标题行格式: {line}")
            continue
        # 处理 `#` 数量计算，即使空格紧随 `#`
        level_str = line[:space_index]
        if all(c == '#' for c in level_str):
             level = len(level_str)
        else:
             # 如果 # 和空格之间有其他字符，视为无效或非标准格式，尝试按空格前 # 数量处理
             level = line.count('#', 0, space_index)
             if level == 0: # 如果根本没有 # 开头（理论上被 startswith 过滤了，但保险）
                 print(f"警告: 跳过非标题行（二次检查）: {line}")
                 continue
                 
        title = line[space_index+1:].strip()
        
        # 第一次遇到标题时，如果级别不是1，创建一个默认的父章节
        if not first_header_found and level != 1:
            print(f"信息: Markdown 内容未以一级标题 (#) 开始。将创建一个默认父章节。")
            current_section = {
                'title': 'Default Parent Section', # 或者可以尝试从其他地方获取标题
                'subsections': []
            }
            sections.append(current_section) # <--- 重要：立即添加到 sections 列表
        first_header_found = True # 标记已找到第一个标题
        
        if level == 1:  # 一级标题
            # 如果之前因为处理非一级标题开头的md而创建了默认章节，
            # 并且这个默认章节还没有子内容，则替换它。
            # 否则，正常创建新章节。
            if current_section and current_section.get('title') == 'Default Parent Section' and not current_section.get('subsections'):
                print("信息: 遇到真正的一级标题，替换之前创建的默认父章节。")
                current_section['title'] = title # 直接修改默认章节的标题
            else:
                # 创建新的章节并添加到sections列表
                current_section = {
                    'title': title,
                    'subsections': []
                }
                sections.append(current_section)
            current_subsection = None # 重置二级标题引用
            
        elif level == 2:  # 二级标题
            if current_section:  # 确保已存在一级标题 (或默认父级)
                current_subsection = {
                    'title': title,
                    'subsections': []
                }
                current_section['subsections'].append(current_subsection)
            else:
                # 理论上因为上面的 first_header_found 逻辑，不应该到达这里，但作为保险
                print(f"警告: 遇到二级标题 '{title}' 但没有有效的父章节，已跳过。")
                
        elif level == 3:  # 三级标题
            if current_subsection:  # 确保已存在二级标题
                # 创建新的三级标题字典，并确保它有 subsections 键以备四级标题使用
                new_subsubsection = {
                    'title': title,
                    'subsections': []
                }
                current_subsection['subsections'].append(new_subsubsection)
            else:
                print(f"警告: 遇到三级标题 '{title}' 但没有有效的父二级标题，已跳过。")
                
        elif level == 4:  # 四级标题
            # 确保 current_subsection 存在，并且其 subsections 列表不为空
            if current_subsection and current_subsection['subsections']:
                # 获取最后一个三级标题
                last_subsubsection = current_subsection['subsections'][-1]
                # 直接向最后一个三级标题的 subsections 添加四级标题
                last_subsubsection['subsections'].append({
                    'title': title
                })
            else:
                print(f"警告: 遇到四级标题 '{title}' 但没有有效的三级父标题，已跳过。")
    
    # # 添加最后一个章节 - 这个逻辑在修改后不再需要，因为章节在创建时就添加了
    # if current_section:
    #     # 如果最后一个章节是默认章节且为空，则不添加
    #     if not (current_section.get('title') == 'Default Parent Section' and not current_section.get('subsections')):
    #          # 检查是否已添加 (防止重复添加)
    #          if not sections or sections[-1] is not current_section:
    #             sections.append(current_section)
                
    # 过滤掉可能存在的空的默认父章节（如果整个输入都是空的或无效的）
    sections = [s for s in sections if not (s.get('title') == 'Default Parent Section' and not s.get('subsections'))]
    
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



if __name__ == "__main__":
    overview = """## 1. 行业背景与宏观环境分析
### 1.1 全球新能源汽车市场发展概况
#### 1.1.1 全球电动化趋势及区域市场表现
#### 1.1.2 各国新能源汽车政策框架与实施路径
#### 1.1.3 碳中和目标对新能源汽车产业的长期影响

### 1.2 中国新能源汽车政策与市场驱动因素
#### 1.2.1 政策补贴与双积分政策的影响
#### 1.2.2 主要经济体新能源汽车补贴与激励措施
#### 1.2.3 技术突破与产业链协同发展
## 2. 市场现状与竞争格局
### 2.1 新能源汽车市场渗透率与增长趋势
#### 2.1.1 中国市场销量与渗透率变化
#### 2.1.2 全球新能源汽车市场总量及区域分布特征
#### 2.1.3 国产品牌市场份额与出口表现

### 2.2 细分市场表现与竞争态势
#### 2.2.1 动力电池装机量与技术发展
#### 2.2.2 驱动电机与电控系统市场格局
#### 2.2.3 主要企业市场份额与战略布局分析

## 3. 用户行为与消费趋势分析
### 3.1 新能源汽车用户画像与需求特征
#### 3.1.1 年轻群体与女性用户的智能化偏好
#### 3.1.2 下沉市场与家庭刚需的增长潜力

### 3.2 用户关注点与消费决策因素
#### 3.2.1 续航里程与电池技术的关注度
#### 3.2.2 安全性与充电便利性的需求提升

## 4. 技术创新与产业变革
### 4.1 智能化与网联化发展趋势
#### 4.1.1 自动驾驶与智能网联技术进展
#### 4.1.2 智能驾驶技术在新能源汽车中的集成应用
#### 4.1.3 座舱智能化与电子电气架构升级

### 4.2 核心技术突破与产业链优化
#### 4.2.1 动力电池技术创新与应用趋势
#### 4.2.2 动力电池回收与梯次利用
#### 4.2.3 充电基础设施建设与补能需求

## 5. 产业链供需关系与优化路径
### 5.1 供应链结构与上下游协同分析
#### 5.1.1 动力电池原材料供给与需求平衡研究
#### 5.1.2 核心零部件供应链的全球化布局与风险

### 5.2 产业链优化与可持续发展策略
#### 5.2.1 新能源汽车生产环节的绿色化转型路径
#### 5.2.2 循环经济模式在动力电池回收中的应用

## 6. 行业挑战与风险分析
### 6.1 发展瓶颈与资源约束
#### 6.1.1 补贴退坡与芯片短缺的影响
#### 6.1.2 资源储备与供应链稳定性

### 6.2 市场竞争与商业模式创新
#### 6.2.1 差异化营销策略的重要性
#### 6.2.2 新商业模式探索与盈利点挖掘

## 7. 行业发展趋势与未来展望
### 7.1 新能源汽车行业长期增长潜力
#### 7.1.1 低线城市与非限购市场的扩展空间
#### 7.1.2 高端化与个性化需求的崛起
### 7.2 产业融合与生态体系建设
#### 7.2.1 后市场发展潜力与服务优化
#### 7.2.2 元宇宙技术应用与数字化转型"""

    extracted_headlines = extract_headlines(overview)
    print("Extracted Headlines:")
    print(json.dumps(extracted_headlines, indent=2, ensure_ascii=False))

    section_list = generate_section_list(extracted_headlines)
    print("\nGenerated Section List:")
    print(json.dumps(section_list, indent=2, ensure_ascii=False))

    all_combined_titles = []
    for section in section_list:
        # 现在 combine_titles 接收的是单个 section 字典
        combined_titles_for_section = combine_titles(section)
        all_combined_titles.extend(combined_titles_for_section)

    print("\nCombined Titles:")
    # 打印所有合并后的标题，每个标题占一行
    for title in all_combined_titles:
        print(title)