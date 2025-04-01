import time

from database.neo4j_query import query_file_batch_nodes
from palyground import extract_headlines, generate_section_list
from scrpit.overview_report import build_overview_with_report, generate_comprehensive_toc_v2
from scrpit.overview_title import generate_comprehensive_toc_with_focus_points
from Agent.Overview_agent import overview_conclusion
from utils.format_tool import markdown_catalog_to_json

input_title = "中国新能源汽车产业可持续发展报告2023"

##Pipeline A
print("开始A第一阶段处理，语义增强...")
start_time = time.time()
title ,reports_node ,keywords,time_cost = build_overview_with_report(input_title)
print(f"语义增强耗时: {time.time() - start_time:.2f}秒")

#
start_time = time.time()
relative_reports = query_file_batch_nodes(reports_node)

print(f"查询相关研报耗时: {time.time() - start_time:.2f}秒")
#
start_time = time.time()
reports_overview,all_reports,reports_cost = generate_comprehensive_toc_v2(input_title, relative_reports,keywords)
print(all_reports)
print(f"生成研报目录耗时: {time.time() - start_time:.2f}秒")
#
# #Pipeline B
print("开始B第一阶段处理...")
start_time = time.time()
general_overview, focus_points = generate_comprehensive_toc_with_focus_points(input_title,keywords)
print(f"生成指标耗时: {time.time() - start_time:.2f}秒")

print('-'*100)
print('历史研报目录')
print(reports_overview)
print('---------------'*10)
print('关注点 直接生成目录')
print(general_overview[0])

# #stage2 总结研报 获得目录。
# start_time = time.time()
final_overview,cost = overview_conclusion(reports_overview,general_overview[0],input_title)
print(f"总结研报生成目录耗时: {time.time() - start_time:.2f}秒")
print('--'*20)
print('总目录')
print(final_overview)

print(f"关键词: {keywords}")
print(f'关注点:{focus_points}')

# content_json = extract_headlines(final_overview)
# section_list = generate_section_list(content_json)







#Pipeline C
# print("开始C第一阶段处理，根据title的的行业标签来查找相关政策")
#
# policy_ids = search_policy_relation(input_title)
# if len(policy_ids)==1:
#     policy_detail = get_policy_detail_by_id(policy_ids)
# else:
#     policy_detail = get_policy_details_by_ids(policy_ids)
# all_cics = []
# if isinstance(policy_detail, list):
#     for policy in policy_detail:
#         if policy.get('industry') and policy['industry'] not in all_cics:
#             all_cics.append(policy['industry'])
# elif isinstance(policy_detail, dict):
#     if policy_detail.get('industry'):
#         all_cics.append(policy_detail['industry'])
#
# print(all_cics)

# print(policy_detail[0])
#
# #Pipeline D
# #print("根据强化后的关键词和title 来进行指标查找")
# #先获得相关的指标id
# indicators_ids = search_indicators(input_title)
# # print(indicators_ids)
# data_points = eco_indicators_query_batch(indicators_ids)
# # print(data_points)
#
#








#
# # 1.1 reports_overview
# #                         overview + policy + data_points ==> final_overview(生成一级标题（cot写作思路） 然后二级标题（cot写作思路） 三级标题（cot写作思路）+ 研报+政策+涉及指标)
# # 1.2 general_overview
#
#
# print(f"研报历史目录：{reports_overview}")
# print(f"研报通用目录：{general_overview}")
# print(f"研报关注点：{focus_points}")
# print(f"政策：{policy_detail}")
# print(f"指标：{data_points}")