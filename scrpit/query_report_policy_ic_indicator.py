import datetime
import os
import sys
import json
from decimal import Decimal

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)

# 现在可以导入项目模块
from database.query_ic_indicators import get_cics_id_by_name, query_ic_trend_score, query_ic_current_rating
from scrpit.analyze_ic_trend_score import analyze_industry_trends, get_analysis_summary
from pg2es_hybrid.es_vector_query import es_vector_query, es_vector_query_eco_indicators, \
    es_vector_query_eco_indicators_v2, es_vector_query_policy_info
# import pandas as pd
import json

from Agent.Overview_agent import generate_analysis_methods, conclude_from_ic_trend_score, get_potential_ic_trend_labels, \
    filter_ic_current_rating, conclude_from_cat_analysis
from database.faiss_query import search_and_query
from database.neo4j_query import query_file_node_by_header,query_content_under_header
from scrpit.indicator_query_v4 import search_policy_relation, get_policy_details_by_ids
from scrpit.analyze_eco_indicators import analyze_eco_indicators, generate_summary_report
def query_relative_data(year,current_title):
    try:
        # #     # # # 生成分析方法
        analysis_response, cost = generate_analysis_methods(current_title)
        analysis_dict = json.loads(analysis_response)
        query_text = f"{current_title}\n 分析思路：{analysis_dict['analysis']}"
        potential_ic_trend_labels = get_potential_ic_trend_labels(query_text)
        report_query_response = search_and_query(query_text, index_type='header')
        
        # 确保report_query_response不为None
        if report_query_response is None:
            report_query_response = []
            
        # 为每个header_id查询对应的file_node_id并添加到字典中
        for item in report_query_response:
            file_node_id = query_file_node_by_header(item['header_id'])
            current_headers_content = query_content_under_header(item['header_id'])
            if file_node_id:
                item['file_node_id'] = file_node_id
            else:
                item['file_node_id'] = None
            if current_headers_content:
                item['current_headers_content'] = current_headers_content
            else:
                item['current_headers_content'] = None
                
        #输出查询结果
        # print(report_query_response)

        #政策v1
        #query 政策： 通过cics 匹配行业来获得所有的政策id，从而返回政策
        # policy_ids = search_policy_relation(query_text)
        # print(f"旧--检索出来的政策id{policy_ids}\n")
        # # 确保policy_ids不为None
        # if policy_ids is None:
        #     policy_ids = []
        # if len(policy_ids)>10:
        #     policy_ids = policy_ids[:10]
        # policy_details = get_policy_details_by_ids(policy_ids)
        # # 确保policy_details不为None
        # if policy_details is None:
        #     policy_details = []

        #政策v2
        # policy,policy_ids = es_vector_query(query_text)
        policy,policy_ids = es_vector_query_policy_info(query_text)
        # 将policy_ids中的每个元素从字符串转换为整数
        policy_ids = [int(pid) for pid in policy_ids if pid.isdigit()]
        # print(f"es_vecotor 检索出来的政策id{policy_ids}\n")
        # print(f"es_vecotor 检索出来的政策{policy}\n")
        policy_details = get_policy_details_by_ids(policy_ids)
        # print(f"es_vecotor 检索后query出来的政策{policy_details}\n")
            
        # #     # print(policy_details)
        # #     # 创建一个新的列表来存储简化后的政策信息
        simplified_policies = []
        all_cics_label = []
        
        # 确保policy_details是可迭代的列表
        if not isinstance(policy_details, list):
            policy_details = []
            
        for policy in policy_details:
            if not isinstance(policy, dict):
                continue
                
            #     # 创建一个新的字典，只包含需要的字段
            simplified_policy = {
                'id': policy.get('id'),
                'policy_title': policy.get('policy_title'),
                'policy_summary': policy.get('policy_summary'),
                'industry': policy.get('industry',None),
                'policy_start_time':policy.get('policy_start_date',None),
                'policy_end_time':policy.get('policy_end_date',None),
                'org_name':policy.get('org_name',None)
            }
            
            # 只有当industry有值时才添加
            if policy.get('industry'):
                all_cics_label.append(policy.get('industry'))
                
            #     #     # 将简化后的政策信息添加到列表中
            simplified_policies.append(simplified_policy)

        # # query 景气度
        # 确保有行业标签再查询
        industry_analysis = {"overall_analysis": "暂无行业分析数据"}
        cat_indicators = "暂无当前行业指标数据"
        
        if all_cics_label:
            try:
                cics_ids = get_cics_id_by_name(all_cics_label)
                
                # 检查cics_ids是否为None或空列表
                if cics_ids is None:
                    # 处理SQL错误情况
                    print(f"查询CICS ID时发生SQL错误")
                    industry_analysis = {"overall_analysis": "查询数据时发生数据库错误"}
                    cat_indicators = "查询数据时发生数据库错误"
                elif cics_ids:
                    # 查询并处理ic_trend_scores
                    ic_trend_scores = query_ic_trend_score(cics_ids, year)
                    # print(ic_trend_scores)

                    if ic_trend_scores:
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
                        if analysis_results_ictrend:
                            analysis_results_ictrend_v2 = get_analysis_summary(analysis_results_ictrend)
                            industry_analysis, cost = conclude_from_ic_trend_score(analysis_results_ictrend_v2)
                    
                    # 查询并处理ic_current_rating
                    try:
                        ic_current_rating = query_ic_current_rating(cics_ids, year)
                        if ic_current_rating:
                            potential_cat_labels = ['profitability_cat', 'supply_demand']
                            filtered_result = filter_ic_current_rating(ic_current_rating, potential_cat_labels)
                            if filtered_result:
                                cat_indicators, cost = conclude_from_cat_analysis(filtered_result)
                                # print(cat_indicators)
                    except Exception as e:
                        print(f"处理 ic_current_rating 时出错: {str(e)}")
            except Exception as e:
                print(f"查询行业指标时出错: {str(e)}")


        #query eco 指标
        # eco_indicators,eco_ids,eco_indicators_sum =  es_vector_query_eco_indicators(query_text)
        eco_indicators,eco_ids,eco_indicators_sum = es_vector_query_eco_indicators_v2(query_text,year)
        # 用year来修改eco_indicators_sum
        return report_query_response, simplified_policies, industry_analysis, cat_indicators, analysis_response,eco_indicators,eco_indicators_sum
    
    except Exception as e:
        print(f"query_relative_data 函数执行出错: {str(e)}")
        # 返回默认值，确保不会中断整个流程
        return [], [], {"overall_analysis": "数据获取失败"}, "数据获取失败", "{\"analysis\": \"无法生成分析方法\"}"


def query_relative_data_v2(year, current_title, analysis_response=None):
    try:
        # #     # # # 生成分析方法
        if analysis_response is None:
            analysis_response, cost = generate_analysis_methods(current_title)
        analysis_dict = json.loads(analysis_response)
        # query_text = f"{current_title}\n 分析思路：{analysis_dict['analysis']}"
        query_text = f"{current_title}\n 分析思路：{analysis_dict}"

        potential_ic_trend_labels = get_potential_ic_trend_labels(query_text)
        report_query_response = search_and_query(query_text, index_type='header')
        # print(report_query_response)

        # 确保report_query_response不为None
        if report_query_response is None:
            report_query_response = []

        # 为每个header_id查询对应的file_node_id并添加到字典中
        for item in report_query_response:
            file_node_id = query_file_node_by_header(item['header_id'])
            current_headers_content = query_content_under_header(item['header_id'])
            if file_node_id:
                item['file_node_id'] = file_node_id
            else:
                item['file_node_id'] = None
            if current_headers_content:
                item['current_headers_content'] = current_headers_content
            else:
                item['current_headers_content'] = None
        # 政策v2
        # policy,policy_ids = es_vector_query(query_text)
        policy, policy_ids = es_vector_query_policy_info(query_text)
        # 将policy_ids中的每个元素从字符串转换为整数
        policy_ids = [int(pid) for pid in policy_ids if pid.isdigit()]
        # print(f"es_vecotor 检索出来的政策id{policy_ids}\n")
        policy_details = get_policy_details_by_ids(policy_ids)
        # print(f"es_vecotor 检索后query出来的政策{policy_details}\n")

        # #     # print(policy_details)
        # #     # 创建一个新的列表来存储简化后的政策信息
        simplified_policies = []
        all_cics_label = []

        # 确保policy_details是可迭代的列表
        if not isinstance(policy_details, list):
            policy_details = []

        for policy in policy_details:
            if not isinstance(policy, dict):
                continue
            #     # 创建一个新的字典，只包含需要的字段
            simplified_policy = {
                'id': policy.get('id'),
                'policy_title': policy.get('policy_title'),
                'policy_summary': policy.get('policy_summary'),
                'industry': policy.get('industry', None),
                'policy_start_time': policy.get('policy_start_date', None),
                'policy_end_time': policy.get('policy_end_date', None),
                'org_name': policy.get('org_name', None)
            }

            # 只有当industry有值时才添加
            if policy.get('industry'):
                all_cics_label.append(policy.get('industry'))

            #     #     # 将简化后的政策信息添加到列表中
            simplified_policies.append(simplified_policy)

        # # query 景气度
        # 确保有行业标签再查询
        industry_analysis = {"overall_analysis": "暂无行业分析数据"}
        cat_indicators = "暂无当前行业指标数据"
        # print(f"all_cics_label: {all_cics_label}")

        if all_cics_label:
            try:
                cics_ids = get_cics_id_by_name(all_cics_label)
                # print(f"cics_ids:{cics_ids}")
                # 检查cics_ids是否为None或空列表
                if cics_ids is None:
                    # 处理SQL错误情况
                    print(f"查询CICS ID时发生SQL错误")
                    industry_analysis = {"overall_analysis": "查询数据时发生数据库错误"}
                    cat_indicators = "查询数据时发生数据库错误"
                elif cics_ids:
                    # 查询并处理ic_trend_scores
                    ic_trend_scores = query_ic_trend_score(cics_ids, year)
                    # print(ic_trend_scores)
                    print('已查询到ic_trend_scores')
                    if ic_trend_scores:
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
                        if analysis_results_ictrend:
                            analysis_results_ictrend_v2 = get_analysis_summary(analysis_results_ictrend)
                            # print(analysis_results_ictrend_v2)
                            industry_analysis, cost = conclude_from_ic_trend_score(analysis_results_ictrend_v2)
                            # print('industry_analysis')
                            # print(industry_analysis)
                            # print('---'*20)
                    # 查询并处理ic_current_rating
                    try:
                        ic_current_rating = query_ic_current_rating(cics_ids, year)
                        if ic_current_rating:
                            potential_cat_labels = ['profitability_cat', 'supply_demand']
                            filtered_result_ic_current_rating = filter_ic_current_rating(ic_current_rating, potential_cat_labels)
                            # print('已查询到ic_current_rating')
                            # print(filtered_result)
                            if filtered_result_ic_current_rating:
                                cat_indicators, cost = conclude_from_cat_analysis(filtered_result_ic_current_rating)
                                # print(cat_indicators)
                    except Exception as e:
                        print(f"处理 ic_current_rating 时出错: {str(e)}")
            except Exception as e:
                print(f"查询行业指标时出错: {str(e)}")

        # query eco 指标
        # eco_indicators,eco_ids,eco_indicators_sum =  es_vector_query_eco_indicators(query_text)
        eco_indicators, eco_ids, eco_indicators_sum = es_vector_query_eco_indicators_v2(query_text, year)
        eco_indicators_analysis_stage_0 = analyze_eco_indicators(eco_indicators)
        eco_indicators_report = generate_summary_report(eco_indicators_analysis_stage_0)


        # 用year来修改eco_indicators_sum
        return report_query_response, simplified_policies, industry_analysis, cat_indicators, analysis_response, eco_indicators, eco_indicators_sum,eco_indicators_report,analysis_results_ictrend_v2,filtered_result_ic_current_rating

    except Exception as e:
        print(f"query_relative_data_v2 函数执行出错: {str(e)}")
        # 返回默认值，确保不会中断整个流程
        return [], [], {"overall_analysis": "数据获取失败"}, "数据获取失败", "{\"analysis\": \"无法生成分析方法\"}",[],[],[],[]



def query_relative_data_v3(year, current_title):
    # #     # # # 生成分析方法
    analysis_response, cost = generate_analysis_methods(current_title)
    analysis_dict = json.loads(analysis_response)
    query_text = f"{current_title}\n 分析思路：{analysis_dict['analysis']}"
    potential_ic_trend_labels = get_potential_ic_trend_labels(query_text)
    report_query_response = search_and_query(query_text, index_type='header')
    return report_query_response


if __name__ == "__main__":
    year = 2024
    query_title = '2023年新能源汽车行业'
    report_query_response, simplified_policies, industry_analysis, cat_indicators, analysis_response,eco_indicators, eco_indicators_sum,eco_indicators_report,analysis_results_ictrend_v2,filtered_result_ic_current_rating = query_relative_data_v2(2022,"2023年新能源汽车")
    # print("report_query_response:", report_query_response)
    # print("simplified_policies:", simplified_policies)
    # print("industry_analysis:", industry_analysis)
    # print("cat_indicators:", cat_indicators)
    # print("analysis_response:", analysis_response)
    # print("eco_indicators:", eco_indicators)
    # print("eco_indicators_sum:", eco_indicators_sum)
    # print("analysis_results_ictrend_v2:", analysis_results_ictrend_v2)
    # print("filtered_result_ic_current_rating:", filtered_result_ic_current_rating)
    print("eco_indicators_report:", eco_indicators_report)
    
    
    # print(filtered_result_ic_current_rating)
    # print(analysis_results_ictrend_v2)
    # print(cat_indicators)
    # print('------'*20)
    # print(ic_trend_scores)
    # print(industry_analysis)
    # # print(ic_trend_scores)
    # print('------'*20)
    # print(cat_indicators)
    # print(analysis_response)
    # # print('--'*30)
    # # # # print(simplified_policies)
    # # # print('--'*30)
    # # print(industry_analysis)
    # print('--'*30)
    # print(eco_indicators)
    # print('--'*30)
    # print(eco_indicators_sum)
    #
    # report_query_response, simplified_policies, industry_analysis, cat_indicators, analysis_response,eco_indicators, eco_indicators_sum= query_relative_data(2024,"新能源")
    # print(industry_analysis)
    # print('---'*30)
    # print(cat_indicators)
