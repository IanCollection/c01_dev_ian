import concurrent
import datetime
import os
import sys
import json
from decimal import Decimal

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)
from Agent.surpervisor_agent import judge_title_relevance, industry_indicator_relevance, judge_topic_relevance, \
    eco_indicator_relevance, judge_area_topic_relevance
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
from database.neo4j_query import query_file_node_by_header, query_content_under_header, \
    query_file_node_and_name_by_header
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
            print(report_query_response)
            # file_node_id = query_file_node_by_header(item['header_id'])
            file_node_id, file_name = query_file_node_and_name_by_header(item['header_id'])
            current_headers_content = query_content_under_header(item['header_id'])
            if file_node_id:
                item['file_node_id'] = file_node_id
            else:
                continue
            if current_headers_content:
                item['current_headers_content'] = current_headers_content
            else:
                continue

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
        cat_indicators = []

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
                    try:
                        ic_trend_scores = query_ic_trend_score(cics_ids, year)
                    except Exception as e:
                        print(f"[ERROR] 景气度查询失败: {str(e)}")
                        industry_analysis["error"] = f"景气度数据异常：{str(e)}"
                        ic_trend_scores = []

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
        eco_indicators, eco_ids, eco_indicators_sum = es_vector_query_eco_indicators_v2(query_text,year)
        # 用year来修改eco_indicators_sum
        return report_query_response, simplified_policies, industry_analysis, cat_indicators, analysis_response,eco_indicators,eco_indicators_sum

    except Exception as e:
        print(f"query_relative_data 函数执行出错: {str(e)}")
        # 返回默认值，确保不会中断整个流程
        return [], [], {"overall_analysis": "数据获取失败"}, "数据获取失败", "{\"analysis\": \"无法生成分析方法\"}"


def query_relative_data_v2(year, current_title, analysis_response=None):
    # 在函数开头初始化所有返回值变量，确保它们有正确的空类型
    report_query_response = []
    simplified_policies = []
    industry_analysis = {"overall_analysis": "暂无行业分析数据"}  # 保持 dict 类型
    cat_indicators = []
    # analysis_response 会在 try 块中生成或传入，先不初始化
    eco_indicators = []
    eco_indicators_sum = []
    eco_indicators_report = ""  # 期望是 str，初始化为空字符串
    analysis_results_ictrend_v2 = {}  # 期望是 dict，初始化为空字典
    filtered_result_ic_current_rating = []

    try:
        print(current_title)
        # #     # # # 生成分析方法
        if analysis_response is None or len(analysis_response)==0:
            # print('当前分析思路为空，开始生成分析思路')
            _analysis_response_str, cost = generate_analysis_methods(current_title) # Use temp var
            analysis_dict = json.loads(_analysis_response_str)
            analysis_response = analysis_dict['analysis'] # analysis_response is str
        # else: # analysis_response is already a string passed in
            # analysis_dict remains the string analysis_response for query_text
        # print(analysis_dict)
        # query_text = f"{current_title}\n 分析思路：{analysis_dict['analysis']}" # Old logic assumes dict
        query_text = f"{current_title}\n 分析思路：{analysis_response}" # Use str analysis_response directly

        potential_ic_trend_labels = get_potential_ic_trend_labels(query_text)
        # top_k = 10
        report_query_response_raw = search_and_query(query_text, index_type='header',top_k=10) # Assign to temp var
        # print(report_query_response_raw)
        # 确保report_query_response_raw不为None
        # print(len(report_query_response_raw))
        if report_query_response_raw is None:
            report_query_response_raw = []
        # 为每个header_id查询对应的file_node_id并添加到字典中，同时剔除current_headers_content为None的元素
        filtered_response = []
        for item in report_query_response_raw: # Use temp var
            # file_node_id = query_file_node_by_header(item['header_id'])
            file_node_id, file_node_name = query_file_node_and_name_by_header(item['header_id'])
            current_headers_content = query_content_under_header(item['header_id'])
            # 如果current_headers_content为None，则跳过该元素
            if not current_headers_content:
                continue

            # 添加file_node_id和current_headers_content
            item['file_node_id'] = file_node_id if file_node_id else None
            item['current_headers_content'] = current_headers_content
            item['file_node_name'] = file_node_name
            # 将有效元素添加到过滤后的列表中
            filtered_response.append(item)

        # 使用线程池并行处理每个content的相关性判断
        final_response_reports = [] # Use new var for final result
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交所有判断任务，仅处理有content的元素
            future_to_item = {
                executor.submit(judge_title_relevance, current_title, item['content']): item
                for item in filtered_response if item.get('content')
            }

            # 过滤保留相关的内容
            for future in concurrent.futures.as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    if future.result():  # 如果返回True则保留
                        final_response_reports.append(item) # Append to new var
                except Exception as e:
                    print(f"判断标题相关性时出错: {e}")

            # 更新report_query_response为最终过滤后的结果
            report_query_response = final_response_reports # Assign final result

        # print(report_query_response)
        # print(len(report_query_response))
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
        simplified_policies_raw = [] # Use temp var
        all_cics_label_raw = [] # Use temp var

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

            # # 只有当industry有值时才添加 (commented out in original, keep it)
            # if policy.get('industry'):
            #     all_cics_label_raw.append(policy.get('industry'))

            #     #     # 将简化后的政策信息添加到列表中
            simplified_policies_raw.append(simplified_policy) # Append to temp var
        # print(f"简化后的政策数量为{len(simplified_policies_raw)}")
        # 并行处理判断每个simplified_policies_raw的policy_title与current_title的相关性
        final_policies = [] # Use new var for final result
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交所有判断任务，仅处理有policy_title的元素
            futures = []
            for policy in simplified_policies_raw: # Use temp var
                if policy.get('policy_title'):
                    future = executor.submit(judge_title_relevance, current_title, policy.get('policy_title'))
                    futures.append((policy, future))

            # 等待所有任务完成并处理结果
            for policy, future in futures:
                try:
                    if future.result():  # 如果返回True则保留
                        final_policies.append(policy) # Append to new var
                except Exception as e:
                    print(f"判断政策标题相关性时出错: {e}")

            # 更新simplified_policies为最终过滤后的结果
            simplified_policies = final_policies # Assign final result
        # print(f"筛选后的政策数量为{len(simplified_policies)}")

        # 遍历simplified_policies，收集所有industry字段
        for policy in simplified_policies: # Use final policies list
            if policy.get('industry'):
                all_cics_label_raw.append(policy.get('industry')) # Append to temp var

        # 使用线程池并行处理行业标签相关性判断
        relevant_labels = [] # Use new var for final result
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交所有判断任务
            futures = {executor.submit(industry_indicator_relevance, [label], current_title): label for label in all_cics_label_raw} # Use temp var

            # 收集相关标签
            for future in concurrent.futures.as_completed(futures):
                label = futures[future]
                try:
                    if future.result():  # 如果返回True则保留
                        relevant_labels.append(label) # Append to new var
                except Exception as e:
                    print(f"判断行业标签'{label}'相关性时出错: {e}")

            # 更新all_cics_label为最终过滤后的结果
            all_cics_label = relevant_labels # Assign final result


        print(f"all_cics_label: {all_cics_label}")
        # # query 景气度
        # 重置/确保这些变量在进入 if 块前是初始状态
        industry_analysis = {"overall_analysis": "暂无行业分析数据"}
        cat_indicators = []
        analysis_results_ictrend_v2 = {} # Reset to default empty dict
        filtered_result_ic_current_rating = [] # Reset to default empty list

        if all_cics_label:
            try:
                cics_ids = get_cics_id_by_name(all_cics_label)
                print(f"cics_ids:{cics_ids}")
                # print(f"cics_ids:{cics_ids}")
                # 检查cics_ids是否为None或空列表
                if cics_ids is None:
                    # 处理SQL错误情况
                    # print(f"查询CICS ID时发生SQL错误")
                    industry_analysis = {"overall_analysis": "查询CICS ID时发生数据库错误"}
                    # cat_indicators 保持为 [] (已初始化/重置)
                elif cics_ids:
                    # 查询并处理ic_trend_scores
                    ic_trend_scores = [] # Initialize before try
                    try:
                        ic_trend_scores = query_ic_trend_score(cics_ids, year)
                    except Exception as e:
                        print(f"[ERROR] 景气度查询失败: {str(e)}")
                        industry_analysis["error"] = f"景气度数据异常：{str(e)}"
                        # ic_trend_scores remains []

                    analysis_results_ictrend = [] # Initialize before check
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
                            ic_trend_scores = filtered_scores # Overwrite with filtered list

                        analysis_results_ictrend = analyze_industry_trends(ic_trend_scores) # Assign result

                    # Ensure analysis_results_ictrend_v2 and industry_analysis are assigned dicts
                    if analysis_results_ictrend:
                        _analysis_results_ictrend_v2_temp = get_analysis_summary(analysis_results_ictrend) # Use temp
                        if _analysis_results_ictrend_v2_temp: # Check if summary is not empty
                           analysis_results_ictrend_v2 = _analysis_results_ictrend_v2_temp # Assign dict
                           industry_analysis, cost = conclude_from_ic_trend_score(analysis_results_ictrend_v2) # Assign dict
                        # else: analysis_results_ictrend_v2 and industry_analysis keep default empty/initial values
                    # else: analysis_results_ictrend_v2 and industry_analysis keep default empty/initial values

                    # 查询并处理ic_current_rating
                    ic_current_rating = [] # Initialize before try
                    filtered_result_ic_current_rating_temp = [] # Use temp var
                    try:
                        print(f"开始查询ic_current_rating：{cics_ids}")
                        ic_current_rating = query_ic_current_rating(cics_ids, year)
                        if ic_current_rating:
                            potential_cat_labels = ['profitability_cat', 'supply_demand']
                            filtered_result_ic_current_rating_temp = filter_ic_current_rating(ic_current_rating, potential_cat_labels) # Assign temp var
                            # print('已查询到ic_current_rating')
                            # print(filtered_result_ic_current_rating_temp)
                    except Exception as e:
                        print(f"处理 ic_current_rating 查询时出错: {str(e)}")
                        # ic_current_rating and filtered_result_ic_current_rating_temp remain []

                    # Assign final value for filtered_result_ic_current_rating
                    filtered_result_ic_current_rating = filtered_result_ic_current_rating_temp

                    # Ensure cat_indicators is assigned a list
                    if filtered_result_ic_current_rating: # Check the final list
                        cat_indicators, cost = conclude_from_cat_analysis(filtered_result_ic_current_rating) # Assign list
                        # print(cat_indicators)
                    # else: cat_indicators keeps default empty list []


            except Exception as e:
                print(f"查询行业指标时出错: {str(e)}")
                # Upon error here, reset to defaults ensures correct types
                industry_analysis = {"overall_analysis": f"查询行业指标时出错: {e}"}
                cat_indicators = []
                analysis_results_ictrend_v2 = {}
                filtered_result_ic_current_rating = []

        # query eco 指标
        # eco_indicators,eco_ids,eco_indicators_sum =  es_vector_query_eco_indicators(query_text)
        eco_indicators, eco_ids, eco_indicators_sum = es_vector_query_eco_indicators_v2(query_text, year) # Assume returns lists



        # Reset eco_indicators_report before assignment
        eco_indicators_report = ""
        if eco_indicators: # Check if list is not empty
            try:
                eco_indicators_analysis_stage_0 = analyze_eco_indicators(eco_indicators)
                eco_indicators_report = generate_summary_report(eco_indicators_analysis_stage_0) # Assume returns str
            except Exception as e:
                print(f"分析宏观经济指标时出错: {e}")
                eco_indicators_report = "宏观经济指标分析失败" # Assign error string
        else:
             eco_indicators_report = "无相关宏观经济指标数据" # Assign default string


        # 用year来修改eco_indicators_sum (This comment seems outdated/misplaced)

        # 确保 analysis_response 变量存在且是字符串 (it should be handled at the start of try)
        if 'analysis_response' not in locals() or not isinstance(analysis_response, str):
             analysis_response = "{\"analysis\": \"分析方法处理出错\"}" # Fallback str

        # 返回所有变量，它们现在应该具有正确的类型
        return report_query_response, simplified_policies, industry_analysis, cat_indicators, analysis_response, eco_indicators, eco_indicators_sum,eco_indicators_report,analysis_results_ictrend_v2,filtered_result_ic_current_rating

    except Exception as e:
        print(f"query_relative_data_v2 函数执行出错: {str(e)}")
        # 在函数顶层捕获到未知错误时，返回在函数开头初始化的、具有正确类型的变量
        # 确保 analysis_response 也是字符串类型
        _analysis_response_final = "{\"analysis\": \"无法生成分析方法\"}" # Default error string
        if 'analysis_response' in locals() and isinstance(analysis_response, str):
             _analysis_response_final = analysis_response # Use generated one if available

        # 返回初始化的变量，确保类型正确
        return report_query_response, simplified_policies, industry_analysis, cat_indicators, _analysis_response_final, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating


#添加topic
def query_relative_data_v3(year, current_title, analysis_response=None,topic = None):
    # 在函数开头初始化所有返回值变量，确保它们有正确的空类型
    print(f"正在调用query_relative_data_v3,当前处理的主题是: {topic}")
    report_query_response = []
    simplified_policies = []
    industry_analysis = {"overall_analysis": "暂无行业分析数据"}  # 保持 dict 类型
    cat_indicators = []
    # analysis_response 会在 try 块中生成或传入，先不初始化
    eco_indicators = []
    eco_indicators_sum = []
    eco_indicators_report = ""  # 期望是 str，初始化为空字符串
    analysis_results_ictrend_v2 = {}  # 期望是 dict，初始化为空字典
    filtered_result_ic_current_rating = []
    print(f"当前大标题：{topic}")
    try:
        # #     # # # 生成分析方法
        if not analysis_response or len(analysis_response)==0:
    # print('当前分析思路为空，开始生成分析思路')
            _analysis_response_str, cost = generate_analysis_methods(current_title) # Use temp var
            print(f'Raw response from generate_analysis_methods: {_analysis_response_str}') # <-- 添加打印
            try: # <-- 添加 try-except 处理可能的 JSON 解析错误
                analysis_dict = json.loads(_analysis_response_str)
                generated_analysis = analysis_dict.get('analysis', '') # <-- 使用 .get() 更安全
                print(f'Parsed analysis from generate_analysis_methods: {generated_analysis}') # <-- 添加打印
                w_instruction = generated_analysis # Use the newly generated analysis
            except json.JSONDecodeError as json_err:
                print(f"Error decoding JSON from generate_analysis_methods: {json_err}")
                print(f"Received string: {_analysis_response_str}")
                w_instruction = "" # 或者设置为默认值
        else:
            w_instruction = analysis_response
        # else: # analysis_response is already a string passed in
            # analysis_dict remains the string analysis_response for query_text
        # print(analysis_dict)
        # query_text = f"{current_title}\n 分析思路：{analysis_dict['analysis']}" # Old logic assumes dict
        query_text = f"{current_title}\n - {w_instruction} - {topic}" # Use str analysis_response directly

        potential_ic_trend_labels = get_potential_ic_trend_labels(query_text)
        # top_k = 10
        report_query_response_raw = search_and_query(query_text, index_type='header',top_k=5) # Assign to temp var
        # print(report_query_response_raw)
        # 确保report_query_response_raw不为None
        # print(len(report_query_response_raw))
        if report_query_response_raw is None:
            report_query_response_raw = []
        # 为每个header_id查询对应的file_node_id并添加到字典中，同时剔除current_headers_content为None的元素
        filtered_response = []
        for item in report_query_response_raw: # Use temp var
            # file_node_id = query_file_node_by_header(item['header_id'])
            file_node_id, file_node_name = query_file_node_and_name_by_header(item['header_id'])
            current_headers_content = query_content_under_header(item['header_id'])
            # 如果current_headers_content为None，则跳过该元素
            if not current_headers_content:
                continue

            # 添加file_node_id和current_headers_content
            item['file_node_id'] = file_node_id if file_node_id else None
            item['current_headers_content'] = current_headers_content
            item['file_node_name'] = file_node_name
            # 将有效元素添加到过滤后的列表中
            filtered_response.append(item)
        # print(f"filtered_response的长度为{len(filtered_response)}")
        # print(f"filtered_response:{filtered_response}")
        # 使用线程池并行处理每个content的相关性判断

        final_response_reports = []  # 使用新变量存储最终结果
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交所有判断任务，仅处理有content的元素
            future_to_item = {
                executor.submit(judge_title_relevance, current_title, item['content']): item
                for item in filtered_response if item.get('content')
            }

            # 在提交任务时打印current_title
            print(f"正在并行处理current_title: {current_title}")

            # 过滤保留相关的内容
            for future in concurrent.futures.as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    if future.result():  # 如果返回True则保留
                        final_response_reports.append(item)
                except Exception as e:
                    print(f"判断标题相关性时出错: {e}")
                    # 可以考虑记录日志或进行其他错误处理

        # 更新report_query_response为第一次过滤后的结果
        report_query_response = final_response_reports
        print(f"第一次过滤后的report_query_response的长度为{len(report_query_response)}")
        print(f"第一次过滤后的report_query_response:{report_query_response}")

        # 并行处理判断每个report_query_response的content与topic的相关性


        final_reports = []  # 使用新变量存储最终结果
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交所有判断任务，仅处理有content的元素
            future_to_report = {
                executor.submit(judge_topic_relevance, topic, report['file_node_name']): report
                for report in report_query_response if report.get('file_node_name')
            }

            # 在提交任务时打印topic和file_node_name
            print(f"正在并行处理topic: {topic}")
            for report in report_query_response:
                if report.get('file_node_name'):
                    print(f"处理报告: {report['file_node_name']}")

            # 过滤保留相关的内容
            for future in concurrent.futures.as_completed(future_to_report):
                report = future_to_report[future]
                try:
                    if future.result():  # 如果返回True则保留
                        final_reports.append(report)
                except Exception as e:
                    print(f"判断报告主题相关性时出错: {e}")
                    # 可以考虑记录日志或进行其他错误处理

        # 更新report_query_response为最终过滤后的结果
        report_query_response = final_reports
        print(f"第二次过滤后的report_query_response的长度为{len(report_query_response)}")
        print(f"第二次过滤后的report_query_response:{report_query_response}")

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
        simplified_policies_raw = [] # Use temp var
        all_cics_label_raw = [] # Use temp var

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

            # # 只有当industry有值时才添加 (commented out in original, keep it)
            # if policy.get('industry'):
            #     all_cics_label_raw.append(policy.get('industry'))

            #     #     # 将简化后的政策信息添加到列表中
            simplified_policies_raw.append(simplified_policy) # Append to temp var
        # print(f"简化后的政策数量为{len(simplified_policies_raw)}")
        # 并行处理判断每个simplified_policies_raw的policy_title与current_title的相关性
        # print(f"simplified_policies_raw的长度为{len(simplified_policies_raw)}")
        # print(f"simplified_policies_raw:{simplified_policies_raw}")
        final_policies = [] # Use new var for final result
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交所有判断任务，仅处理有policy_title的元素
            futures = []
            for policy in simplified_policies_raw: # Use temp var
                if policy.get('policy_title'):
                    future = executor.submit(judge_title_relevance, current_title, policy.get('policy_title'))
                    futures.append((policy, future))
            # 等待所有任务完成并处理结果
            for policy, future in futures:
                try:
                    if future.result():  # 如果返回True则保留
                        final_policies.append(policy) # Append to new var
                except Exception as e:
                    print(f"判断政策标题相关性时出错: {e}")

            # 更新simplified_policies为最终过滤后的结果
        simplified_policies = final_policies # Assign final result
        # print(f"筛选后的政策数量为{len(simplified_policies)}")
        # print(f"筛选后的政策数量为{len(simplified_policies)}")
        # print(f"筛选后的政策:{simplified_policies}")

        final_policies_v2 = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交所有判断任务，仅处理有policy_title的元素
            futures = []
            for policy in simplified_policies: # Use temp var
                if policy.get('policy_summary'):
                    print(f"政策摘要: {policy.get('policy_summary')}")  # 打印每个政策摘要
                    future = executor.submit(judge_topic_relevance, topic, policy.get('policy_summary'))
                    futures.append((policy, future))
            # 等待所有任务完成并处理结果
            for policy, future in futures:
                try:
                    if future.result():  # 如果返回True则保留
                        final_policies_v2.append(policy) # Append to new var
                except Exception as e:
                    print(f"判断政策标题相关性时出错: {e}")

            # 更新simplified_policies为最终过滤后的结果
            simplified_policies = final_policies_v2 # Assign final result
        print(f"筛选后的政策数量为_v2{len(simplified_policies)}")
        print(f"筛选后的政策_v2:{simplified_policies}")


        # #区域相关性判断
        # final_policies_v3 = []
        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     # 提交所有判断任务，仅处理有policy_title的元素
        #     futures = []
        #     for policy in simplified_policies:  # Use temp var
        #         if policy.get('policy_summary'):
        #             print(f"政策摘要: {policy.get('policy_summary')}")  # 打印每个政策摘要
        #             future = executor.submit(judge_area_topic_relevance, topic, policy.get('policy_title'),policy.get('org_name'))
        #             futures.append((policy, future))
        #     # 等待所有任务完成并处理结果
        #     for policy, future in futures:
        #         try:
        #             if future.result():  # 如果返回True则保留
        #                 final_policies_v3.append(policy)  # Append to new var
        #         except Exception as e:
        #             print(f"判断政策标题相关性时出错: {e}")
        #
        #     # 更新simplified_policies为最终过滤后的结果
        #     simplified_policies = final_policies_v3  # Assign final result
        # print(f"筛选后的政策数量为_v3{len(simplified_policies)}")
        # print(f"筛选后的政策_v3:{simplified_policies}")


        # 遍历simplified_policies，收集所有industry字段
        for policy in simplified_policies: # Use final policies list
            if policy.get('industry'):
                all_cics_label_raw.append(policy.get('industry')) # Append to temp var

        # 使用线程池并行处理行业标签相关性判断
        relevant_labels = [] # Use new var for final result
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交所有判断任务
            futures = {executor.submit(industry_indicator_relevance, [label], current_title): label for label in all_cics_label_raw} # Use temp var

            # 收集相关标签
            for future in concurrent.futures.as_completed(futures):
                label = futures[future]
                try:
                    if future.result():  # 如果返回True则保留
                        relevant_labels.append(label) # Append to new var
                except Exception as e:
                    print(f"判断行业标签'{label}'相关性时出错: {e}")

            # 更新all_cics_label为最终过滤后的结果
            all_cics_label = relevant_labels # Assign final result


        print(f"all_cics_label: {all_cics_label}")
        # # query 景气度
        # 重置/确保这些变量在进入 if 块前是初始状态
        industry_analysis = {"overall_analysis": "暂无行业分析数据"}
        cat_indicators = []
        analysis_results_ictrend_v2 = {} # Reset to default empty dict
        filtered_result_ic_current_rating = [] # Reset to default empty list

        if all_cics_label:
            try:
                cics_ids = get_cics_id_by_name(all_cics_label)
                print(f"[DEBUG] CICS IDs: {cics_ids}") # 确认 CICS ID

                if cics_ids is None:
                    # 处理SQL错误情况
                    # print(f"查询CICS ID时发生SQL错误")
                    industry_analysis = {"overall_analysis": "查询CICS ID时发生数据库错误"}
                    # cat_indicators 保持为 [] (已初始化/重置)
                elif cics_ids:
                    # 查询并处理ic_trend_scores
                    ic_trend_scores = [] # Initialize before try
                    try:
                        ic_trend_scores = query_ic_trend_score(cics_ids, year)
                        print(f"[DEBUG] 景气度查询结果: {ic_trend_scores}")
                    except Exception as e:
                        print(f"[ERROR] 景气度查询失败: {str(e)}")
                        industry_analysis["error"] = f"景气度数据异常：{str(e)}"
                        # ic_trend_scores remains []

                    analysis_results_ictrend = [] # Initialize before check
                    if ic_trend_scores:
                        print(f"[DEBUG] 景气度查询结果: {ic_trend_scores}")
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
                            ic_trend_scores = filtered_scores # Overwrite with filtered list
                        print(f"开始行业景气度分析_742：{ic_trend_scores}")
                        analysis_results_ictrend = analyze_industry_trends(ic_trend_scores) # Assign result
                        print(f"[DEBUG] 行业景气度分析结果_742: {analysis_results_ictrend}")
                    # Ensure analysis_results_ictrend_v2 and industry_analysis are assigned dicts
                    if analysis_results_ictrend:
                        print(f"开始行业景气度分析_747：{analysis_results_ictrend}")
                        _analysis_results_ictrend_v2_temp = get_analysis_summary(analysis_results_ictrend) # Use temp
                        print(f"[DEBUG] 行业景气度分析结果_746: {_analysis_results_ictrend_v2_temp}")
                        if _analysis_results_ictrend_v2_temp: # Check if summary is not empty
                           analysis_results_ictrend_v2 = _analysis_results_ictrend_v2_temp # Assign dict
                           print(f"开始行业景气度分析：{analysis_results_ictrend_v2}")
                           industry_analysis, cost = conclude_from_ic_trend_score(analysis_results_ictrend_v2) # Assign dict
                           print(f"[DEBUG] 行业景气度分析结果_750: {industry_analysis}")
                        # else: analysis_results_ictrend_v2 and industry_analysis keep default empty/initial values
                    # else: analysis_results_ictrend_v2 and industry_analysis keep default empty/initial values

                    # 查询并处理ic_current_rating
                    ic_current_rating = [] # Initialize before try
                    filtered_result_ic_current_rating_temp = [] # Use temp var
                    try:
                        ic_current_rating = query_ic_current_rating(cics_ids, year)
                        print(f"[DEBUG] ic_current_rating: {ic_current_rating}")
                        if ic_current_rating:
                            potential_cat_labels = ['profitability_cat', 'supply_demand']
                            print(f"开始过滤ic_current_rating：{ic_current_rating}")
                            filtered_result_ic_current_rating_temp = filter_ic_current_rating(ic_current_rating, potential_cat_labels) # Assign temp var
                            print(f"[DEBUG] filtered_result_ic_current_rating_temp: {filtered_result_ic_current_rating_temp}")
                            # print('已查询到ic_current_rating')
                            # print(filtered_result_ic_current_rating_temp)
                    except Exception as e:
                        print(f"[ERROR] 处理 ic_current_rating 时出错: 类型={type(e)}, 错误={e}")
                        import traceback
                        traceback.print_exc() # 打印详细堆栈
                        # ic_current_rating and filtered_result_ic_current_rating_temp remain []

                    # Assign final value for filtered_result_ic_current_rating
                    filtered_result_ic_current_rating = filtered_result_ic_current_rating_temp

                    # Ensure cat_indicators is assigned a list
                    if filtered_result_ic_current_rating: # Check the final list
                        cat_indicators, cost = conclude_from_cat_analysis(filtered_result_ic_current_rating) # Assign list
                        print(f"[DEBUG] cat_indicators: {cat_indicators}")
                        # print(cat_indicators)
                    # else: cat_indicators keeps default empty list []


            except Exception as e:
                print(f"查询行业指标时出错: {str(e)}")
                # Upon error here, reset to defaults ensures correct types
                industry_analysis = {"overall_analysis": f"查询行业指标时出错: {e}"}
                cat_indicators = []
                analysis_results_ictrend_v2 = {}
                filtered_result_ic_current_rating = []

        # query eco 指标
        # eco_indicators,eco_ids,eco_indicators_sum =  es_vector_query_eco_indicators(query_text)
        eco_indicators, eco_ids, eco_indicators_sum = es_vector_query_eco_indicators_v2(query_text, year) # Assume returns lists

        # Reset eco_indicators_report before assignment
        eco_indicators_report = ""

        print(f"eco_indicators筛选前的长度: {len(eco_indicators)}")
        # 使用并行方式筛选相关的经济指标
        # if eco_indicators:  # 检查列表是否为空
        #     with concurrent.futures.ThreadPoolExecutor() as executor:
        #         # 创建future到indicator的映射
        #         future_to_indicator = {
        #             executor.submit(eco_indicator_relevance, indicator.get('name_cn', ''), topic): indicator
        #             for indicator in eco_indicators
        #         }
        #
        #         # 收集相关指标
        #         relevant_eco_indicators = []
        #         for future in concurrent.futures.as_completed(future_to_indicator):
        #             indicator = future_to_indicator[future]
        #             try:
        #                 if future.result():  # 如果相关
        #                     relevant_eco_indicators.append(indicator)
        #             except Exception as e:
        #                 print(f"处理经济指标 {indicator.get('name_cn', '')} 时出错: {e}")
        #
        #     eco_indicators = relevant_eco_indicators  # 重新赋值
        #     print(f"筛选后的eco_indicators: {eco_indicators}")
        #     print(f"筛选后的eco_indicators的长度: {len(eco_indicators)}")


        #判断indicator是否和当前的三级标题相关
        if eco_indicators:  # 检查列表是否为空
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # 创建future到indicator的映射
                future_to_indicator = {
                    executor.submit(eco_indicator_relevance, indicator.get('name_cn', ''), current_title): indicator
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

            # 加一个 一二三级研报标题和指标进行筛选


        if eco_indicators: # Check if list is not empty
            try:
                eco_indicators_analysis_stage_0 = analyze_eco_indicators(eco_indicators)
                eco_indicators_report = generate_summary_report(eco_indicators_analysis_stage_0) # Assume returns str
            except Exception as e:
                print(f"分析宏观经济指标时出错: {e}")
                eco_indicators_report = "宏观经济指标分析失败" # Assign error string
        else:
             eco_indicators_report = "无相关宏观经济指标数据" # Assign default string


        # 用year来修改eco_indicators_sum (This comment seems outdated/misplaced)

        # 确保 analysis_response 变量存在且是字符串 (it should be handled at the start of try)
        if 'analysis_response' not in locals() or not isinstance(w_instruction, str):
             w_instruction = "{\"analysis\": \"分析方法处理出错\"}" # Fallback str

        # 返回所有变量，它们现在应该具有正确的类型
        return report_query_response, simplified_policies, industry_analysis, cat_indicators, w_instruction, eco_indicators, eco_indicators_sum,eco_indicators_report,analysis_results_ictrend_v2,filtered_result_ic_current_rating

    except Exception as e:
        print(f"query_relative_data_v3 函数执行出错: {str(e)}")
        # 在函数顶层捕获到未知错误时，返回在函数开头初始化的、具有正确类型的变量
        # 确保 analysis_response 也是字符串类型
        _analysis_response_final = "{\"analysis\": \"无法生成分析方法\"}" # Default error string
        if 'analysis_response' in locals() and isinstance(analysis_response, str):
             _analysis_response_final = analysis_response # Use generated one if available

        # 返回初始化的变量，确保类型正确
        return report_query_response, simplified_policies, industry_analysis, cat_indicators, w_instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating

if __name__ == "__main__":
    # year = 2024
    # query_title = '2024年新能源汽车行业'
    # instruction = ''
    year = 2024
    query_title = "人形机器人"
    instruction = ''
    # 调用函数并解包所有返回值
    (report_query_response, simplified_policies, industry_analysis,
     cat_indicators, analysis_response, eco_indicators, eco_indicators_sum,
     eco_indicators_report, analysis_results_ictrend_v2,
     filtered_result_ic_current_rating) = query_relative_data_v3(
        2023, "龙头企业：市场份额与全产业链数字化优势", instruction,query_title)

    # print("="*50)
    # print(f"report_query_response (type: {type(report_query_response)}):", report_query_response)
    print("="*50)
    print(f"simplified_policies (type: {type(simplified_policies)}):", simplified_policies)
    print("="*50)
    # print(f"industry_analysis (type: {type(industry_analysis)}):", industry_analysis)
    # print("="*50)
    # print(f"cat_indicators (type: {type(cat_indicators)}):", cat_indicators)
    # print("="*50)
    # print(f"analysis_response (type: {type(analysis_response)}):", analysis_response)
    # print("="*50)
    # print(f"eco_indicators (type: {type(eco_indicators)}):", eco_indicators)
    # print("="*50)
    # print(f"eco_indicators_sum (type: {type(eco_indicators_sum)}):", eco_indicators_sum)
    # print("="*50)
    # print(f"eco_indicators_report (type: {type(eco_indicators_report)}):", eco_indicators_report)
    # print("="*50)
    # print(f"analysis_results_ictrend_v2 (type: {type(analysis_results_ictrend_v2)}):", analysis_results_ictrend_v2)
    # print("="*50)
    # print(f"filtered_result_ic_current_rating (type: {type(filtered_result_ic_current_rating)}):", filtered_result_ic_current_rating)
    # print("="*50)
    #