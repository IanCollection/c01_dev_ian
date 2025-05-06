import concurrent
import datetime
import os
import sys
import json
from decimal import Decimal
import time # 确保导入 time 模块

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)
from Agent.surpervisor_agent import judge_title_relevance, industry_indicator_relevance, judge_topic_relevance, \
    eco_indicator_relevance, judge_area_topic_relevance, filter_ic_trend_scores_by_relevance
# 现在可以导入项目模块
from database.query_ic_indicators import get_cics_id_by_name, query_ic_trend_score, query_ic_current_rating
from scrpit.analyze_ic_trend_score import analyze_industry_trends, get_analysis_summary
from pg2es_hybrid.es_vector_query import es_vector_query, es_vector_query_eco_indicators, \
    es_vector_query_eco_indicators_v2, es_vector_query_policy_info, process_indicators
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
        query_text_for_indicators = f"{current_title}\n"
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
        eco_indicators, eco_ids, eco_indicators_sum = es_vector_query_eco_indicators_v2(query_text_for_indicators,year)
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
        query_text_for_indicators = f"{current_title}"
        potential_ic_trend_labels = get_potential_ic_trend_labels(query_text)
        # top_k = 10
        report_query_response_raw = search_and_query(query_text, index_type='header',top_k=10) # Assign to temp var
        # print(report_query_response_raw)
        # 确保report_query_response_raw不为None
        # print(len(report_query_response_raw))
        if report_query_response_raw is None:
            report_query_response_raw = []

        filtered_response = []
        # 串行查询文件节点信息，避免过多并发数据库连接
        # 检查 report_query_response_raw 是否为预期的结构
        try:
            if isinstance(report_query_response_raw, list):
                # 正常处理返回的列表
                for item in report_query_response_raw:
                    # 确保 item 是字典并且有 header_id 键
                    if not isinstance(item, dict) or 'header_id' not in item:
                        print(f"[WARNING] Unexpected item structure in report_query_response_raw: {item}")
                        continue
                        
                    file_node_id, file_node_name = query_file_node_and_name_by_header(item['header_id'])
                    current_headers_content = query_content_under_header(item['header_id'])
                    if not current_headers_content:
                        continue
                    item['file_node_id'] = file_node_id if file_node_id else None
                    item['current_headers_content'] = current_headers_content
                    item['file_node_name'] = file_node_name
                    filtered_response.append(item)
            # 这里添加处理其他情况的代码
            elif hasattr(report_query_response_raw, '__iter__') and not isinstance(report_query_response_raw, str):
                # 如果是可迭代对象但不是字符串，尝试迭代处理
                print(f"[DEBUG] report_query_response_raw 是可迭代对象但不是列表，尝试迭代处理")
                for i, header_id in enumerate(report_query_response_raw):
                    print(f"[DEBUG] 处理第 {i} 个元素: {header_id}")
                    try:
                        file_node_id, file_node_name = query_file_node_and_name_by_header(header_id)
                        current_headers_content = query_content_under_header(header_id)
                        if not current_headers_content:
                            continue
                        item = {
                            'header_id': header_id,
                            'file_node_id': file_node_id if file_node_id else None,
                            'current_headers_content': current_headers_content,
                            'file_node_name': file_node_name,
                            'content': '' # 添加空的 content 字段
                        }
                        filtered_response.append(item)
                    except Exception as e:
                        print(f"[ERROR] 处理元素 {header_id} 时出错: {e}")
            else:
                print(f"[WARNING] report_query_response_raw 不是预期的格式: {type(report_query_response_raw)}")
        except Exception as e:
            print(f"[ERROR] 处理 report_query_response_raw 时出错: {e}")
            import traceback
            traceback.print_exc()

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
    overall_start_time = time.time() # 记录整体开始时间
    timings = {} # 用于存储各部分耗时

    # 在函数开头初始化所有返回值变量，确保它们有正确的空类型
    print(f"正在调用query_relative_data_v3 (Title: {current_title}), 主题是: {topic}")
    report_query_response = []
    simplified_policies = []
    industry_analysis = {"overall_analysis": "暂无行业分析数据"}
    cat_indicators = []
    eco_indicators = []
    eco_indicators_sum = []
    eco_indicators_report = ""
    analysis_results_ictrend_v2 = {}
    filtered_result_ic_current_rating = []
    w_instruction = analysis_response # 初始化 w_instruction

    try:
        # --- 1. 生成分析方法 (w_instruction) ---
        step_start_time = time.time()
        if not analysis_response or len(analysis_response)==0:
            _analysis_response_str, cost = generate_analysis_methods(current_title)
            try:
                analysis_dict = json.loads(_analysis_response_str)
                w_instruction = analysis_dict.get('analysis', '')
            except json.JSONDecodeError as json_err:
                print(f"Error decoding JSON from generate_analysis_methods: {json_err}")
                w_instruction = ""
        timings['w_instruction_generation'] = time.time() - step_start_time
        # --- 分析方法生成结束 ---

        query_text = f"{current_title}\n - {w_instruction} - {topic}"
        query_text_for_indicators = f"{current_title}\n - {topic}"
        # --- 2. 相关研报查询 (report_query_response) ---
        step_start_time = time.time()
        print(f"[DEBUG] 开始查询ic_trend_score_labels")
        potential_ic_trend_labels = get_potential_ic_trend_labels(query_text)
        # 添加调试信息，检查 search_and_query 的返回值
        print(f"[DEBUG] 调用 search_and_query 前")
        result = search_and_query(query_text, index_type='header', top_k=5)

        if result:
            report_query_response_raw = result
        else:
            report_query_response_raw = []

        print('539:report_query_response_raw:',report_query_response_raw)
        
        filtered_response = []
        # 串行查询文件节点信息，避免过多并发数据库连接
        # 检查 report_query_response_raw 是否为预期的结构
        try:
            if isinstance(report_query_response_raw, list):
                # 正常处理返回的列表
                for item in report_query_response_raw:
                    # 确保 item 是字典并且有 header_id 键
                    if not isinstance(item, dict) or 'header_id' not in item:
                        print(f"[WARNING] Unexpected item structure in report_query_response_raw: {item}")
                        continue
                        
                    file_node_id, file_node_name = query_file_node_and_name_by_header(item['header_id'])
                    current_headers_content = query_content_under_header(item['header_id'])
                    if not current_headers_content:
                        continue
                    item['file_node_id'] = file_node_id if file_node_id else None
                    item['current_headers_content'] = current_headers_content
                    item['file_node_name'] = file_node_name
                    filtered_response.append(item)
            # 这里添加处理其他情况的代码
            elif hasattr(report_query_response_raw, '__iter__') and not isinstance(report_query_response_raw, str):
                # 如果是可迭代对象但不是字符串，尝试迭代处理
                print(f"[DEBUG] report_query_response_raw 是可迭代对象但不是列表，尝试迭代处理")
                for i, header_id in enumerate(report_query_response_raw):
                    print(f"[DEBUG] 处理第 {i} 个元素: {header_id}")
                    try:
                        file_node_id, file_node_name = query_file_node_and_name_by_header(header_id)
                        current_headers_content = query_content_under_header(header_id)
                        if not current_headers_content:
                            continue
                        item = {
                            'header_id': header_id,
                            'file_node_id': file_node_id if file_node_id else None,
                            'current_headers_content': current_headers_content,
                            'file_node_name': file_node_name,
                            'content': '' # 添加空的 content 字段
                        }
                        filtered_response.append(item)
                    except Exception as e:
                        print(f"[ERROR] 处理元素 {header_id} 时出错: {e}")
            else:
                print(f"[WARNING] report_query_response_raw 不是预期的格式: {type(report_query_response_raw)}")
        except Exception as e:
            print(f"[ERROR] 处理 report_query_response_raw 时出错: {e}")
            import traceback
            traceback.print_exc()

        # 并行判断标题相关性
        final_response_reports_title_relevant = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_item = {
                executor.submit(judge_title_relevance, current_title, item['content']): item
                for item in filtered_response if item.get('content')
            }
            for future in concurrent.futures.as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    if future.result():
                        final_response_reports_title_relevant.append(item)
                except Exception as e:
                    print(f"判断报告标题相关性时出错: {e}")

        # 并行判断主题相关性
        final_reports_topic_relevant = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_report = {
                executor.submit(judge_topic_relevance, topic, report['file_node_name']): report
                for report in final_response_reports_title_relevant if report.get('file_node_name')
            }
            for future in concurrent.futures.as_completed(future_to_report):
                report = future_to_report[future]
                try:
                    if future.result():
                        final_reports_topic_relevant.append(report)
                except Exception as e:
                    print(f"判断报告主题相关性时出错: {e}")

        # 根据header_id去重
        header_id_set = set()
        report_query_response = []
        for item in final_reports_topic_relevant:
            header_id = item.get('header_id')
            if header_id and header_id not in header_id_set:
                header_id_set.add(header_id)
                report_query_response.append(item)
        print(f"去重后的报告数量: {len(report_query_response)}")

        timings['report_query'] = time.time() - step_start_time
        # --- 相关研报查询结束 ---


        # --- 3. 相关政策查询 (simplified_policies) ---
        step_start_time = time.time()
        policy_raw, policy_ids = es_vector_query_policy_info(query_text)
        print(f'628_policy:{len(policy_raw)}')
        policy_ids = [int(pid) for pid in policy_ids if pid.isdigit()]
        policy_details = get_policy_details_by_ids(policy_ids) if policy_ids else []

        simplified_policies_raw = []
        all_cics_label_raw = []
        if not isinstance(policy_details, list):
            policy_details = []
        for policy_detail in policy_details:
             if not isinstance(policy_detail, dict): continue
             simplified_policy = {
                 'id': policy_detail.get('id'),
                 'policy_title': policy_detail.get('policy_title'),
                 'policy_summary': policy_detail.get('policy_summary'),
                 'industry': policy_detail.get('industry', None),
                 'policy_start_time': policy_detail.get('policy_start_date', None),
                 'policy_end_time': policy_detail.get('policy_end_date', None),
                 'org_name': policy_detail.get('org_name', None),
                 'involved_region':policy_detail.get('involved_region',None)
             }
             if policy_detail.get('industry'):
                 all_cics_label_raw.append(policy_detail.get('industry'))
             simplified_policies_raw.append(simplified_policy)

        # 并行判断政策标题相关性
        policies_title_relevant = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(judge_title_relevance, current_title, p.get('policy_title')): p for p in simplified_policies_raw if p.get('policy_title')}
            for future in concurrent.futures.as_completed(futures):
                p = futures[future]
                try:
                    if future.result():
                        policies_title_relevant.append(p)
                except Exception as e:
                    print(f"判断政策标题相关性时出错: {e}")

        print(f'policies_title_relevant_v2:{len(policies_title_relevant)}')
        # 并行判断政策摘要主题相关性
        policies_topic_relevant = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(judge_topic_relevance, topic, p.get('policy_summary')): p for p in policies_title_relevant if p.get('policy_summary')}
            for future in concurrent.futures.as_completed(futures):
                p = futures[future]
                try:
                    if future.result():
                        policies_topic_relevant.append(p)
                except Exception as e:
                    print(f"判断政策摘要主题相关性时出错: {e}")
        print(f'政策筛选v3:{len(policies_topic_relevant)}')
        simplified_policies = policies_topic_relevant
        # # 并行判断政策区域相关性
        # policies_region_relevant = []
        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     futures = {executor.submit(judge_area_topic_relevance, topic, p.get('involved_region'), p.get('org_name')): p for p in policies_topic_relevant if p.get('involved_region')}
        #     for future in concurrent.futures.as_completed(futures):
        #          p = futures[future]
        #          try:
        #              if future.result():
        #                  policies_region_relevant.append(p)
        #          except Exception as e:
        #              print(f"判断政策区域相关性时出错: {e}")
        #
        # simplified_policies = policies_region_relevant
        # timings['policy_query'] = time.time() - step_start_time
        # --- 相关政策查询结束 ---


        # --- 4. 行业指标查询与分析 (industry_analysis, cat_indicators, analysis_results_ictrend_v2, filtered_result_ic_current_rating) ---
        step_start_time = time.time()
        
        # --- 4.1 筛选相关行业标签 ---
        label_filter_start_time = time.time()
        relevant_labels = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(industry_indicator_relevance, [label], current_title): label for label in all_cics_label_raw}
            for future in concurrent.futures.as_completed(futures):
                label = futures[future]
                try:
                    if future.result():
                        relevant_labels.append(label)
                except Exception as e:
                    print(f"判断行业标签'{label}'相关性时出错: {e}")
        all_cics_label = relevant_labels
        label_filter_time = time.time() - label_filter_start_time
        print(f"筛选相关行业标签耗时: {label_filter_time:.2f} 秒")

        # 重置/确保这些变量在进入 if 块前是初始状态
        industry_analysis = {"overall_analysis": "暂无行业分析数据"}
        cat_indicators = []
        analysis_results_ictrend_v2 = {}
        filtered_result_ic_current_rating = []

        if all_cics_label:
            try:
                # --- 4.2 获取CICS ID ---
                cics_id_start_time = time.time()
                cics_ids = get_cics_id_by_name(all_cics_label)
                cics_id_time = time.time() - cics_id_start_time
                print(f"获取CICS ID耗时: {cics_id_time:.2f} 秒")
                
                if cics_ids is None:
                    industry_analysis = {"overall_analysis": "查询CICS ID时发生数据库错误"}
                elif cics_ids:
                    # --- 4.3 查询行业景气度趋势 ---
                    ic_trend_query_start_time = time.time()
                    ic_trend_scores = []
                    try:
                        ic_trend_scores = query_ic_trend_score(cics_ids, year)
                        # print(f'738_ic_trend_scores_v2_this_query is based on cics_ids:{ic_trend_scores}')
                        ic_trend_scores = filter_ic_trend_scores_by_relevance(ic_trend_scores, topic)

                    except Exception as e:
                        industry_analysis["error"] = f"景气度数据异常：{str(e)}"
                    ic_trend_query_time = time.time() - ic_trend_query_start_time
                    print(f"查询行业景气度趋势耗时: {ic_trend_query_time:.2f} 秒")

                    # --- 4.4 分析行业趋势 ---
                    trend_analysis_start_time = time.time()
                    analysis_results_ictrend = []
                    if ic_trend_scores:
                        if potential_ic_trend_labels:
                            base_fields = ['id', 'date', 'cics_id', 'cics_name', 'updated_at']
                            keep_fields = base_fields.copy()
                            for label in potential_ic_trend_labels:
                                keep_fields.extend([f'{label}_score', f'{label}_grade'])
                            ic_trend_scores = [{k: v for k, v in score.items() if k in keep_fields} for score in ic_trend_scores]
                        analysis_results_ictrend = analyze_industry_trends(ic_trend_scores)
                    trend_analysis_time = time.time() - trend_analysis_start_time
                    print(f"分析行业趋势耗时: {trend_analysis_time:.2f} 秒")

                    # --- 4.5 获取分析摘要 ---
                    summary_start_time = time.time()
                    if analysis_results_ictrend:
                        # 获取分析摘要
                        get_summary_start_time = time.time()
                        _analysis_results_ictrend_v2_temp = get_analysis_summary(analysis_results_ictrend)
                        print(f'_analysis_results_ictrend_v2_temp_762:{_analysis_results_ictrend_v2_temp}')
                        get_summary_time = time.time() - get_summary_start_time
                        print(f"获取分析摘要(get_analysis_summary)耗时: {get_summary_time:.2f} 秒")
                        
                        if _analysis_results_ictrend_v2_temp:
                            analysis_results_ictrend_v2 = _analysis_results_ictrend_v2_temp
                            print(f"[DEBUG Before conclude_from_ic_trend_score] Type of analysis_results_ictrend_v2: {type(analysis_results_ictrend_v2)}")
                            print(f"analysis_results_ictrend_v2:{analysis_results_ictrend_v2}")
                            # 从趋势分数得出结论
                            conclude_start_time = time.time()
                            industry_analysis, cost = conclude_from_ic_trend_score(analysis_results_ictrend_v2)
                            conclude_time = time.time() - conclude_start_time
                            print(f"得出行业趋势结论(conclude_from_ic_trend_score)耗时: {conclude_time:.2f} 秒")
                    summary_time = time.time() - summary_start_time
                    print(f"获取分析摘要耗时: {summary_time:.2f} 秒")

                    # --- 4.6 查询当前行业评级 ---
                    current_rating_start_time = time.time()
                    ic_current_rating = []
                    filtered_result_ic_current_rating_temp = []
                    try:
                        ic_current_rating = query_ic_current_rating(cics_ids, year)
                        if ic_current_rating:
                            potential_cat_labels = ['profitability_cat', 'supply_demand']
                            print(f"789 ic_current_rating:{ic_current_rating}")

                            filtered_result_ic_current_rating_temp = filter_ic_current_rating(ic_current_rating, potential_cat_labels)
                            print(f"[DEBUG] filtered_result_ic_current_rating_temp_筛选前的cics_name: {filtered_result_ic_current_rating_temp}") # 修改日志区分筛选前后
                            # 在这里添加对 filtered_result_ic_current_rating_temp 的相关性筛选
                            filtered_result_ic_current_rating_temp = filter_ic_trend_scores_by_relevance(filtered_result_ic_current_rating_temp, topic)
                            print(f"[DEBUG] filtered_result_ic_current_rating_temp_筛选后的cics_name: {filtered_result_ic_current_rating_temp}") # 确认筛选结果
                    except Exception as e:
                         print(f"[ERROR] 处理 ic_current_rating 时出错: {e}")
                         import traceback
                         traceback.print_exc()
                    current_rating_time = time.time() - current_rating_start_time
                    print(f"查询当前行业评级耗时: {current_rating_time:.2f} 秒")

                    filtered_result_ic_current_rating = filtered_result_ic_current_rating_temp

                    # --- 4.7 分析类别指标 ---
                    cat_analysis_start_time = time.time()
                    if filtered_result_ic_current_rating:
                        print(f"[DEBUG Before conclude_from_cat_analysis] Type of filtered_result_ic_current_rating: {type(filtered_result_ic_current_rating)}")
                        cat_indicators, cost = conclude_from_cat_analysis(filtered_result_ic_current_rating)
                    cat_analysis_time = time.time() - cat_analysis_start_time
                    print(f"分析类别指标耗时: {cat_analysis_time:.2f} 秒")

            except Exception as e:
                print(f"查询行业指标时出错: {str(e)}")
                industry_analysis = {"overall_analysis": f"查询行业指标时出错: {e}"}
                cat_indicators = []
                analysis_results_ictrend_v2 = {}
                filtered_result_ic_current_rating = []
        
        total_industry_time = time.time() - step_start_time
        timings['industry_indicators'] = total_industry_time
        print(f"行业指标查询与分析总耗时: {total_industry_time:.2f} 秒")
        # --- 行业指标查询与分析结束 ---




        # --- 5. 宏观经济指标查询与分析 (eco_indicators, eco_indicators_sum, eco_indicators_report) ---
        step_start_time = time.time()
        eco_indicators_raw, eco_ids = es_vector_query_eco_indicators_v2(query_text_for_indicators, year)
        print(f"820_eco_indicators_raw:{len(eco_indicators_raw)}")
        print(f"830_eco_indicators_raw:{eco_indicators_raw}")
        relevant_eco_indicators = []
        if eco_indicators_raw:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_indicator = {
                    executor.submit(eco_indicator_relevance, indicator.get('name_cn', ''), topic): indicator
                    for indicator in eco_indicators_raw
                }
                for future in concurrent.futures.as_completed(future_to_indicator):
                    indicator = future_to_indicator[future]
                    try:
                        if future.result():
                            relevant_eco_indicators.append(indicator)
                    except Exception as e:
                        print(f"处理经济指标 {indicator.get('name_cn', '')} 时出错: {e}")
        print(f"筛选后的eco_indicators:{len(relevant_eco_indicators)}")
        print(f"筛选后的eco_indicators:{relevant_eco_indicators}")
        print(f"eco_indicators 筛选耗时：{time.time()-step_start_time}")
        eco_indicators = relevant_eco_indicators
        eco_indicators_sum = process_indicators(eco_indicators)
        eco_indicators_report = "无相关宏观经济指标数据"
        if eco_indicators:
            try:
                eco_indicators_analysis_stage_0 = analyze_eco_indicators(eco_indicators)
                eco_indicators_report = generate_summary_report(eco_indicators_analysis_stage_0)
            except Exception as e:
                print(f"分析宏观经济指标时出错: {e}")
                eco_indicators_report = "宏观经济指标分析失败"
        timings['eco_indicators'] = time.time() - step_start_time
        # --- 宏观经济指标查询与分析结束 ---


        # --- 最终处理和返回 ---
        overall_end_time = time.time() # 记录整体结束时间
        overall_processing_time = overall_end_time - overall_start_time # 计算总耗时

        print(f"--- query_relative_data_v3 执行完成 (Title: {current_title}) ---")
        print(f"总耗时: {overall_processing_time:.2f} 秒")
        # 打印各部分耗时
        for step, duration in timings.items():
            print(f"  - {step} 耗时: {duration:.2f} 秒")
        # 打印返回结果摘要
        # print(f"返回 report_query_response: {len(report_query_response)} 条")
        # print(f"返回 simplified_policies: {len(simplified_policies)} 条")
        # print(f"返回 industry_analysis: {'有数据' if industry_analysis else '无数据'}")
        # print(f"返回 cat_indicators: {'有数据' if cat_indicators else '无数据'}")
        # print(f"返回 w_instruction: {'有内容' if w_instruction else '无内容'}")
        # print(f"返回 eco_indicators: {len(eco_indicators)} 条")
        # print(f"返回 eco_indicators_sum: {'有数据' if eco_indicators_sum else '无数据'}")
        # print(f"返回 eco_indicators_report: {'有内容' if eco_indicators_report else '无内容'}")
        # print(f"返回 analysis_results_ictrend_v2: {'有数据' if analysis_results_ictrend_v2 else '无数据'}")
        # print(f"返回 filtered_result_ic_current_rating: {len(filtered_result_ic_current_rating)} 条")
        # print(f"--- 返回数据打印结束 ---")

        return report_query_response, simplified_policies, industry_analysis, cat_indicators, w_instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating

    except Exception as e:
        overall_end_time = time.time() # 记录异常结束时间
        overall_processing_time = overall_end_time - overall_start_time # 计算总耗时
        print(f"--- query_relative_data_v3 执行出错 (Title: {current_title}) ---")
        print(f"总耗时: {overall_processing_time:.2f} 秒")
         # 打印已记录的各部分耗时
        if timings:
             print("已完成步骤耗时:")
             for step, duration in timings.items():
                 print(f"  - {step} 耗时: {duration:.2f} 秒")
        print(f"错误信息: {str(e)}")
        import traceback
        traceback.print_exc()

        _w_instruction_final = "{\"analysis\": \"无法生成分析方法\"}"
        if 'w_instruction' in locals() and isinstance(w_instruction, str):
             _w_instruction_final = w_instruction
        else:
             _w_instruction_final = analysis_response if isinstance(analysis_response, str) else _w_instruction_final

        # 返回初始化的变量
        return report_query_response, simplified_policies, industry_analysis, cat_indicators, _w_instruction_final, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating

if __name__ == "__main__":
    # year = 2024
    query_title = '2024年新能源汽车行业'
    # instruction = ''
    # year = 2024
    # query_title = "人形机器人"
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
    print(f"industry_analysis (type: {type(industry_analysis)}):", industry_analysis)
    print("="*50)
    print(f"cat_indicators (type: {type(cat_indicators)}):", cat_indicators)
    print("="*50)
    print(f"analysis_response (type: {type(analysis_response)}):", analysis_response)
    print("="*50)
    print(f"eco_indicators (type: {type(eco_indicators)}):", eco_indicators)
    print("="*50)
    print(f"eco_indicators_sum (type: {type(eco_indicators_sum)}):", eco_indicators_sum)
    print("="*50)
    print(f"eco_indicators_report (type: {type(eco_indicators_report)}):", eco_indicators_report)
    print("="*50)
    print(f"analysis_results_ictrend_v2 (type: {type(analysis_results_ictrend_v2)}):", analysis_results_ictrend_v2)
    print("="*50)
    print(f"filtered_result_ic_current_rating (type: {type(filtered_result_ic_current_rating)}):", filtered_result_ic_current_rating)
    print("="*50)
    