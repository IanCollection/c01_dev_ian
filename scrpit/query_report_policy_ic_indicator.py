import concurrent
import datetime
import os
import sys
import json
from decimal import Decimal
from typing import List, Dict, Any, Tuple, Optional


# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)
from Agent.surpervisor_agent import judge_title_relevance, industry_indicator_relevance
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

# --- Robust Default Values ---
DEFAULT_ANALYSIS_RESPONSE = {"analysis": ""}
DEFAULT_INDUSTRY_ANALYSIS = {"overall_analysis": ""}
DEFAULT_CAT_INDICATORS = ""
DEFAULT_EMPTY_LIST = []
DEFAULT_EMPTY_DICT = {}

def safe_json_loads(json_string: Optional[str], default: Any = None) -> Any:
    """Safely load JSON string, return default on error."""
    if not isinstance(json_string, str):
        return default
    try:
        return json.loads(json_string)
    except json.JSONDecodeError:
        print(f"Warning: Failed to decode JSON: {json_string[:100]}...")
        return default
    except Exception as e:
        print(f"Warning: Unexpected error during JSON load: {e}")
        return default

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
        industry_analysis = {"overall_analysis": ""}
        cat_indicators = ""
        
        if all_cics_label:
            try:
                cics_ids = get_cics_id_by_name(all_cics_label)
                
                # 检查cics_ids是否为None或空列表
                if cics_ids is None:
                    # 处理SQL错误情况
                    print(f"查询CICS ID时发生SQL错误")
                    industry_analysis = {"overall_analysis": ""}
                    cat_indicators = ""
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
        eco_indicators, eco_ids, eco_indicators_sum = es_vector_query_eco_indicators_v2(query_text,year)
        # 用year来修改eco_indicators_sum
        return report_query_response, simplified_policies, industry_analysis, cat_indicators, analysis_response,eco_indicators,eco_indicators_sum
    
    except Exception as e:
        print(f"query_relative_data 函数执行出错: {str(e)}")
        # 返回默认值，确保不会中断整个流程
        return [], [], {"overall_analysis": ""}, "", "{\"analysis\": \"\"}"

def query_relative_data_v2(year: Optional[int], current_title: Optional[str], analysis_response: Optional[str] = None) -> Tuple[List, List, Dict, str, Dict, List, Dict, List, List, Dict]:
    """
    查询与标题相关的报告、政策、行业指标和宏观经济数据 v2 (Robust Version)

    Args:
        year (Optional[int]): 年份，可能为None
        current_title (Optional[str]): 当前标题，可能为None
        analysis_response (Optional[str]): 预设的分析思路 (JSON string)，可能为None

    Returns:
        Tuple containing:
        - report_query_response: 相关报告列表
        - simplified_policies: 相关政策列表
        - industry_analysis: 行业景气度分析结果
        - cat_indicators: 行业分类指标分析结果
        - analysis_dict: 使用的分析思路
        - eco_indicators: 宏观经济指标原始数据
        - eco_indicators_sum: 宏观经济指标摘要
        - eco_indicators_report: 宏观经济指标分析报告
        - analysis_results_ictrend_v2: IC趋势分析摘要
        - filtered_result_ic_current_rating: IC当前评级过滤结果
    """
    # --- Initialize return variables with safe defaults ---
    report_query_response: List = DEFAULT_EMPTY_LIST
    simplified_policies: List = DEFAULT_EMPTY_LIST
    industry_analysis: Dict = DEFAULT_INDUSTRY_ANALYSIS
    cat_indicators: str = DEFAULT_CAT_INDICATORS
    analysis_dict: Dict = DEFAULT_ANALYSIS_RESPONSE
    eco_indicators: List = DEFAULT_EMPTY_LIST
    eco_indicators_sum: Dict = DEFAULT_EMPTY_DICT
    eco_indicators_report: List = DEFAULT_EMPTY_LIST
    analysis_results_ictrend_v2: List = DEFAULT_EMPTY_LIST
    filtered_result_ic_current_rating: Dict = DEFAULT_EMPTY_DICT
    potential_ic_trend_labels: List = DEFAULT_EMPTY_LIST
    
    try:
        if not current_title:
            print("Warning: current_title is empty, returning defaults.")
            return report_query_response, simplified_policies, industry_analysis, cat_indicators, analysis_dict, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating

        print(f"Processing title: {current_title}")

        # --- Generate/Load Analysis Methods ---
        analysis_source = "default" # Track where the analysis came from

        if isinstance(analysis_response, str) and len(analysis_response) > 0:
            # 优先尝试解析为JSON '{"analysis": "..."}'
            loaded_analysis = safe_json_loads(analysis_response, default=None) # Use None to distinguish failure

            if isinstance(loaded_analysis, dict) and loaded_analysis.get("analysis"):
                 # 情况 1: 输入是有效的 JSON '{"analysis": "..."}'
                 analysis_dict = loaded_analysis
                 analysis_source = "provided_json"
                 print(f"Using provided JSON analysis: {analysis_dict.get('analysis', '')[:50]}...")
            else:
                 # 情况 2: 输入是普通字符串，直接使用它作为分析内容
                 analysis_dict = {"analysis": analysis_response}
                 analysis_source = "provided_string"
                 print(f"Using provided raw string analysis: {analysis_dict.get('analysis', '')[:50]}...")

        # 如果没有提供有效的 analysis_response (无论哪种格式)，则尝试生成
        if analysis_source == "default":
            print("No valid analysis provided, attempting to generate.")
            try:
                generated_analysis_str, _ = generate_analysis_methods(current_title)
                # 尝试解析生成的JSON
                generated_dict = safe_json_loads(generated_analysis_str, default=DEFAULT_ANALYSIS_RESPONSE)
                # 确保生成的也是有效结构
                if isinstance(generated_dict, dict) and "analysis" in generated_dict:
                    analysis_dict = generated_dict
                    analysis_source = "generated"
                    print(f"Generated analysis: {analysis_dict.get('analysis', '')[:50]}...")
                else:
                     analysis_dict = DEFAULT_ANALYSIS_RESPONSE # 使用默认值
                     analysis_source = "generation_failed"
                     print("Warning: Failed to generate valid analysis methods.")

            except Exception as e:
                print(f"Error generating analysis methods: {e}")
                analysis_dict = DEFAULT_ANALYSIS_RESPONSE # 发生异常也使用默认值
                analysis_source = "generation_error"

        # 确保 analysis_dict 始终是一个字典
        if not isinstance(analysis_dict, dict):
             analysis_dict = DEFAULT_ANALYSIS_RESPONSE

        # 使用最终确定的分析思路构建 query_text
        query_text = f"{current_title}\n 分析思路：{analysis_dict.get('analysis', '无有效分析思路')}" # 添加回退文本
        print(f"Final analysis source: {analysis_source}")

        # --- Get Potential Labels ---
        try:
            potential_ic_trend_labels = get_potential_ic_trend_labels(query_text) or DEFAULT_EMPTY_LIST
            if not isinstance(potential_ic_trend_labels, list):
                print(f"Warning: get_potential_ic_trend_labels did not return a list. Got: {type(potential_ic_trend_labels)}")
                potential_ic_trend_labels = DEFAULT_EMPTY_LIST
        except Exception as e:
            print(f"Error getting potential IC trend labels: {e}")
            potential_ic_trend_labels = DEFAULT_EMPTY_LIST

        # --- Query Reports ---
        try:
            raw_report_response = search_and_query(query_text, index_type='header', top_k=20)
            print(raw_report_response)
            if not isinstance(raw_report_response, list):
                 print(f"Warning: search_and_query did not return a list. Got: {type(raw_report_response)}")
                 raw_report_response = DEFAULT_EMPTY_LIST
            
            # Filter and enrich reports
            valid_reports_for_relevance = []
            for item in raw_report_response:
                if not isinstance(item, dict): continue
                header_id = item.get('header_id')
                if not header_id: continue

                try:
                    file_node_id = query_file_node_by_header(header_id)
                    current_headers_content = query_content_under_header(header_id)
                except Exception as e:
                    print(f"Error querying node/content for header {header_id}: {e}")
                    continue # Skip this item if DB query fails

                # Add data if content exists
                if current_headers_content:
                    item['file_node_id'] = file_node_id # Can be None
                    item['current_headers_content'] = current_headers_content
                    # Add to list for relevance check only if content exists for comparison
                    if item.get('content'):
                        valid_reports_for_relevance.append(item)

            # Judge relevance in parallel
            final_report_response = []
            if valid_reports_for_relevance:
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_item = {}
                    for item in valid_reports_for_relevance:
                         # Ensure content is string for judge_title_relevance
                         content_str = item.get('content')
                         if isinstance(content_str, str):
                            future = executor.submit(judge_title_relevance, current_title, content_str)
                            future_to_item[future] = item
                         else:
                            print(f"Warning: Skipping relevance check for header {item.get('header_id')} due to invalid content type: {type(content_str)}")


                    for future in concurrent.futures.as_completed(future_to_item):
                        item = future_to_item[future]
                        try:
                            if future.result() is True:
                                final_report_response.append(item)
                        except Exception as exc:
                            print(f"Relevance check failed for header {item.get('header_id')}: {exc}")
            report_query_response = final_report_response
            print(f"Found {len(report_query_response)} relevant reports.")

        except Exception as e:
            print(f"Error querying or processing reports: {e}")
            report_query_response = DEFAULT_EMPTY_LIST

        # --- Query Policies ---
        try:
            raw_policy_data, raw_policy_ids = es_vector_query_policy_info(query_text)
            
            policy_ids_int: List[int] = []
            if isinstance(raw_policy_ids, list):
                for pid in raw_policy_ids:
                    if isinstance(pid, str) and pid.isdigit():
                        policy_ids_int.append(int(pid))
                    elif isinstance(pid, int):
                         policy_ids_int.append(pid)
            
            policy_details: List = []
            if policy_ids_int:
                policy_details = get_policy_details_by_ids(policy_ids_int) or DEFAULT_EMPTY_LIST
                if not isinstance(policy_details, list):
                     print(f"Warning: get_policy_details_by_ids did not return a list. Got: {type(policy_details)}")
                     policy_details = DEFAULT_EMPTY_LIST

            # Simplify and filter policies
            policies_for_relevance = []
            all_cics_label_raw = [] # Collect labels before relevance check
            for policy in policy_details:
                if not isinstance(policy, dict): continue
                
                simplified_policy = {
                    'id': policy.get('id'),
                    'policy_title': policy.get('policy_title'),
                    'policy_summary': policy.get('policy_summary'),
                    'industry': policy.get('industry'),
                    'policy_start_time': policy.get('policy_start_date'),
                    'policy_end_time': policy.get('policy_end_date'),
                    'org_name': policy.get('org_name')
                }
                # Add for relevance check only if title exists
                if simplified_policy.get('policy_title'):
                     policies_for_relevance.append(simplified_policy)
                
                # Collect industry label if present
                industry_label = simplified_policy.get('industry')
                if isinstance(industry_label, str) and industry_label:
                    all_cics_label_raw.append(industry_label)

            # Judge policy relevance in parallel
            relevant_policies = []
            if policies_for_relevance:
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_policy = {
                        executor.submit(judge_title_relevance, current_title, policy['policy_title']): policy
                        for policy in policies_for_relevance
                    }
                    for future in concurrent.futures.as_completed(future_to_policy):
                        policy = future_to_policy[future]
                        try:
                            if future.result() is True:
                                relevant_policies.append(policy)
                        except Exception as exc:
                            print(f"Policy relevance check failed for ID {policy.get('id')}: {exc}")
            simplified_policies = relevant_policies
            print(f"Found {len(simplified_policies)} relevant policies.")

            # --- Filter Industry Labels based on Relevant Policies ---
            relevant_policy_industries = set()
            for policy in simplified_policies:
                 industry = policy.get('industry')
                 if isinstance(industry, str) and industry:
                      relevant_policy_industries.add(industry)
            
            # Use only labels from relevant policies for further relevance check
            all_cics_label_filtered = list(relevant_policy_industries)
            
            # Judge industry label relevance
            final_relevant_labels = []
            if all_cics_label_filtered:
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_label = {
                        executor.submit(industry_indicator_relevance, [label], current_title): label
                        for label in all_cics_label_filtered
                    }
                    for future in concurrent.futures.as_completed(future_to_label):
                        label = future_to_label[future]
                        try:
                            if future.result() is True:
                                final_relevant_labels.append(label)
                        except Exception as exc:
                            print(f"Industry label relevance check failed for '{label}': {exc}")
            all_cics_label = final_relevant_labels # Final list of relevant labels
            print(f"Final relevant CICS labels: {all_cics_label}")

        except Exception as e:
            print(f"Error querying or processing policies/labels: {e}")
            simplified_policies = DEFAULT_EMPTY_LIST
            all_cics_label = DEFAULT_EMPTY_LIST


        # --- Query Industry Indicators (IC Trend/Current) ---
        if all_cics_label: # Only proceed if we have relevant labels
            try:
                cics_ids = get_cics_id_by_name(all_cics_label)
                if not isinstance(cics_ids, list):
                    print(f"Warning: get_cics_id_by_name did not return a list. Got: {type(cics_ids)}")
                    cics_ids = DEFAULT_EMPTY_LIST
                
                cics_ids_int = [cid for cid in cics_ids if isinstance(cid, int)] # Ensure IDs are integers

                if cics_ids_int:
                    # --- IC Trend Scores ---
                    try:
                        ic_trend_scores = query_ic_trend_score(cics_ids_int, year) or DEFAULT_EMPTY_LIST
                        if not isinstance(ic_trend_scores, list):
                            print(f"Warning: query_ic_trend_score did not return a list. Got: {type(ic_trend_scores)}")
                            ic_trend_scores = DEFAULT_EMPTY_LIST

                        if ic_trend_scores and isinstance(potential_ic_trend_labels, list) and potential_ic_trend_labels:
                             base_fields = ['id', 'date', 'cics_id', 'cics_name', 'updated_at']
                             keep_fields = set(base_fields) # Use set for faster lookup
                             for label in potential_ic_trend_labels:
                                 if isinstance(label, str):
                                      keep_fields.add(f'{label}_score')
                                      keep_fields.add(f'{label}_grade')

                             filtered_scores = []
                             for score_item in ic_trend_scores:
                                 if isinstance(score_item, dict):
                                      filtered_score = {k: v for k, v in score_item.items() if k in keep_fields}
                                      filtered_scores.append(filtered_score)
                             ic_trend_scores = filtered_scores

                        if ic_trend_scores:
                            analysis_results_ictrend = analyze_industry_trends(ic_trend_scores) or DEFAULT_EMPTY_LIST
                            if analysis_results_ictrend:
                                analysis_results_ictrend_v2 = get_analysis_summary(analysis_results_ictrend) or DEFAULT_EMPTY_LIST
                                if analysis_results_ictrend_v2:
                                     # Make sure analysis_results_ictrend_v2 is suitable for conclude_from_ic_trend_score
                                    industry_analysis_str, _ = conclude_from_ic_trend_score(analysis_results_ictrend_v2)
                                    industry_analysis = safe_json_loads(industry_analysis_str, default=DEFAULT_INDUSTRY_ANALYSIS)
                                    if not isinstance(industry_analysis, dict): # Ensure dict type
                                         industry_analysis = DEFAULT_INDUSTRY_ANALYSIS

                    except Exception as e:
                        print(f"Error processing IC Trend Scores: {e}")
                        analysis_results_ictrend_v2 = DEFAULT_EMPTY_LIST
                        industry_analysis = DEFAULT_INDUSTRY_ANALYSIS

                    # --- IC Current Rating ---
                    try:
                        ic_current_rating = query_ic_current_rating(cics_ids_int, year) or DEFAULT_EMPTY_LIST
                        if not isinstance(ic_current_rating, list):
                             print(f"Warning: query_ic_current_rating did not return a list. Got: {type(ic_current_rating)}")
                             ic_current_rating = DEFAULT_EMPTY_LIST

                        if ic_current_rating:
                            potential_cat_labels = ['profitability_cat', 'supply_demand'] # Example
                            # Ensure filter function handles None/errors
                            filtered_result_ic_current_rating = filter_ic_current_rating(ic_current_rating, potential_cat_labels) or DEFAULT_EMPTY_DICT
                            if not isinstance(filtered_result_ic_current_rating, dict):
                                print(f"Warning: filter_ic_current_rating did not return a dict. Got: {type(filtered_result_ic_current_rating)}")
                                filtered_result_ic_current_rating = DEFAULT_EMPTY_DICT

                            if filtered_result_ic_current_rating:
                                cat_indicators_str, _ = conclude_from_cat_analysis(filtered_result_ic_current_rating)
                                # Assuming conclude_from_cat_analysis returns a string summary
                                cat_indicators = cat_indicators_str if isinstance(cat_indicators_str, str) else DEFAULT_CAT_INDICATORS

                    except Exception as e:
                        print(f"Error processing IC Current Rating: {e}")
                        filtered_result_ic_current_rating = DEFAULT_EMPTY_DICT
                        cat_indicators = DEFAULT_CAT_INDICATORS
                else:
                     print("No valid CICS IDs found after filtering.")

            except Exception as e:
                print(f"Error querying CICS IDs or indicators: {e}")
                industry_analysis = DEFAULT_INDUSTRY_ANALYSIS
                cat_indicators = DEFAULT_CAT_INDICATORS
                analysis_results_ictrend_v2 = DEFAULT_EMPTY_LIST
                filtered_result_ic_current_rating = DEFAULT_EMPTY_DICT
        else:
             print("No relevant CICS labels found, skipping industry indicator query.")


        # --- Query ECO Indicators ---
        try:
            eco_indicators_raw, eco_ids, eco_indicators_sum_raw = es_vector_query_eco_indicators_v2(query_text, year)

            eco_indicators = eco_indicators_raw if isinstance(eco_indicators_raw, list) else DEFAULT_EMPTY_LIST
            eco_indicators_sum = eco_indicators_sum_raw if isinstance(eco_indicators_sum_raw, dict) else DEFAULT_EMPTY_DICT
            
            if eco_indicators:
                eco_indicators_analysis_stage_0 = analyze_eco_indicators(eco_indicators) or DEFAULT_EMPTY_LIST
                if eco_indicators_analysis_stage_0:
                    eco_indicators_report = generate_summary_report(eco_indicators_analysis_stage_0) or DEFAULT_EMPTY_LIST
                    if not isinstance(eco_indicators_report, list):
                         print(f"Warning: generate_summary_report did not return a list. Got: {type(eco_indicators_report)}")
                         eco_indicators_report = DEFAULT_EMPTY_LIST
        except Exception as e:
            print(f"Error querying or processing ECO indicators: {e}")
            eco_indicators = DEFAULT_EMPTY_LIST
            eco_indicators_sum = DEFAULT_EMPTY_DICT
            eco_indicators_report = DEFAULT_EMPTY_LIST

        # --- Final Return ---
        return (
            report_query_response,
            simplified_policies,
            industry_analysis,
            cat_indicators,
            analysis_dict,  # Return the analysis dict used
            eco_indicators,
            eco_indicators_sum,
            eco_indicators_report,
            analysis_results_ictrend_v2,
            filtered_result_ic_current_rating
        )

    except Exception as e:
        print(f"CRITICAL ERROR in query_relative_data_v2 for title '{current_title}': {str(e)}")
        # Return safe defaults matching the expected types
        return (
            DEFAULT_EMPTY_LIST, DEFAULT_EMPTY_LIST, DEFAULT_INDUSTRY_ANALYSIS,
            DEFAULT_CAT_INDICATORS, DEFAULT_ANALYSIS_RESPONSE, DEFAULT_EMPTY_LIST,
            DEFAULT_EMPTY_DICT, DEFAULT_EMPTY_LIST, DEFAULT_EMPTY_LIST, DEFAULT_EMPTY_DICT
        )

if __name__ == "__main__":
    year = 2024
    query_title = '2024年新能源汽车行业'
    instruction = '请着重分析上下游产业'
    report_query_response, simplified_policies, industry_analysis, cat_indicators, analysis_response,eco_indicators, eco_indicators_sum,eco_indicators_report,analysis_results_ictrend_v2,filtered_result_ic_current_rating = query_relative_data_v2(2023,"2023年新能源汽车",instruction)
    
    print("=== report_query_response ===")
    print(report_query_response)
    
    print("=== simplified_policies ===")
    print(simplified_policies)
    
    print("=== industry_analysis ===")
    print(industry_analysis)
    
    print("=== cat_indicators ===")
    print(cat_indicators)
    
    print("=== analysis_response ===")
    print(analysis_response)
    
    print("=== eco_indicators ===")
    print(eco_indicators)
    
    print("=== eco_indicators_sum ===")
    print(eco_indicators_sum)
    
    print("=== eco_indicators_report ===")
    print(eco_indicators_report)
    
    print("=== analysis_results_ictrend_v2 ===")
    print(analysis_results_ictrend_v2)
    
    print("=== filtered_result_ic_current_rating ===")
    print(filtered_result_ic_current_rating)



    
    
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
