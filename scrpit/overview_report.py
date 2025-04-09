from Agent.Overview_agent import title_augement, generate_final_toc, extract_h_single_report, generate_final_toc_v2, \
    title_augement_without_cot, generate_final_toc_v2_stream, generate_final_toc_v2_stream_no_title, \
    extract_h_single_report_v2
from database.faiss_query import search
from Agent.Overview_agent import extract_headers_from_text_qwen
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Tuple, Generator

# 假设这些客户端已经在其他地方初始化
from Agent.client_manager import qwen_client, silicon_client



def build_overview_with_report(input_title,purpose = None):
    # result_json,reasoning_content,time = title_augement(input_title,purpose)
    result_json,reasoning_content,time = title_augement_without_cot(input_title,purpose)#这里不用cot了

    new_title = result_json["expanded_title"]
    keywords = result_json["keywords"]
    print(keywords)
    # print(new_title)
    # 提取所有关键词并合并
    all_keywords = []
    all_keywords.extend(keywords.get('core_keywords', []))
    all_keywords.extend(keywords.get('domain_keywords', []))
    all_keywords.extend(keywords.get('focus_keywords', []))
    
    # 去重并转换为字符串，用逗号分隔
    unique_keywords = list(set(all_keywords))
    keywords_str = ', '.join(unique_keywords)
    
    # 将输入标题和关键词拼接起来
    combined_title = f"{input_title} - {keywords_str}"
    #根据new_title来查询filename_faiss,返回前10相似的研报，然后查询neo4j 的file节点，对所有研报进行总结。获得一个根据历史研报总结出来的标题
    relative_reports = search(input_title,index_type='filename',top_k=10)
    return new_title,relative_reports,keywords,time

    # file_node_ids = [report['file_node_id'] for report in relative_reports]
    
    # return relative_reports
    #根据relative_reports来查询neo4j 的file节点，对所有研报进行总结。获得一个根据历史研报总结出来的标题

def extract_all_headers(file_nodes):
    from concurrent.futures import ThreadPoolExecutor
    
    headers_dict_list = []
    total_cost = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for node in file_nodes:
            future = executor.submit(lambda n: {
                n['file_node_id']: extract_headers_from_text_qwen(n['headers_content'])[0],
                'cost': extract_headers_from_text_qwen(n['headers_content'])[1]
            }, node)
            futures.append(future)
            
        results = [future.result() for future in futures]
        headers_dict_list = results
        total_cost = sum(result['cost'] for result in results)
    
    return headers_dict_list, total_cost


def extract_all_headers_and_conclude(file_nodes):
    from concurrent.futures import ThreadPoolExecutor

    headers_dict_list = []
    total_cost = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for node in file_nodes:
            future = executor.submit(lambda n: {
                n['file_node_id']: extract_headers_from_text_qwen(n['headers_content'])[0],
                'cost': extract_headers_from_text_qwen(n['headers_content'])[1]
            }, node)
            futures.append(future)

        results = [future.result() for future in futures]
        headers_dict_list = results
        total_cost = sum(result['cost'] for result in results)

    return headers_dict_list, total_cost

def generate_comprehensive_toc(report_headers_list: List[Dict[str, Any]],
                               max_workers: int = 5) -> Tuple[str, float]:
    """
    主函数：基于多个研报目录生成一个综合性的新目录，并行处理第一阶段

    Args:
        report_headers_list: 包含多个研报信息的列表
        max_workers: 并行处理的最大工作线程数

    Returns:
        Tuple[str, float]: 生成的综合目录和总处理成本
    """
    all_summaries = []
    total_cost = 0

    # 第一阶段：并行处理每个研报
    print(f"开始第一阶段处理，共 {len(report_headers_list)} 个研报...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有研报处理任务
        future_to_report = {executor.submit(extract_h_single_report, report): i
                            for i, report in enumerate(report_headers_list)}

        # 收集结果
        for future in concurrent.futures.as_completed(future_to_report):
            report_index = future_to_report[future]
            try:
                summary, report_cost = future.result()
                # 构建包含policy_id, s3_url, policy_summary的字典
                report_data = {
                    'policy_id': report_headers_list[report_index].get('file_node_id', ''),
                    's3_url': report_headers_list[report_index].get('s3_url', ''),
                    'policy_summary': summary
                }
                all_summaries.append(report_data)
                total_cost += report_cost
                print(f"研报 {report_index + 1}/{len(report_headers_list)} 处理完成，成本: {report_cost:.6f}元")
            except Exception as e:
                print(f"研报 {report_index + 1}/{len(report_headers_list)} 处理失败: {e}")

    print(f"第一阶段处理完成，共生成 {len(all_summaries)} 个摘要，总成本: {total_cost:.6f}元")

    # 第二阶段：生成最终目录
    print("开始生成最终目录...", datetime.now())
    # 从all_summaries中提取policy_summary用于生成最终目录
    summary_texts = [item['policy_summary'] for item in all_summaries if 'policy_summary' in item]
    final_toc, final_cost = generate_final_toc(summary_texts)
    total_cost += final_cost
    print("最终目录生成完成", datetime.now())

    return final_toc, total_cost
def generate_comprehensive_toc_v2(title, report_headers_list,keywords: List[Dict[str, Any]],
                               max_workers: int = 5) -> Tuple[str, float]:
    """
    主函数：基于多个研报目录生成一个综合性的新目录，并行处理第一阶段

    Args:
        report_headers_list: 包含多个研报信息的列表
        max_workers: 并行处理的最大工作线程数

    Returns:
        Tuple[str, float]: 生成的综合目录和总处理成本
    """
    all_summaries = []
    total_cost = 0

    # 第一阶段：并行处理每个研报
    print(f"开始第一阶段处理，共 {len(report_headers_list)} 个研报...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有研报处理任务
        future_to_report = {executor.submit(extract_h_single_report, report): i
                            for i, report in enumerate(report_headers_list)}

        # 收集结果
        for future in concurrent.futures.as_completed(future_to_report):
            report_index = future_to_report[future]
            try:
                summary, report_cost = future.result()
                # 构建包含policy_id, s3_url, policy_summary的字典
                report_data = {
                    'policy_id': report_headers_list[report_index].get('file_node_id', ''),
                    's3_url': report_headers_list[report_index].get('s3_url', ''),
                    'policy_summary': summary,
                    'report_name': report_headers_list[report_index].get('name', '')
                }
                all_summaries.append(report_data)
                total_cost += report_cost
                print(f"研报 {report_index + 1}/{len(report_headers_list)} 处理完成，成本: {report_cost:.6f}元")
            except Exception as e:
                print(f"研报 {report_index + 1}/{len(report_headers_list)} 处理失败: {e}")

    print(f"第一阶段处理完成，共生成 {len(all_summaries)} 个摘要，总成本: {total_cost:.6f}元")

    # 第二阶段：生成最终目录
    print("开始第二阶段处理，生成综合目录...")
    print("开始生成最终目录...", datetime.now())
    # 从all_summaries中提取policy_summary用于生成最终目录
    # summary_texts = [item['policy_summary'] for item in all_summaries if 'policy_summary' in item]
    final_toc, final_cost = generate_final_toc_v2(all_summaries,title,keywords['core_keywords'])
    total_cost += final_cost
    print("最终目录生成完成", datetime.now())

    return final_toc, all_summaries, total_cost
def generate_comprehensive_toc_v2_stream(title, report_headers_list, keywords: List[Dict[str, Any]], max_workers: int = 5) -> Generator[Tuple[str, float], None, None]:
    """
    主函数：基于多个研报目录生成一个综合性的新目录，并行处理第一阶段

    Args:
        report_headers_list: 包含多个研报信息的列表
        max_workers: 并行处理的最大工作线程数

    Returns:
        Generator: 生成器，逐块返回目录内容和累计成本
    """
    all_summaries = []
    total_cost = 0

    # 第一阶段：并行处理每个研报
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_report = {executor.submit(extract_h_single_report, report): i
                            for i, report in enumerate(report_headers_list)}

        # 收集结果
        for future in concurrent.futures.as_completed(future_to_report):
            report_index = future_to_report[future]
            try:
                summary, report_cost = future.result()
                report_data = {
                    'policy_id': report_headers_list[report_index].get('file_node_id', ''),
                    's3_url': report_headers_list[report_index].get('s3_url', ''),
                    'policy_summary': summary,
                    'report_name': report_headers_list[report_index].get('name', '')
                }
                all_summaries.append(report_data)
                # total_cost += report_cost
            except Exception as e:
                pass
    # 收集所有生成的内容
    # full_content = ""
    # for chunk in generate_final_toc_v2_stream(all_summaries, title, keywords['core_keywords']):
    #     if isinstance(chunk, str):
    #         # 过滤掉以单个#开头的标题行
    #         lines = chunk.split('\n')
    #         filtered_lines = [line for line in lines if not line.strip().startswith('# ')]
    #         if filtered_lines:
    #             filtered_chunk = '\n'.join(filtered_lines)
    #             full_content += filtered_chunk
    #             # 对于生成过程中的片段，使用 toc_progress 事件
    #             yield {'event': 'toc_progress', 'data': filtered_chunk}
    #
    # # 生成完成后，发送完整的目录内容，使用 final_toc 事件
    # if full_content:
    #     # 再次过滤一遍完整内容，以防万一
    #     lines = full_content.split('\n')
    #     filtered_lines = [line for line in lines if not line.strip().startswith('# ')]
    #     filtered_full_content = '\n'.join(filtered_lines)
    #     yield {'event': 'final_toc', 'data': filtered_full_content}
    full_content = ""
    for chunk in generate_final_toc_v2_stream(all_summaries, title, keywords['core_keywords']):
        if isinstance(chunk, str):
            full_content += chunk
            # 对于生成过程中的片段，使用 toc_progress 事件
            yield {'event': 'toc_progress', 'data': chunk}

    # 生成完成后，发送完整的目录内容，使用 final_toc 事件
    if full_content:
        yield {'event': 'final_toc', 'data': full_content}

    # # 流式返回最终目录
    # # for chunk, chunk_cost in generate_final_toc_v2_stream(all_summaries, title, keywords['core_keywords']):
    # #     total_cost += chunk_cost
    # #     yield chunk, total_cost
    # for chunk in generate_final_toc_v2_stream(all_summaries, title, keywords['core_keywords']):
    #     yield chunk


def generate_comprehensive_toc_v2_stream_no_title(title, report_headers_list, keywords: List[Dict[str, Any]], max_workers: int = 5) -> Generator[Tuple[str, float], None, None]:
    """
    主函数：基于多个研报目录生成一个综合性的新目录，并行处理第一阶段

    Args:
        report_headers_list: 包含多个研报信息的列表
        max_workers: 并行处理的最大工作线程数

    Returns:
        Generator: 生成器，逐块返回目录内容和累计成本
    """
    all_summaries = []
    total_cost = 0

    # 第一阶段：并行处理每个研报
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_report = {executor.submit(extract_h_single_report_v2, report, title): i
                            for i, report in enumerate(report_headers_list)}

        # 收集结果
        for future in concurrent.futures.as_completed(future_to_report):
            report_index = future_to_report[future]
            try:
                summary, report_cost = future.result()
                report_data = {
                    'policy_id': report_headers_list[report_index].get('file_node_id', ''),
                    's3_url': report_headers_list[report_index].get('s3_url', ''),
                    'policy_summary': summary,
                    'report_name': report_headers_list[report_index].get('name', '')
                }
                all_summaries.append(report_data)
                # total_cost += report_cost
            except Exception as e:
                pass

    # # 第一阶段：并行处理每个研报
    # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     future_to_report = {executor.submit(extract_h_single_report, report): i
    #                         for i, report in enumerate(report_headers_list)}
    #
    #     # 收集结果
    #     for future in concurrent.futures.as_completed(future_to_report):
    #         report_index = future_to_report[future]
    #         try:
    #             summary, report_cost = future.result()
    #             report_data = {
    #                 'policy_id': report_headers_list[report_index].get('file_node_id', ''),
    #                 's3_url': report_headers_list[report_index].get('s3_url', ''),
    #                 'policy_summary': summary,
    #                 'report_name': report_headers_list[report_index].get('name', '')
    #             }
    #             all_summaries.append(report_data)
    #             # total_cost += report_cost
    #         except Exception as e:
    #             pass
    # 收集所有生成的内容
    # 改为直接透传generate_final_toc_v2_stream的输出
    # 初始化完整内容容器
    full_content = []
    
    # 流式生成目录内容
    for chunk in generate_final_toc_v2_stream_no_title(all_summaries, title, keywords['core_keywords']):
        if isinstance(chunk, str):
            # 过滤掉以#开头的行
            # lines = chunk.split('\n')
            # filtered_lines = [line for line in lines if not line.strip().startswith('# ')]
            
            # if filtered_lines:
            #     # 自然段落拼接（保留原有换行结构）
            #     filtered_chunk = '\n'.join(filtered_lines)
                # 将内容添加到完整容器中
            full_content.append(chunk)
            # 实时流式输出进度
            yield {'event': 'toc_progress', 'data': chunk}

    # 生成最终完整目录
    if full_content:
        # 合并所有内容
        merged_content = ''.join(full_content)
        # 发送最终完整目录事件
        yield {'event': 'final_toc', 'data': merged_content}


if __name__ == "__main__":
    input_title = "AI芯片"
    title,reports_node,keywords = build_overview_with_report(input_title)
    print("--------------------------------")
    print(title)
    print("--------------------------------")
    print(reports_node)
    print('--------------------------------')
    print(keywords)