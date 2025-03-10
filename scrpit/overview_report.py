from Agent.Overview_agent import title_augement, generate_final_toc, extract_h_single_report
from database.faiss_query import search
from Agent.Overview_agent import extract_headers_from_text_qwen
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Tuple

# 假设这些客户端已经在其他地方初始化
from Agent.client_manager import qwen_client, silicon_client



def build_overview_with_report(input_title):
    result_json,reasoning_content = title_augement(input_title)

    new_title = result_json["expanded_title"]
    keywords = result_json["keywords"]
    print(new_title)
    #根据new_title来查询filename_faiss,返回前10相似的研报，然后查询neo4j 的file节点，对所有研报进行总结。获得一个根据历史研报总结出来的标题
    relative_reports = search(new_title,index_type='filename',top_k=10)
    return new_title,relative_reports

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
                # print(summary)
                all_summaries.append(summary)
                total_cost += report_cost
                print(f"研报 {report_index + 1}/{len(report_headers_list)} 处理完成，成本: {report_cost:.6f}元")
            except Exception as e:
                print(f"研报 {report_index + 1}/{len(report_headers_list)} 处理失败: {e}")

    print(f"第一阶段处理完成，共生成 {len(all_summaries)} 个摘要，总成本: {total_cost:.6f}元")

    # 第二阶段：生成最终目录
    print("开始第二阶段处理，生成综合目录...")
    print("开始生成最终目录...", datetime.now())
    final_toc, final_cost = generate_final_toc(all_summaries)
    total_cost += final_cost
    print("最终目录生成完成", datetime.now())

    # # 将结果保存到文件
    # with open("generated_comprehensive_toc.md", "w", encoding="utf-8") as f:
    #     f.write(final_toc)

    # print(f"综合目录已生成并保存到 generated_comprehensive_toc.md，总成本: {total_cost:.6f}元")

    return final_toc, total_cost

# if __name__ == "__main__":
#     input_title = "AI芯片"
#     title,reports_node = build_overview_with_report(input_title)
#     print("--------------------------------")
#     print(title)
#     print("--------------------------------")
#     print(reports_node)