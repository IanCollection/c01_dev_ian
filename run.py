from Agent.Overview_agent import title_augement,extract_headers_from_text_qwen
from database.faiss_query import search, search_and_query
from database.neo4j_query import query_file_all_headers, query_file_batch_nodes,query_file_batch_nodes_return_node_with_all_headers
import time

from scrpit.overview_report import extract_all_headers, generate_comprehensive_toc


###1. 生成研报目录（历史研报）
###2. 生成研报目录（关注点）
    ###根据研报目录打上标签。 核心关注点（一级和二级）
###3. 整合研报目录
###4. 微调研报目录（根据政策点和指标）
###5. 模块眼研报目录。（一个一级标题来作为一个模块） 来进行研报撰写

def main():
    ###1. 生成研报目录（历史研报）
    ##语义增强

    input_title = "AI芯片"
    start_time = time.time()
    result_json,reasoning_content = title_augement(input_title)
    new_title = result_json["expanded_title"]
    keywords = result_json["keywords"]
    print(new_title)
    print(f"语义增强耗时: {time.time() - start_time:.2f}秒")


    # new_title = '人工智能芯片技术发展、市场趋势及行业应用研究'
    ##语义增强后 对历史研报的标题进行检索
    #根据new_title来查询filename_faiss,返回前10相似的研报，然后查询neo4j 的file节点，对所有研报进行总结。获得一个根据历史研报总结出来的标题
    start_time = time.time()
    relative_ids = search(new_title,index_type='filename',top_k=10)
    print(f"FAISS检索耗时: {time.time() - start_time:.2f}秒")
    # relative_reports = search_and_query(new_title,index_type='filename',top_k=10)
    
    # start_time = time.time()
    # relative_reports = query_file_batch_nodes(relative_ids)
    # print(f"查询相关报告耗时: {time.time() - start_time:.2f}秒")

    start_time = time.time()
    file_node_with_allheaders=query_file_batch_nodes_return_node_with_all_headers(relative_ids)
    # print(len(file_node_with_allheaders))
    print(f"查询相关报告标题耗时: {time.time() - start_time:.2f}秒")
    print(file_node_with_allheaders)


    comprehensive_toc, total_cost = generate_comprehensive_toc(file_node_with_allheaders)
    print(comprehensive_toc)
    print(total_cost)

    # # batch 形式调用v3 生成所有目录
    # start_time = time.time()
    # ids_headers,total_cost = extract_all_headers(file_node_with_allheaders)
    # print(f"提取所有目录耗时: {time.time() - start_time:.2f}秒")
    # print(ids_headers)
    # print(total_cost)

    ###总结研报目录
    # overvirw_from_reports =




    # all_report_ids = []
    # s3_urls = []
    # ##根据relative_reports来查询neo4j 的file节点，对所有研报进行总结。获得一个根据历史研报总结出来的标题
    # start_time = time.time()
    # for report in relative_reports:
    #     all_report_ids.append(report['file_node_id'])
    #     s3_urls.append(report['s3_url'])
    # print(f"提取报告ID和URL耗时: {time.time() - start_time:.2f}秒")

    # start_time = time.time()
    # all_report_headers = []
    # for report_id in all_report_ids:
    #     headers_content = query_file_all_headers(report_id)
    #     all_report_headers.append(headers_content)
    # print(f"查询所有报告标题耗时: {time.time() - start_time:.2f}秒")

    # print(all_report_headers)


if __name__ == "__main__":
    main()