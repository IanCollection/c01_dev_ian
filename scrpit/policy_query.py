import concurrent

import requests
import json
from Agent.policy_agent import policy_summary_agent

def search_es(keyword):
    url = 'http://es.cjpolicy.com/cj-report-es_production/_search'
    auth = ('cjpolicy', 'mf5eoLnp2gk47Hxh')
    headers = {'Content-Type': 'application/json'}
    
    query = {
        "query": {
            "bool": {
                "minimum_should_match": 1,
                "should": [
                    {
                        "match": {
                            "title": keyword
                        }
                    },
                    {
                        "match": {
                            "content": keyword
                        }
                    }
                ]
            }
        }
    }

    response = requests.post(
        url,
        auth=auth,
        headers=headers,
        json=query
    )
    
    return response.json()

def search_es_policy_v2(report_title, augement_json=False,size = 10):
    """
    基于研报标题和增强JSON内容搜索政策文档
    
    Args:
        report_title (str): 研报标题
        augement_json (dict/bool): 语义增强后的JSON数据，包含各类关键词
        size (int): 返回结果的最大数量，默认为10
    
    Returns:
        dict: ES搜索结果，包含匹配的政策文档
    """
    url = 'http://es.cjpolicy.com/cj-report-es_production/_search'
    auth = ('cjpolicy', 'mf5eoLnp2gk47Hxh')
    headers = {'Content-Type': 'application/json'}
    
    # 构建基础查询
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            "title": {
                                "query": report_title,
                                "boost": 4  # 标题匹配给予最高权重
                            }
                        }
                    },
                    {
                        "match": {
                            "content": {
                                "query": report_title,
                                "boost": 3  # 内容匹配给予较高权重
                            }
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        },
        "size": size
    }

    # 如果有传入增强JSON，添加关键词到查询中
    if isinstance(augement_json, dict):
        # 添加扩展标题作为查询条件
        if "expanded_title" in augement_json:
            query["query"]["bool"]["should"].append({
                "match": {
                    "content": {
                        "query": augement_json["expanded_title"],
                        "boost": 2.5  # 扩展标题给予较高权重
                    }
                }
            })
        
        # 添加搜索关键词
        if "search_keywords" in augement_json:
            for keyword in augement_json["search_keywords"]:
                query["query"]["bool"]["should"].append({
                    "match": {
                        "content": {
                            "query": keyword,
                            "boost": 2  # 搜索关键词给予中等权重
                        }
                    }
                })
        
        # 添加政策术语，这些是最相关的政策文件名称
        if "policy_terms" in augement_json:
            for term in augement_json["policy_terms"]:
                query["query"]["bool"]["should"].append({
                    "match": {
                        "title": {
                            "query": term,
                            "boost": 3  # 政策术语在标题中匹配给予高权重
                        }
                    }
                })
                query["query"]["bool"]["should"].append({
                    "match": {
                        "content": {
                            "query": term,
                            "boost": 2  # 政策术语在内容中匹配给予中等权重
                        }
                    }
                })
        
        # 添加行业术语和技术术语
        for field in ["industry_terms", "technical_terms"]:
            if field in augement_json:
                for term in augement_json[field]:
                    query["query"]["bool"]["should"].append({
                        "match": {
                            "content": {
                                "query": term,
                                "boost": 1.5  # 行业和技术术语给予较低权重
                            }
                        }
                    }
                )
    
    # 发送请求
    response = requests.post(
        url,
        auth=auth,
        headers=headers,
        json=query
    )
    
    # 解析响应结果
    result = response.json()
    
    # 提取每个研报的id、title和content
    formatted_results = []
    if 'hits' in result and 'hits' in result['hits']:
        for hit in result['hits']['hits']:
            formatted_results.append({
                'id': hit['_id'],
                'title': hit['_source'].get('title', ''),
                'content': hit['_source'].get('content', '')
            })
    # 如果有结果，使用policy_summary_agent进行批量处理
    if formatted_results:
        # 准备批量处理的内容
        contents = [item['content'] for item in formatted_results]
        
        # 使用并行处理加速多个政策的总结生成
        summaries = []
        total_cost = 0  # 初始化总成本
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有任务
            future_to_content = {executor.submit(policy_summary_agent, content): i 
                                for i, content in enumerate(contents)}
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_content):
                index = future_to_content[future]
                try:
                    summary, cost = future.result()  # 现在返回摘要和成本
                    summaries.append((index, summary))
                    total_cost += cost  # 累加成本
                except Exception as e:
                    print(f"处理第{index}个政策时出错: {e}")
                    summaries.append((index, "无法生成政策概括。"))
        # 按原始顺序排序结果
        summaries.sort(key=lambda x: x[0])
        
        # 更新结果
        for i, (_, summary) in enumerate(summaries):
            try:
                # 尝试解析JSON格式的摘要
                if isinstance(summary, str):
                    import json
                    summary_json = json.loads(summary)
                    # 直接提取summary字段的内容，避免嵌套
                    if "summary" in summary_json:
                        formatted_results[i]['summary'] = summary_json["summary"]
                    else:
                        formatted_results[i]['summary'] = summary_json
                else:
                    # 如果已经是字典格式，检查是否有summary字段
                    if isinstance(summary, dict) and "summary" in summary:
                        formatted_results[i]['summary'] = summary["summary"]
                    else:
                        formatted_results[i]['summary'] = summary
            except Exception as e:
                print(f"解析第{i}个政策摘要时出错: {e}")
                formatted_results[i]['summary'] = "摘要格式错误"

        # 添加总成本到返回结果中
        # 移除content字段以减少返回数据量
        for item in formatted_results:
            if 'content' in item:
                del item['content']
        return formatted_results, total_cost
    
    # 如果没有结果，返回0成本，同样不返回content
    return formatted_results, 0






if __name__ == "__main__":
    augement_json = {'expanded_title': '全球及中国AI芯片市场分析：技术趋势、竞争格局与政策驱动下的发展前景', 'search_keywords': ['AI芯片', '人工智能芯片', '芯片市场', '半导体产业', 'AI硬件', '算力芯片', 'GPU', 'ASIC', 'FPGA', '市场分析', '技术创新', '政策支持'], 'policy_terms': ['国家集成电路产业发展推进纲要', '十四五规划', '人工智能发展规划', '芯片国产化政策', '出口管制条例', '科技自主创新政策', '美国芯片法案'], 'industry_terms': ['半导体行业', '集成电路设计', 'EDA工具', '晶圆制造', '封装测试', '供应链安全', '高端制造', '智能制造'], 'technical_terms': ['深度学习加速器', '神经网络处理器', '异构计算', '7nm工艺', '5nm工艺', '边缘计算芯片', '云端AI芯片', 'TPU', 'NPU', 'HBM内存', '低功耗设计']}
    print(search_es_policy_v2("2024年AI市场",augement_json,10))