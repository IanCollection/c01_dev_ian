from search import HybridSearch
import json

def print_result(hit):
    score = hit['_score']
    source = hit['_source']
    
    print(f"\n得分: {score:.2f}")
    print("文档内容:")
    important_fields = ['title', 'policy_summary']
    for field in important_fields:
        if field in source:
            print(f"{field}: {source[field]}")
    
    print("其他字段:")
    for key, value in source.items():
        if key not in important_fields and not key.endswith('_vector'):
            print(f"{key}: {value}")

def test_search():
    searcher = HybridSearch('config.yaml')
    
    # 测试场景
    test_cases = [
         {
            "table_name": "eco_info_deloitte",
            "name": "混合搜索测试",
            "query": "新能源汽车保有量 补贴 锂电池 全国",
            "vector_field": "name_cn",
            "size": 3
        },
        {
            "table_name": "sc_policy_detail",
            "name": "混合搜索测试",
            "query": "结膜松弛",
            "vector_field": "policy_summary",
            "size": 3
        },
        # {
        #     "table_name": "sc_policy_detail",
        #     "name": "纯文本搜索测试",
        #     "query": "结膜松弛",
        #     "vector_field": None,
        #     "size": 3
        # },
        # {
        #     "table_name": "sc_policy_detail",
        #     "name": "标题向量搜索测试",
        #     "query": "结膜松弛",
        #     "vector_field": "title",
        #     "size": 3
        # }
    ]
    
    for test_case in test_cases:
        print(f"\n=== {test_case['name']} ===")
        print(f"查询: {test_case['query']}")
        print(f"向量字段: {test_case['vector_field']}")
        
        try:
            results = searcher.search(
                table_name=test_case['table_name'],
                query=test_case['query'],
                vector_field=test_case['vector_field'],
                size=test_case['size']
            )
            
            print(f"\n找到 {len(results)} 条结果:")
            for hit in results:
                print_result(hit)
                
        except Exception as e:
            print(f"搜索出错: {str(e)}")

if __name__ == "__main__":
    test_search() 