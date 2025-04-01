import requests
import json

def test_build_overview():
    # API 地址
    url = "http://localhost:5009/build_overview_with_report"

    # 请求参数
    payload = {
        "title": "2025年童车行业市场分析",
        "purpose": "洞察市场趋势"
    }

    # 设置headers接收SSE (Server-Sent Events)
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream"
    }

    try:
        # 发送POST请求并设置stream=True
        response = requests.post(url, json=payload, headers=headers, stream=True)
        response.encoding = 'utf-8'  # 设置响应编码为utf-8
        
        # 检查响应状态
        if response.status_code != 200:
            print(f"错误: 状态码 {response.status_code}")
            print(response.text)
            return

        event_type = None  # 初始化事件类型变量
        
        # 处理流式响应
        for line in response.iter_lines(decode_unicode=True):
            if line:
                # 解析SSE格式
                if line.startswith("event:"):
                    event_type = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    data = line.split(":", 1)[1].strip()
                    try:
                        # 根据不同的事件类型处理数据
                        if event_type == "think":
                            print("\n思考过程:", data.encode('utf-8').decode('utf-8'))
                        elif event_type == "title":
                            print("\n扩展标题:", data.encode('utf-8').decode('utf-8'))
                        elif event_type == "keyword":
                            print("\n关键词:", data.encode('utf-8').decode('utf-8'))
                        elif event_type == "result":
                            result = json.loads(data)
                            print("\n最终结果:")
                            print(json.dumps(result, ensure_ascii=False, indent=2))
                    except json.JSONDecodeError as e:
                        print(f"JSON解析错误: {e}")
                        print(f"原始数据: {data}")
                    except UnicodeError as e:
                        print(f"编码错误: {e}")
                        print(f"原始数据: {data}")

    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")



def test_query_filenode_get_report_info():
    """
    测试批量查询多个file_node_id的文件节点及其关联信息API
    """
    url = "http://localhost:5009/query_filenode_get_report_info"
    
    # 准备请求数据
    payload = {
        "file_node_ids": [
            3587744,
            3480384,
            3762280,
            3744296,
            3950186,
            3296593,
            4124275,
            3489687,
            3761561,
            3564347
        ]
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        # 发送POST请求
        response = requests.post(url, json=payload, headers=headers)
        
        # 检查响应状态
        if response.status_code != 200:
            print(f"错误: 状态码 {response.status_code}")
            print(response.text)
            return
        
        # 解析响应数据
        result = response.json()
        
        # 打印结果
        print("查询结果:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")
    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {e}")
        print(f"原始响应: {response.text}")





if __name__ == "__main__":
    # test_build_overview()
    # test_query_filenode_get_report_info()
    print(1)