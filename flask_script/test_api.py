import requests
import json
import time


def stage_0_stream_get_keywords(url="http://localhost:5001/augment_title", title="人工智能在金融领域的应用"):
    """
    测试流式输出API

    Args:
        url (str): API的URL地址
        title (str): 要测试的研报标题
    """

    # print(f"开始测试流式输出API: {url}")
    # print(f"测试标题: {title}")
    # print("-" * 50)

    headers = {"Content-Type": "application/json"}
    data = {"title": title}

    try:
        # 记录开始时间
        start_time = time.time()

        # 发送POST请求，设置stream=True以启用流式接收
        response = requests.post(url, headers=headers, json=data, stream=True)

        # 检查响应状态
        if response.status_code != 200:
            # print(f"错误: 服务器返回状态码 {response.status_code}")
            print(response.text)
            return

        # # 处理流式响应
        # print("接收流式数据:")
        # print("-" * 50)

        # 用于累积完整输出的变量
        full_output = ""
        chunk_count = 0
        
        # 用于存储最终结果的变量
        keywords = []
        new_title = ""
        final_json = {}

        # 迭代响应的每一行
        for line in response.iter_lines():
            if line:
                # 解码行内容
                line_text = line.decode('utf-8')

                # 检查是否是SSE格式的数据行
                if line_text.startswith('data: '):
                    # 提取实际内容
                    content = line_text[6:]  # 移除 "data: " 前缀
                    full_output += content
                    chunk_count += 1

                    # 打印内容
                    print(content, end='', flush=True)
                    
                    # 尝试解析JSON（最后一个块通常包含完整结果）
                    try:
                        json_data = json.loads(content)
                        if 'keywords' in json_data:
                            keywords = json_data['keywords']
                        if 'new_title' in json_data:
                            new_title = json_data['new_title']
                        final_json = json_data
                    except json.JSONDecodeError:
                        # 不是有效的JSON，可能是部分输出
                        pass

        print(json.dumps(final_json, ensure_ascii=False, indent=2))

        return json.dumps(final_json, ensure_ascii=False, indent=2)

    except requests.exceptions.ConnectionError:
        print("错误: 无法连接到服务器。请确保服务器正在运行。")
    except Exception as e:
        print(f"错误: {str(e)}")


if __name__ == "__main__":
    # 可以在这里修改URL和测试标题
    thins = stage_0_stream_get_keywords()
    print(thins)
    # 如果要测试远程服务器，可以使用:
    # test_stream_api("http://服务器IP:5000/augment_title", "远程测试标题")