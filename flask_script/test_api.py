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
    headers = {"Content-Type": "application/json"}
    data = {"title": title}

    try:
        response = requests.post(url, headers=headers, json=data, stream=True)

        if response.status_code != 200:
            print(response.text)
            return

        full_output = ""
        chunk_count = 0
        final_json = {}

        # 迭代响应的每一行
        for line in response.iter_lines():
            if line:
                line_text = line.decode('utf-8')
                
                # 解析事件类型和数据
                if line_text.startswith('event: '):
                    event_type = line_text[7:]  # 获取事件类型
                    continue
                
                if line_text.startswith('data: '):
                    content = line_text[6:]  # 移除 "data: " 前缀
                    full_output += content
                    chunk_count += 1

                    # 根据不同事件类型打印不同颜色的输出
                    if 'event_type' in locals():
                        if event_type == 'think':
                            print(content, end='', flush=True)
                        # elif event_type == 'title':
                        #     print(content, end='', flush=True)
                        # elif event_type == 'keyword':
                        #     print(content, end='', flush=True)
                        elif event_type == 'result':
                            # print(content, end='', flush=True)
                            try:
                                final_json = json.loads(content)
                            except json.JSONDecodeError:
                                pass

        if final_json:
            # 输出扩展标题
            # print("\n扩展标题:", final_json.get("expanded_title", ""))
            
            # 输出关键词
            print("\n关键词:")
            keywords = final_json.get("keywords", {})
            print(keywords)
            # for key, values in keywords.items():
            #     print(f"- {key}: {', '.join(values)}")
            #
            # # 输出政策列表
            # policy_list = final_json.get("policy_list", [])
            # if policy_list:
            #     print("\n相关政策:")
            #     for i, policy in enumerate(policy_list, 1):
            #         print(f"{i}. {policy.get('title', '')}")
            
            # # 输出相关报告ID
            # report_ids = final_json.get("report_ids_list", [])
            # if report_ids:
            #     print("\n相关报告ID:")
            #     for i, report_id in enumerate(report_ids, 1):
            #         print(f"{i}. {report_id}")
            
            return final_json

    except requests.exceptions.ConnectionError:
        print("错误: 无法连接到服务器。请确保服务器正在运行。")
    except Exception as e:
        print(f"错误: {str(e)}")


if __name__ == "__main__":
    # 可以在这里修改URL和测试标题
    # result = stage_0_stream_get_keywords()
    # 如果要测试远程服务器，可以使用:
    # test_stream_api("http://服务器IP:5000/augment_title", "远程测试标题")
    import requests
    import json

    # API 地址
    url = "http://localhost:5001/search_policy"

    # 请求参数
    payload = {
        "title": "新能源汽车产业发展趋势分析",
        "size": 10
    }

    # 发送 POST 请求
    response = requests.post(url, json=payload)

    # 打印响应结果
    print("状态码:", response.status_code)
    print("响应内容:")
    print(json.dumps(response.json(), ensure_ascii=False, indent=2))