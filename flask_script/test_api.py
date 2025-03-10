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
                            print("\033[90m" + content + "\033[0m", end='', flush=True)  # 灰色
                        elif event_type == 'title':
                            print("\033[30m" + content + "\033[0m", end='', flush=True)  # 黑色
                        elif event_type == 'keyword':
                            print("\033[30m" + content + "\033[0m", end='', flush=True)  # 黑色
                        elif event_type == 'result':
                            print("\033[30m" + content + "\033[0m", end='', flush=True)  # 黑色
                            try:
                                final_json = json.loads(content)
                            except json.JSONDecodeError:
                                pass

        if final_json:
            print("\n最终结果:")
            # print(json.dumps(final_json, ensure_ascii=False, indent=2))
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