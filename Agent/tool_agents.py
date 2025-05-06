import os
import sys
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)


from Agent.client_manager import qwen_client, silicon_client
import json
client = silicon_client

def json_format_agent(original_text):
    """
    将输入的文本格式化为JSON格式
    
    Args:
        original_text (str): 需要格式化的原始文本
    
    Returns:
        dict: 格式化后的JSON对象
    """
    # 预处理输入文本，移除可能的代码块标记和多余的换行符
    cleaned_text = original_text.strip()
    if cleaned_text.startswith("```json"):
        cleaned_text = cleaned_text[7:]
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-3]
    cleaned_text = cleaned_text.strip()
    
    prompt = f"""
    请将以下文本内容转换为结构化的JSON格式：
    
    {cleaned_text}
    
    请直接输出有效的JSON对象，不要包含任何代码块标记（如```json或```）或额外的说明文字。
    确保：
    1. 使用适当的键名反映内容的本质
    2. 正确处理嵌套结构
    3. 保留所有重要信息
    4. 遵循JSON语法规范
    5. 只输出纯JSON内容，不要有其他任何标记或文本
    """
    # 非流式输出方式获取响应
    # response = client.chat.completions.create(
    #     model="Pro/deepseek-ai/DeepSeek-V3",
    #     messages=[{"role": "user", "content": prompt}],
    #     temperature=0.1,
    #     top_p=0.1,
    #     stream=False
    # )
    response = qwen_client.chat.completions.create(
        model = "qwen-plus-latest",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
        top_p=0.1,
        response_format={"type": "json_object"}
    )
    
    content = response.choices[0].message.content
    
    # 尝试解析JSON内容
    try:
        json_content = json.loads(content)
        return json_content
    except json.JSONDecodeError:
        # 如果解析失败，返回原始内容
        return {"error": "无法解析为JSON", "raw_content": content}

def code_title_spliter(input_title):
    """
    Args:
        input_title (str): 输入的标题，格式如 "1.1 新能源汽车产业定义与市场技术发展"
        
    Returns:
        dict: 包含title_code和title的字典
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 构建提示词
            prompt = f"""
            请将以下标题拆分为编码和标题内容，并以JSON格式返回：
            
            {input_title}
            
            请返回如下格式的JSON：
            {{
                "title_code": "编码部分",
                "title": "标题内容部分"
            }}
            
            例如，对于"1.1 新能源汽车产业定义与市场技术发展"，应返回：
            {{
                "title_code": "1.1",
                "title": "新能源汽车产业定义与市场技术发展"
            }}
            
            只返回JSON对象，不要有其他任何文字。
            注意不要把年份和标题号混淆
            """
            
            # 调用大模型API
            response = qwen_client.chat.completions.create(
                model="qwen-plus-latest",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                top_p=0.1,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            # print(content)
            # 解析JSON响应
            result = json.loads(content)
            return result['title_code'],result['title']
            
        except Exception as e:
            print(f"标题拆分失败 (尝试 {retry_count+1}/{max_retries}): {str(e)}")
            retry_count += 1
    
    # 如果多次尝试后仍然失败，回退到简单的字符串处理方法
    print("使用备选策略提取标题编码和内容")
    
    # 策略1: 查找第一个空格
    space_index = input_title.find(' ')
    if space_index != -1:
        title_code = input_title[:space_index].strip()
        title = input_title[space_index:].strip()
        return {
            "title_code": title_code,
            "title": title
        }
    
    # 策略2: 查找数字和点号模式
    import re
    match = re.match(r'^(\d+(\.\d+)*)\s*(.*?)$', input_title)
    if match:
        title_code = match.group(1)
        title = match.group(3)
        return {
            "title_code": title_code,
            "title": title
        }
    
    # 如果所有策略都失败，返回空编码和原始标题
    return {
        "title_code": "",
        "title": input_title
    }


if __name__ == "__main__":
    code,title = code_title_spliter("1.2.1 主要国家新余汽车政蔬")
    print(code)
    print(title)

