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
    response = client.chat.completions.create(
        model="Pro/deepseek-ai/DeepSeek-V3",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
        top_p=0.1,
        stream=False
    )
    
    content = response.choices[0].message.content
    
    # 尝试解析JSON内容
    try:
        json_content = json.loads(content)
        return json_content
    except json.JSONDecodeError:
        # 如果解析失败，返回原始内容
        return {"error": "无法解析为JSON", "raw_content": content}

if __name__ == "__main__":
    text = "\n\n```json\n{\n  \"expanded_title\": \"人工智能芯片技术发展、应用场景及全球市场格局分析（2023-2030）\",\n  \"keywords\": {\n    \"core_keywords\": [\n      \"AI芯片\",\n      \"半导体产业\",\n      \"算力基础设施\",\n      \"市场格局分析\"\n    ],\n    \"domain_keywords\": [\n      \"云计算数据中心\",\n      \"智能驾驶系统\",\n      \"边缘计算设备\",\n      \"芯片架构创新\"\n    ],\n    \"focus_keywords\": [\n      \"算力需求激增\",\n      \"国产替代进程\",\n      \"美国出口管制\",\n      \"异构计算发展\"\n    ]\n  }\n}"

    formatted_json = format_agent(text)
    print(json.dumps(formatted_json, ensure_ascii=False, indent=2))
