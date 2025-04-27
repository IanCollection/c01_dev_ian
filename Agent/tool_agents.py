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
        """
        
        # 调用大模型API
        response = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            top_p=0.1,
            response_format={"type": "json_object"}
        )
        
        content = response.choices[0].message.content
        
        # 解析JSON响应
        result = json.loads(content)
        return result['title_code'], result['title']
        
    except Exception as e:
        print(f"标题拆分失败: {str(e)}")
        # 如果大模型处理失败，回退到简单的字符串处理方法
        space_index = input_title.find(' ')
        if space_index != -1:
            title_code = input_title[:space_index].strip()
            title = input_title[space_index:].strip()
        else:
            title_code = ""
            title = input_title
            
        return {
            "title_code": title_code,
            "title": title
        }


if __name__ == "__main__":
#     text = "\n\n```json\n{\n  \"expanded_title\": \"人工智能芯片技术发展、应用场景及全球市场格局分析（2023-2030）\",\n  \"keywords\": {\n    \"core_keywords\": [\n      \"AI芯片\",\n      \"半导体产业\",\n      \"算力基础设施\",\n      \"市场格局分析\"\n    ],\n    \"domain_keywords\": [\n      \"云计算数据中心\",\n      \"智能驾驶系统\",\n      \"边缘计算设备\",\n      \"芯片架构创新\"\n    ],\n    \"focus_keywords\": [\n      \"算力需求激增\",\n      \"国产替代进程\",\n      \"美国出口管制\",\n      \"异构计算发展\"\n    ]\n  }\n}"
#     text = '```markdown\n# 人工智能与AI芯片发展趋势综合报告 [来源: 4125532, 3857597, 3609983, 3752807, 3455563, 4138294, 3313087, 4065821, 4080201, 3767310]\n\n## 一、行业宏观环境与市场趋势分析\n### 1.1 全球人工智能与AI芯片市场概况 [来源: 3313087, 3857597]\n#### 1.1.1 市场规模与增长驱动因素\n- 半导体供应短缺持续至2023年，传感器/MEMS成为增长最快领域 [来源: 3313087]\n- AI芯片市场规模与政策支持推动行业发展\n\n#### 1.1.2 区域市场分布与特点\n- 国内外大模型竞争格局及国产化路径分析 [来源: 4080201]\n- 区域市场分布与供应链安全挑战 [来源: 3857597]\n\n### 1.2 技术生态与应用场景 [来源: 3455563, 4080201, 3767310]\n#### 1.2.1 AI芯片在人工智能生态中的核心作用\n- AI芯片与算法协同发展现状 [来源: 3455563]\n- 大模型多模态能力深化与关键瓶颈 [来源: 3767310]\n\n#### 1.2.2 AI技术在垂直领域的应用创新\n- 教育、工业视觉、文学艺术等领域的AI赋能 [来源: 4138294, 3455563, 4080201]\n\n## 二、技术创新与产品发展分析\n### 2.1 AI芯片技术架构与设计特点 [来源: 3857597, 3313087]\n#### 2.1.1 主流架构对比与新型计算突破\n- GPU、TPU、ASIC等主流架构对比 [来源: 3857597]\n- 存算一体与类脑计算技术探索 [来源: 3313087]\n\n#### 2.1.2 能效比与算力优化的关键技术\n- 先进制程工艺的应用进展 [来源: 3313087]\n- 能效比与算力优化的技术突破 [来源: 3857597]\n\n### 2.2 AI技术演进与场景适配 [来源: 4080201, 3767310]\n#### 2.2.1 AIGC技术发展路径\n- 从文本到逻辑推理的技术进步 [来源: 4080201]\n- 开源与闭源生态并存的发展模式 [来源: 3609983]\n\n#### 2.2.2 消费级硬件与垂直行业解决方案\n- AI与硬件深度融合推动产品升级 [来源: 4125532]\n- 消费级终端与垂直行业解决方案 [来源: 3767310]\n\n## 三、竞争格局与市场动态分析\n### 3.1 行业竞争格局概览 [来源: 3857597, 3313087]\n#### 3.1.1 主要厂商市场份额与排名\n- 国际巨头与本土企业的竞争态势 [来源: 3857597]\n- 新兴企业与初创公司的崛起路径 [来源: 3313087]\n\n#### 3.1.2 竞争策略与商业模式\n- 自研芯片与代工模式的优劣势 [来源: 3857597]\n- 生态合作与平台化战略 [来源: 3313087]\n\n### 3.2 未来竞争趋势与潜在变局 [来源: 3857597, 3313087]\n#### 3.2.1 技术壁垒与专利布局的影响\n- 技术壁垒与专利布局的重要性 [来源: 3857597]\n- 政策监管与供应链安全挑战 [来源: 3313087]\n\n#### 3.2.2 跨界融合与新兴玩家的威胁\n- 跨界融合与新兴玩家的潜在影响 [来源: 3857597]\n\n## 四、发展趋势与投资机会展望\n### 4.1 技术演进方向与可持续发展 [来源: 3313087, 4080201]\n#### 4.1.1 下一代芯片架构与量子计算潜力\n- 下一代芯片架构预测 [来源: 3313087]\n- 量子计算与AI芯片结合潜力 [来源: 4080201]\n\n#### 4.1.2 可持续发展与绿色计算趋势\n- 可持续发展与绿色计算的重要性 [来源: 3313087]\n\n### 4.2 市场增长点与投资热点 [来源: 3313087, 3767310]\n#### 4.2.1 细分领域的高增长机会\n- 高性能计算与边缘计算需求 [来源: 3313087]\n- 消费级AI硬件与场景创新 [来源: 4125532]\n\n#### 4.2.2 关键技术突破的投资价值\n- 关键技术突破与区域市场整合机遇 [来源: 3767310]\n\n### 4.3 风险与挑战应对策略 [来源: 3857597, 3313087]\n#### 4.3.1 技术迭代与市场竞争风险\n- 技术迭代带来的不确定性 [来源: 3857597]\n- 市场竞争加剧的风险评估 [来源: 3313087]\n\n#### 4.3.2 政策与国际环境变化的应对措施\n- 政策与国际环境变化的应对策略 [来源: 3857597]\n```'
#     json_result = json_format_agent(text)
#     print(json_result)
#     formatted_json = format_agent(text)
#     print(json.dumps(formatted_json, ensure_ascii=False, indent=2))
    code,title = code_title_spliter("1.1.3 碳中和政策对产业发展的深远影响")
    print(code)
    print(title)