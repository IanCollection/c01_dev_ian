import time
import os
import sys
import logging

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)

from Agent.client_manager import qwen_client, silicon_client
import json
import os
from openai import OpenAI
import time
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Tuple, Generator

# 假设这些客户端已经在其他地方初始化
from Agent.client_manager import qwen_client, silicon_client

client = silicon_client
qwen_client = qwen_client
from Agent.tool_agents import json_format_agent

logger = logging.getLogger(__name__)

def semantic_enhancement_agent(title, max_keywords=5):
    """
    对用户输入的研报标题进行语义增强，提取关键词以便于ES检索
    
    Args:
        title (str): 用户输入的研报标题
        max_keywords (int): 最大关键词数量
        
    Returns:
        tuple: (增强后的关键词列表, 总成本)
    """
    try:
        # 初始化Qwen客户端
        qwen_client = OpenAI(
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            api_key=os.getenv("DASHSCOPE_API_KEY")
        )
        
        enhancement_prompt = f"""
        你是一个专业的语义增强专家，请对以下研报标题进行关键词提取和语义增强：

        研报标题：{title}

        请执行以下任务：
        1. 分析标题中的核心主题和行业领域
        2. 提取最重要的关键词和术语
        3. 扩展相关的同义词和相关概念
        4. 识别可能的技术术语和专业词汇
        5. 考虑不同的表达方式和常见检索词

        要求：
        1. 关键词应当简洁明确，便于ES检索
        2. 每个关键词不超过5个字
        3. 总共提供不超过{max_keywords}个关键词
        4. 关键词应按重要性排序
        5. 避免过于宽泛的词语

        请以JSON格式返回结果，格式为：{{"keywords": ["关键词1", "关键词2", ...],"yaer":"当前研报标题的年份"}}
        """
        
        enhancement_completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的语义增强专家，能够从标题中提取和扩展关键词。"},
                {"role": "user", "content": enhancement_prompt}
            ],
            temperature=0.3,
            response_format={"type": "json_object"}
        )
        
        result = enhancement_completion.choices[0].message.content
        enhanced_keywords = json.loads(result)
        keywords_list = enhanced_keywords.get("keywords", [])
        
        # 计算成本
        input_cost = enhancement_completion.usage.prompt_tokens / 1000 * 0.0024
        output_cost = enhancement_completion.usage.completion_tokens / 1000 * 0.0096
        total_cost = input_cost + output_cost
        
        return keywords_list, total_cost
        
    except Exception as e:
        print(f"语义增强过程中出错: {e}")
        # 返回原标题作为关键词和零成本
        return [title], 0.0
    


def title_augement_without_cot(title, purpose=None):
    """
    对用户输入的标题进行语义增强，扩展其含义和关键词，生成三组关键词用于研报筛选
    Args:
        title (str): 用户输入的原始标题
        purpose (str, optional): 研报的研究目的，如市场分析、投资决策等
    
    Returns:
        str: 语义增强后的标题内容，JSON格式
    """
    purpose_text = f"研究目的: {purpose}\n" if purpose else ""
    
    prompt = f"""
    请对以下研报标题进行语义增强和关键词扩展，目的是用于研报检索和筛选：
    
    标题: {title}
    {purpose_text}
    请深入思考这个标题的核心含义、相关领域和可能的研究方向，考虑：
    1. 这个标题的主要研究对象是什么？
    2. 相关的行业、技术或市场领域有哪些？
    3. 可能的研究角度包括哪些方面？
    4. 有哪些相关的上下游产业、政策或趋势？
    5. 如果提供了写作目的，请特别关注与该目的相关的关键词和领域
    6. 标题中涉及的时间范围或年份是什么？如果没有明确时间，请根据内容推断最合适的研究时间范围
    
    请以JSON格式提供以下内容：
    1. expanded_title: 扩展后的标题表述（保持原意但更全面）
    2. keywords: 分为三组关键词（每组关键词不超过4个）：
       - core_keywords: 核心必要关键词，与主题直接相关的关键术语
       - domain_keywords: 细分领域关键词，包括相关行业、市场、技术的细分类别
       - focus_keywords: 聚焦领域关键词，包括可能的研究方向、趋势、政策和上下游产业链相关术语
    3. time: 研报最适合的研究时间年份（如2024、2023-2024等）
    
    请直接输出有效的JSON格式，不要包含任何代码块标记（如```json或```）或额外的说明文字。
    确保每组关键词简洁明确，便于后续检索使用，且关键词之间应有明显区别，以提高检索的广度和精确度。
    """
    response = client.chat.completions.create(
        model="Pro/deepseek-ai/DeepSeek-V3",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.4
    )
    content = response.choices[0].message.content
    # 解析JSON内容
    json_content = json.loads(content)
    # 确保返回的JSON包含三组关键词和时间
    if 'keywords' not in json_content:
        json_content['keywords'] = {
            'core_keywords': [],
            'domain_keywords': [],
            'focus_keywords': []
        }
    if 'time' not in json_content:
        json_content['time'] = datetime.now().year
    return json_content, "", json_content['time']


def title_augement(title, purpose=None):
    """
    对用户输入的标题进行语义增强，扩展其含义和关键词，生成三组关键词用于研报筛选
    
    Args:
        title (str): 用户输入的原始标题
        purpose (str, optional): 研报的研究目的，如市场分析、投资决策等
    
    Returns:
        str: 语义增强后的标题内容，JSON格式
    """
    purpose_text = f"研究目的: {purpose}\n" if purpose else ""
    
    prompt = f"""
    请对以下研报标题进行语义增强和关键词扩展，目的是用于研报检索和筛选：
    
    标题: {title}
    {purpose_text}
    请深入思考这个标题的核心含义、相关领域和可能的研究方向，考虑：
    1. 这个标题的主要研究对象是什么？
    2. 相关的行业、技术或市场领域有哪些？
    3. 可能的研究角度包括哪些方面？
    4. 有哪些相关的上下游产业、政策或趋势？
    5. 如果提供了写作目的，请特别关注与该目的相关的关键词和领域
    6. 标题中涉及的时间范围或年份是什么？如果没有明确时间，请根据内容推断最合适的研究时间范围
    
    请以JSON格式提供以下内容：
    1. expanded_title: 扩展后的标题表述（保持原意但更全面）
    2. keywords: 分为三组关键词（每组关键词不超过4个）：
       - core_keywords: 核心必要关键词，与主题直接相关的关键术语
       - domain_keywords: 细分领域关键词，包括相关行业、市场、技术的细分类别
       - focus_keywords: 聚焦领域关键词，包括可能的研究方向、趋势、政策和上下游产业链相关术语
    3. time: 研报最适合的研究时间年份（如2024、2023-2024等）
    
    请直接输出有效的JSON格式，不要包含任何代码块标记（如```json或```）或额外的说明文字。
    确保每组关键词简洁明确，便于后续检索使用，且关键词之间应有明显区别，以提高检索的广度和精确度。
    """
    content = ""
    reasoning_content = ""

    response = client.chat.completions.create(
        model="Pro/deepseek-ai/DeepSeek-R1",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.6,
        stream=True
    )
    for chunk in response:
        if chunk.choices[0].delta.content:
            content += chunk.choices[0].delta.content
        if chunk.choices[0].delta.reasoning_content:
            print(chunk.choices[0].delta.reasoning_content, end="", flush=True)
            reasoning_content += chunk.choices[0].delta.reasoning_content
    
    # 尝试解析JSON内容
    # try:
    json_content = json.loads(content)
    # 确保返回的JSON包含三组关键词和时间
    if 'keywords' not in json_content:
        json_content['keywords'] = {
            'core_keywords': [],
            'domain_keywords': [],
            'focus_keywords': []
        }
    if 'time' not in json_content:
        json_content['time'] = datetime.now().year
    return json_content, reasoning_content,json_content['time']
    # except json.JSONDecodeError:
    #     # 如果解析失败,使用format_agent进行格式化
    #     formatted_json = json_format_agent(content)
    #     # 确保格式化后的JSON包含三组关键词
    #     if 'keywords' not in formatted_json:
    #         formatted_json['keywords'] = {
    #             'core_keywords': [],
    #             'domain_keywords': [],
    #             'focus_keywords': []
    #         }
    #     return formatted_json, reasoning_content
    
def title_augement_stream(title, purpose=None):
    """
    对用户输入的标题进行语义增强的流式版本，用于API接口的实时输出
    
    Args:
        title (str): 用户输入的原始标题
        purpose (str, optional): 研究目的
    
    Returns:
        generator: 生成推理过程和最终结果的流
    """
    purpose_text = f"研究目的: {purpose}\n" if purpose else ""

    prompt = f"""
    请对以下研报标题进行语义增强和关键词扩展，目的是用于研报检索和筛选：
    
    标题: {title}
    {purpose_text}
    请深入思考这个标题的核心含义、相关领域和可能的研究方向，考虑：
    1. 这个标题的主要研究对象是什么？
    2. 相关的行业、技术或市场领域有哪些？
    3. 可能的研究角度包括哪些方面？
    4. 有哪些相关的上下游产业、政策或趋势？
    5. 如果提供了写作目的，请特别关注与该目的相关的关键词和领域
    6. 标题中涉及的时间范围或年份是什么？如果没有明确时间，请根据内容推断最合适的研究时间范围
    
    请以JSON格式提供以下内容：
    1. expanded_title: 扩展后的标题表述（保持原意但更全面）
    2. keywords: 分为三组关键词（每组关键词不超过4个）：
       - core_keywords: 核心必要关键词，与主题直接相关的关键术语
       - domain_keywords: 细分领域关键词，包括相关行业、市场、技术的细分类别
       - focus_keywords: 聚焦领域关键词，包括可能的研究方向、趋势、政策和上下游产业链相关术语
    3. time: 研报最适合的研究时间年份（如2024、2023-2024等）
    
    请直接输出有效的JSON格式，不要包含任何代码块标记（如```json或```）或额外的说明文字。
    确保每组关键词简洁明确，便于后续检索使用，且关键词之间应有明显区别，以提高检索的广度和精确度。
    """

    # 获取LLM响应
    content = ""
    response = client.chat.completions.create(
        model="Pro/deepseek-ai/DeepSeek-R1",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.6,
        stream=True
    )
    
    # 处理流式输出
    for chunk in response:
        if chunk.choices[0].delta.content:
            content += chunk.choices[0].delta.content
        if chunk.choices[0].delta.reasoning_content:
            yield chunk.choices[0].delta.reasoning_content

    def get_year_from_title():
        """从标题中提取年份,如果没有则使用当前年份"""
        import re
        from datetime import datetime
        year_match = re.search(r'20\d{2}', title)
        return year_match.group() if year_match else str(datetime.now().year)
            
    def format_keywords_output(json_data):
        """格式化关键词输出"""
        yield f"\n\n扩展标题: {json_data.get('expanded_title', '')}\n"
        yield "\n关键词:\n"
        for key in ['core_keywords', 'domain_keywords', 'focus_keywords']:
            keywords = json_data.get('keywords', {}).get(key, [])
            yield f"- {key.replace('_', ' ').title()}: {', '.join(keywords)}\n"

    def build_final_result(json_data, year):
        """构建最终JSON结果"""
        return {
            "new_title": json_data.get('expanded_title', ''),
            "keywords": {
                key: json_data.get('keywords', {}).get(key, [])
                for key in ['core_keywords', 'domain_keywords', 'focus_keywords']
            },
            'time': year
        }

    try:
        # 解析JSON内容
        json_content = json.loads(content)
        year = get_year_from_title()
        
        # 输出格式化的关键词
        yield from format_keywords_output(json_content)
        
        # 输出最终结果
        yield build_final_result(json_content, year)
        
    except json.JSONDecodeError:
        # 处理解析失败的情况
        formatted_json = json_format_agent(content)
        
        # 输出格式化的关键词
        yield from format_keywords_output(formatted_json)
        
        # 输出最终结果(不包含year)
        yield build_final_result(formatted_json, None)

def generate_overview_stage_0(all_headers):
    """
    将研报标题内容转换为markdown格式的目录结构
    
    Args:
        all_headers (str): 研报的所有标题内容,以换行符分隔
        
    Returns:
        str: markdown格式的目录结构
    """
    prompt = f"""
    请将以下研报标题内容转换为markdown格式的一二三级目录结构:
    
    {all_headers}
    
    要求:
    1. 使用markdown标准格式表示层级:
       - 一级标题: # 标题内容
       - 二级标题: ## 标题内容
       - 三级标题: ### 标题内容
    2. 只提取一二三级标题
    3. 保持原有标题的层级关系
    4. 每个标题独占一行
    5. 直接输出markdown格式,不要包含其他说明文字
    6. 确保标题前的#号后有一个空格
    """

    response = client.chat.completions.create(
        model="Pro/deepseek-ai/DeepSeek-V3", 
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )
            
    return response.choices[0].message.content

def generate_overview_from_multiple_headers(headers_list):
    """
    处理多个研报的目录列表，生成一个综合的目录结构
    
    Args:
        headers_list (list): 包含多个研报目录内容的列表
        
    Returns:
        str: 综合后的markdown格式目录结构
    """
    # 方法1: 先将每个目录单独处理成markdown格式，然后合并
    import concurrent.futures
    
    # 并行处理每个目录
    with concurrent.futures.ThreadPoolExecutor() as executor:
        markdown_results = list(executor.map(generate_overview_stage_0, headers_list))
    
    # 合并所有处理后的目录
    combined_headers = "\n\n".join(markdown_results)
    
    # 对合并后的目录进行整合和去重
    prompt = f"""
    以下是多份研报的目录结构，请将它们整合为一个完整、系统的目录结构：
    
    {combined_headers}
    
    要求:
    1. 保持markdown格式（# 一级标题，## 二级标题，### 三级标题）
    2. 合并相似主题，去除重复内容
    3. 形成一个逻辑连贯、结构清晰的综合目录
    4. 保留所有重要主题，确保覆盖面广泛
    5. 直接输出markdown格式，不要包含其他说明文字
    """
    
    response = client.chat.completions.create(
        model="Pro/deepseek-ai/DeepSeek-V3", 
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )
    
    return response.choices[0].message.content

    # 方法2（替代方案）: 直接将所有目录合并后一次性处理
    # combined_raw_headers = "\n\n===新研报===\n\n".join(headers_list)
    # return generate_overview_stage_0(combined_raw_headers)


def extract_headers_from_text_qwen(text):
    """
    从文本中提取目录结构并生成markdown格式的目录
    
    Args:
        text (str): 包含目录内容的文本
        
    Returns:
        str: markdown格式的目录结构
    """
    try:
        prompt = f"""
        请从以下文本中根据语义提取目录,并生成一个清晰的markdown格式目录:

        {text}

        要求:
        1. 只提取目录相关的内容，简明扼要
        2. 使用markdown标准格式(# 一级标题, ## 二级标题, ### 三级标题)
        3. 保持层级结构清晰
        4. 去除作者、公司等无关信息
        5. 直接输出markdown格式,不要包含其他说明文字
        """

        completion = qwen_client.chat.completions.create(
            model="qwen-turbo",
            messages=[
                {"role": "system", "content": "你是一个专业的目录结构提取助手"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            top_p=0.1
        )
        # 计算token费用
        input_cost = (completion.usage.completion_tokens / 1000) * 0.0003  # 输入0.0003/千token
        output_cost = (completion.usage.prompt_tokens / 1000) * 0.0006  # 输出0.0006/千token
        cost = input_cost + output_cost
        # print(f"使用token数: {total_tokens}, 费用: {cost:.6f}元")
        return completion.choices[0].message.content,cost
        
    except Exception as e:
        print(f"提取目录结构时发生错误: {e}")
        return None



def extract_h_single_report(report: Dict[str, Any]) -> Tuple[str, float]:
    """
    第一阶段：处理单个研报目录，提取关键主题和结构

    Args:
        report: 单个研报信息的字典

    Returns:
        Tuple[str, float]: 处理结果和处理成本
    """
    # 提取报告信息
    report_id = report.get('file_node_id', 'unknown')
    report_name = report.get('name', 'unnamed report')
    org_name = report.get('org_name', 'unknown organization')
    date = report.get('to_char', 'unknown date')
    headers_content = report.get('headers_content', '')

    # 限制内容长度
    if len(headers_content) > 2000:
        headers_content = headers_content[:2000] + "..."

    # 准备提示
    prompt = f"""
        请提取以下研报目录的关键主题和结构，简明扼要地总结：

        ## 研报ID: {report_id}
        ## 研报名称: {report_name}
        ## 机构: {org_name}
        ## 日期: {date}
        ## 目录内容:
        {headers_content}
        请对以上研报目录进行分析，提取出：
        1. 主要研究领域和关键主题
        2. 目录的层级结构和逻辑关系，经可能的保留目录信息
        3. 重要的行业趋势和观点

        请简洁明了地总结，不要详细展开每个小节。直接输出总结内容，不要包含其他说明文字。
    """

    try:
        # 调用qwen-turbo API进行处理
        completion = qwen_client.chat.completions.create(
            model="qwen-turbo",
            messages=[
                {"role": "system", "content": "你是一个专业的研究报告分析专家，擅长提取研报目录的核心主题和结构。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            top_p=0.1
        )

        # 计算成本
        input_cost = (completion.usage.prompt_tokens / 1000) * 0.0003
        output_cost = (completion.usage.completion_tokens / 1000) * 0.0006
        cost = input_cost + output_cost

        return completion.choices[0].message.content, cost
    except Exception as e:
        print(f"处理研报 {report_id} 时出错: {e}")
        return f"处理出错: {str(e)}", 0.0


def extract_h_single_report_v2(report: Dict[str, Any], topic: str = None) -> Tuple[str, float]:
    """
    第一阶段：处理单个研报目录，提取关键主题和结构

    Args:
        report: 单个研报信息的字典
        topic: 指定的主题，用于筛选相关内容

    Returns:
        Tuple[str, float]: 处理结果和处理成本
    """
    # 提取报告信息
    report_id = report.get('file_node_id', 'unknown')
    report_name = report.get('name', 'unnamed report')
    org_name = report.get('org_name', 'unknown organization')
    date = report.get('to_char', 'unknown date')
    headers_content = report.get('headers_content', '')

    # 限制内容长度
    if len(headers_content) > 2000:
        headers_content = headers_content[:2000] + "..."

    # 准备提示
    prompt = f"""
        请提取以下研报目录中与"{topic}"相关的关键主题和结构，简明扼要地总结：

        ## 研报ID: {report_id}
        ## 研报名称: {report_name}
        ## 机构: {org_name}
        ## 日期: {date}
        ## 目录内容:
        {headers_content}
        
        请对以上研报目录进行分析，仅提取与"{topic}"主题相关的内容：
        1. 与"{topic}"相关的研究领域和关键主题
        2. 相关内容的层级结构和逻辑关系，尽可能保留目录信息
        3. 与"{topic}"相关的行业趋势和观点

        如果研报内容与"{topic}"完全不相关，请直接返回："该研报与指定主题不相关"。
        
        请简洁明了地总结，不要详细展开每个小节。直接输出总结内容，不要包含其他说明文字。
    """

    try:
        # 调用qwen-turbo API进行处理
        completion = qwen_client.chat.completions.create(
            model="qwen-long-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的研究报告分析专家，擅长提取研报目录中与特定主题相关的核心内容和结构。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            top_p=0.1
        )

        # 计算成本
        input_cost = (completion.usage.prompt_tokens / 1000) * 0.0003
        output_cost = (completion.usage.completion_tokens / 1000) * 0.0006
        cost = input_cost + output_cost

        return completion.choices[0].message.content, cost
    except Exception as e:
        print(f"处理研报 {report_id} 时出错: {e}")
        return f"处理出错: {str(e)}", 0.0

def generate_final_toc(summaries: List[str], model='qwen-max-latest') -> Tuple[str, float]:
    """
    第二阶段：使用DeepSeek-V3基于所有摘要生成最终的综合目录

    Args:
        summaries: 第一阶段生成的所有摘要列表

    Returns:
        Tuple[str, float]: 生成的综合目录和处理成本
    """
    # 准备最终提示
    final_prompt = """
        基于以下研报目录的摘要，生成一个综合性的新目录，并且每一个标题下面都有研报的引用源标注，最后以Markdown格式返回：

        {}

        要求:
        1. 使用markdown标准格式（# 一级标题，## 二级标题，### 三级标题）
        2. 合并相似主题，去除重复内容
        3. 形成一个逻辑连贯、结构清晰的综合目录
        4. 保留所有重要主题，确保覆盖面广泛
        5. 目录应该反映行业的整体趋势、关键技术和市场动态
        6. 直接输出markdown格式，不要包含其他说明文字
        7. 目录层级不要超过三级
        8. 每个层级的标题要简洁明了,不要太长
        9. 标题用词专业规范,避免口语化表达
    """.format("\n\n".join([f"### 摘要 {i + 1}:\n{summary}" for i, summary in enumerate(summaries)]))

    try:
        completion = qwen_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system",
                 "content": "你是一个专业的研究报告分析专家，擅长整合多份研报摘要并生成结构清晰的综合目录。"},
                {"role": "user", "content": final_prompt}
            ],
            temperature=0.1,
        )
    
        if model == 'qwen-max-latest':
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.0024
            output_cost = (completion.usage.completion_tokens / 1000) * 0.0096
        else:  # qwen-turbo
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.0003
            output_cost = (completion.usage.completion_tokens / 1000) * 0.0006
        cost = input_cost + output_cost

        return completion.choices[0].message.content,cost
    except Exception as e:
        print(f"生成最终目录时出错: {e}")
        return f"生成最终目录失败: {str(e)}", 0.0
    
def generate_final_toc_v2(all_reports_with_summaries: List[Dict], title,core_keywords,model='qwen-max-latest') -> Tuple[str, float]:
    """
    第二阶段：基于所有研报摘要生成最终的综合目录，并在每个目录项下添加研报引用

    Args:
        all_reports_with_summaries: 包含研报信息和摘要的字典列表，每个字典包含policy_id、s3_url、policy_summary和report_name
        title: 研报标题，用于指导目录生成

    Returns:
        Tuple[str, float]: 生成的综合目录和处理成本
    """
    # 优化：预处理摘要，减少文本量，并过滤与title无关的研报
    summaries_with_sources = []
    for i, report in enumerate(all_reports_with_summaries):
        # 提取摘要的关键部分，减少token数量
        summary = report.get('policy_summary', '')
        
        # 过滤与title无关的研报
        if title.lower() not in summary.lower():
            continue
            
        if len(summary) > 1000:  # 如果摘要太长，截取前1000个字符
            summary = summary[:1000] + "..."
            
        report_info = f"### 摘要 {i + 1}:\\n"
        report_info += f"研报ID: {report.get('report_id', '未知ID')}\\n"
        report_info += f"研报名称: {report.get('report_name', '未知名称')}\\n"
        report_info += f"摘要内容:\\n{summary}"
        summaries_with_sources.append(report_info)
    
    # 优化：使用更简洁明确的提示词，并加入title指导
    final_prompt = """
            以下输入是与"{title}"主题相关的各个研报的目录整理而成的目录框架，请你基于相关研报的目录框架并围绕"{title}"主题，生成一个包含完整引用源的综合目录：
            {summaries}
            要求:
            1. 使用markdown格式（#研报名称，## 一级标题，### 二级标题 #### 三级标题）
            2. 合并相似标题，去除重复内容, 标题号必须是数字
            3. 确保整个综合目录包含三级标题结构
            4. 标题简洁专业，避免口语化表达
            5. 直接输出markdown格式
            6. 目录结构要紧密围绕"{title}"的核心主题展开
            7. 确保每个章节都服务于标题所表达的{core_keywords}
            8. 如果相关研报的某些标题或内容与目标主题无关，请直接过滤
            9. 注意：相关研报的目录总结的参考权重与其索引位置相关，索引越小的目录总结参考权重越高
            10. 优先考虑前5个目录总结的核心框架和结构
            11. 在整合相似主题时，优先采用索引靠前的摘要中的表述
        """.format(title=title, summaries="\n\n".join(summaries_with_sources), core_keywords=core_keywords)

    try:
        # 优化：如果报告数量少，可以使用更快的模型
        use_model = 'qwen-plus' if len(all_reports_with_summaries) < 5 else model
        
        completion = qwen_client.chat.completions.create(
            model=use_model,
            messages=[
                {"role": "system",
                 "content": "你是专业研报分析专家，确保目录结构完整。"},
                {"role": "user", "content": final_prompt}
            ],
            temperature=0.1,
        )
    
        if use_model == 'qwen-max-latest':
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.0024
            output_cost = (completion.usage.completion_tokens / 1000) * 0.0096
        else:  # qwen-turbo
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.0003
            output_cost = (completion.usage.completion_tokens / 1000) * 0.0006
        cost = input_cost + output_cost

        # 优化：使用简化的验证逻辑
        content = completion.choices[0].message.content
        if '[来源:' not in content or content.count('#') < 10:  # 简单检查是否有引用源和足够的标题
            # 如果结果不满足要求，使用增强函数但不再递归调用
            enhanced_content, enhance_cost = _enhance_toc_with_citations(content, all_reports_with_summaries, use_model)
            return enhanced_content, cost + enhance_cost
            
        return content, cost
    except Exception as e:
        print(f"生成最终目录时出错: {e}")
        return f"生成最终目录失败: {str(e)}", 0.0
    
    
def generate_final_toc_v2_stream(all_reports_with_summaries: List[Dict], title, core_keywords, model='qwen-max-latest') -> Generator[Tuple[str, float], None, None]:
    """
    流式生成最终目录
    
    Args:
        all_reports_with_summaries: 包含研报信息和摘要的字典列表
        title: 研报标题
        core_keywords: 核心关键词
        model: 使用的模型
        
    Returns:
        Generator: 生成器，逐块返回目录内容和累计成本
    """
    try:
        # 优化：预处理摘要，减少文本量，并过滤与title无关的研报
        summaries_with_sources = []
        for i, report in enumerate(all_reports_with_summaries):
            # 提取摘要的关键部分，减少token数量
            summary = report.get('policy_summary', '')
            
            # 过滤与title无关的研报
            if title.lower() not in summary.lower():
                continue
                
            if len(summary) > 1000:  # 如果摘要太长，截取前1000个字符
                summary = summary[:1000] + "..."
                
            report_info = f"### 摘要 {i + 1}:\\n"
            report_info += f"研报ID: {report.get('report_id', '未知ID')}\\n"
            report_info += f"研报名称: {report.get('report_name', '未知名称')}\\n"
            report_info += f"摘要内容:\\n{summary}"
            summaries_with_sources.append(report_info)
        
        # 优化：使用更简洁明确的提示词，并加入title指导
        final_prompt = """
                以下输入是与"{title}"主题相关的各个研报的目录整理而成的目录框架，请你基于相关研报的目录框架并围绕"{title}"主题，生成一个于主题有关的综合目录：
                {summaries}
                要求:
                1. 使用markdown格式（# 标题，## 一级子标题，### 二级子标题 #### 三级子标题)
                2. 合并相似标题，去除重复内容
                3. 确保整个综合目录包含三级标题结构
                4. 标题简洁专业，避免口语化表达
                5. 直接输出markdown格式，不要包含任何解释性文字、多余字符或markdown标识（如```）
                6. 目录结构要紧密围绕"{title}"的核心主题展开
                7. 确保每个章节都服务于标题所表达的{core_keywords}
                8. 如果相关研报的某些标题或内容与目标主题无关，请直接过滤
                9. 注意：相关研报的目录总结的参考权重与其索引位置相关，索引越小的目录总结参考权重越高
                10. 优先考虑前5个目录总结的核心框架和结构
                11. 在整合相似主题时，优先采用索引靠前的摘要中的表述
            """.format(title=title, summaries="\n\n".join(summaries_with_sources), core_keywords=core_keywords)

        use_model = 'qwen-turbo' if len(all_reports_with_summaries) > 5 else model
        
        completion = qwen_client.chat.completions.create(
            model=use_model,
            messages=[
                {"role": "system", "content": "你是专业研报分析专家，确保目录结构完整。"},
                {"role": "user", "content": final_prompt}
            ],
            temperature=0.1,
            stream=True
        )
        
        content = ""
        total_cost = 0.0
        for chunk in completion:
            if chunk.choices[0].delta.content:
                chunk_content = chunk.choices[0].delta.content
                content += chunk_content
                
                # 计算当前chunk的成本
                # if use_model == 'qwen-max-latest':
                #     chunk_cost = (len(chunk_content) / 1000) * 0.0096  # 仅计算输出成本
                # else:
                #     chunk_cost = (len(chunk_content) / 1000) * 0.0006
                # total_cost += chunk_cost
                
                # 返回当前chunk和累计成本
                yield chunk_content

    
    except Exception as e:
        yield f"生成最终目录时出错: {str(e)}", 0.0

def generate_final_toc_v2_stream_no_title(all_reports_with_summaries: List[Dict], title, core_keywords, purpose,
                                 model='qwen-max-latest') -> Generator[Tuple[str, float], None, None]:
    """
    流式生成最终目录

    Args:
        all_reports_with_summaries: 包含研报信息和摘要的字典列表
        title: 研报标题
        core_keywords: 核心关键词
        model: 使用的模型
        purpose: 研报目的

    Returns:
        Generator: 生成器，逐块返回目录内容和累计成本
    """
    logger.info(f"开始执行 generate_final_toc_v2_stream_no_title，标题: {title}, 报告数量: {len(all_reports_with_summaries)}") # 添加日志
    try:
        # 优化：预处理摘要，减少文本量，并过滤与title无关的研报
        summaries_with_sources = []
        for i, report in enumerate(all_reports_with_summaries):
            # 提取摘要的关键部分，减少token数量
            summary = report.get('report_summary', '') # <-- 修改键名： 'report_id' -> 'summary' (或者正确的键名)
            if not isinstance(summary, str):
                 # 如果摘要不是字符串，可以选择跳过或者记录日志
                logger.warning(f"报告 ID {report.get('report_id', '未知ID')} 的摘要不是字符串，已跳过。摘要内容: {summary}")
                continue
            
            # 添加类型检查，确保 title 是字符串
            if not isinstance(title, str):
                logger.error(f"传入的标题不是字符串: {title}")
                # 根据需要处理错误，例如抛出异常或返回错误信息
                yield f"错误：标题不是字符串", 0.0
                return # 或者 raise TypeError("Title must be a string")

            # 过滤与title无关的研报 (现在 summary_content 和 title 都是字符串)
            if title.lower() not in summary.lower():
                continue

            # 截取过长的摘要
            if len(summary) > 1000:
                summary = summary[:1000] + "..."

            report_info = f"### 摘要 {i + 1}:\n"
            report_info += f"研报ID: {report.get('report_id', '未知ID')}\n"
            report_info += f"研报名称: {report.get('report_name', '未知名称')}\n"
            report_info += f"摘要内容:\n{summary}" # 使用修正后的 summary_content
            summaries_with_sources.append(report_info)

        # 将摘要信息保存到txt文件
        try:
            import os
            import re
            from datetime import datetime
            
            # 处理标题用于文件名
            if isinstance(title, str):
                # 提取标题的前20个字符，并移除非法文件名字符
                safe_title = re.sub(r'[\\/*?:"<>|]', "", title)[:20]
                safe_title = safe_title.strip()
            else:
                safe_title = "未知标题"
            
            # 生成带时间戳和标题的文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{safe_title}_{timestamp}.txt"
            
            # 写入文件到根目录
            with open(filename, "w", encoding="utf-8") as f:
                f.write(f"研报标题: {title}\n")
                f.write(f"核心关键词: {core_keywords}\n")
                f.write(f"研报目的: {purpose}\n\n")
                f.write("摘要信息:\n")
                f.write("\n\n".join(summaries_with_sources))
            
            logger.info(f"摘要信息已保存到根目录文件: {filename}")
        except Exception as e:
            logger.error(f"保存摘要信息到文件时出错: {str(e)}")

        # 优化：使用更简洁明确的提示词，并加入title指导
        final_prompt = """
                你是一个专业的研报分析师。你需要基于研究目的，根据历史研报信息即输入数据生成新的目录大纲，且目录需要契合研报标题的行业特征，并充分引用与关键词相关的输入数据进行目录撰写，具体任务要求如下。

                输入数据：{summaries}
                研报标题: {title}
                关键词: {keywords}
                研报目的:{purpose}

                任务要求：
                1. 格式规范：
                   - 严格使用Markdown格式（## 二级标题，### 三级标题，#### 四级标题），必须生成四级标题
                   - 跳过# 一级标题，即研报标题，直接从## 二级标题开始输出
                   - 确保标题编号使用数字（如1，1.1, 1.1.1）
                   - 输出纯Markdown内容，不包含任何解释或标识符
                   - 必须保留#符号作为Markdown标题标识符
                   - 确保每个标题都以#开头，后跟空格和标题内容
                   - 不要返回'---'这种分隔线
                   - 请直接输出符合要求的Markdown格式目录，不要包含任何解释性文字。特别注意：必须保留#符号作为标题标识符，确保输出格式完全符合Markdown规范。
                   - 请直接返回Markdown格式的研报大纲。不要有其他任何多余字符比如```markdown```。
                2. 优先级处理：
                   - 优先考虑与{title}所属行业及关键词最相关的输入数据
                   - 在引用输入数据生成新的目录时，优先采用索引靠前的表述，特别是前5个索引的内容
                3. 内容要求：
                   -你需要根据研报目的的不同撰写差异化的目录，因为不同的研究目录关注的重点不同，例如若是投资人导向，应强化数据、估值与趋势，以此类推。
                   --大纲需契合{title}的行业特性，如能源报告应强调政策与上下游协同，消费报告更强调用户分层与场景等，请检查核对，确保目录中不要提到该标题所属的行业中不常用的指标、名词、术语等。
                   -各级标题的设置应遵循MECE原则（相互独立、完全穷尽），并结合研报标题、关键词、输入数据灵活调整、具体问题具体分析
                   -生成目录各个标题时可以包含观点，但是观点必须来源于输入数据的总结，禁止凭空编造，如果输入数据中无用于支撑的信息，则只输出陈述性标题，即以客观、中立的方式描述标题的主题或范围，而禁止包含主观观点、评价
                   -生成目录时，如输入数据中含相关信息，则标题应包含时间、区域等信息
                   -降低包含定量模型的二级标题（如盈利测算、投资回报等）及其子标题在目录中的占比
                4. 结构、逻辑要求
                   -确保各个二级标题间的逻辑衔接顺畅，递进逻辑清晰，从大范围到小范围、宏观中观到微观、从现象到原因、从问题到建议，层层递进、环环相扣，展现清晰的内容故事线设计，例如“政策与宏观环境分析-市场分析-技术/产品分析-产业链分析-竞争格局-行业风险与挑战分析-发展趋势/投资建议”等，整体目录结构体现“总—分—点”思路，按“背景—分析—结论—建议”等清晰展开
                   -每一个子标题要围绕上级标题的主题来撰写，覆盖该子标题所属的上级标题的所有关键要点，保证二三四级标题逻辑顺畅
                5. 语言风格要求：
                   -标题表达简洁、具有洞察力，保证各级标题用词专业，使用研报标题所属行业的术语
                   -语言风格与行业一致，避免使用泛用词或模糊词（如“可能”、“或许”、“比较多”等）。
                   -任何级别标题中禁止出现数字、数据（包含中文数字，例如一二三等），但可以包含年份的数字表示。
            """.format(title=title, summaries="\n\n".join(summaries_with_sources), keywords=core_keywords, purpose=purpose)
        logger.info("构建 final_prompt 完成。") # 添加日志
        # 使用deepseek-ai/DeepSeek-V3模型
        logger.info("准备调用DeepSeek-V3模型...") # 添加日志

        # try:
        #     completion = client.chat.completions.create(
        #         model="Pro/deepseek-ai/DeepSeek-V3",
        #         messages=[
        #             {"role": "system", "content": "你是专业研报分析专家，确保目录结构完整。"},
        #             {"role": "user", "content": final_prompt}
        #         ],
        #         temperature=0.1,
        #         stream=True
        #     )
        #
        #     logger.info("DeepSeek-V3模型调用成功，开始接收流式响应...") # 添加日志
        #     content = ""
        #     for chunk in completion:
        #         if chunk.choices[0].delta.content:
        #             chunk_content = chunk.choices[0].delta.content
        #             content += chunk_content
        #             yield chunk_content
        #     logger.info("流式响应接收完毕。") # 添加日志
        #
        # except Exception as e:
        #     logger.error(f"调用DeepSeek-V3模型时发生错误: {str(e)}", exc_info=True)
        #     yield f"调用模型时出错: {str(e)}"
        use_model = 'qwen-max-latest' if len(all_reports_with_summaries) > 5 else model
        logger.info(f"准备调用模型: {use_model}") # 添加日志

        completion = qwen_client.chat.completions.create(
            model=use_model,
            messages=[
                {"role": "system", "content": "你是专业研报分析专家，确保目录结构完整。"},
                {"role": "user", "content": final_prompt}
            ],
            temperature=0.1,
            stream=True
        )
        logger.info("模型调用成功，开始接收流式响应...") # 添加日志
        content = ""
        for chunk in completion:
            if chunk.choices[0].delta.content:
                chunk_content = chunk.choices[0].delta.content
                content += chunk_content
                yield chunk_content
        logger.info("流式响应接收完毕。") # 添加日志

    except Exception as e:
        # 使用 logger 记录详细错误信息和堆栈跟踪
        logger.error(f"生成最终目录时发生严重错误: {str(e)}", exc_info=True)
        # 仍然 yield 错误信息给调用者，但日志中已有更详细的信息
        yield f"生成最终目录时出错: {str(e)}", 0.0


def _verify_citations_in_toc(toc_content: str) -> bool:
    """
    验证目录中的每个标题是否都有引用源
    
    Args:
        toc_content: 生成的目录内容
        
    Returns:
        bool: 如果每个标题都有引用源则返回True，否则返回False
    """
    # 优化：使用正则表达式进行更快的验证
    import re
    
    # 提取所有标题行
    title_pattern = re.compile(r'^(#+)\s+(.+)$', re.MULTILINE)
    titles = title_pattern.findall(toc_content)
    
    # 检查是否有引用源
    citation_pattern = re.compile(r'\[来源:.*?\]')
    
    for _, title in titles:
        if not citation_pattern.search(title):
            # 检查下一行是否有引用源
            title_index = toc_content.find(title)
            next_line_start = toc_content.find('\n', title_index) + 1
            next_line_end = toc_content.find('\n', next_line_start)
            if next_line_end == -1:
                next_line_end = len(toc_content)
            next_line = toc_content[next_line_start:next_line_end]
            
            if not citation_pattern.search(next_line):
                return False
    
    return True


def _enhance_toc_with_citations(toc_content: str, all_reports: List[Dict], model='qwen-max-latest') -> Tuple[str, float]:
    """
    增强目录，确保每个标题都有引用源
    
    Args:
        toc_content: 原始生成的目录内容
        all_reports: 包含研报信息的字典列表
        model: 使用的模型
        
    Returns:
        Tuple[str, float]: 增强后的目录和处理成本
    """
    # 优化：只提取必要的报告信息
    report_info = "\n".join([f"ID: {report.get('policy_id', '未知ID')}, 名称: {report.get('report_name', '未知名称')}" 
                            for report in all_reports])
    
    # 优化：简化提示词
    prompt = f"""
    完善以下研报目录，为每个标题添加引用源：

    报告信息:
    {report_info}
    
    目录内容:
    {toc_content}
    
    要求:
    1. 为每个标题添加引用源：[来源: ID1, ID2, ...]
    2. 保留已有引用源
    3. 确保包含三级标题结构
    4. 直接返回完整目录
    """
    
    try:
        # 优化：使用更快的模型
        use_model = 'qwen-turbo' if model == 'qwen-max-latest' and len(all_reports) < 5 else model
        
        completion = qwen_client.chat.completions.create(
            model=use_model,
            messages=[
                {"role": "system", "content": "你是专业研报编辑，为目录添加引用源并完善结构。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
        )
        
        if use_model == 'qwen-max-latest':
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.0024
            output_cost = (completion.usage.completion_tokens / 1000) * 0.0096
        else:  # qwen-turbo
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.0003
            output_cost = (completion.usage.completion_tokens / 1000) * 0.0006
        cost = input_cost + output_cost
        
        return completion.choices[0].message.content, cost
    except Exception as e:
        print(f"增强目录引用源时出错: {e}")
        return toc_content, 0.0


def match_focus_points(title: str) -> tuple:
    """
    将研报标题与预定义的二级关注点进行匹配
    
    Args:
        title: 研报标题
        model: 使用的模型名称
        
    Returns:
        tuple: (匹配结果列表, API调用成本)
    """
    
    # 优化：使用缓存避免重复处理相同标题
    # 可以在函数外部定义一个缓存字典
    global focus_points_cache
    if not hasattr(match_focus_points, 'focus_points_cache'):
        match_focus_points.focus_points_cache = {}
    
    # 检查缓存
    if title in match_focus_points.focus_points_cache:
        return match_focus_points.focus_points_cache[title]
    # focus_points=用sql查询
    # 从数据库查询二级关注点


    # 预定义的二级关注点列表
    focus_points = ['行业定义', '行业概况', '行业特征', '宏观环境', '行业政策趋势分析', '市场划分/结构', 
                   '市场容量', '市场规模', '市场发展速度', '市场吸引力/增长潜力', '市场驱动', 
                   '行业生命周期分析', '市场限制', '重点区域市场', '进出口市场', '产品功能和性能评估',
                   '技术创新跟踪', '行业竞争格局', '行业竞争趋势', '行业内主要玩家', '潜在对手分析', 
                   '产业链上下游图谱', '供给与需求', '主要原材料的价格变化及影响因素', '行业内的主要盈利模式',
                   '成本结构和利润空间分析', '定价策略', '投资回报和风险收益分析', '市场风险', '财务风险',
                   '运营风险', '供应链分析', '法律和合规分析', '文化社会风险', '用户画像分析', '需求特征',
                   '消费者及下游产业对产品的购买需求规模', '议价能力', '营销策略', '推广策略', '品牌建设',
                   '核心能力', '产品布局', '竞争策略', '竞争优势', '新兴技术及影响', '创新发展趋势', 
                   '行业技术转型趋势']
    
    # 优化：使用更简洁的提示词
    prompt = f"""
    分析研报标题与关注点的相关性:
    
    标题: {title}
    
    关注点列表:
    {json.dumps(focus_points, ensure_ascii=False)}
    
    返回JSON格式:
    {{
        "二级关注点": ["关注点1","关注点2","关注点3",...]
    }}
    
    只返回最相关的12个关注点。
    """

    try:
        # 优化：使用更快的模型
        use_model = "qwen-plus-latest"
        
        completion = qwen_client.chat.completions.create(
            model=use_model,
            messages=[
                {"role": "system", "content": "你是专业研报分析助手，擅长主题匹配。"},
                {"role": "user", "content": prompt}

            ],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        
        result = json.loads(completion.choices[0].message.content)
        
        # 计算API调用成本
        if use_model == "Pro/deepseek-ai/DeepSeek-V3":
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.002 
            output_cost = (completion.usage.completion_tokens / 1000) * 0.008
        elif use_model == "qwen-turbo":
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.0003
            output_cost = (completion.usage.completion_tokens / 1000) * 0.0006
        else:
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.0024
            output_cost = (completion.usage.completion_tokens / 1000) * 0.0096
        cost = input_cost + output_cost
        
        # 保存到缓存
        match_focus_points.focus_points_cache[title] = (result, cost)
        
        return result, cost
        
    except Exception as e:
        print(f"匹配关注点时出错: {e}")
        return {"matches": []}, 0.0


def generate_toc_from_focus_points(title: str, focus_points: str, keywords) -> tuple[str, float]:
    """
    根据标题和关注点生成研报目录结构
    
    Args:
        title:中国新能源汽车产业可持续发展报告2023

        focus_points: 关注点字符串

    Returns:
        tuple: 包含生成的Markdown格式目录结构(str)和API调用成本(float)
    """

    prompt = f"""
    团队里的其他分析师基于研报的标题，整理了该研报目录框架需要重点关注的关注点清单，并且给你了整个研报目录框架和正文撰写需要围绕的关键词。请基于以下关注点、关键词和研报目的，整理得到符合研报标题逻辑的研报目录。
    标题: {title}
    关注点: {focus_points}
    关键词: {keywords}
    研报目的：洞察市场趋势，行业报告通常涉及市场规模、增长趋势、消费者行为、技术变化等方面，为公司提供对目标市场的深刻洞察，帮助其制定产品或服务的定位。
    要求:
    1. 先根据研报标题，参考行研报告的一般逻辑生成一个完整的三级目录结构。目录结构要符合研报写作逻辑,客观分析行业的发展现状和未来发展趋势。行业研究主要是通过综合分析特定行业的发展态势，产出深刻洞察和观点。方法论涵盖从宏观的产业层到微观的产品层的分析，对企业战略、政策制定和金融决策等产生显著影响。
    2. 再根据关注点和关键词调整目录结构,不必严格遵循关注点的层级结构。每个标题必须紧密围绕核心关注点。各级标题之间要有逻辑关联性，注意关键点是框架中必不可少的内容，你仍需要根据研报标题增加其他关注点。请在写标题时用词专业准确
    3.需要检查是否覆盖了所有重要方面，比如是否包括了产业链的上下游，是否分析了国内外市场，是否有政策、技术、市场的多维度分析。另外，用户可能需要这个目录既全面又有深度，所以三级标题要足够详细，但也不能过于冗杂。可能需要平衡各个部分的比例，确保逻辑连贯，层次分明。还要注意术语的准确性和行业热点。
    4. 标题号必须用数字
    5. 使用Markdown格式:
       - 一级标题: # 标题
       - 二级标题: ## 标题  
       - 三级标题: ### 标题
       
    请直接返回Markdown格式的目录文本,不要包含任何额外说明。
    """

    try:
        # # 使用 DeepSeek-V3
        # completion_deepseek = client.chat.completions.create(
        #     model="Pro/deepseek-ai/DeepSeek-V3",
        #     messages=[
        #         {"role": "system", "content": "你是一个专业的研报写作助手,擅长生成研报大纲。"},
        #         {"role": "user", "content": prompt}
        #     ],
        #     temperature=0.7
        # )
        # 使用 DeepSeek-V3 的结果
        # toc = completion_deepseek.choices[0].message.content

        # 使用 Qwen-Max
        completion_qwen = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的研报写作助手,擅长生成研报大纲。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )


        toc_qwen = completion_qwen.choices[0].message.content
        # 计算成本
        input_cost = (completion_qwen.usage.prompt_tokens / 1000) * 0.0024
        output_cost = (completion_qwen.usage.completion_tokens / 1000) * 0.0096
        cost = input_cost + output_cost
        
        return toc_qwen,cost
        
    except Exception as e:
        print(f"生成目录时出错: {e}")
        return "", 0.0
    




def generate_toc_from_focus_points_stream(title: str, focus_points: str, keywords) -> tuple[str, float]:
    """
    根据标题和关注点生成研报目录结构
    
    Args:
        title:中国新能源汽车产业可持续发展报告2023

        focus_points: 关注点字符串

    Returns:
        tuple: 包含生成的Markdown格式目录结构(str)和API调用成本(float)
    """

    prompt = f"""
    团队里的其他分析师基于研报的标题，整理了该研报目录框架需要重点关注的关注点清单，并且给你了整个研报目录框架和正文撰写需要围绕的关键词。请基于以下关注点、关键词和研报目的，整理得到符合研报标题逻辑的研报目录。
    标题: {title}
    关注点: {focus_points}
    关键词: {keywords}
    研报目的：洞察市场趋势，行业报告通常涉及市场规模、增长趋势、消费者行为、技术变化等方面，为公司提供对目标市场的深刻洞察，帮助其制定产品或服务的定位。
    要求:
    1. 先根据研报标题，参考行研报告的一般逻辑生成一个完整的四级目录结构。目录结构要符合研报写作逻辑,客观分析行业的发展现状和未来发展趋势。行业研究主要是通过综合分析特定行业的发展态势，产出深刻洞察和观点。方法论涵盖从宏观的产业层到微观的产品层的分析，对企业战略、政策制定和金融决策等产生显著影响。
    2. 再根据关注点和关键词调整目录结构,不必严格遵循关注点的层级结构。每个标题必须紧密围绕核心关注点。各级标题之间要有逻辑关联性，注意关键点是框架中必不可少的内容，你仍需要根据研报标题增加其他关注点。请在写标题时用词专业准确
    3.需要检查是否覆盖了所有重要方面，比如是否包括了产业链的上下游，是否分析了国内外市场，是否有政策、技术、市场的多维度分析。另外，用户可能需要这个目录既全面又有深度，所以三级标题要足够详细，但也不能过于冗杂。可能需要平衡各个部分的比例，确保逻辑连贯，层次分明。还要注意术语的准确性和行业热点。
    4. 使用Markdown格式:
       - 一级研报题目: # 标题
       - 二子级标题: ## 标题  
       - 三级标题: ### 标题
       - 四级子标题: #### 标题
    5. 注意一级研报题目不用返回，我只要研报题目下的所有子标题
    请直接返回Markdown格式的目录文本,不要包含任何额外说明。
    """
    # 使用 Qwen-Max
    try:
        # 使用流式API调用
        completion_qwen = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的研报写作助手,擅长生成研报大纲。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            stream=True
        )

        # 初始化变量
        toc_qwen = ""
        prompt_tokens = 0
        completion_tokens = 0

        # 流式处理响应
        for chunk in completion_qwen:
            if chunk.choices[0].delta.content:
                toc_qwen += chunk.choices[0].delta.content
                yield toc_qwen  # 流式返回部分结果
            
            # 记录token使用情况
            if chunk.usage:
                prompt_tokens = chunk.usage.prompt_tokens
                completion_tokens = chunk.usage.completion_tokens

        # 计算成本
        # input_cost = (prompt_tokens / 1000) * 0.0024
        # output_cost = (completion_tokens / 1000) * 0.0096
        # cost = input_cost + output_cost
        
        # yield {"final": toc_qwen, "cost": cost}  # 返回最终结果和成本
   
        yield {"final": toc_qwen}  # 只返回最终结果

    except Exception as e:
        print(f"生成目录时出错: {e}")
        yield {"final": "", "cost": 0.0}

def generate_toc_from_focus_points_stream_no_title(title: str, focus_points: str, keywords, purpose) -> tuple[str, float]:
    """
    根据标题和关注点生成研报目录结构

    Args:
        title:中国新能源汽车产业可持续发展报告2023
        focus_points: 关注点字符串

    Returns:
        tuple: 包含生成的Markdown格式目录结构(str)和API调用成本(float)
    """

    prompt = f"""
           你是一个专业的行业研究分析师，擅长撰写研报的目录大纲。当前分析师已经基于研报的标题，整理了该研报目录框架需要重点关注的关注点清单，并且给了你撰写整个研报目录框架和正文撰写需要围绕的关键词。你需要首先基于研报标题生成一份基础的研报目录，然后根据关键词和关注点微调这个基础目录，最终生成一份逻辑清晰且不偏离主题的标准目录大纲，该目录大纲需要满足下列要求。
    
           标题: {title}
           关注点: {focus_points}
           关键词: {keywords}
           研报目的:{purpose}
           要求:
            1. 必须同时满足以下各级标题数量限制：
            - 二级标题总数量在[6,8]之间；
            - 每个二级标题下三级标题数量在[2,4]之间；
            - 每个三级标题下四级标题数量在[2,4]之间。
            2. 内容要求：
            -基础目录需要基于研报标题和关键词的要点生成，做到主题聚焦，这两点是首要的，此外，你可以适当结合关注点进行补充，可根据具体情况适当地增加其他相关的关注重点
            -你需要根据研报目的的不同撰写差异化的目录，因为不同的研究目录关注的重点不同，例如若是投资人导向，应强化数据、估值与趋势，以此类推。
            -降低包含定量模型的二级标题（如盈利测算、投资回报等）及其子标题在目录中的占比，同时确保标题数量符合限制要求。
            -各级标题的设置应遵循MECE原则（相互独立、完全穷尽），并结合研报标题具体问题具体分析：（1）完全穷尽：确保内容覆盖全面，无遗漏。例如，若标题未提及某个区域或提及国内外，则目录需全面涵盖国内外相关情况，例如，按照先全球后具体国家再国内地方市场、先主要地区后新兴地区的逻辑展开，以此类推。注意适度细分，避免过度拆分导致复杂化或偏离重点（2）相互独立：避免内容重叠，确保各部分内容界限清晰。相似主题应整合在同一个标题下，防止分散表达；而对于不同主题则不应合并到同一标题下。反面案例包括将政策与市场、市场与竞争格局等内容合在同一个二级标题下，导致逻辑混乱。
           -大纲需契合{title}的行业特性，如能源报告应强调政策与上下游协同，消费报告更强调用户分层与场景等，请检查核对，确保目录中不要提到该标题所属的行业中不常用的指标、名词、术语等。
            3. 结构、逻辑要求
            -确保各个二级标题间的逻辑衔接顺畅，递进逻辑清晰，从大范围到小范围、宏观中观到微观、从现象到原因、从问题到建议，层层递进、环环相扣，展现清晰的内容故事线设计，例如“政策与宏观环境分析-市场分析-技术/产品分析-产业链分析-竞争格局-行业风险与挑战分析-发展趋势/投资建议”等，具体逻辑需要根据研报标题、关键词进行调整。
            -每一个子标题要围绕上级标题的主题来撰写，覆盖该子标题所属的上级标题的所有关键要点，保证二三四级标题逻辑顺畅
            4. 语言风格要求：
            -输出陈述性标题，即以客观、中立的方式描述标题的主题或范围，禁止包含主观观点、评价，并确保标题内容足够具体、聚焦，例如：“跨国供应链协同的成本竞争力提升路径”
            -标题表达简洁、具有洞察力，保证各级标题用词专业，使用研报标题所属行业的术语
            -语言风格与行业一致，避免使用泛用词或模糊词（如“可能”、“或许”、“比较多”等）。
            -禁止使用括号作为补充性内容，并通过重新组织语言将括号中直接显示在标题中，确保逻辑清晰、无歧义。
            -任何级别标题中禁止出现数字、数据（包含中文数字，例如一二三等），但可以包含年份的数字表示。
            5. 格式规范：
            - 严格使用以下Markdown格式输出: ## 二级标题；### 三级标题；#### 四级标题
            - 跳过# 一级标题，即研报标题，直接从## 二级标题开始输出
            - 直接输出Markdown格式的目录文本而不是输出带格式的结果，从二级标题开始，不要包含一级标题（研报标题本身），不要包含任何额外说明或解释
            - 必须保留#符号作为Markdown标题标识符，确保每个标题都以#开头，后跟空格和标题内容
            - 不要返回'---'这种分隔线
            - 请直接返回Markdown格式的研报大纲。不要有其他任何多余字符比如```markdown```。
           """
    try:
        # 使用流式API调用
        completion_qwen = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的研报写作助手,擅长生成研报大纲。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            stream=True
        )

        # 初始化变量
        toc_qwen = ""
        prompt_tokens = 0
        completion_tokens = 0

        # 流式处理响应
        for chunk in completion_qwen:
            if chunk.choices[0].delta.content:
                toc_qwen += chunk.choices[0].delta.content
                yield toc_qwen  # 流式返回部分结果
            
            # 记录token使用情况
            if chunk.usage:
                prompt_tokens = chunk.usage.prompt_tokens
                completion_tokens = chunk.usage.completion_tokens

        # 返回最终结果
        yield {"final": toc_qwen}

    except Exception as e:
        print(f"生成目录时出错: {e}")
        yield {"final": "", "cost": 0.0}
def overview_conclusion(overview1, overview2, title, purpose):
    """
    对两个研报进行总结生成一个有三级标题的研报

    Args:
        overview1 (str): 第一个研报目录
        overview2 (str): 第二个研报目录
        title (str): 研报标题

    Returns:
        str: 生成的研报内容
        float: API调用成本
    """
    """
        对两个研报进行总结生成一个有三级标题的研报

        Args:
            overview1 (str): 第一个研报目录
            overview2 (str): 第二个研报目录
            title (str): 研报标题

        Returns:
            str: 生成的研报内容
            float: API调用成本
        """

    # prompt = f"""
    # 	【分析目的】
    # 	我会给你两个研报目录，研报目录1是根据分析师认为的分析重点得到的初始标准研报目录，研报目录2是基于研报标题找到的相关历史研报整理得到的综合目录。请整合标准研报目录（研报目录1）和历史研报生成的目录（研报目录2），确保生成完整的涵盖一、二、三级标题的新的综合目录。

    #     研报目录1:
    #     {overview1}

    #     研报目录2: 
    #     {overview2}

    #     研报标题: {title}

    #     研究目的：{purpose}

    #     【核心要求】
	#     1、根据研报标题和分析目的，对标准研报目录和历史研报目录进行合理的整合,互相补充分析要点，但是如果历史研报目录中的某些内容与研报标题、标准研报目录所属行业相关度极低，则不需要将对应的历史研报目录与标准目录整合。
	#     2、确保目录所有标题都符合一、二、三级标题的结构,每个二级标题下至少有两个三级标题，且必须生成三级标题，每一个子标题要围绕上级标题的主题来撰写，保证一二三级标题逻辑顺畅。
    #     3、目录结构要符合行业研究报告写作逻辑,深入洞察特定行业的发展动态、竞争格局及未来趋势，其方法论通常从宏观到微观层层递进：在宏观层面，关注政策环境、经济周期、技术变革及社会趋势对行业的整体影响；在中观层面，聚焦产业链结构、市场竞争格局及商业模式演变；在微观层面，则深入剖析企业运营、产品创新及用户需求变化。
	#     4、研报目录大纲的所有标题需要全篇风格统一，在风格统一的基础上，请确保生成的一、二、三级标题使用专业术语，体现行业深度；层次分明，逻辑清晰，包含关键研究要素，如背景、分析、趋势、策略、机制、路径等；语言正式，具有权威性和可信度；突出核心主题，注重概括性与精准性。
	#     5、避免新生成的综合研报目录前后重复。
	#     6、同层级的标题往往结构相似（对仗或平行结构），便于读者理解各部分的关系。
	#     7、标题直接揭示该章节的核心内容或研究对象，让读者能快速了解报告结构和重点。
	#     8、确保标题间逻辑关联性。
	#     9、使用Markdown格式。
	#     10、每个目录的标题必须用数字，不要出现"第一部分"，"第一章"等。
	#     11、请直接返回Markdown格式的研报大纲。不要有其他任何多余字符比如```markdown```。

    #     """

    prompt = f"""
    	我会给你两个研报目录，标准目录是根据分析师认为的分析重点得到的初始标准研报目录，历史目录是基于研报标题找到的相关历史研报整理得到的目录。请整合标准研报目录和历史研报生成的目录，确保生成完整的涵盖一、二、三级标题的新的综合目录,具体要求如下。
        标准目录:
        {overview1}
        历史目录: 
        {overview2}
        研报标题: {title}
        研报目的:{purpose}

        任务要求：
    1. 格式规范：
       - Markdown 内容未以一级标题 (#) 开始
       - 严格使用Markdown格式（## 二级标题，### 三级标题，#### 四级标题）
       - 跳过# 一级标题，即研报标题，直接从## 二级标题开始输出
       - 确保标题编号使用数字（如1，1.1, 1.1.1）
       - 输出纯Markdown内容，不包含任何解释或标识符
       - 必须保留#符号作为Markdown标题标识符
       - 确保每个标题都以#开头，后跟空格和标题内容
       - 不要返回'---'这种分隔线
       - 请直接返回Markdown格式的研报大纲。不要有其他任何多余字符比如```markdown```。
    2. 优先级处理：
       - 优先考虑与{title}所属行业及关键词最相关的输入数据
       - 在引用输入数据生成新的目录时，优先采用索引靠前的表述，特别是前5个索引的内容
    3. 内容要求：
        -请你根据研报标题和研报目的，以标准目录为基准，将符合标准的历史目录标题整合到标准目录标题中。整合时必须同时符合四个原则：一，保持综合目录的整体结构、标题的语言风格与标准目录一致，可以适当调整用词使标题内容具体、聚焦主题；二，如果历史目录中的某些标题与{title}所属行业相关度极低，则禁止用于整合；三，如果某个历史目录标题的行业相关度高，需要最先判断该标题是否契合标准目录的某个二级标题的内容（##），仅在契合时将其整合到二级标题下，如果根据历史目录新增二级标题，需要保证加入该二级标题后整个综合目录的结构合理、逻辑顺畅；四，如果确定某个历史目录标题可以整合进标准目录，且该历史目录标题带有观点，则可以生成有观点的综合目录标题，如果该历史目录标题无观点，禁止模型自己发挥生成带观点的综合目录标题，保持原有的标准目录陈述性标题风格即可；
        -如果历史目录标题包含时间、区域信息，要确保历史目录标题包含的时间、区域等信息与标准目录一致时再对两个目录对应内容进行整合。
        -聚焦研报目的，例如若是投资人导向，应强化数据、估值与趋势；若是产业客户导向，应突出痛点、解决路径与落地建议。
        -大纲需契合{title}的行业特性，如能源报告应强调政策与上下游协同，消费报告更强调用户分层与场景等，请检查核对，确保目录中不要提到该标题所属的行业中不常用的指标、名词、术语等。
        -降低包含定量模型的二级标题（如盈利测算、投资回报等）及其子标题在目录中的占比，同时确保标题数量符合限制要求。
        -各级标题的设置应遵循MECE原则（相互独立、完全穷尽），例如产业链分析一定要包括上中下游分析；不要存在重复的主题，并结合研报标题、两个目录灵活调整，具体问题具体分析
        -任何级别标题中禁止出现数字、数据（包含中文数字，例如一二三等），但可以包含年份。
    4. 结构、逻辑要求
        -层次清晰、逻辑严密，整体目录结构体现“总—分—点”思路，按“背景—分析—结论—建议”等清晰展开；
        -确保各个二级标题以及其下级标题间的逻辑衔接顺畅，递进逻辑清晰，从大范围到小范围、宏观中观到微观、从现象到原因、从问题到建议，层层递进、环环相扣，展现清晰的内容故事线设计，具体逻辑需要根据研报标题、关键词进行调整。
        -每一个子标题要围绕上级标题的主题来撰写，覆盖该子标题所属的上级标题的所有关键要点，保证二三四级标题逻辑顺畅
    5. 语言风格要求：
        -标题精炼，保证各级标题用词专业，使用研报标题所属行业的术语
        -语言风格与行业一致，避免使用泛用词或模糊词（如“可能”、“或许”、“比较多”等）。
        -禁止使用括号作为补充性内容，并通过重新组织语言将括号中直接显示在标题中，确保逻辑清晰、无歧义。
        -语言风格与标准目录保持一致，无需参考历史目录的语言风格，并确保标题内容足够具体、聚焦主题，例如：“跨国供应链协同的成本竞争力提升路径”

        """
    try:
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的研报分析师,擅长总结和整合研报内容。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )

        overview = completion.choices[0].message.content

        # 计算成本
        input_cost = (completion.usage.prompt_tokens / 1000) * 0.0024
        output_cost = (completion.usage.completion_tokens / 1000) * 0.0096
        cost = input_cost + output_cost

        return overview, cost

    except Exception as e:
        print(f"生成研报总结时出错: {e}")
        return "", 0.0



def overview_conclusion_stream(overview1, overview2, title, purpose):
    """
    对两个研报进行总结生成一个有三级标题的研报

    Args:
        overview1 (str): 第一个研报目录
        overview2 (str): 第二个研报目录
        title (str): 研报标题

    Returns:
        str: 生成的研报内容
        float: API调用成本
    """
    """
        对两个研报进行总结生成一个有三级标题的研报

        Args:
            overview1 (str): 第一个研报目录
            overview2 (str): 第二个研报目录
            title (str): 研报标题

        Returns:
            str: 生成的研报内容
            float: API调用成本
        """


    prompt = f"""
    	我会给你两个研报目录，标准目录是根据分析师认为的分析重点得到的初始标准研报目录，历史目录是基于研报标题找到的相关历史研报整理得到的目录。请整合标准研报目录和历史研报生成的目录，确保生成完整的涵盖一、二、三级标题的新的综合目录,具体要求如下。
        标准目录:
        {overview1}
        历史目录: 
        {overview2}
        研报标题: {title}
        研报目的:{purpose}

        任务要求：
    1. 格式规范：
       - 严格使用Markdown格式（## 二级标题，### 三级标题，#### 四级标题）
       - 跳过# 一级标题，即研报标题，直接从## 二级标题开始输出
       - 确保标题编号使用数字（如1，1.1, 1.1.1）
       - 输出纯Markdown内容，不包含任何解释或标识符
       - 必须保留#符号作为Markdown标题标识符
       - 确保每个标题都以#开头，后跟空格和标题内容
       - 不要返回'---'这种分隔线
       - 请直接返回Markdown格式的研报大纲。不要有其他任何多余字符比如```markdown```。
    2. 优先级处理：
       - 优先考虑与{title}所属行业及关键词最相关的输入数据
       - 在引用输入数据生成新的目录时，优先采用索引靠前的表述，特别是前5个索引的内容
    3. 内容要求：
        -请你根据研报标题和研报目的，以标准目录为基准，将符合标准的历史目录标题整合到标准目录标题中。整合时必须同时符合四个原则：一，保持综合目录的整体结构、标题的语言风格与标准目录一致，可以适当调整用词使标题内容具体、聚焦主题；二，如果历史目录中的某些标题与{title}所属行业相关度极低，则禁止用于整合；三，如果某个历史目录标题的行业相关度高，需要最先判断该标题是否契合标准目录的某个二级标题的内容（##），仅在契合时将其整合到二级标题下，如果根据历史目录新增二级标题，需要保证加入该二级标题后整个综合目录的结构合理、逻辑顺畅；四，如果确定某个历史目录标题可以整合进标准目录，且该历史目录标题带有观点，则可以生成有观点的综合目录标题，如果该历史目录标题无观点，禁止模型自己发挥生成带观点的综合目录标题，保持原有的标准目录陈述性标题风格即可；
        -如果历史目录标题包含时间、区域信息，要确保历史目录标题包含的时间、区域等信息与标准目录一致时再对两个目录对应内容进行整合。
        -聚焦研报目的，例如若是投资人导向，应强化数据、估值与趋势；若是产业客户导向，应突出痛点、解决路径与落地建议。
        -大纲需契合{title}的行业特性，如能源报告应强调政策与上下游协同，消费报告更强调用户分层与场景等，请检查核对，确保目录中不要提到该标题所属的行业中不常用的指标、名词、术语等。
        -降低包含定量模型的二级标题（如盈利测算、投资回报等）及其子标题在目录中的占比，同时确保标题数量符合限制要求。
        -各级标题的设置应遵循MECE原则（相互独立、完全穷尽），例如产业链分析一定要包括上中下游分析；不要存在重复的主题，并结合研报标题、两个目录灵活调整，具体问题具体分析
        -任何级别标题中禁止出现数字、数据（包含中文数字，例如一二三等），但可以包含年份。
    4. 结构、逻辑要求
        -层次清晰、逻辑严密，整体目录结构体现“总—分—点”思路，按“背景—分析—结论—建议”等清晰展开；
        -确保各个二级标题以及其下级标题间的逻辑衔接顺畅，递进逻辑清晰，从大范围到小范围、宏观中观到微观、从现象到原因、从问题到建议，层层递进、环环相扣，展现清晰的内容故事线设计，具体逻辑需要根据研报标题、关键词进行调整。
        -每一个子标题要围绕上级标题的主题来撰写，覆盖该子标题所属的上级标题的所有关键要点，保证二三四级标题逻辑顺畅
    5. 语言风格要求：
        -标题精炼，保证各级标题用词专业，使用研报标题所属行业的术语
        -语言风格与行业一致，避免使用泛用词或模糊词（如“可能”、“或许”、“比较多”等）。
        -禁止使用括号作为补充性内容，并通过重新组织语言将括号中直接显示在标题中，确保逻辑清晰、无歧义。
        -语言风格与标准目录保持一致，无需参考历史目录的语言风格，并确保标题内容足够具体、聚焦主题，例如：“跨国供应链协同的成本竞争力提升路径”

        """
    try:
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的研报分析师,擅长总结和整合研报内容。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            stream=True
        )

        # 用于存储累积的内容
        accumulated_content = ""
        
        for chunk in completion:
            if chunk.choices[0].delta.content is not None:
                # 获取新的内容片段
                new_content = chunk.choices[0].delta.content
                # 更新累积的内容
                accumulated_content += new_content
                # 只yield新的内容片段
                yield {
                    'type': 'content',
                    'content': new_content,
                    'is_final': False
                }

        # 最后yield完整的内容，不再计算成本
        if accumulated_content:
            yield {
                'type': 'final',
                'content': accumulated_content,
                'is_final': True
            }

    except Exception as e:
        logger.error(f"生成研报总结时出错: {e}", exc_info=True)
        yield {
            'type': 'error',
            'content': f"生成研报总结时出错: {str(e)}",
            'is_final': True
        }

    
def generate_analysis_methods(current_concat_heading):
    """
    根据当前标题生成分析思路
    
    参数:
        current_concat_heading (str): 当前标题

    返回:
        str: 生成的分析思路
        float: API调用成本
        """

    prompt = f"""
    请根据以下标题生成详细的分析思路:
    
    三级标题的拼接: {current_concat_heading},最左边的是一级标题，中间的是二级标题，右边的是三级标题
    
    要求:
    1. 根据三级标题，结合三级标题的上级标题（对应的二级标题和一级标题）生成三级标题分析思路，以指导正文的生成，输出内容中不得出现“一级标题”“二级标题”“三级标题”等类似表述，而是直接输出下文需要怎么分析，无需任何前置描述或解释性文字。确保输出内容聚焦于具体建议，避免对标题层级关系的分析或说明。输出内容必须紧扣主题，不得偏离主线逻辑。
    3. 分析思路应当简洁明了，不超过100字
    4. 输出格式必须为严格的JSON格式，包含一个"analysis"字段
    5. 示例输出格式：{{"analysis": "分析思路内容"}}
    6. 仅返回JSON格式内容，不包含任何其他字符或解释
    """

    try:
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的行业分析师，严格按照要求生成JSON格式的分析思路。"},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.6
        )
        time.sleep(0.1)
        # 验证返回格式
        analysis_methods = completion.choices[0].message.content
        if not analysis_methods.startswith('{') or not analysis_methods.endswith('}'):
            raise ValueError("返回格式不符合JSON要求")
        
        # 计算成本
        input_cost = (completion.usage.prompt_tokens / 1000) * 0.0024
        output_cost = (completion.usage.completion_tokens / 1000) * 0.0096
        cost = input_cost + output_cost
        
        return analysis_methods, cost
        
    except Exception as e:
        print(f"生成分析思路时出错: {e}")
        return '{"analysis": ""}', 0.0

def conclude_from_ic_trend_score(analysis_of_ic_trend):
    """
    根据ic_trend_score指标数据总结行业景气度，优化后的版本

    返回:
        dict: 包含各行业景气度判断及整体行业景气度
        float: 计算成本
    """
    # 定义默认返回值
    default_result = {
        "supply_demand": {"level": "", "reason": ""},
        "capital_market": {"level": "", "reason": ""},
        "policy_direction": {"level": "", "reason": ""}
    }

    # 输入数据校验
    if not analysis_of_ic_trend:
        print("警告：没有提供行业景气度分析数据")
        return default_result, 0.0

    # 优化后的提示词模板
    prompt_template = """
    <任务目标>
    你是一个专业的首席行业分析师，擅长分析行业景气度，你有3个子模型分析师，分别负责分析供需价格景气度模型supply_demand、政策风向景气度模型policy_direction、资本市场景气度模型capital_market，每个模型都包含研究主题相关的几个行业的模型结果。我会给你这3个模型的整体趋势分析、季度表现、主要景气等级，你需要综合这些行业的景气度，给出整体行业景气度的判断（高/中/低），并结合这三个模型对应的描述，对研究主题相关的这几个行业做出综合的模型结果解读reason。
    1. 重点关注行业的整体趋势分析、季度表现、主要景气等级；
    2、如果没有返回模型结果，则不需要解读输出reason，返回空字符串即可；
    3. 解读模型结果时需要融入模型描述；
    4. 结果解读reason中不要有数字，仅定性解读即可，并且一定要方式cics_name；
    5. 输出格式必须为严格的JSON格式.
    <model description>
    1. 供需价格景气度模型(supply_demand)：通过分析行业的产销量和盈利趋势，衡量行业在供需关系下的经营状况
    2. 政策风向景气度模型(policy_direction) ：量化政策对行业的支持或限制程度，评估政策环境对行业发展的影响
    3. 资本市场景气度模型(capital_market)：通过股债市及投融资数据，判断资本市场对行业的资金支持和融资环境
    
    <分析要求>
    1. 对每个模型给出景气度等级（高/中/低）
    2. 结合模型特点进行定性分析，形成reason
    3. 若无数据，返回空字符串
    4. 输出严格遵循JSON格式

    <输出示例>
    {{
        "supply_demand": {{
            "level": "高/中/低",
            "reason": "分析理由"
        }},
        "capital_market": {{
            "level": "高/中/低", 
            "reason": "分析理由"
        }},
        "policy_direction": {{
            "level": "高/中/低",
            "reason": "分析理由"
        }}
    }}

    以下是行业景气度分析结果：
    {analysis}
    """

    # 配置重试机制
    max_retries = 3
    retry_delay = 1  # 重试间隔时间（秒）

    for retry_count in range(max_retries):
        try:
            # 构建完整提示词
            prompt = prompt_template.format(analysis=analysis_of_ic_trend)

            # 调用模型
            completion = qwen_client.chat.completions.create(
                model="qwen-max-latest",
                messages=[
                    {"role": "system", "content": "你是专业的行业分析师，请严格按照要求生成JSON格式的行业景气度分析。"},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.1  # 降低随机性
            )

            # 解析结果
            analysis_result = json.loads(completion.choices[0].message.content)

            # 计算成本
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.0024
            output_cost = (completion.usage.completion_tokens / 1000) * 0.0096
            cost = input_cost + output_cost

            return analysis_result, cost

        except Exception as e:
            print(f"第{retry_count + 1}次尝试失败: {str(e)}")
            if retry_count < max_retries - 1:
                time.sleep(retry_delay)  # 重试前等待

    # 所有重试失败后返回默认值
    print(f"达到最大重试次数{max_retries}次，返回默认值")
    return default_result, 0.0

def get_potential_ic_trend_labels(query_text):
    try:
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "分析当前query_text（标题+写作思路），判断撰写该标题是否需要以下五个指标：供需景气度(supply_demand)、资本市场景气度(capital_market)、政策方向景气度(policy_direction)、盈利(profitability_cat)、杠杆(financial_cat)。返回需要的指标英文名,以json的形式返回数据，输出格式样例：{labels:['label1','label2']}"},
                {"role": "user", "content": query_text}
            ],
            response_format={"type": "json_object"},
            temperature=0.6
        )
        
        # 解析返回的指标
        result = json.loads(completion.choices[0].message.content)
        return result.get("labels", [])
        
    except Exception as e:
        print(f"分析query_text时出错: {e}")
        return ["supply_demand", "capital_market", "policy_direction", "profitability_cat", "financial_cat"]



def filter_ic_current_rating(ic_current_rating, potential_ic_trend_labels):
    """
    筛选ic_current_rating中的列，只保留基础字段和potential_ic_trend_labels中存在的字段
    
    参数:
    ic_current_rating (list): 包含评级数据的字典列表
    potential_ic_trend_labels (list): 需要保留的字段列表
    
    返回:
    list: 筛选后的字典列表
    """
    # 定义需要保留的基础字段
    base_fields = ['id', 'cics_id', 'cics_name', 'year', 'quarter']
    
    # 检查每个potential label是否存在于数据中
    # 获取数据中第一条记录的所有字段名
    if ic_current_rating:
        available_fields = set(ic_current_rating[0].keys())
        # 只保留实际存在的字段
        valid_trend_labels = [label for label in potential_ic_trend_labels if label in available_fields]
        
        # 将基础字段和存在的trend labels合并为完整的保留字段列表
        keep_fields = base_fields + valid_trend_labels
        
        # 筛选字段
        filtered_rating = []
        for rating in ic_current_rating:
            filtered_item = {k: v for k, v in rating.items() if k in keep_fields}
            filtered_rating.append(filtered_item)
        
        return filtered_rating
    
    return []
def conclude_from_cat_analysis(cat_analysis):
    """
    根据cat_analysis中的分类数据总结行业景气度

    返回:
        dict: 包含盈利能力分类和财务杠杆分类的判断
    """
    # 检查输入数据是否为空
    if not cat_analysis:
        return {
            "profitability_cat": {
                "level": "",
                "reason": ""
            },
            "financial_cat": {
                "level": "",
                "reason": ""
            }
        }, 0.0

    # 转义JSON中的特殊字符
    try:
        escaped_data = json.dumps(cat_analysis, ensure_ascii=False).replace('{', '{{').replace('}', '}}')
    except Exception as e:
        print(f"处理分类数据时出错: {e}")
        escaped_data = "[]"
    
    # 将prompt定义放在try块外部，确保在except块中也能访问
    prompt = f"""
    你是一个专业的首席行业分析师，擅长分析行业的盈利水平和杠杆水平，你有2个子模型分析师，分别负责分析行业盈利能力、行业杠杆水平，每个模型都包含研究主题相关的几个行业的模型结果，我会给你这2个模型在不同行业的模型结果及评估结果时的年度和季度。你需要综合这些行业的盈利水平和杠杆水平，给出整体行业level判断，并结合这2个模型对应的描述，对研究主题相关的这几个行业做出综合的模型结果解读reason。
    分析思路：
        1. 如果输入数据中没有"profitability_cat"或"financial_cat"字段，直接返回对应字段下空字符串
        2. 解读模型结果时融入模型简要描述
        3. 分析各季度分类结果的变化趋势
        4. 输出格式必须为严格的JSON格式，包含以下字段：
           - "profitability_cat": 行业盈利能力判断
           - "financial_cat": 行业杠杆水平判断
    模型描述
    行业盈利能力：反映行业企业整体的赚钱能力和利润水平；
    行业杠杆水平：衡量行业企业的负债压力与偿债能力，反映其财务稳定性和风险。
    
    示例输出格式（注意转义字符）：
    {{
        "profitability_cat": {{
            "level": "靠前/中游/靠后",
            "reason": "整体分析理由"
        }},
        "financial_cat": {{
            "level": "靠前/中游/靠后", 
            "reason": "整体分析理由"
        }}
    }}  
    以下是行业分类数据：
    {escaped_data}
    """

    try:
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的财务分析师，严格按照要求生成JSON格式的分类分析。"},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.1
        )

        # 验证返回格式
        analysis_result = completion.choices[0].message.content
        if not analysis_result.strip().startswith('{') or not analysis_result.strip().endswith('}'):
            raise ValueError("返回格式不符合JSON要求")

        # 解析结果并验证必要字段
        result = json.loads(analysis_result)
        for key in ["profitability_cat", "financial_cat"]:
            if key not in result:
                result[key] = {"level": "", "reason": ""}
            elif not isinstance(result[key], dict):
                result[key] = {"level": str(result[key]), "reason": ""}

        # 计算成本
        input_cost = (completion.usage.prompt_tokens / 1000) * 0.0024
        output_cost = (completion.usage.completion_tokens / 1000) * 0.0096
        cost = input_cost + output_cost

        return result, cost

    except Exception as e:
        print(f"生成分类分析时出错: {e}")
        return {
            "profitability_cat": {
                "level": "",
                "reason": ""
            },
            "financial_cat": {
                "level": "",
                "reason": ""
            }
        }, 0.0


def tuning_third_heading(reference, writing_instructions, current_title, topic=None):
    """
    根据参考内容生成详细的结构化信息摘要报告，并基于报告微调当前标题

    参数:
        reference (dict): 包含参考内容和调整建议的JSON对象
        writing_instructions (str): 分析思路和指导
        current_title (str): 当前需要调整的标题

    返回:
        tuple: (调整后的标题, 结构化信息摘要报告, 成本)
    """
    # 第一步：生成结构化信息摘要报告
    report_prompt = f"""
    你是一个专业的行业分析师，请根据以下参考内容生成一份详细的结构化信息摘要报告。报告应包括以下部分：

    1. 行业概况
        - 基于report_source中的研报信息，总结行业现状和发展趋势
        - 结合industry_indicator_part_1和industry_indicator_part_2，分析行业景气度

    2. 政策影响
        - 根据policy_source中的政策信息，分析政策对行业的影响
        - 评估政策实施效果和未来政策方向

    3. 产业链分析
        - 基于indicators中的产业链数据，分析上下游关系
        - 评估产业链各环节的竞争力和发展潜力

    4. 投资建议
        - 综合以上分析，给出行业投资建议
        - 识别潜在风险和机会

    报告要求：
    - 使用结构化格式，每个部分使用二级标题
    - 数据引用需注明来源
    - 分析需有数据支撑
    - 结论需清晰明确
    - 必须严格符合研报主旨
    参考内容：
    {reference}
    分析思路：
    {writing_instructions}
    研报主旨：
    {topic}

    """

    try:
        # 生成摘要报告
        report_completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的行业分析师，能够生成结构化的信息摘要报告。"},
                {"role": "user", "content": report_prompt}
            ],
            temperature=0.2
        )
        report_content = report_completion.choices[0].message.content

        # 第二步：基于摘要报告微调标题
        tuning_prompt = f"""
        你是一个专业的标题优化专家，请根据以下信息优化当前的三级标题：

        当前拼接的一级标题+二级标题+三级标题：{current_title}
        相关信息摘要：
        {report_content}

        优化要求：
        1、根据当前拼接的一级标题、二级标题、三级标题以及分析思路的内容，在相关信息摘要中选择与这些内容的主题和行业相关的信息优化对应的三级标题，如果相关信息摘要中没有相关的内容则不要改变原三级标题；同时，优化时不要改变原标题的核心主题，不要剔除关键要点，因为这些是分析师主要关注的内容，你仅需要在原标题的基础上结合相关主题和行业的信息摘要让三级标题更能体现观点，而不只是描述一个现象，例如：分析师更偏好"xx政策驱动下，企业xx意愿持续增强"这种带有观点的三级标题，而不是"xx政策对企业影响分析"；
        2.保持标题简洁精炼（15-25字），准确反映行业现状和关键趋势；
        3.注意不要在三级标题中显示任何指标数据，仅输出定性标题内容；
        4.一定要保留当前表的数字序号（比如1.1.1，1.1.2等）；
        5.如果topic参数非空，优化后的标题必须符合{topic}的主题，如果在相关信息中有无关的摘要信息，则忽略。
        请直接返回优化后的三级标题，不要包含其他内容。以json形式返回标题。返回格式为：{{"title":"优化后的标题"}}
        """

        tuning_completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的标题优化专家，能够根据内容优化标题。"},
                {"role": "user", "content": tuning_prompt}
            ],
            temperature=0.8,
            response_format={"type": "json_object"},

        )
        tuned_title = tuning_completion.choices[0].message.content
        tuned_title = json.loads(tuned_title)
        tuned_title = tuned_title.get("title", current_title)
        # 计算总成本
        input_cost = (report_completion.usage.prompt_tokens + tuning_completion.usage.prompt_tokens) / 1000 * 0.0024
        output_cost = (
                                  report_completion.usage.completion_tokens + tuning_completion.usage.completion_tokens) / 1000 * 0.0096
        total_cost = input_cost + output_cost

        return tuned_title, report_content, total_cost

    except Exception as e:
        print(f"生成报告或优化标题时出错: {e}")
        return current_title, "", 0.0

def tuning_second_heading(third_level_headings, original_second_level_heading, topic=None):
    """
    根据三级标题内容优化二级标题

    Args:
        third_level_headings (list): 三级标题列表
        original_second_level_heading (str): 原始二级标题

    Returns:
        str: 优化后的二级标题
    """
    try:
        # 构建提示词
        prompt = f"""
        你是一个专业的标题优化专家，请根据以下信息优化当前的二级标题：

        当前二级标题：{original_second_level_heading}
        相关三级标题内容：
        {third_level_headings}

        优化要求：
        1. 对所有相关三级标题的核心内容进行概括和提炼，优化当前二级标题
        2. 保持标题简洁（10-20字），准确反映整体内容
        3. 必须包含核心关键词
        4. 突出最重要的主题和方向
        5. 保留原标题合理部分，仅对需要强化的要素进行调整，并且保留当前二级标题的语言风格
        6. 一定要保留当前标题的数字序号（比如1.1，1.2等）
        7. 如果topic参数非空，优化后的标题必须符合{topic}的主题，如果在相关信息中有无关的摘要信息，则忽略。

        请直接返回优化后的标题，不要包含其他内容。以json形式返回标题。返回格式为：{{"new_second_heading":"优化后的标题"}}
        """

        # 调用Qwen模型进行优化
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的标题优化专家，能够根据内容优化标题。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.6,
            response_format={"type": "json_object"}
        )

        # 解析并返回优化后的标题
        optimized_title = json.loads(completion.choices[0].message.content)
        return optimized_title.get("new_second_heading", original_second_level_heading)

    except Exception as e:
        print(f"优化二级标题时出错: {e}")
        return original_second_level_heading


def tuning_first_heading(second_level_headings, original_first_level_heading, topic=None):
    """
    根据二级标题内容优化一级标题

    Args:
        third_level_headings (list): 二级标题列表
        original_second_level_heading (str): 原始一级标题

    Returns:
        str: 优化后的二级标题
    """
    try:
        # 构建提示词
        prompt = f"""
        你是一个专业的标题优化专家，请根据以下信息优化当前的一级标题：

        当前一级标题：{original_first_level_heading}
        相关二级标题内容：
        {second_level_headings}

        优化要求：
        1. 对所有相关二级标题的核心内容进行概括和提炼，优化当前一级标题
        2. 保持标题简洁（10-20字），准确反映整体内容
        3. 必须包含核心关键词
        4. 突出最重要的主题和方向
        5. 保留原标题合理部分，仅对需要强化的要素进行调整，并且保留当前一级标题的语言风格
        6. 一定要保留当前标题的数字序号
        7. 如果topic参数非空，优化后的标题必须符合{topic}的主题，如果在相关信息中有无关的摘要信息，则忽略。
        请直接返回优化后的标题，不要包含其他内容。以json形式返回标题。返回格式为：{{"new_second_heading":"优化后的标题"}}
        """

        # 调用Qwen模型进行优化
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的标题优化专家，能够根据内容优化标题。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.6,
            response_format={"type": "json_object"}
        )

        # 解析并返回优化后的标题
        optimized_title = json.loads(completion.choices[0].message.content)
        return optimized_title.get("new_second_heading", original_first_level_heading)

    except Exception as e:
        print(f"优化一级标题时出错: {e}")
        return original_first_level_heading

def year_extract_from_title(title):
    """
    从标题中提取年份信息，返回单个年份数字
    
    Args:
        title (str): 输入标题
        
    Returns:
        int: 提取出的年份数字，如果未找到则返回None
    """
    try:
        # 构建提示词
        # current_year = datetime.now().year

        prompt = f"""
        你是一个专业的年份提取工具，请从以下标题中提取最主要的年份信息：
        
        标题：{title}

        要求：
        1. 识别标题中最主要的年份（如2023、2024等）
        2. 年份格式为4位数字
        3. 如果标题中没有年份，返回{2024}
        4. 以json格式返回结果，格式为：{{"year": 年份数字}}
        5. 如果标题中包含年份范围（如2021-2023），优先返回最新年份
        6. 返回的年份必须是数字类型，不是字符串

        请直接返回结果，不要包含其他解释。
        """

        # 调用Qwen模型进行年份提取
        completion = qwen_client.chat.completions.create(
            model="qwen-plus",
            messages=[
                {"role": "system", "content": "你是一个专业的研报撰写专家，你会从研报的标题判断出需要的具体年份，并返回单个年份数字。例如：如果标题是'2023年新能源汽车出海趋势'，那么返回的就是2023。如果标题是'2021年-2024年的消费洞察'，那么返回的就是2024。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            response_format={"type": "json_object"}
        )

        # 解析并返回年份数字
        result = json.loads(completion.choices[0].message.content)
        year = result.get("year")
        return year  # 直接返回数字

    except Exception as e:
        print(f"提取年份时出错: {e}")
        return None
    
def get_ana_instruction_for_first_level(second_level_titles, first_level_title):
    """
    基于所有的二级标题和当前的一级标题，生成分析思路
    
    Args:
        second_level_titles (list): 二级标题列表
        first_level_title (str): 一级标题
        
    Returns:
        str: 分析思路说明
    """
    try:
        # 构建提示词
        prompt = f"""
        你是一个专业的研报分析专家，请基于以下信息生成分析思路：

        一级标题：{first_level_title}
        二级标题列表：{second_level_titles}

        要求：
        1. 分析一级标题与二级标题之间的逻辑关系，说明一级标题如何指导生成其下级的二级标题，输出内容中不得出现“一级标题”“二级标题”“三级标题”等类似表述，而是直接输出下文需要怎么分析，无需任何前置描述或解释性文字。确保输出内容聚焦于具体建议，避免对标题层级关系的分析或说明。输出内容必须紧扣主题，不得偏离主线逻辑。
        2、 分析思路需要确保行业、主题相关性
        3. 指出可能的数据支持点和论证方向
        4. 分析思路应当简洁明了，不超过100字
        5. 一级标题分析思路要基于一级标题、对应的所有二级标题，不要凭空生成任何内容
        
        请直接JSON形式返回分析思路，不要包含其他解释。
        返回格式为：{{"ana_instruction":"分析思路"}}
        """

        # 调用Qwen模型生成分析思路
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的研报分析专家，擅长提供研究报告的分析思路和建议。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            response_format={"type": "json_object"}
        )
        
        # 解析JSON响应并返回分析思路
        result = json.loads(completion.choices[0].message.content)
        return result.get("ana_instruction", "")
        
    except Exception as e:
        print(f"生成分析思路时出错: {e}")
        return ""


def generate_ana_instruction(all_titles):
    """
    基于所有的三级标题和当前的二级标题，生成分析思路

    Args:
        third_level_titles (list): 三级标题列表
        current_second_level_title (str): 二级标题

    Returns:
        str: 分析思路说明
    """
    try:
        # 构建提示词
        prompt = f"""
        你是一个专业的研报分析专家，请基于以下信息生成分析思路：

        标题列表：{all_titles}
        要求：
    
        1. 提出完善这部分内容的分析思路和建议
        2. 指出可能的数据支持点和论证方向
        3. 分析思路应当简洁明了，不超过100字

        请直接JSON形式返回分析思路，不要包含其他解释。
        返回格式为：{{"ana_instruction":"分析思路"}}
        """

        # 调用Qwen模型生成分析思路
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的研报分析专家，擅长提供研究报告的分析思路和建议。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            response_format={"type": "json_object"}
        )

        # 解析JSON响应并返回分析思路
        result = json.loads(completion.choices[0].message.content)
        return result.get("ana_instruction", "")

    except Exception as e:
        print(f"生成分析思路时出错: {e}")
        return ""

def get_ana_instruction_for_second_level(third_level_titles, current_second_level_title):
    """
    基于所有的三级标题和当前的二级标题，生成分析思路

    Args:
        third_level_titles (list): 三级标题列表
        current_second_level_title (str): 二级标题

    Returns:
        str: 分析思路说明
    """
    try:
        # 构建提示词
        prompt = f"""
                你是一个专业的研报分析专家，请基于以下信息生成分析思路：

                二级标题：{current_second_level_title}
                三级标题列表：{third_level_titles}

                要求：
        1. 分析二级标题与三级标题之间的逻辑关系，说明二级标题如何指导生成其下级的三级标题，输出内容中不得出现“一级标题”“二级标题”“三级标题”等类似表述，而是直接输出下文需要怎么分析，无需任何前置描述或解释性文字。确保输出内容聚焦于具体建议，避免对标题层级关系的分析或说明。输出内容必须紧扣主题，不得偏离主线逻辑。
        2. 分析思路需要确保行业、主题相关性
        3. 指出可能的数据支持点和论证方向
        4. 分析思路应当简洁明了，不超过100字
        5. 二级标题分析思路要基于该二级标题、对应的所有三级标题生成，不要凭空生成任何内容

                请直接JSON形式返回分析思路，不要包含其他解释。
                返回格式为：{{"ana_instruction":"分析思路"}}
                """

        # 调用Qwen模型生成分析思路
        completion = qwen_client.chat.completions.create(
            model="qwen-max-latest",
            messages=[
                {"role": "system", "content": "你是一个专业的研报分析专家，擅长提供研究报告的分析思路和建议。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            response_format={"type": "json_object"}
        )

        # 解析JSON响应并返回分析思路
        result = json.loads(completion.choices[0].message.content)
        return result.get("ana_instruction", "")

    except Exception as e:
        print(f"生成分析思路时出错: {e}")
        return ""

    # start_time = time.time()
    # result, cost = match_focus_points(title)
    # end_time = time.time()
    # print(f"匹配耗时: {end_time - start_time:.2f}秒")
    # print(result)
    # print(cost)
    # # content, reasoning_content = title_augement(title)
    # # print(json.dumps(content, ensure_ascii=False, indent=2))
    # print(1)
    # title = "AI芯片市场分析"
    # result, cost = semantic_enhancement_agent(title)
    # print(result)
    # print(cost)

    # result, text= title_augement(title)
    # print(result)
    # print(text)
    # focus_points = "一级关注点:市场规模分析(二级关注点:市场规模、二级关注点:市场发展速度)，一级关注点:市场机会评估(二级关注点:市场吸引力/增长潜力)"
    # toc, cost, toc_qwen = generate_toc_from_focus_points(title, focus_points)
    # print(toc)
    # print(cost)
    # print(toc_qwen)


if __name__ == "__main__":
    data = [
        {'id': 57209, 'cics_id': 6, 'profitability_cat': '靠前', 'year': 2024, 'quarter': 'Q1',
         'cics_name': '交通运输设备'},
        {'id': 60289, 'cics_id': 6, 'profitability_cat': '靠前', 'year': 2024, 'quarter': 'Q2',
         'cics_name': '交通运输设备'},
        {'id': 310158, 'cics_id': 6, 'profitability_cat': '靠前', 'year': 2024, 'quarter': 'Q3',
         'cics_name': '交通运输设备'}]

    result,cost = conclude_from_cat_analysis(data)
    print(result)
    # filtered_result_ic_current_rating_temp: [
    #     {'id': 57209, 'cics_id': 6, 'profitability_cat': '靠前', 'year': 2024, 'quarter': 'Q1',
    #      'cics_name': '交通运输设备'},
    #     {'id': 60289, 'cics_id': 6, 'profitability_cat': '靠前', 'year': 2024, 'quarter': 'Q2',
    #      'cics_name': '交通运输设备'},
    #     {'id': 310158, 'cics_id': 6, 'profitability_cat': '靠前', 'year': 2024, 'quarter': 'Q3',
    #      'cics_name': '交通运输设备'}]

    # result = year_extract_from_title("AI芯片市场分析")
    # print(result)

