import time

from Agent.client_manager import qwen_client, silicon_client
import json
import os
from openai import OpenAI
import time
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Tuple

# 假设这些客户端已经在其他地方初始化
from Agent.client_manager import qwen_client, silicon_client




client = silicon_client
qwen_client = qwen_client
from Agent.tool_agents import json_format_agent
def title_augement(title):
    """
    对用户输入的标题进行语义增强，扩展其含义和关键词，生成三组关键词用于研报筛选
    
    Args:
        title (str): 用户输入的原始标题
    
    Returns:
        str: 语义增强后的标题内容，JSON格式
    """
    prompt = f"""
    请对以下研报标题进行语义增强和关键词扩展，目的是用于研报检索和筛选：
    
    标题: {title}
    
    请深入思考这个标题的核心含义、相关领域和可能的研究方向，考虑：
    1. 这个标题的主要研究对象是什么？
    2. 相关的行业、技术或市场领域有哪些？
    3. 可能的研究角度包括哪些方面？
    4. 有哪些相关的上下游产业、政策或趋势？
    
    请以JSON格式提供以下内容：
    1. expanded_title: 扩展后的标题表述（保持原意但更全面）
    2. keywords: 分为三组关键词（每组关键词不超过4个）：
       - core_keywords: 核心必要关键词，与主题直接相关的关键术语
       - domain_keywords: 细分领域关键词，包括相关行业、市场、技术的细分类别
       - focus_keywords: 聚焦领域关键词，包括可能的研究方向、趋势、政策和上下游产业链相关术语
    
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
    try:
        json_content = json.loads(content)
        return json_content, reasoning_content
    except json.JSONDecodeError:
        # 如果解析失败,使用format_agent进行格式化
        formatted_json = json_format_agent(content)
        return formatted_json, reasoning_content
def title_augement_stream(title):
    """
    对用户输入的标题进行语义增强的流式版本，用于API接口的实时输出
    
    Args:
        title (str): 用户输入的原始标题
    
    Returns:
        generator: 生成推理过程和最终结果的流
    """
    prompt = f"""
    请对以下研报标题进行语义增强和关键词扩展，目的是用于研报检索和筛选：
    
    标题: {title}
    
    请深入思考这个标题的核心含义、相关领域和可能的研究方向，考虑：
    1. 这个标题的主要研究对象是什么？
    2. 相关的行业、技术或市场领域有哪些？
    3. 可能的研究角度包括哪些方面？
    4. 有哪些相关的上下游产业、政策或趋势？
    
    请以JSON格式提供以下内容：
    1. expanded_title: 扩展后的标题表述（保持原意但更全面）
    2. keywords: 分为三组关键词（每组关键词不超过4个）：
       - core_keywords: 核心必要关键词，与主题直接相关的关键术语
       - domain_keywords: 细分领域关键词，包括相关行业、市场、技术的细分类别
       - focus_keywords: 聚焦领域关键词，包括可能的研究方向、趋势、政策和上下游产业链相关术语
    
    请直接输出有效的JSON格式，不要包含任何代码块标记（如```json或```）或额外的说明文字。
    确保每组关键词简洁明确，便于后续检索使用，且关键词之间应有明显区别，以提高检索的广度和精确度。
    """
    
    content = ""
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
            yield chunk.choices[0].delta.reasoning_content
            
    # 尝试解析JSON内容
    try:
        json_content = json.loads(content)
        # 输出中间结果
        yield f"\n\n扩展标题: {json_content.get('expanded_title', '')}\n"
        yield "\n关键词:\n"
        yield f"- 核心关键词: {', '.join(json_content.get('keywords', {}).get('core_keywords', []))}\n"
        yield f"- 领域关键词: {', '.join(json_content.get('keywords', {}).get('domain_keywords', []))}\n"
        yield f"- 聚焦关键词: {', '.join(json_content.get('keywords', {}).get('focus_keywords', []))}\n"
        
        # 最后输出完整的JSON结果
        final_result = {
            "new_title": json_content.get('expanded_title', ''),
            "keywords": {
                "core_keywords": json_content.get('keywords', {}).get('core_keywords', []),
                "domain_keywords": json_content.get('keywords', {}).get('domain_keywords', []),
                "focus_keywords": json_content.get('keywords', {}).get('focus_keywords', [])
            }
        }
        yield final_result
        
    except json.JSONDecodeError:
        # 如果解析失败,使用format_agent进行格式化
        formatted_json = json_format_agent(content)
        # 输出中间结果
        yield f"\n\n扩展标题: {formatted_json.get('expanded_title', '')}\n"
        yield "\n关键词:\n"
        yield f"- 核心关键词: {', '.join(formatted_json.get('keywords', {}).get('core_keywords', []))}\n"
        yield f"- 领域关键词: {', '.join(formatted_json.get('keywords', {}).get('domain_keywords', []))}\n"
        yield f"- 聚焦关键词: {', '.join(formatted_json.get('keywords', {}).get('focus_keywords', []))}\n"
        
        # 最后输出完整的JSON结果
        final_result = {
            "new_title": formatted_json.get('expanded_title', ''),
            "keywords": {
                "core_keywords": formatted_json.get('keywords', {}).get('core_keywords', []),
                "domain_keywords": formatted_json.get('keywords', {}).get('domain_keywords', []),
                "focus_keywords": formatted_json.get('keywords', {}).get('focus_keywords', [])
            }
        }
        yield final_result

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
        2. 目录的层级结构和逻辑关系
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
        基于以下研报目录的摘要，生成一个综合性的新目录，以Markdown格式返回：

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
        # 调用DeepSeek-V3 API
        # response = silicon_client.chat.completions.create(
        #     model="Pro/deepseek-ai/DeepSeek-V3",
        #     messages=[
        #         {"role": "system",
        #          "content": "你是一个专业的研究报告分析专家，擅长整合多份研报摘要并生成结构清晰的综合目录。"},
        #         {"role": "user", "content": final_prompt}
        #     ],
        #     temperature=0.1,
        # )
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
    
def match_focus_points(title: str, model: str = "Pro/deepseek-ai/DeepSeek-V3") -> tuple:
    """
    将研报标题与预定义的二级关注点进行匹配
    
    Args:
        title: 研报标题
        model: 使用的模型名称
        
    Returns:
        tuple: (匹配结果列表, API调用成本)
    """
    
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
    
    prompt = f"""
    请分析以下研报标题,判断它与哪些预定义的二级关注点最相关:
    
    标题: {title}
    
    预定义的二级关注点列表:
    {json.dumps(focus_points, ensure_ascii=False, indent=2)}
    
    请返回最相关的3个二级关注点(如果相关度低于50%则不返回),格式如下:
    {{
                "二级关注点": ["二级关注点标签1","二级关注点标签2"....，"..."]
    }}
    
    注意:
    1. 只返回置信度大于70的匹配结果
    2. 最多返回5个匹配结果
    3. 只返回JSON格式结果,不要其他说明文字
    """

    try:
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "你是一个专业的研报分析助手,擅长对研报主题进行分类和匹配。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1
        )
        
        result = json.loads(completion.choices[0].message.content)
        
        # 计算API调用成本
        if model == "Pro/deepseek-ai/DeepSeek-V3":
            input_cost = (completion.usage.prompt_tokens / 1000) * 0.002 
            output_cost = (completion.usage.completion_tokens / 1000) * 0.008
        cost = input_cost + output_cost
        
        return result, cost
        
    except Exception as e:
        print(f"匹配关注点时出错: {e}")
        return {"matches": []}, 0.0



if __name__ == "__main__":
    title = "AI芯片市场分析"
    start_time = time.time()
    result, cost = match_focus_points(title)
    end_time = time.time()
    print(f"匹配耗时: {end_time - start_time:.2f}秒")
    print(result)
    print(cost)
    # content, reasoning_content = title_augement(title)
    # print(json.dumps(content, ensure_ascii=False, indent=2))
    print(1)
    