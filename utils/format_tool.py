from Agent.client_manager import qwen_client
import mistune
import json
import os
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

def markdown_to_json(markdown_data, model='qwen-max-latest'):
    """
    使用Qwen模型将Markdown数据转换为JSON格式
    
    Args:
        markdown_data: 需要转换的Markdown数据
        model (str): 使用的模型名称，默认为'qwen-max-latest'
        
    Returns:
        dict: 转换后的JSON对象
    """
    try:
        # 准备提示词
        prompt = f"""
        请将以下Markdown格式的数据转换为标准JSON格式:
        {markdown_data}
        要求:
        1. 将Markdown的标题层级转换为JSON的嵌套结构
        2. 保留所有原始内容和层级关系
        3. 确保输出是有效的JSON格式
        4. 直接返回JSON对象，不要包含其他说明文字
        """
        
        # 调用Qwen API
        completion = qwen_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "你是一个专业的格式转换工具，能够将Markdown格式转换为标准JSON格式。"},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
        )
        
        # 提取结果
        json_result = completion.choices[0].message.content
        
        # 返回转换的结果
        return {"type": "json_object", "content": json_result}
        
    except Exception as e:
        return {"type": "json_object", "error": f"Markdown转JSON失败: {str(e)}"}

def parse_markdown_toc_to_json(markdown_toc):
    """
    将Markdown格式的目录结构解析为结构化JSON
    
    Args:
        markdown_toc (str): Markdown格式的目录文本
        
    Returns:
        dict: 解析后的JSON结构
    """
    result = {"title": "", "sections": []}
    current_levels = {1: result}  # 用于跟踪每个级别的当前节点
    
    lines = markdown_toc.strip().split('\n')
    
    # 提取主标题
    if lines and not lines[0].startswith('#'):
        result["title"] = lines[0].strip()
        lines = lines[1:]
    elif lines and lines[0].startswith('# '):
        title_line = lines[0][2:].strip()
        # 检查主标题中是否包含来源信息
        if '[来源:' in title_line:
            title_parts = title_line.split('[来源:', 1)
            result["title"] = title_parts[0].strip()
            result["source"] = '[来源:' + title_parts[1]
        else:
            result["title"] = title_line
        lines = lines[1:]
    
    for line in lines:
        line = line.strip()
        if not line or line == '---':  # 跳过空行和分隔线
            continue
            
        # 计算标题级别
        if line.startswith('#'):
            level_count = 0
            for char in line:
                if char == '#':
                    level_count += 1
                else:
                    break
                    
            title_text = line[level_count:].strip()
            
            # 检查是否包含来源信息
            source_info = None
            if '[来源:' in title_text:
                title_parts = title_text.split('[来源:', 1)
                title_text = title_parts[0].strip()
                source_info = '[来源:' + title_parts[1]
            
            # 创建新节点
            new_node = {
                "title": title_text,
                "level": level_count,
            }
            
            if source_info:
                new_node["source"] = source_info
                
            if level_count < 4:  # 如果不是最底层级别，添加子节点列表
                new_node["sections"] = []
            
            # 将节点添加到适当的父节点
            parent_level = max(k for k in current_levels.keys() if k < level_count)
            current_levels[parent_level]["sections"].append(new_node)
            current_levels[level_count] = new_node
    
    return result

def markdown_catalog_to_json(markdown_data):
    """
    将Markdown格式的目录解析为JSON结构，不依赖外部AI模型
    
    Args:
        markdown_data (str): Markdown格式的目录数据
        
    Returns:
        dict: 解析后的JSON结构
    """
    try:
        parsed_json = parse_markdown_toc_to_json(markdown_data)
        return {"type": "json_object", "content": parsed_json}
    except Exception as e:
        return {"type": "json_object", "error": f"Markdown目录解析失败: {str(e)}"}

def markdown_catalog_to_json_with_mistune(markdown_data):
    """
    使用mistune解析Markdown目录结构并转换为JSON
    
    Args:
        markdown_data (str): Markdown格式的目录数据
        
    Returns:
        dict: 解析后的JSON结构
    """
    try:
        # 创建mistune解析器
        markdown_parser = mistune.create_markdown(renderer=None)
        # 解析Markdown获取抽象语法树(AST)
        ast = markdown_parser(markdown_data)
        
        # 处理解析结果，构建结构化JSON
        result = {"title": "", "sections": []}
        current_sections = {0: result["sections"]}
        current_level = 0
        
        # 辅助函数：提取来源ID列表
        def extract_source_ids(source_text):
            # 从 "[来源: 1234, 5678]" 格式提取数字ID列表
            if not source_text or '[来源:' not in source_text:
                return []
            
            # 提取中括号内的内容
            start_idx = source_text.find('[来源:') + 6
            end_idx = source_text.find(']', start_idx)
            if end_idx == -1:  # 如果没有找到结束括号
                source_content = source_text[start_idx:].strip()
            else:
                source_content = source_text[start_idx:end_idx].strip()
            
            # 分割并转换为整数
            source_ids = []
            for id_str in source_content.split(','):
                try:
                    source_ids.append(int(id_str.strip()))
                except ValueError:
                    # 如果无法转换为整数，跳过
                    continue
            
            return source_ids
        
        for token in ast:
            if token["type"] == "heading":
                level = token["level"]
                text = token["text"]
                
                # 处理一级标题作为主标题
                if level == 1 and not result["title"]:
                    # 检查是否包含来源信息
                    if '[来源:' in text:
                        title_parts = text.split('[来源:', 1)
                        result["title"] = title_parts[0].strip()
                        source_text = '[来源:' + title_parts[1]
                        result["source"] = extract_source_ids(source_text)
                    else:
                        result["title"] = text
                    continue
                
                # 处理其他标题级别
                node = {"title": text, "level": level}
                
                # 检查标题中是否包含来源信息
                if '[来源:' in text:
                    title_parts = text.split('[来源:', 1)
                    node["title"] = title_parts[0].strip()
                    source_text = '[来源:' + title_parts[1]
                    node["source"] = extract_source_ids(source_text)
                
                # 为非叶子节点添加子节点列表
                if level < 4:  # 假设4级以下的标题可能有子节点
                    node["sections"] = []
                
                # 确定应该将节点添加到哪个父节点
                while current_level >= level:
                    current_level -= 1
                
                # 添加到适当的父节点
                current_sections[current_level].append(node)
                
                # 更新当前级别和节点列表
                current_level = level
                if "sections" in node:
                    current_sections[level] = node["sections"]
        
        return {"type": "json_object", "content": result}
    
    except Exception as e:
        return {"type": "json_object", "error": f"使用mistune解析Markdown失败: {str(e)}"}
def search_policy_relation(query_text, size=15, boost=3, field="public_sc_policy_relation_industry"):
    """
    搜索政策关系数据，并返回public_sc_policy_relation_id的值

    Args:
        query_text (str): 搜索关键词
        size (int): 返回结果数量
        boost (int): 搜索权重
        field (str): 搜索字段，默认为"public_sc_policy_relation_industry"

    Returns:
        list: 包含每个结果的public_sc_policy_relation_id值的列表（已去重）
    """
    # 从环境变量加载ES服务器信息
    load_dotenv()
    es_host = os.getenv("ES_HOST", "http://172.31.137.25:9200")
    es_index = os.getenv("ES_INDEX", "search-sc_policy_relation")
    es_user = os.getenv("ES_USER", "elastic")
    es_password = os.getenv("ES_PASSWORD", "es@c012025")

    # 构建查询
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            field: {
                                "query": query_text,
                                "boost": boost,
                                "operator": "and"
                            }
                        }
                    },
                    {
                        "multi_match": {
                            "query": query_text,
                            "fields": [field, "public_sc_policy_relation_title^2"],
                            "type": "best_fields",
                            "tie_breaker": 0.3,
                            "minimum_should_match": "75%"
                        }
                    }
                ],
                "minimum_should_match": 1,
                "boost": 1.0
            }
        },
        "size": size,
        "_source": ["public_sc_policy_relation_id", "public_sc_policy_relation_industry", "public_sc_policy_relation_title"],
        "highlight": {
            "fields": {
                field: {}
            }
        }
    }

    try:
        # 发送请求
        response = requests.post(
            f"{es_host}/{es_index}/_search",
            auth=HTTPBasicAuth(es_user, es_password),
            headers={"Content-Type": "application/json"},
            data=json.dumps(query)
        )

        # 检查响应状态
        if response.status_code == 200:
            result = response.json()
            # 提取public_sc_policy_relation_id的值并去重
            id_values = []
            for hit in result.get('hits', {}).get('hits', []):
                source = hit.get('_source', {})
                id_value = source.get('public_sc_policy_relation_id')
                if id_value is not None and id_value not in id_values:
                    id_values.append(id_value)
            return id_values
        else:
            print(f"搜索政策关系数据失败: HTTP状态码 {response.status_code}")
            print(f"错误信息: {response.text}")
            return []

    except Exception as e:
        print(f"搜索政策关系数据时发生错误: {str(e)}")
        return []

if __name__ == "__main__":
    md = '# 2024年人工智能与AI芯片产业发展趋势综合报告 [来源: 4125532, 3857597, 3609983, 3752807, 3455563, 4138294, 3313087, 4065821, 4080201, 3767310]\n\n## 一、人工智能与AI芯片产业宏观环境分析\n### 1.1 全球市场现状与政策支持 [来源: 3313087, 3857597]\n#### 1.1.1 AI技术与芯片市场规模及增长趋势  \n#### 1.1.2 主要驱动因素与政策导向  \n#### 1.1.3 区域市场分布与特点  \n\n### 1.2 技术生态与产业链协同 [来源: 3455563, 4080201, 3857597]\n#### 1.2.1 AI芯片在人工智能生态中的核心作用  \n#### 1.2.2 芯片与算法协同发展现状  \n#### 1.2.3 半导体产业链全景分析  \n\n---\n\n## 二、AI芯片技术创新与产品发展\n### 2.1 技术架构与设计突破 [来源: 3313087, 3857597]\n#### 2.1.1 GPU、TPU、ASIC等主流架构对比  \n#### 2.1.2 新型计算架构（存算一体、类脑计算）进展  \n#### 2.1.3 能效比与算力优化的关键技术  \n\n### 2.2 AI芯片与垂直领域需求匹配 [来源: 4125532, 4138294, 3752807]\n#### 2.2.1 高性能计算场景下的优势  \n#### 2.2.2 边缘计算与终端设备的定制化需求  \n#### 2.2.3 成本控制与规模化生产的平衡  \n\n---\n\n## 三、AI技术与芯片的应用场景拓展\n### 3.1 AI赋能消费级硬件与场景创新 [来源: 4125532, 4138294, 3752807]\n#### 3.1.1 移动穿戴、家居、办公等场景需求增长  \n#### 3.1.2 AI+消费出海：翻译、选品、运营提效  \n#### 3.1.3 品牌服务与人力资源效率提升  \n\n### 3.2 AI在垂直领域的应用创新 [来源: 3455563, 4138294, 4080201]\n#### 3.2.1 教育领域AI化：个性化学习与智能化工具  \n#### 3.2.2 工业视觉与计算机视觉行业应用  \n#### 3.2.3 AIGC在文学、艺术创作中的创造力跃迁  \n\n---\n\n## 四、竞争格局与市场动态分析\n### 4.1 行业竞争格局概览 [来源: 3313087, 3857597]\n#### 4.1.1 主要厂商市场份额与排名  \n#### 4.1.2 国际巨头与本土企业的竞争态势  \n#### 4.1.3 新兴企业与初创公司的崛起路径  \n\n### 4.2 竞争策略与商业模式 [来源: 3313087, 3857597]\n#### 4.2.1 自研芯片与代工模式的优劣势  \n#### 4.2.2 生态合作与平台化战略  \n#### 4.2.3 定价策略与客户粘性提升  \n\n---\n\n## 五、未来趋势与投资机会展望\n### 5.1 技术演进方向与前沿探索 [来源: 3313087, 4080201]\n#### 5.1.1 下一代芯片架构的预测  \n#### 5.1.2 量子计算与AI芯片的结合潜力  \n#### 5.1.3 可持续发展与绿色计算趋势  \n\n### 5.2 市场增长点与投资热点 [来源: 3313087, 3857597]\n#### 5.2.1 细分领域的高增长机会  \n#### 5.2.2 关键技术突破的投资价值  \n#### 5.2.3 区域市场与产业链整合机遇  \n\n### 5.3 风险与挑战应对策略 [来源: 3313087, 3857597]\n#### 5.3.1 技术迭代带来的不确定性  \n#### 5.3.2 市场竞争加剧的风险评估  \n#### 5.3.3 政策与国际环境变化的应对措施'
    result = markdown_catalog_to_json(md)
    print(result["content"]) 