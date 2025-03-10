import json
import os
from openai import OpenAI
from typing import List, Dict, Any

class SemanticMatcher:
    def __init__(self, json_file: str):
        # 加载结构化的关注点数据
        with open(json_file, 'r', encoding='utf-8') as f:
            self.structure = json.load(f)
        
        # 初始化 OpenAI 客户端
        self.client = OpenAI()
        
        # 准备系统提示词
        self.system_prompt = self._prepare_system_prompt()
    
    def _prepare_system_prompt(self) -> str:
        """准备系统提示词，包含所有的关注点结构"""
        # 构建关注点结构的描述
        structure_desc = []
        for level1, content1 in self.structure.items():
            structure_desc.append(f"\n一级关注点 - {level1}:")
            for level2, content2 in content1.items():
                structure_desc.append(f"  二级关注点 - {level2}:")
                structure_desc.append(f"    详情: {content2['详情']}")
        
        # 组装系统提示词
        prompt = f"""你是一个专业的研报分析助手。你的任务是将研报标题与预定义的关注点体系进行匹配。
这个关注点体系包含以下结构：

{''.join(structure_desc)}

对于给定的研报标题，请：
1. 分析标题的核心主题和关注点
2. 找出最匹配的一级和二级关注点（可以给出多个匹配结果）
3. 给出匹配的理由
4. 给出匹配的置信度（0-100）

请以 JSON 格式返回结果，格式如下：
{{
    "matches": [
        {{
            "一级关注": "...",
            "二级关注点": "...",
            "匹配理由": "...",
            "置信度": 85
        }},
        ...
    ]
}}

注意：
1. 只返回置信度大于 50 的匹配结果
2. 按置信度从高到低排序
3. 最多返回 3 个匹配结果
4. 只返回 JSON 格式的结果，不要有其他文字
"""
        return prompt

    def match_title(self, title: str) -> List[Dict[str, Any]]:
        """使用 OpenAI API 进行语义匹配"""
        try:
            # 构建用户提示词
            user_prompt = f"研报标题：{title}"
            
            # 调用 OpenAI API
            response = self.client.chat.completions.create(
                model="gpt-4",  # 或使用 gpt-3.5-turbo
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,  # 降低随机性
                response_format={"type": "json_object"}  # 确保返回 JSON
            )
            
            # 解析响应
            result = json.loads(response.choices[0].message.content)
            
            # 添加可用工具信息
            for match in result["matches"]:
                level1 = match["一级关注"]
                level2 = match["二级关注点"]
                if level1 in self.structure and level2 in self.structure[level1]:
                    match["可用工具"] = self.structure[level1][level2]["工具"]
            
            return result["matches"]
            
        except Exception as e:
            print(f"匹配过程中出现错误: {str(e)}")
            return []

def main():
    # 使用示例
    matcher = SemanticMatcher('optimized_output.json')
    
    while True:
        title = input("\n请输入研报标题（输入 'q' 退出）：")
        if title.lower() == 'q':
            break
        
        matches = matcher.match_title(title)
        print("\n匹配结果：")
        for i, match in enumerate(matches, 1):
            print(f"\n{i}. 推荐标签：")
            print(f"   一级关注：{match['一级关注']}")
            print(f"   二级关注点：{match['二级关注点']}")
            print(f"   匹配理由：{match['匹配理由']}")
            print(f"   置信度：{match['置信度']}%")
            print("   可用工具：")
            for tool_type, tools in match['可用工具'].items():
                if tools:  # 只显示非空的工具列表
                    print(f"     {tool_type}：{', '.join(tools)}")

if __name__ == "__main__":
    main() 