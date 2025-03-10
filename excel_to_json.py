import pandas as pd
import json
from collections import defaultdict

def excel_to_optimized_json(excel_file):
    # 读取 Excel 文件
    df = pd.read_excel(excel_file)
    
    # 创建优化后的层次结构
    hierarchy = {}
    
    # 遍历数据框的每一行
    for _, row in df.iterrows():
        一级关注 = row['一级关注']
        二级关注点 = row['二级关注点']
        工具类型 = row['分析工具/结构化框架']
        
        # 如果一级关注不存在，创建它
        if 一级关注 not in hierarchy:
            hierarchy[一级关注] = {}
            
        # 如果二级关注点不存在，创建它
        if 二级关注点 not in hierarchy[一级关注]:
            hierarchy[一级关注][二级关注点] = {
                "详情": row['二级关注详情'],
                "工具": {
                    "分析模型": [],
                    "写作框架": []
                }
            }
        
        # 添加具体的分析工具到对应类型
        if pd.notna(row['内容']):
            if 工具类型 == "分析模型":
                hierarchy[一级关注][二级关注点]["工具"]["分析模型"].append(row['内容'])
            elif 工具类型 == "写作框架":
                hierarchy[一级关注][二级关注点]["工具"]["写作框架"].append(row['内容'])
    
    # 去重工具列表
    for 一级关注 in hierarchy:
        for 二级关注点 in hierarchy[一级关注]:
            for 工具类型 in ["分析模型", "写作框架"]:
                current_tools = hierarchy[一级关注][二级关注点]["工具"][工具类型]
                hierarchy[一级关注][二级关注点]["工具"][工具类型] = list(set(current_tools))
    
    return hierarchy

if __name__ == "__main__":
    excel_file = "data/关注点及框架对应.xlsx"
    result = excel_to_optimized_json(excel_file)
    
    # 将结果保存为 JSON 文件
    with open('optimized_output.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print("优化后的 JSON 文件已生成: optimized_output.json") 