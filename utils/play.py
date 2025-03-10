import pandas as pd

# 读取Excel文件
file_path = '/Users/linxuanxuan/PycharmProjects/C01_dev/data/关注点及框架对应.xlsx'  # 请根据实际情况修改路径
df = pd.read_excel(file_path)

# 初始化一个空字典来存储结果
result_dict = {}

# 遍历DataFrame的每一行数据
for _, row in df.iterrows():
    focus_area = row['关注点']
    analysis_type = row['分析类型']
    description = row['描述']
    model = row['分析模型']
    framework = row['写作框架']

    # 如果关注点不在结果字典中，则添加它
    if focus_area not in result_dict:
        result_dict[focus_area] = {}

    # 如果分析类型不在当前关注点的子字典中，则添加它
    if analysis_type not in result_dict[focus_area]:
        result_dict[focus_area][analysis_type] = []

    # 添加描述、分析模型和写作框架到当前分析类型的列表中
    entry = {
        "description": description,
        "model": model,
        "framework": framework
    }
    result_dict[focus_area][analysis_type].append(entry)

# 打印或返回结果字典
print(result_dict)
# 将结果字典保存为JSON文件
import json
import os

# 确保data目录存在
data_dir = 'data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# 指定JSON文件的保存路径
json_file_path = os.path.join(data_dir, 'analysis_framework.json')

# 将字典保存为JSON文件,设置ensure_ascii=False以正确保存中文
with open(json_file_path, 'w', encoding='utf-8') as f:
    json.dump(result_dict, f, ensure_ascii=False, indent=4)

print(f'结果已保存到 {json_file_path}')

