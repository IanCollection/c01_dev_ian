import json

# 读取overview_v3_result.json文件
with open('overview_v3_result.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 将数据保存为新的json文件，保持格式
with open('overview_v3_result_new.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=4, ensure_ascii=False)
