import json

# 读取overview_v3_result.json文件


with open('norefine_output(2).json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 将数据保存为新的json文件，保持格式
with open('no_refine_output_0430.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=4, ensure_ascii=False)

#
# with open('first_level_title_output.json', 'r', encoding='utf-8') as f:
#     data = json.load(f)
#
# # 将数据保存为新的json文件，保持格式
# with open('first_level_title_output_v1.json', 'w', encoding='utf-8') as f:
#     json.dump(data, f, indent=4, ensure_ascii=False)
#
