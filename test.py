# import psycopg2
#
# import json
#
#
# with open("filename_index_flat_ids.json", "r", encoding="utf-8") as f:
#     data = json.load(f)
#
#
#
# # 检查数据是否为列表
# if isinstance(data, list):
#     print(f"列表内容: {data}")  # 打印列表内容（可选）
#     print(f"列表长度: {len(data)}")  # 打印列表长度
# else:
#     print("JSON 文件中的数据不是列表类型！")
# import numpy as np
#
# # .npy 文件路径
# file_path = "content_ids.npy"
#
# # 加载 .npy 文件
# array = np.load(file_path)
#
# # 打印数组内容
# print("数组内容:")
# print(array)
#
# # 打印数组形状
# print("数组形状:", array.shape)
#
# # 打印数组数据类型
# print("数组数据类型:", array.dtype)

import faiss

# Faiss 索引文件路径
index_file = "content_index_IVFPQ.index"

# 加载 Faiss 索引
index = faiss.read_index(index_file)

# 获取向量个数
vector_count = index.ntotal

# 打印向量个数
print(f"索引中存储的向量个数: {vector_count}")