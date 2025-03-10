import pandas as pd

# 读取Excel文件
df = pd.read_excel('/Users/linxuanxuan/PycharmProjects/C01_dev/data/关注点及框架对应.xlsx')

# 假设一级关注点和二级关注点分别在不同的列中
# 请根据实际的列名进行调整
一级关注点列名 = '一级关注'  # 根据实际Excel中的列名修改
二级关注点列名 = '二级关注点'  # 根据实际Excel中的列名修改

# 获取唯一的一级关注点和二级关注点
一级关注点数量 = len(df[一级关注点列名].unique())
二级关注点数量 = len(df[二级关注点列名].unique())

# 打印结果
print(f'一级关注点数量：{一级关注点数量}')
print(f'二级关注点数量：{二级关注点数量}')
print(df[一级关注点列名].unique())
print('--------------')
print(df[二级关注点列名].unique())