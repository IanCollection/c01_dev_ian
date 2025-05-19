"""
全局 FAISS 资源存储模块

这个模块提供了全局变量，用于存储和访问从 Flask 应用程序中加载的 FAISS 资源。
这样可以避免在函数调用间传递这些资源，使用简单的导入语句即可访问。
"""

# 全局变量，用于存储从 Flask 应用中加载的 FAISS 资源
# 使用 None 初始化
FAISS_RESOURCES = None

def set_faiss_resources(resources):
    """设置全局 FAISS 资源"""
    global FAISS_RESOURCES
    FAISS_RESOURCES = resources
    
def get_faiss_resources():
    """获取全局 FAISS 资源"""
    return FAISS_RESOURCES 