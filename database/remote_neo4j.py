from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 获取数据库连接信息
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USERNAME") 
password = os.getenv("NEO4J_PASSWORD")

# 创建数据库驱动
driver = GraphDatabase.driver(uri, auth=(user, password))

def create_node(label, properties):
    """
    创建一个节点
    
    参数:
        label (str): 节点标签
        properties (dict): 节点属性字典
        
    返回:
        None
    """
    try:
        with driver.session() as session:
            # 构建属性字符串
            props_str = ", ".join([f"{k}: ${k}" for k in properties.keys()])
            
            # 使用MERGE语句创建节点,避免重复
            session.execute_write(
                lambda tx: tx.run(
                    f"""
                    MERGE (n:{label} {{{props_str}}})
                    RETURN n
                    """,
                    **properties
                )
            )
            print(f"成功创建{label}节点")
    except Exception as e:
        print(f"创建节点时发生错误: {str(e)}")

def query_nodes(label=None, properties=None):
    """
    查询节点
    
    参数:
        label (str): 节点标签,可选
        properties (dict): 查询条件,可选
        
    返回:
        list: 查询结果列表
    """
    try:
        with driver.session() as session:
            # 构建查询语句
            if label and properties:
                query = f"MATCH (n:{label}) WHERE "
                conditions = []
                for key, value in properties.items():
                    conditions.append(f"n.{key} = ${key}")
                query += " AND ".join(conditions)
            elif label:
                query = f"MATCH (n:{label})"
            else:
                query = "MATCH (n)"
            
            query += " RETURN n"
            
            # 执行查询
            result = session.run(query, properties or {})
            nodes = [record["n"] for record in result]
            
            print(f"查询到 {len(nodes)} 个节点")
            return nodes
            
    except Exception as e:
        print(f"查询节点时发生错误: {str(e)}")
        return []
def delete_nodes(label=None, properties=None):
    """
    删除节点
    
    参数:
        label (str): 节点标签,可选
        properties (dict): 删除条件,可选
        
    返回:
        bool: 是否删除成功
    """
    try:
        with driver.session() as session:
            # 构建删除语句
            if label and properties:
                query = f"MATCH (n:{label}) WHERE "
                conditions = []
                for key, value in properties.items():
                    conditions.append(f"n.{key} = ${key}")
                query += " AND ".join(conditions)
            elif label:
                query = f"MATCH (n:{label})"
            else:
                query = "MATCH (n)"
            
            query += " DETACH DELETE n"
            
            # 执行删除
            session.run(query, properties or {})
            print(f"成功删除节点")
            return True
            
    except Exception as e:
        print(f"删除节点时发生错误: {str(e)}")
        return False


if __name__ == "__main__":
    # 测试连接
    with driver.session() as session:
        result = session.run("MATCH (n) RETURN COUNT(n)")
        for record in result:
            print(record)
            
    # 测试创建和查询节点
    create_node("Person", {"name": "John", "age": 30})
    nodes = query_nodes(label="Person", properties={"name": "John"})
    print(nodes)
    
    # 关闭驱动
    driver.close()
    # 删除测试节点
    delete_nodes(label="Person", properties={"name": "John"})
    # 测试查询所有节点
    nodes = query_nodes()
    print("所有节点:", nodes)
    
    # 测试按标签查询节点
    nodes = query_nodes(label="File") 
    print("File节点:", nodes)
    
    # 测试按属性查询节点
    nodes = query_nodes(properties={"page_idx": 0})
    print("page_idx为0的节点:", nodes)