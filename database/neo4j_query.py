from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

def get_neo4j_driver():
    """获取Neo4j数据库连接"""
    uri = os.environ.get("NEO4J_URI")
    username = os.environ.get("NEO4J_USERNAME")
    password = os.environ.get("NEO4J_PASSWORD")
    
    if not all([uri, username, password]):
        raise ValueError("错误：缺少Neo4j连接配置，请检查环境变量")
        
    return GraphDatabase.driver(uri, auth=(username, password))

def query_file_batch_nodes(file_node_ids):
    """
    批量查询多个file_node_id的文件节点及其关联信息
    
    Args:
        file_node_ids (list): 文件节点ID列表
        
    Returns:
        list: 包含多个文件信息的字典列表
    """
        
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 批量查询文件节点
            result = session.run(
                """
                MATCH (f:File) 
                WHERE f.file_node_id IN $file_node_ids
                RETURN f
                """,
                file_node_ids=file_node_ids
            )
            
            file_infos = []
            for record in result:
                file_info = dict(record["f"])
                file_infos.append(file_info)
                
            return file_infos
            
    except Exception as e:
        print(f"批量查询文件节点时发生错误: {e}")
        return []
    finally:
        driver.close()
def query_header_batch_nodes(header_ids):
    """
    批量查询多个header_id的标题节点
    
    Args:
        header_ids (list): 标题节点ID列表
        
    Returns:
        list: 包含多个标题信息的字典列表
    """
    if not header_ids:
        return []
        
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 批量查询标题节点
            result = session.run(
                """
                MATCH (h:Header)
                WHERE h.header_id IN $header_ids
                RETURN h
                """,
                header_ids=header_ids
            )
            
            header_infos = []
            for record in result:
                header_info = dict(record["h"])
                header_infos.append(header_info)
                
            return header_infos
            
    except Exception as e:
        print(f"批量查询标题节点时发生错误: {e}")
        return []
    finally:
        driver.close()

def query_content_batch_nodes(content_ids):
    """
    批量查询多个content_id的内容节点
    
    Args:
        content_ids (list): 内容节点ID列表
        
    Returns:
        list: 包含多个内容信息的字典列表
    """
    if not content_ids:
        return []
        
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 批量查询内容节点
            result = session.run(
                """
                MATCH (c:Content)
                WHERE c.content_id IN $content_ids
                RETURN c
                """,
                content_ids=content_ids
            )
            
            content_infos = []
            for record in result:
                content_info = dict(record["c"])
                content_infos.append(content_info)
                
            return content_infos
            
    except Exception as e:
        print(f"批量查询内容节点时发生错误: {e}")
        return []
    finally:
        driver.close()

def query_file_node(file_node_id):
    """
    查询指定file_node_id的文件节点及其关联信息
    
    Args:
        file_node_id: 文件节点ID
        
    Returns:
        dict: 包含文件信息的字典
    """
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 查询文件节点
            result = session.run(
                """
                MATCH (f:File {file_node_id: $file_node_id})
                RETURN f
                """,
                file_node_id=file_node_id
            ).single()
            
            if not result:
                return None
                
            file_info = dict(result["f"])
            
            return file_info
            
    finally:
        driver.close()

def query_file_batch_nodes_return_node_with_all_headers(file_node_ids):
    """
    批量查询多个file_node_id的文件节点及其关联信息
    
    Args:
        file_node_ids (list): 文件节点ID列表
        
    Returns:
        list: 包含多个文件信息的字典列表
    """

        
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 批量查询文件节点及其关联的AllHeaders节点
            result = session.run(
                """
                MATCH (f:File) 
                WHERE f.file_node_id IN $file_node_ids
                OPTIONAL MATCH (f)-[:HAS_ALL_HEADERS]->(h:AllHeaders)
                RETURN f, h.content as headers_content
                """,
                file_node_ids=file_node_ids
            )   

            file_infos = []
            for record in result:
                file_info = dict(record["f"])
                file_info["headers_content"] = record["headers_content"]
                file_infos.append(file_info)        
                
            return file_infos
            
    except Exception as e:
        print(f"批量查询文件节点时发生错误: {e}")
        return []
    finally:
        driver.close()
        
def query_file_all_headers(file_node_id):
    """
    查询指定file_node_id的文件节点的所有标题内容
    
    Args:
        file_node_id: 文件节点ID
        
    Returns:
        list: 包含所有标题内容的列表
    """
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 查询文件节点关联的所有标题
            result = session.run(
                """
                MATCH (f:File {file_node_id: $file_node_id})-[:HAS_ALL_HEADERS]->(h:AllHeaders)
                RETURN h.content as content
                """,
                file_node_id=file_node_id
            ).single()
            
            if not result:
                return None
                
            headers_content = result["content"]
            
            return headers_content
            
    finally:
        driver.close()

def query_header_node(header_id):
    """
    查询指定file_node_id的文件节点及其关联信息

    Args:
        file_node_id: 文件节点ID

    Returns:
        dict: 包含文件信息的字典
    """
    driver = get_neo4j_driver()

    try:
        with driver.session() as session:
            # 查询文件节点
            result = session.run(
                """
                MATCH (f:Header {header_id: $header_id})
                RETURN f
                """,
                header_id=header_id
            ).single()

            if not result:
                return None

            header_info = dict(result["f"])

            return header_info

    finally:
        driver.close()

def query_content_node(content_id):
    """
    查询指定file_node_id的文件节点及其关联信息

    Args:
        file_node_id: 文件节点ID

    Returns:
        dict: 包含文件信息的字典
    """
    driver = get_neo4j_driver()

    try:
        with driver.session() as session:
            # 查询文件节点
            result = session.run(
                """
                MATCH (f:Content {content_id: $content_id})
                RETURN f
                """,
                content_id=content_id
            ).single()

            if not result:
                return None

            content_info = dict(result["f"])

            return content_info

    finally:
        driver.close()


def query_node_by_id(node_id):
    """
    查询指定内部节点ID的节点信息
    
    Args:
        node_id: Neo4j内部节点ID
        
    Returns:
        dict: 包含节点信息的字典
    """
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 查询节点
            result = session.run(
                """
                MATCH (n)
                WHERE id(n) = $node_id
                RETURN n
                """,
                node_id=int(node_id)
            ).single()
            
            if not result:
                return None
                
            node_info = dict(result["n"])
            
            return node_info
            
    finally:
        driver.close()

def query_node_by_file_node_id(file_node_id):
    """
    查询指定file_node_id的节点信息
    
    Args:
        file_node_id: 文件节点ID
        
    Returns:
        dict: 包含节点信息的字典
    """
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 查询节点
            result = session.run(
                """
                MATCH (n:File {file_node_id: $file_node_id})
                RETURN n
                """,
                file_node_id=file_node_id
            ).single()
            
            if not result:
                return None
                
            node_info = dict(result["n"])
            
            return node_info
            
    finally:
        driver.close()

def query_node_by_content_id(content_id):
    """
    查询指定content_id的节点信息
    
    Args:
        content_id: 内容节点ID
        
    Returns:
        dict: 包含节点信息的字典,如果未找到则返回None
    """
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 查询节点
            result = session.run(
                """
                MATCH (n:Content {content_id: $content_id})
                RETURN n
                """,
                content_id=content_id
            ).single()
            
            if not result:
                return None
                
            node_info = dict(result["n"])
            
            return node_info
            
    finally:
        driver.close()

def query_node_by_header_node_id(header_node_id):
    """
    查询指定header_node_id的节点信息
    
    Args:
        header_node_id: 头节点ID
        
    Returns:
        dict: 包含节点信息的字典,如果未找到则返回None
    """
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 查询节点
            result = session.run(
                """
                MATCH (n:Header {header_id: $header_id}) 
                RETURN n
                """,
                header_id=header_node_id
            ).single()
            
            if not result:
                return None
                
            node_info = dict(result["n"])
            
            return node_info
            
    finally:
        driver.close()

def query_parent_node(node_id, node_type):
    """
    根据子节点ID查找父节点信息
    
    Args:
        node_id: 子节点ID
        node_type: 节点类型,可选值为 'Content' 或 'Header'
        
    Returns:
        dict: 包含父节点信息的字典,如果未找到则返回None
    """
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            if node_type == 'Content':
                # Content节点查找其Header父节点
                result = session.run(
                    """
                    MATCH (c:Content {content_id: $node_id})-[:BELONGS_TO]->(h:Header)
                    RETURN h
                    """,
                    node_id=node_id
                ).single()
                
                if not result:
                    return None
                    
                return dict(result["h"])
                
            elif node_type == 'Header':
                # Header节点查找其File父节点 
                result = session.run(
                    """
                    MATCH (h:Header {header_id: $node_id})-[:BELONGS_TO]->(f:File)
                    RETURN f
                    """,
                    node_id=node_id
                ).single()
                
                if not result:
                    return None
                    
                return dict(result["f"])
                
            else:
                raise ValueError(f"不支持的节点类型: {node_type}")
                
    finally:
        driver.close()

def query_all_relationships(file_node_id):
    """
    测试查询指定文件节点下的所有关系
    
    Args:
        file_node_id: 文件节点ID
        
    Returns:
        dict: 包含所有关系的字典
    """
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 修改后的查询，避免嵌套collect
            result = session.run(
                """
                MATCH (f:File {file_node_id: $file_node_id})
                OPTIONAL MATCH (f)<-[r1:BELONGS_TO]-(h:Header)
                OPTIONAL MATCH (h)<-[r2:BELONGS_TO]-(c:Content)
                RETURN {
                    file: f,
                    relationships: collect({
                        header: h,
                        content: c,
                        header_relationship: type(r1),
                        content_relationship: type(r2)
                    })
                } as result
                """,
                file_node_id=str(file_node_id)
            ).single()
            
            if not result:
                return None
                
            return result["result"]
            
    finally:
        driver.close()

def test_relationships():
    """
    测试所有节点关系的示例代码
    """
    # 1. 首先查询一个 Content 节点
    content_node = query_node_by_content_id("288007216793911308")
    if not content_node:
        print("未找到 Content 节点")
        return
        
    print("\n1. Content 节点信息:")
    print(content_node)
    
    # 2. 通过 Content 节点查找其父 Header 节点
    header_node = query_parent_node(content_node["content_id"], "Content")
    if not header_node:
        print("未找到 Header 节点")
        return
        
    print("\n2. 对应的 Header 节点信息:")
    print(header_node)
    
    # 3. 通过 Header 节点查找其父 File 节点
    file_node = query_parent_node(header_node["header_id"], "Header")
    if not file_node:
        print("未找到 File 节点")
        return
        
    print("\n3. 对应的 File 节点信息:")
    print(file_node)
    
def query_by_header(header_node_id):
    """
    通过header节点ID查询相关的content和file节点
    
    Args:
        header_node_id: header节点ID
        
    Returns:
        dict: 包含相关file和content节点的信息
    """
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            result = session.run(
                """
                MATCH (h:Header {header_id: $header_node_id})
                OPTIONAL MATCH (h)-[:BELONGS_TO]->(f:File)
                OPTIONAL MATCH (c:Content)-[:BELONGS_TO]->(h)
                WITH h, f, collect(c) as contents
                RETURN {
                    header: properties(h),
                    file: properties(f),
                    contents: [x in contents | properties(x)]
                } as result
                """,
                header_node_id=header_node_id
            ).single()
            
            if not result:
                return None
                
            return result["result"]
            
    finally:
        driver.close()

def test_query_by_header(header_id):
    """
    测试通过header查询相关节点的函数
    """
    # 这里替换成实际的header_node_id进行测试
    header_node_id = header_id
    result = query_by_header(header_node_id)
    
    if result:
        print("相关文件:", result["file"])
        print("Header信息:", result["header"])
        print("Content列表:", result["contents"])
    else:
        print("未找到相关信息")

def query_file_contents(file_node_id):
    """
    通过file_node_id查询文件的所有内容，包括headers和contents
    
    Args:
        file_node_id: 文件节点ID
        
    Returns:
        dict: 包含文件所有内容的字典，按header组织content
    """
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            result = session.run(
                """
                MATCH (f:File {file_node_id: $file_node_id})
                OPTIONAL MATCH (h:Header)-[:BELONGS_TO]->(f)
                OPTIONAL MATCH (c:Content)-[:BELONGS_TO]->(h)
                WITH f, h, collect(c) as contents
                ORDER BY h.position
                WITH f, collect({header: h, contents: contents}) as sections
                RETURN {
                    file: properties(f),
                    sections: sections
                } as result
                """,
                file_node_id=file_node_id
            ).single()
            
            if not result:
                return None
                
            return result["result"]
            
    finally:
        driver.close()

def query_file_contents_diyid(file_node_id):
    """
    测试查询文件内容的函数
    """
    # 替换成实际的file_node_id进行测试

    result = query_file_contents(file_node_id)

    if result:
        print("文件信息:", result["file"])
        print("\n文件内容结构:")
        for section in result["sections"]:
            print("\nHeader:", section["header"])
            print("Contents:", section["contents"])
    else:
        print("未找到文件或文件内容")

# 如果需要直接测试，取消下面的注释
if __name__ == "__main__":
    # result = query_file_contents(2969078)
    # print(result)
    result = test_query_by_header(288229945581240343)
    print(result)
    # query_file_contents_diyid(2969078)
# if __name__ == "__main__":
#     # test_relationships()
#     print(query_by_header("288007216793911321"))