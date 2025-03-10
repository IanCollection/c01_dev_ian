from neo4j import GraphDatabase
import json
import os
from tqdm import tqdm
from dotenv import load_dotenv
from utils.snowflakeID import SnowflakeID

load_dotenv()

# def build_neo4j_nodes(content_json_path, images_json_path):
def build_neo4j_nodes(json_data, images_map, id, title, org_name, file_url, publish_at, s3_url):
    """
    根据JSON文件结构构建Neo4j节点，并使用图片的绝对URL路径
    参数:
    json_data: 内容JSON数据
    images_map: 图片映射数据
    id: 文件节点ID
    title: 文件标题
    org_name: 组织名称
    file_url: 文件URL
    publish_at: 时间字符串
    s3_url: S3存储URL
    """
    # 初始化雪花ID生成器
    snowflake = SnowflakeID()
    # 初始化存储节点ID和内容的字典
    filename_dict = {}  # 存储文件节点ID和文件名
    headers_dict = {}   # 存储标题节点ID和标题内容
    content_dict = {}   # 存储内容节点ID和文本内容
    all_headers_list = []  # 存储所有标题内容

    # 从环境变量读取Neo4j配置
    uri = os.environ.get("NEO4J_URI")
    username = os.environ.get("NEO4J_USERNAME") 
    password = os.environ.get("NEO4J_PASSWORD")
    # 确保所有必要的配置都存在
    if not all([uri, username, password]):
        raise ValueError("错误：缺少Neo4j连接配置，请检查环境变量")
    driver = GraphDatabase.driver(uri, auth=(username, password))

    try:
        with driver.session() as session:
            print("开始处理数据...")
            # 创建文件节点并使用传入的ID
            file_node_id = id
            filename_dict[file_node_id] = title
            
            # 批量处理数据的准备工作
            header_nodes = []
            content_nodes = []
            image_nodes = []
            file_header_rels = []
            header_content_rels = []
            header_image_rels = []
            file_image_rels = []
            
            # 创建所有需要的索引
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE INDEX file_node_id_index IF NOT EXISTS
                    FOR (f:File) ON (f.file_node_id)
                    """
                )
            )
            
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE INDEX header_id_index IF NOT EXISTS
                    FOR (h:Header) ON (h.header_id)
                    """
                )
            )
            
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE INDEX content_id_index IF NOT EXISTS
                    FOR (c:Content) ON (c.content_id)
                    """
                )
            )
            
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE INDEX image_id_index IF NOT EXISTS
                    FOR (i:Image) ON (i.image_id)
                    """
                )
            )
            
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE INDEX all_headers_id_index IF NOT EXISTS
                    FOR (ah:AllHeaders) ON (ah.all_headers_id)
                    """
                )
            )
            
            # 创建文件节点
            session.execute_write(
                lambda tx: tx.run(
                    """
                    CREATE (f:File {
                        name: $name, 
                        file_node_id: $id,
                        org_name: $org_name,
                        file_url: $file_url,
                        s3_url: $s3_url,
                        to_char: $to_char,
                        page_idx: 0
                    })
                    RETURN f.file_node_id AS node_id
                    """,
                    name=title,
                    id=file_node_id,
                    org_name=org_name,
                    file_url=file_url,
                    s3_url=s3_url,
                    to_char=publish_at
                ).single()["node_id"]
            )
            # 跟踪当前标题节点
            current_title_node_id = None
            
            # 使用tqdm显示处理进度
            for item in tqdm(json_data, desc="处理文档内容"):
                item_type = item.get("type")
                page_idx = item.get("page_idx", 0)  # 获取页码，默认为0
                
                if item_type == "text" and item.get("text_level", 0) == 1:
                    # 创建新的标题节点
                    header_id = snowflake.next_id()
                    header_text = item.get('text')
                    headers_dict[header_id] = header_text
                    all_headers_list.append(header_text)  # 添加到所有标题列表
                    
                    # 添加到批处理列表
                    header_nodes.append({
                        "content": header_text,
                        "header_id": header_id,
                        "page_idx": page_idx
                    })
                    file_header_rels.append({
                        "file_node_id": file_node_id,
                        "header_id": header_id
                    })
                    
                    current_title_node_id = header_id
                    
                elif item_type == "text" and item.get("text_level", 0) == 0:
                    # 直接为每个文本内容创建节点，不进行拼接
                    if item.get('text'):
                        content_text = item.get('text')
                        content_id = snowflake.next_id()
                        content_dict[content_id] = content_text
                        
                        # 添加到批处理列表
                        content_nodes.append({
                            "content": content_text,
                            "content_id": content_id,
                            "page_idx": page_idx
                        })
                        # 如果有当前标题，则创建标题-内容关系
                        if current_title_node_id:
                            header_content_rels.append({
                                "header_id": current_title_node_id,
                                "content_id": content_id
                            })
                
                elif item_type == "image":
                    # 获取图片的绝对URL路径
                    image_path = item.get('img_path')
                    # 检查image_path是否为空或None
                    if not image_path:
                        continue
                    # 如果image_path是相对路径,需要处理成绝对路径
                    if image_path.startswith('images/'):
                        image_path = image_path.replace('images/', '')
                    absolute_url = images_map.get(image_path, image_path)
                    
                    image_id = snowflake.next_id()
                    # 添加到批处理列表
                    image_nodes.append({
                        "path": image_path,
                        "url": absolute_url,
                        "image_id": image_id,
                        "page_idx": page_idx
                    })
                    
                    # 创建图片节点关系
                    if current_title_node_id is not None:
                        header_image_rels.append({
                            "header_id": current_title_node_id,
                            "image_id": image_id
                        })
                    else:
                        file_image_rels.append({
                            "file_node_id": file_node_id,
                            "image_id": image_id
                        })
            # 批量创建节点和关系
            print("开始批量创建节点和关系...")
            
            # 批量创建标题节点
            if header_nodes:
                session.execute_write(
                    lambda tx: tx.run(
                        """
                        UNWIND $nodes AS node
                        MERGE (h:Header {header_id: node.header_id})
                        SET h.content = node.content,
                            h.page_idx = node.page_idx
                        """,
                        nodes=header_nodes
                    )
                )
            
            # 批量创建内容节点
            if content_nodes:
                session.execute_write(
                    lambda tx: tx.run(
                        """
                        UNWIND $nodes AS node
                        MERGE (c:Content {content_id: node.content_id})
                        SET c.content = node.content,
                            c.page_idx = node.page_idx
                        """,
                        nodes=content_nodes
                    )
                )
            
            # 批量创建图片节点
            if image_nodes:
                session.execute_write(
                    lambda tx: tx.run(
                        """
                        UNWIND $nodes AS node
                        MERGE (i:Image {image_id: node.image_id})
                        SET i.path = node.path, 
                            i.url = node.url,
                            i.page_idx = node.page_idx
                        """,
                        nodes=image_nodes
                    )
                )
            
            # 批量创建文件-标题关系
            if file_header_rels:
                session.execute_write(
                    lambda tx: tx.run(
                        """
                        UNWIND $rels AS rel
                        MATCH (f:File {file_node_id: rel.file_node_id})
                        MATCH (h:Header {header_id: rel.header_id})
                        MERGE (f)-[:HAS_HEADER]->(h)
                        MERGE (h)-[:BELONGS_TO]->(f)
                        """,
                        rels=file_header_rels
                    )
                )
            
            # 批量创建标题-内容关系
            if header_content_rels:
                session.execute_write(
                    lambda tx: tx.run(
                        """
                        UNWIND $rels AS rel
                        MATCH (h:Header {header_id: rel.header_id})
                        MATCH (c:Content {content_id: rel.content_id})
                        MERGE (h)-[:HAS_CONTENT]->(c)
                        MERGE (c)-[:BELONGS_TO]->(h)
                        WITH c, rel
                        MATCH (f:File {file_node_id: rel.file_node_id})
                        MERGE (c)-[:BELONGS_TO]->(f)
                        """,
                        rels=header_content_rels
                    )
                )
            
            # 批量创建标题-图片关系
            if header_image_rels:
                session.execute_write(
                    lambda tx: tx.run(
                        """
                        UNWIND $rels AS rel
                        MATCH (h:Header {header_id: rel.header_id})
                        MATCH (i:Image {image_id: rel.image_id})
                        MERGE (h)-[:HAS_IMAGE]->(i)
                        MERGE (i)-[:BELONGS_TO]->(h)
                        WITH i, rel
                        MATCH (f:File {file_node_id: rel.file_node_id})
                        MERGE (i)-[:BELONGS_TO]->(f)
                        """,
                        rels=header_image_rels
                    )
                )
            
            # 批量创建文件-图片关系
            if file_image_rels:
                session.execute_write(
                    lambda tx: tx.run(
                        """
                        UNWIND $rels AS rel
                        MATCH (f:File {file_node_id: rel.file_node_id})
                        MATCH (i:Image {image_id: rel.image_id})
                        MERGE (f)-[:HAS_IMAGE]->(i)
                        MERGE (i)-[:BELONGS_TO]->(f)
                        """,
                        rels=file_image_rels
                    )
                )
            
            # 创建all_headers节点，包含所有标题
            if all_headers_list:
                all_headers_id = snowflake.next_id()
                all_headers_content = "\n".join(all_headers_list)
                
                session.execute_write(
                    lambda tx: tx.run(
                        """
                        MERGE (ah:AllHeaders {content: $content, all_headers_id: $id, page_idx: 0})
                        WITH ah
                        MATCH (f:File {file_node_id: $file_node_id})
                        MERGE (f)-[:HAS_ALL_HEADERS]->(ah)
                        """,
                        content=all_headers_content,
                        id=all_headers_id,
                        file_node_id=file_node_id
                    )
                )
                
            print(f"成功创建文件节点及其相关内容，图片路径已更新为绝对URL")
            return filename_dict, headers_dict, content_dict
    finally:
        driver.close()
        
# # 使用示例
# if __name__ == "__main__":
#     a,b,c = build_neo4j_nodes(
#         "/Users/linxuanxuan/PycharmProjects/C01_dev/data/result_data_v2/德勤-2024年商业地产行业展望-2024-02-05_content_list.json",
#         "/Users/linxuanxuan/PycharmProjects/C01_dev/data/result_data_v2/德勤-2024年商业地产行业展望-2024-02-05_images.json"
#     )
#     print(a)
#     print(b)
#     print(c)