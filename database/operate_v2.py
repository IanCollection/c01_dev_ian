import os
import pandas as pd
import json
from dotenv import load_dotenv
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from utils.parse_pdf_with_s3 import parse_pdf
from database.build_neo4j import build_neo4j_nodes
from database.faiss_IVFPQ import build_index_flat, build_index_IVFPQ

def check_file_exists_in_neo4j(id):
    """检查Neo4j数据库中是否已存在指定ID的文件节点"""
    # 加载环境变量
    load_dotenv()
    
    # 连接Neo4j数据库
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        with driver.session() as session:
            # 首先检查数据库中是否有节点
            check_nodes_query = """
            MATCH (n) RETURN count(n) > 0 AS has_nodes LIMIT 1
            """
            has_nodes_result = session.run(check_nodes_query).single()
            if not has_nodes_result or not has_nodes_result["has_nodes"]:
                driver.close()
                return False  # 数据库为空，不进行判断
                
            # 查询是否存在指定ID的File节点
            query = """
            MATCH (f:File {id: $id}) 
            RETURN count(f) > 0 AS exists
            """
            result = session.run(query, id=id).single()
            exists = result and result["exists"]
            driver.close()
            return exists
    except Exception as e:
        print(f"连接Neo4j数据库时出错: {str(e)}")
        return False

def build_all(id, title, org_name, file_url,publish_at, s3_url):
    """构建所有数据"""
    # 检查数据库中是否已存在该ID的节点
    # if check_file_exists_in_neo4j(id):
    #     print(f"ID为{id}的文件节点已存在于Neo4j数据库中，跳过处理")
    #     return {}, {}, {}  # 返回空字典，表示不需要进一步处理
    
    # 1. 获取pdf_parse数据
    response = parse_pdf(s3_url)
    content_list = response.get('content_list', [])
    images = response.get('images', [])
    
    # 2. 构建neo4j节点
    filename_dict, headers_dict, content_dict = build_neo4j_nodes(
        content_list, images, id, title, org_name, file_url, publish_at, s3_url
    )
    
    # 返回构建的三个字典
    return filename_dict, headers_dict, content_dict

def process_single_report(row_tuple):
    """处理单个报告的函数"""
    _, row = row_tuple
    return build_all(
        row['id'], 
        row['title'], 
        row['org_name'], 
        row['file_url'], 
        row['publish_at'],
        row['s3_url']
    )

if __name__ == "__main__":
    # 读取研报信息
    reports_info = pd.read_excel("/Users/linxuanxuan/PycharmProjects/C01_dev/data/Copy of 洞见研报报告列表-for test(1).xlsx")
    reports_info = reports_info.iloc[10:100]  # 读取第29行（索引从0开始，所以是28）及以后的所有数据
    all_filename_dicts = {}
    all_headers_dicts = {}
    all_content_dicts = {}
    
    # 使用线程池并行处理
    with ThreadPoolExecutor(max_workers=3) as executor:
        # 提交所有任务
        future_to_row = {
            executor.submit(process_single_report, row_tuple): i 
            for i, row_tuple in enumerate(reports_info.iterrows())
        }

        # 处理结果
        for future in tqdm(as_completed(future_to_row), desc="处理研报文件", total=len(reports_info)):
            try:
                # 获取当前处理的行索引
                row_index = future_to_row[future]
                current_id = reports_info.iloc[row_index]['id']
                print(f"正在处理文件ID: {current_id}")
                
                filename_dict, headers_dict, content_dict = future.result()
                all_filename_dicts.update(filename_dict)
                all_headers_dicts.update(headers_dict)
                all_content_dicts.update(content_dict)
     
                
                # 每次三个dicts update一次就保存一次
                # 使用相对路径或用户目录下的路径，避免写入根目录导致权限问题
                save_dir = "./text_dicts"  # 或者使用 "~/text_dicts" 或其他有写入权限的路径
                os.makedirs(save_dir, exist_ok=True)
                
                # 检查文件是否存在，不存在则创建新文件
                filename_path = os.path.join(save_dir, "filename_dicts.json")
                headers_path = os.path.join(save_dir, "headers_dicts.json")
                content_path = os.path.join(save_dir, "content_dicts.json")
                
                # 处理filename_dicts
                if os.path.exists(filename_path):
                    with open(filename_path, 'r', encoding='utf-8') as f:
                        try:
                            existing_filename_dicts = json.load(f)
                            existing_filename_dicts.update(all_filename_dicts)
                            all_filename_dicts = existing_filename_dicts
                        except json.JSONDecodeError:
                            # 如果文件存在但不是有效的JSON，则使用当前数据
                            pass
                
                # 处理headers_dicts
                if os.path.exists(headers_path):
                    with open(headers_path, 'r', encoding='utf-8') as f:
                        try:
                            existing_headers_dicts = json.load(f)
                            existing_headers_dicts.update(all_headers_dicts)
                            all_headers_dicts = existing_headers_dicts
                        except json.JSONDecodeError:
                            pass
                
                # 处理content_dicts
                if os.path.exists(content_path):
                    with open(content_path, 'r', encoding='utf-8') as f:
                        try:
                            existing_content_dicts = json.load(f)
                            existing_content_dicts.update(all_content_dicts)
                            all_content_dicts = existing_content_dicts
                        except json.JSONDecodeError:
                            pass
                
                # 保存更新后的数据
                with open(filename_path, 'w', encoding='utf-8') as f:
                    json.dump(all_filename_dicts, f, ensure_ascii=False, indent=2)
                
                with open(headers_path, 'w', encoding='utf-8') as f:
                    json.dump(all_headers_dicts, f, ensure_ascii=False, indent=2)
                
                with open(content_path, 'w', encoding='utf-8') as f:
                    json.dump(all_content_dicts, f, ensure_ascii=False, indent=2)
                
                print(f"已保存处理结果到 {save_dir}")
            except Exception as exc:
                print(f'处理报告时发生错误: {exc}')
    
    # 3. 构建faiss索引
    print("开始构建FAISS索引...")
    if all_filename_dicts:
        f_idx,f_cost = build_index_flat(all_filename_dicts, "filename")
    if all_headers_dicts:
        h_idx,h_cost = build_index_IVFPQ(all_headers_dicts, "header")  # 修正参数名称
    if all_content_dicts:
        c_idx,c_cost = build_index_IVFPQ(all_content_dicts, "content")
    print(f_cost+h_cost+c_cost)