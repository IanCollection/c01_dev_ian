import os
import sys
import pandas as pd
import json
from dotenv import load_dotenv
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import faiss
import logging
from logging.handlers import RotatingFileHandler
import threading

# 将项目根目录添加到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from utils.parse_pdf_with_s3 import parse_pdf
from database.build_neo4j import build_neo4j_nodes
from database.faiss_IVFPQ import build_index_flat, build_index_IVFPQ, add_small_batch, get_available_gpu_resources


def setup_logging():
    """配置日志系统"""
    # 创建日志目录
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # 配置根日志记录器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 清除现有处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 创建文件处理器
    log_file = os.path.join(log_dir, "build_db.log")
    file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器到日志记录器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

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
                
            # 查询是否存在指定file_node_id的节点
            query = """
            MATCH (n) 
            WHERE n.file_node_id = $id
            RETURN count(n) > 0 AS exists
            """
            result = session.run(query, id=id).single()
            exists = result and result["exists"]
            driver.close()
            return exists
    except Exception as e:
        print(f"连接Neo4j数据库时出错: {str(e)}")
        return False

def build_all(id, title, org_name, file_url, publish_at, s3_url):
    """构建所有数据"""
    # 检查数据库中是否已存在该ID的节点
    if check_file_exists_in_neo4j(id):
        print(f"文件ID {id} 已存在于数据库中,跳过处理")
        return {}, {}, {}  # 返回空字典而不是None,这样调用方可以直接使用而不需要额外检查None

    # 1. 获取pdf_parse数据
    response = parse_pdf(s3_url)
    
    # 检查response中是否包含error信息
    if response.get('error'):
        print(f"解析PDF时出错: {response.get('error')}")
        return {}, {}, {}
    
    content_list = response.get('content_list', [])
    images = response.get('images', [])

    if content_list:
        # 2. 构建neo4j节点
        filename_dict, headers_dict, content_dict = build_neo4j_nodes(
            content_list, images, id, title, org_name, file_url, publish_at, s3_url
        )
    else:
        filename_dict, headers_dict, content_dict = {}, {}, {}
        
    # 返回构建的三个字典
    return filename_dict, headers_dict, content_dict

def process_single_report(row_tuple):
    """处理单个报告的函数，不输出日志"""
    _, row = row_tuple
    # 不打印任何日志，直接返回结果
    return build_all(
        row['id'], 
        row['title'], 
        row['org_name'], 
        row['file_url'], 
        row['publish_at'],
        row['s3_url']
    )

# 新增：跟踪已处理的ID及增量更新FAISS索引
def update_faiss_indices(new_items, index_type, processed_ids_file):
    """优化后的增量更新函数
    
    Args:
        new_items (dict): 新增的文本ID字典
        index_type (str): 索引类型
        processed_ids_file (str): 已处理ID记录文件
        
    Returns:
        tuple: (成功标志, 处理成本)
    """
    # 移除原有的ID过滤逻辑
    actual_new_items = new_items  # 直接使用传入的新数据
    
    if not actual_new_items:
        print(f"没有新的{index_type} ID需要处理")
        return True, 0
    
    print(f"正在处理{index_type}的{len(actual_new_items)}个新ID")
    
    # 根据索引类型选择更新方式
    success = False
    cost = 0
    try:
        if index_type == "filename":
            index, cost = build_index_flat(actual_new_items, index_type)
            success = index is not None
        else:
            success, cost, _ = add_small_batch(actual_new_items, index_type)
        
        if success:
            # 更新处理记录（保留该功能用于追踪历史）
            processed_ids = set()
            if os.path.exists(processed_ids_file):
                with open(processed_ids_file, 'r', encoding='utf-8') as f:
                    processed_ids = set(json.load(f))
            processed_ids.update(actual_new_items.keys())
            with open(processed_ids_file, 'w', encoding='utf-8') as f:
                json.dump(list(processed_ids), f)
        return success, cost
    except Exception as e:
        print(f"更新{index_type}索引失败: {str(e)}")
        return False, 0


if __name__ == "__main__":
    # 设置日志系统
    logger = setup_logging()
    
    # 获取当前文件的绝对路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 构建Excel文件的绝对路径
    excel_path = os.path.join(current_dir, "..", "data", "25年报告列表-15-30页.xlsx")
    
    # 读取研报信息
    logger.info("开始读取研报信息...")
    reports_info = pd.read_excel(excel_path)
    reports_info = reports_info.iloc[:50]
    logger.info(f"读取了{len(reports_info)}条研报信息")
    
    # 初始化保存目录
    save_dir = os.path.join(current_dir, "..", "text_dicts")
    os.makedirs(save_dir, exist_ok=True)
    
    # 创建处理记录目录
    processed_dir = os.path.join(save_dir, "processed_records")
    os.makedirs(processed_dir, exist_ok=True)
    
    # 处理记录文件路径
    filename_processed = os.path.join(processed_dir, "filename_processed_ids.json")
    headers_processed = os.path.join(processed_dir, "headers_processed_ids.json")
    content_processed = os.path.join(processed_dir, "content_processed_ids.json")

    # 使用线程池并行处理
    with ThreadPoolExecutor(max_workers=12) as executor:
        # 新增三个字典用于记录增量数据
        delta_filename = {}
        delta_headers = {}
        delta_content = {}
        
        # 提交所有任务
        future_to_row = {
            executor.submit(process_single_report, row_tuple): i 
            for i, row_tuple in enumerate(reports_info.iterrows())
        }

        # 处理结果
        for future in tqdm(as_completed(future_to_row), desc="处理研报文件", 
                          total=len(reports_info), ncols=80, mininterval=1.0,
                          position=0, leave=True):
            try:
                # 获取当前处理的行索引
                row_index = future_to_row[future]
                current_id = reports_info.iloc[row_index]['id']
                logger.info(f"正在处理文件ID: {current_id}")  # 使用logger替代print
                
                filename_dict, headers_dict, content_dict = future.result()
                
                # 记录增量数据
                delta_filename.update(filename_dict)
                delta_headers.update(headers_dict)
                delta_content.update(content_dict)
                
                logger.info(f"已处理文件ID: {current_id}")  # 使用logger替代print
            except Exception as exc:
                logger.error(f'处理报告时发生错误: {exc}')  # 使用logger替代print
    
    # 3. 增量更新faiss索引，只处理新增数据
    print("开始更新FAISS索引...")
    total_cost = 0
    
    if delta_filename:
        success, cost = update_faiss_indices(delta_filename, "filename", filename_processed)
        total_cost += cost
    
    if delta_headers:
        success, cost = update_faiss_indices(delta_headers, "header", headers_processed)
        total_cost += cost
    
    if delta_content:
        success, cost = update_faiss_indices(delta_content, "content", content_processed)
        total_cost += cost
    
    print(f"FAISS索引更新完成，总成本: {total_cost}")