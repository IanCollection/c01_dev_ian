import faiss
import os
import psutil
import time
import logging
import numpy as np # 导入 numpy
import json      # 导入 json

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 全局变量存储加载的索引和 IDs ---
index_filename = None
ids_filename = None # 新增：存储 filename IDs
index_header = None
ids_header = None   # 新增：存储 header IDs
index_content = None
ids_content = None  # 新增：存储 content IDs

# --- 配置 ---
# --- 恢复：直接使用指定的相对路径 ---
# 假设 load_faiss_index.py 在 flask_script 目录下
# 项目根目录是上一级
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 正确的索引目录路径
FAISS_INDEX_DIR = os.path.join(project_root, 'database', 'faiss_index_sc')
logging.info(f"[load_faiss_index] 使用固定相对路径计算的 Faiss 索引目录: {FAISS_INDEX_DIR}")
# --- --- --- --- --- --- --- --- --- ---

# --- 使用正确的索引文件名和计算出的 FAISS_INDEX_DIR ---
FILENAME_INDEX_PATH = os.path.join(FAISS_INDEX_DIR, 'filename_index_flat.index')
HEADER_INDEX_PATH = os.path.join(FAISS_INDEX_DIR, 'header_index_IVFPQ.index')
CONTENT_INDEX_PATH = os.path.join(FAISS_INDEX_DIR, 'content_index_IVFPQ.index')
# 新增 ID 文件路径
FILENAME_IDS_PATH = os.path.join(FAISS_INDEX_DIR, 'filename_index_flat_ids.json')
HEADER_IDS_PATH = os.path.join(FAISS_INDEX_DIR, 'header_ids.npy')
CONTENT_IDS_PATH = os.path.join(FAISS_INDEX_DIR, 'content_ids.npy')
GPU_ID = 0 # 使用第一个 GPU

# 尝试导入 pynvml 并初始化
try:
    import pynvml
    pynvml.nvmlInit()
    has_pynvml = True
    logging.info("pynvml 初始化成功。")
except ImportError:
    has_pynvml = False
    logging.warning("未找到 pynvml 库。无法进行 GPU 内存监控。请运行 'pip install nvidia-ml-py' 安装。")
except pynvml.NVMLError as e:
     has_pynvml = False
     logging.warning(f"pynvml 初始化失败: {e}。可能是没有 NVIDIA 驱动或权限问题。无法进行 GPU 内存监控。")


def get_cpu_usage():
    """获取当前 CPU 使用率和内存使用情况"""
    pid = os.getpid()
    process = psutil.Process(pid)
    cpu_percent = process.cpu_percent(interval=0.1) # 获取进程CPU使用率
    memory_info = process.memory_info() # 获取进程内存使用情况
    memory_gb = memory_info.rss / (1024 * 1024 * 1024) # 转换为 GB
    total_memory = psutil.virtual_memory()
    total_memory_gb = total_memory.total / (1024 * 1024 * 1024)
    memory_percent = total_memory.percent # 系统总内存使用率

    logging.info(f"CPU 使用率 (当前进程): {cpu_percent:.2f}%")
    logging.info(f"内存使用 (当前进程): {memory_gb:.2f} GB")
    logging.info(f"内存使用率 (系统): {memory_percent:.2f}% ({total_memory.used / (1024*1024*1024):.2f}/{total_memory_gb:.2f} GB)")


def load_indexes():
    """加载 Faiss 索引和对应的 IDs 到 CPU 内存"""
    global index_filename, index_header, index_content
    global ids_filename, ids_header, ids_content # 声明全局 IDs 变量

    start_time = time.time()
    logging.info("开始加载 Faiss 索引和 IDs 到 CPU...")

    # 检查索引目录是否存在
    if not os.path.isdir(FAISS_INDEX_DIR):
        logging.error(f"Faiss 索引目录未找到或不是一个目录: {FAISS_INDEX_DIR}")
        return False

    # 检查索引和 ID 文件是否存在 (使用修正后的路径变量)
    paths_to_check = {
        "Filename Index": FILENAME_INDEX_PATH,
        "Filename IDs": FILENAME_IDS_PATH, # 检查 ID 文件
        "Header Index": HEADER_INDEX_PATH,
        "Header IDs": HEADER_IDS_PATH,     # 检查 ID 文件
        "Content Index": CONTENT_INDEX_PATH,
        "Content IDs": CONTENT_IDS_PATH    # 检查 ID 文件
    }
    all_files_found = True
    for name, path in paths_to_check.items():
        if not os.path.exists(path):
            logging.error(f"{name} 文件未找到: {path}")
            all_files_found = False

    if not all_files_found:
        logging.error("一个或多个 Faiss 索引文件缺失，无法加载。")
        return False

    try:
        # 1. 加载 filename index 和 IDs 到 CPU
        logging.info(f"加载 filename index 从: {FILENAME_INDEX_PATH} (CPU)")
        index_filename = faiss.read_index(FILENAME_INDEX_PATH)
        logging.info(f"加载 filename IDs 从: {FILENAME_IDS_PATH}")
        with open(FILENAME_IDS_PATH, 'r', encoding='utf-8') as f:
            ids_filename = json.load(f)
        logging.info(f"Filename index 和 IDs 加载完成. 索引包含 {index_filename.ntotal} 个向量, IDs 列表长度 {len(ids_filename)}.")
        get_cpu_usage()

        # 2. 加载 header index 和 IDs 到 CPU
        logging.info(f"加载 header index 从: {HEADER_INDEX_PATH} (CPU)")
        index_header = faiss.read_index(HEADER_INDEX_PATH)
        logging.info(f"加载 header IDs 从: {HEADER_IDS_PATH}")
        ids_header = np.load(HEADER_IDS_PATH)
        logging.info(f"Header index 和 IDs 加载完成. 索引包含 {index_header.ntotal} 个向量, IDs 数组形状 {ids_header.shape}.")
        get_cpu_usage()

        # 3. 加载 content index 和 IDs 到 CPU
        logging.info(f"加载 content index 从: {CONTENT_INDEX_PATH} (CPU)")
        index_content = faiss.read_index(CONTENT_INDEX_PATH)
        logging.info(f"加载 content IDs 从: {CONTENT_IDS_PATH}")
        ids_content = np.load(CONTENT_IDS_PATH)
        logging.info(f"Content index 和 IDs 加载完成. 索引包含 {index_content.ntotal} 个向量, IDs 数组形状 {ids_content.shape}.")
        get_cpu_usage()

        end_time = time.time()
        logging.info(f"所有 Faiss 索引和 IDs 加载到 CPU 完成，耗时 {end_time - start_time:.2f} 秒。")
        get_cpu_usage() # 显示最终 CPU 占用
        
        # 返回加载的资源字典 (不包含 gpu_resources)
        return {
            'index_filename': index_filename,
            'ids_filename': ids_filename,
            'index_header': index_header,
            'ids_header': ids_header,
            'index_content': index_content,
            'ids_content': ids_content,
        }

    except Exception as e:
        logging.error(f"加载索引或 IDs 时出错: {e}", exc_info=True)
        # 加载失败时，确保全局变量被重置为 None
        index_filename = None
        index_header = None
        index_content = None
        ids_filename = None
        ids_header = None
        ids_content = None
        return False

# --- 主程序 ---
if __name__ == "__main__":
    logging.info("开始执行 Faiss 索引加载脚本...")
    get_cpu_usage() # 初始 CPU 状态
    if has_pynvml:
        pass

    success = load_indexes()

    if success:
        logging.info("索引已成功加载到内存。")
        # 让脚本保持运行以便其他进程可以导入变量 (如果需要)
        # 如果只是预加载，不需要保持运行，可以注释掉下面的循环
        # print("按 Ctrl+C 退出...")
        # try:
        #     while True:
        #         time.sleep(60) # 每分钟检查一次资源
        #         get_cpu_usage()
        #         if has_pynvml: get_gpu_usage(GPU_ID)
        # except KeyboardInterrupt:
        #     logging.info("脚本退出。")
    else:
        logging.error("索引加载失败。")

    # 清理 pynvml (如果已初始化)
    if has_pynvml:
        try:
            pynvml.nvmlShutdown()
            logging.info("pynvml 已关闭。")
        except pynvml.NVMLError as e:
             logging.error(f"关闭 pynvml 时出错: {e}") 