import concurrent
import io
import time
import os
import sys
import datetime
import threading
import subprocess
import faiss  # 添加 faiss 导入
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)

from Agent.overview_agent_part2 import generate_third_level_titles, format_third_level_result_to_json, \
    generate_second_level_titles, format_third_level_result_to_json_v2
from scrpit.query_report_policy_ic_indicator import query_relative_data_v2, query_relative_data_v3
import asyncio
import multiprocessing
import time
from asyncio import as_completed
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

import numpy as np
from flask import Flask, request, jsonify, Response, stream_with_context
from Agent.Overview_agent import (
    title_augement_stream, generate_toc_from_focus_points, match_focus_points,
    semantic_enhancement_agent, overview_conclusion, generate_toc_from_focus_points_stream,
    title_augement_without_cot, generate_final_toc_v2_stream_no_title,
    year_extract_from_title, generate_ana_instruction, generate_toc_from_focus_points_stream_no_title,
    overview_conclusion_stream
)
from database.neo4j_query import get_neo4j_driver
from palyground import extract_headlines, generate_section_list
from scrpit.milestone_4 import (
    process_first_level_title, process_third_level_title,
    process_ic_trends, process_second_level_title_for_edit, process_section_tree_serial,
    process_first_level_title_serial, process_first_level_title_no_refine
)
from scrpit.overview_report import (
    generate_comprehensive_toc_v2, build_overview_with_report,
    generate_comprehensive_toc_v2_stream, generate_final_toc_v2_stream, 
    generate_comprehensive_toc_v2_stream_no_title
)
from scrpit.overview_title import generate_comprehensive_toc_with_focus_points, match_focus_points_from_file
from scrpit.policy_query import search_es_policy_v2
from database.faiss_query import search
from Agent.policy_agent import title_semantic_enhancement_agent

import json
import logging
from logging.handlers import RotatingFileHandler  # 导入日志轮转处理器

from scrpit.tune_second_level_headers import modify_second_level_headers, modify_first_level_headers, \
    modify_first_level_headers_stream, modify_second_level_headers_stream, modify_second_level_headers_stream_no_refine, \
    modify_first_level_headers_stream_no_refine

# 导入预加载函数
from flask_script.load_faiss_index import load_indexes
# 导入全局 FAISS 资源模块
from database.faiss_globals import set_faiss_resources

# --- 配置日志 ---
# 移除 basicConfig，使用 Handler 进行更灵活的配置
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf-8')
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(
    'flask.log',  # 指定日志文件名为 flask.log
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
log_handler.setFormatter(log_formatter)

# 获取根 logger 并添加 handler
logger = logging.getLogger() # 获取根 logger
logger.setLevel(logging.INFO) # 设置根 logger 的级别
logger.addHandler(log_handler)

# 可选：如果你还想在控制台输出日志
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)
# --- 日志配置结束 ---


app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # 这个设置在某些情况下可能不够

# 自定义JSON编码器
class CustomJSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        kwargs['ensure_ascii'] = False
        super().__init__(*args, **kwargs)

app.json_encoder = CustomJSONEncoder

# 定期监控 GPU 状态的函数
def gpu_status_monitor():
    """每60秒记录一次 GPU 状态，减少频率"""
    if not hasattr(gpu_status_monitor, "running"):
        gpu_status_monitor.running = True
    else:
        return  # 防止多次启动
        
    logger.info("GPU 状态监控线程已启动")
    
    while True:
        try:
            # 使用 nvidia-smi 获取详细信息
            output = subprocess.check_output(
                "nvidia-smi --query-gpu=timestamp,name,memory.used,memory.total,utilization.gpu,temperature.gpu --format=csv",
                shell=True
            ).decode('utf-8').strip()
            
            logger.info(f"[定期GPU监控] GPU 状态:\n{output}")
            
            # 检查进程使用的 GPU 资源
            try:
                process_output = subprocess.check_output(
                    "nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv",
                    shell=True
                ).decode('utf-8').strip()
                if len(process_output.split('\n')) > 1:  # 有进程在使用 GPU
                    logger.info(f"[定期GPU监控] GPU 进程使用情况:\n{process_output}")
            except:
                pass
                
        except Exception as e:
            logger.error(f"[定期GPU监控] 获取 GPU 状态失败: {e}")
            
        # 每60秒记录一次，降低频率
        time.sleep(120)

# --- 在应用启动时预加载 Faiss 索引 ---
with app.app_context():
    logger.info("开始执行 Faiss 索引预加载...")
    faiss_resources = load_indexes()
    if faiss_resources:
        # 将加载的资源设置到全局变量
        set_faiss_resources(faiss_resources)
        logger.info("Faiss 索引预加载成功并已设置到全局变量！")
        
        # 启动 GPU 监控线程
        if faiss.get_num_gpus() > 0:
            gpu_monitor_thread = threading.Thread(target=gpu_status_monitor, daemon=True)
            gpu_monitor_thread.start()
            logger.info(f"已启动 GPU 监控线程")
    else:
        logger.error("Faiss 索引预加载失败！API 可能无法正常工作或回退到按需加载。")
# --- 预加载结束 ---

# 错误处理装饰器
def error_handler(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error(f"API错误: {str(e)}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    return decorated_function

# 请求验证装饰器
def validate_json_request(required_fields=None):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                data = request.get_json()
                if not data:
                    return jsonify({'error': '请提供有效的JSON数据'}), 400
                
                if required_fields:
                    missing_fields = [field for field in required_fields if field not in data]
                    if missing_fields:
                        return jsonify({'error': f'缺少必要字段: {", ".join(missing_fields)}'}), 400
                
                return f(*args, **kwargs)
            except Exception as e:
                return jsonify({'error': f'请求验证失败: {str(e)}'}), 400
        return decorated_function
    return decorator

@app.route('/overview_v1', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def generate_keywords_overview_v1():
    """
    第一步：研报标题语义增强API，打印思维链，以及返回语义增强后的标题和关键词

    功能:
    1. 对输入的研报标题进行语义增强,扩展标题内容
    2. 提取三个维度的关键词:
       - 核心关键词: 与主题直接相关的关键术语
       - 领域关键词: 相关行业、市场、技术的细分类别
       - 聚焦关键词: 研究方向、趋势、政策和产业链相关术语
    3. 流式输出思维链过程
    4. 根据生成的标题、关注点和关键词流式生成目录结构

    同时调用 函数，进行基础的语义增强，来检索并且返回相关政策（max：10）

    请求参数:
        title (str): 研报标题

    返回:
        Response: 流式响应,包含以下事件:
            - think: 思维链过程
            - title: 扩展后的标题
            - keyword: 关键词内容
            - toc: 目录生成过程
            - result: 最终JSON结果
    """
    data = request.get_json()
    title = data['title']
    purpose = data.get('purpose', None)

    def generate():
        # 初始化变量用于存储标题、关键词和关注点
        enhanced_title = ""
        keywords = ""
        focus_points = ""
        # 先获取完整的语义增强结果
        final_result = None
        for chunk in title_augement_stream(title, purpose):
            # 检查chunk是否为字典类型
            if isinstance(chunk, dict):
                # 如果是字典，则认为是最终结果
                final_result = chunk
                continue  # 跳过后续处理
            
            # 处理字符串类型的chunk
            if isinstance(chunk, str):
                # 如果是扩展标题内容
                if chunk.startswith("\n\n扩展标题:"):
                    enhanced_title = chunk
                    yield f"event: title\ndata: {chunk}\n\n"
                # 如果是关键词内容
                elif chunk.startswith("\n关键词:") or chunk.startswith("- "):
                    keywords += chunk + "\n"
                    yield f"event: keyword\ndata: {chunk}\n\n"
                # 其他思维链内容
                else:
                    yield f"event: think\ndata: {chunk}\n\n"
            # 其他类型的chunk（如果有）
            else:
                print(f"Unexpected chunk type: {type(chunk)}")
        # 处理最终结果
        if final_result:
            # 将标题和关键词拼接
            combined = f"{enhanced_title}"
            # 调用match_focus_points_from_file获取结果
            result, focus_points_mapping, focus_points, second_levels = match_focus_points_from_file(combined)
            # 将结果添加到返回数据中
            final_result.update({
                'combined': combined,
                'result': result,
                'focus_points_mapping': focus_points_mapping,
                'focus_points': focus_points,
                'second_level_focus_points': second_levels
            })
            yield f"event: result\ndata: {json.dumps(final_result, ensure_ascii=False)}\n\n"
            
        previous_toc = ""  # 保存之前的目录内容
        for toc_chunk in generate_toc_from_focus_points_stream_no_title(enhanced_title, focus_points, keywords,purpose):
            if isinstance(toc_chunk, dict):
                # 对于最终结果（字典类型），直接发送
                yield f"event: toc\ndata: {json.dumps(toc_chunk, ensure_ascii=False)}\n\n"
            else:
                # 对于字符串类型，计算增量内容
                if len(toc_chunk) > len(previous_toc):
                    # 只发送新增的内容
                    new_content = toc_chunk[len(previous_toc):]
                    yield f"event: toc\ndata: {new_content}\n\n"
                    previous_toc = toc_chunk

    return Response(stream_with_context(generate()),
                    content_type='text/event-stream')



@app.route('/overview_v1_no_cot', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def generate_keywords_overview_v1_no_cot():
    """
    第一步：研报标题语义增强AP，以及返回语义增强后的标题和关键词

    功能:
    1. 对输入的研报标题进行语义增强,扩展标题内容
    2. 提取三个维度的关键词:
       - 核心关键词: 与主题直接相关的关键术语
       - 领域关键词: 相关行业、市场、技术的细分类别
       - 聚焦关键词: 研究方向、趋势、政策和产业链相关术语
    3. 根据生成的标题、关注点和关键词流式生成目录结构

    同时调用 函数，进行基础的语义增强，来检索并且返回相关政策（max：10）

    请求参数:
        title (str): 研报标题

    返回:
        Response: 流式响应,包含以下事件:
            - title: 扩展后的标题
            - keyword: 关键词内容
            - toc: 目录生成过程
            - result: 最终JSON结果
    """
    data = request.get_json()
    title = data['title']
    purpose = data.get('purpose', None)

    def generate():
        # 初始化变量用于存储标题、关键词和关注点
        enhanced_title = ""
        keywords = ""
        focus_points = ""
        
        # 获取语义增强结果
        final_result, _, _ = title_augement_without_cot(title, purpose)
        
        # 处理扩展标题
        enhanced_title = final_result.get('expanded_title', '')
        yield f"event: title\ndata: {enhanced_title}\n\n"
        
        # 处理关键词
        keywords_dict = final_result.get('keywords', {})
        keywords = ""
        for key, values in keywords_dict.items():
            keywords += f"{key}:\n"
            for value in values:
                keywords += f"- {value}\n"
            yield f"event: keyword\ndata: {keywords}\n\n"

        # 处理最终结果
        if final_result:
            # 将标题和关键词拼接
            combined = f"{enhanced_title}"
            # 调用match_focus_points_from_file获取结果
            result, focus_points_mapping, focus_points = match_focus_points_from_file(combined)
            # 将结果添加到返回数据中
            final_result.update({
                'combined': combined,
                'result': result,
                'focus_points_mapping': focus_points_mapping,
                'focus_points': focus_points
            })
            yield f"event: result\ndata: {json.dumps(final_result, ensure_ascii=False)}\n\n"
            
            # 流式生成目录
            for toc_chunk in generate_toc_from_focus_points_stream(enhanced_title, focus_points, keywords):
                if isinstance(toc_chunk, dict):
                    yield f"event: toc\ndata: {json.dumps(toc_chunk, ensure_ascii=False)}\n\n"
                else:
                    yield f"event: toc\ndata: {toc_chunk}\n\n"

    return Response(stream_with_context(generate()),
                    content_type='text/event-stream')



@app.route('/overview_v2', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def overview_v2():
    """
    生成研报目录的流式API
    
    请求参数:
        title (str): 研报标题
        
    返回:
        Response: 流式响应，包含目录生成过程
    """
    data = request.get_json()
    title = data['title']
    purpose = data.get('purpose', None)

    def generate():
        try:
            # 获取初始数据
            print('开始生成目录')
            new_title, relative_reports, keywords, time = build_overview_with_report(title, purpose)
            print('初步检索完成')

            reports_node = search(title, index_type='filename', top_k=10)
            
            # 将 NumPy 数组转换为 Python 列表
            if isinstance(reports_node, np.ndarray):
                reports_node = reports_node.tolist()
            
            # 确保所有的 numpy.int64 类型都被转换为 Python int
            reports_node = [int(node) if isinstance(node, np.integer) else node for node in reports_node]
            
            # 现在 reports_node 是一个普通的 Python 列表，可以被 JSON 序列化
            yield f"event: reports_node\ndata: {json.dumps({'reports_node': reports_node}, ensure_ascii=False)}\n\n"
            yield f"event: toc_progress\ndata: {json.dumps({'status': 'node检索完成', 'title': new_title, 'keywords': keywords, 'time': time}, ensure_ascii=False)}\n\n"

            relative_reports = query_file_batch_nodes(reports_node)
            yield f"event: toc_progress\ndata: {json.dumps({'status': '相关研报搜索完成', 'title': new_title, 'keywords': keywords, 'time': time}, ensure_ascii=False)}\n\n"
            
            # 生成综合目录（流式）
            # 记录是否已发送目录内容
            # toc_content_sent = False
            
            for chunk in generate_comprehensive_toc_v2_stream_no_title(title, relative_reports, keywords,purpose):
                if isinstance(chunk, dict):
                    event_type = chunk['event']
                    event_data = chunk['data']
                    
                    # 如果是目录内容（不是状态更新），直接发送文本
                    if event_type == 'toc_progress' and not isinstance(event_data, dict):
                        yield f"event: {event_type}\ndata: {event_data}\n\n"
                        # toc_content_sent = True # 如果发送了内容，标记一下
                    else:
                        # 其他类型的数据（如状态更新）仍然使用JSON格式
                        yield f"event: {event_type}\ndata: {json.dumps(event_data, ensure_ascii=False)}\n\n"
                elif isinstance(chunk, tuple) and len(chunk) == 2 and isinstance(chunk[1], float):
                    # 假设这是 (error_message_string, cost_float) 格式的错误元组
                    error_message = chunk[0]
                    logger.warning(f"接收到来自 generate_comprehensive_toc_v2_stream_no_title 的错误元组: {error_message}")
                    # 将其包装成标准的 error event 发送给前端
                    yield f"event: error\ndata: {json.dumps({'error': error_message}, ensure_ascii=False)}\n\n"
                    # 可以考虑在此处停止或继续处理（取决于业务逻辑）
                    break # 发生错误，停止处理
                elif isinstance(chunk, str):
                     # 如果直接 yield 了字符串（例如目录内容）
                     yield f"event: toc_progress\ndata: {chunk}\n\n"
                     # toc_content_sent = True
                else:
                    # 处理未预期的 chunk 类型
                    logger.warning(f"接收到未预期的 chunk 类型: {type(chunk)}, 内容: {chunk}")
                    # 可以选择忽略或发送错误
                    yield f"event: error\ndata: {json.dumps({'error': f'未预期的流数据类型: {type(chunk)}'}, ensure_ascii=False)}\n\n"
                    break # 发生意外情况，停止处理

            # # 如果没有发送过目录内容，输出一个错误提示
            # if not toc_content_sent:
            #     yield f"event: error\ndata: {json.dumps({'error': '目录生成失败，未能获取内容'}, ensure_ascii=False)}\n\n"
            #
            # 完成事件移到最后
            yield f"event: complete\ndata: {json.dumps({'status': 'complete'}, ensure_ascii=False)}\n\n"

        except Exception as e:
            logger.error(f"生成目录时出错: {str(e)}", exc_info=True)
            yield f"event: error\ndata: {json.dumps({'error': str(e)}, ensure_ascii=False)}\n\n"

    return Response(stream_with_context(generate()),
                        content_type='text/event-stream')



@app.route('/overview_v3', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def overview_v3():
    """
    生成研报综合目录的API
    
    参数:
        reports_overview: 研报概览数据
        general_overview: 通用概览数据
        title: 输入标题
        
    返回:
        full_section_list: 处理后的完整章节列表的UTF-8编码
    """
    request_start_time = time.time() # 记录请求开始时间
    logger.info("进入 overview_v3 接口") # 添加日志

    def process_sections(reports_overview, general_overview, topic,purpose):
        process_sections_start_time = time.time()
        logger.info(f"进入 process_sections，topic: {topic}") # 添加日志
        # 生成最终概览
        # write_log('开始生成最终概览') # 改用 logger
        logger.info('开始生成最终概览')
        overview_start_time = time.time()
        try: # 添加 try-except 块
            if isinstance(general_overview, list) and len(general_overview) >= 1: # 检查 general_overview 是否是列表且非空
                logger.info("使用 general_overview[0] 生成最终概览")
                final_overview, _ = overview_conclusion(reports_overview, general_overview[0], topic,purpose)
            else:
                logger.warning("general_overview 不是预期的列表格式或为空，将直接使用")
                final_overview, _ = overview_conclusion(reports_overview, general_overview, topic,purpose)
            overview_end_time = time.time()
            logger.info(f"最终概览生成完成, 耗时: {overview_end_time - overview_start_time:.2f} 秒") # 记录部分概览内容
            # logger.info(f"最终概览生成完成: {final_overview}") # 避免打印过长内容
        except Exception as e:
            overview_end_time = time.time()
            logger.error(f"生成最终概览时出错: {e}, 耗时: {overview_end_time - overview_start_time:.2f} 秒", exc_info=True) # 记录错误
            return [] # 返回空列表表示失败

        print(f"====="*10)
        print(f"完全目录：{final_overview}")
        print(f"=====" * 10)
        
        extract_start_time = time.time()
        try: # 添加 try-except 块
            # 提取章节内容
            content_json = extract_headlines(final_overview)
            section_list = generate_section_list(content_json)
            extract_end_time = time.time()
            logger.info(f"提取到 {len(section_list)} 个一级章节, 耗时: {extract_end_time - extract_start_time:.2f} 秒")
        except Exception as e:
            extract_end_time = time.time()
            logger.error(f"提取章节内容时出错: {e}, 耗时: {extract_end_time - extract_start_time:.2f} 秒", exc_info=True) # 记录错误
            return [] # 返回空列表表示失败

        full_section_list = [None] * len(section_list) # 预分配列表以保持顺序
        # write_log(f"开始处理章节，共 {len(section_list)} 个一级标题") # 改用 logger
        logger.info(f"开始处理章节，共 {len(section_list)} 个一级标题")
        
        # 使用线程池并行处理章节
        print(f"当前处理的主题是: {topic}")  # 打印当前处理的主题
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有章节处理任务
            future_to_section = {executor.submit(process_single_section, section, i, topic): i
                                for i, section in enumerate(section_list)}
            
            processed_count = 0
            # 收集处理结果
            for future in concurrent.futures.as_completed(future_to_section):
                section_index = future_to_section[future]
                section_process_start_time = time.time() # 记录单个 section 开始处理时间 (近似)
                try:
                    # result() 会阻塞直到任务完成
                    modified_content, process_time = future.result() # 获取函数返回的处理时间和结果
                    section_process_end_time = time.time() # 记录单个 section 完成时间
                    section_duration = section_process_end_time - section_process_start_time # 外部测量的实际执行时间（包括等待和线程调度）
                    # logger.info(f"Future for section {section_index+1} completed. Reported internal time: {process_time:.2f}s. Measured external time: {section_duration:.2f}s.")

                    if modified_content is not None: # 检查是否处理成功
                        # 确保结果为UTF-8编码 (虽然通常返回的是dict或str)
                        if isinstance(modified_content, bytes):
                            modified_content = modified_content.decode('utf-8')
                        full_section_list[section_index] = modified_content # 按原索引放入结果列表
                        # write_log(f"章节 {section_index+1} 处理完成") # 改用 logger
                        logger.info(f"章节 {section_index+1} 处理成功。内部耗时: {process_time:.2f} 秒。外部测量总耗时: {section_duration:.2f} 秒。")
                        processed_count += 1
                    else:
                         # write_log(f"章节 {section_index+1} 处理失败，返回 None") # 记录处理失败
                         logger.warning(f"章节 {section_index+1} 处理失败 (返回 None)。内部耗时: {process_time:.2f} 秒。外部测量总耗时: {section_duration:.2f} 秒。")
                except Exception as e:
                    # 这个异常是 future.result() 抛出的，表示线程内部的错误
                    section_process_end_time = time.time()
                    section_duration = section_process_end_time - section_process_start_time
                    logger.error(f"处理章节 {section_index+1} 的线程中发生严重错误: {str(e)}。外部测量总耗时: {section_duration:.2f} 秒。", exc_info=True)
                    # write_log(f"处理章节 {section_index+1} 时出错: {str(e)}") # 改用 logger
                    # full_section_list[section_index] 将保持 None
                    continue
        
        # 过滤掉处理失败的 None 值
        final_list = [item for item in full_section_list if item is not None]
        process_sections_end_time = time.time()
        logger.info(f"process_sections 完成，成功处理 {processed_count}/{len(section_list)} 个章节。总耗时: {process_sections_end_time - process_sections_start_time:.2f} 秒。")
        return final_list

    def process_single_section(section, index, topic=None):
        """处理单个章节的独立函数，用于并行处理"""
        single_section_start_time = time.time()
        section_title = section.get('title', 'N/A')
        logger.info(f"开始处理章节 {index + 1}，topic: {topic}, section_title: '{section_title}'")
        modified_content = None # 初始化为 None
        try:
            # 处理一级标题
            step_start_time = time.time()
            logger.info(f"章节 {index + 1}: 调用 process_first_level_title")
            _, processed_first_level = process_first_level_title(section, index, topic)
            step_end_time = time.time()
            logger.info(f"章节 {index + 1}: process_first_level_title 完成, title: {processed_first_level.get('title')}, 耗时: {step_end_time - step_start_time:.2f} 秒")

            # 调整二级标题
            step_start_time = time.time()
            logger.info(f"章节 {index + 1}: 调用 modify_second_level_headers_stream")
            modified_content_second_headings = modify_second_level_headers_stream(processed_first_level, topic)
            step_end_time = time.time()
            logger.info(f"章节 {index + 1}: modify_second_level_headers_stream 完成, 耗时: {step_end_time - step_start_time:.2f} 秒")

            # 调整一级标题
            step_start_time = time.time()
            logger.info(f"章节 {index + 1}: 调用 modify_first_level_headers_stream")
            modified_content = modify_first_level_headers_stream(modified_content_second_headings, topic)
            step_end_time = time.time()
            logger.info(f"章节 {index + 1}: modify_first_level_headers_stream 完成, 耗时: {step_end_time - step_start_time:.2f} 秒")

            single_section_end_time = time.time()
            total_time = single_section_end_time - single_section_start_time
            logger.info(f"成功完成处理章节 {index + 1} ('{section_title}'). 总耗时: {total_time:.2f} 秒")
            # 返回结果和处理时间
            return modified_content, total_time
        except Exception as e:
            single_section_end_time = time.time()
            total_time = single_section_end_time - single_section_start_time
            logger.error(f"处理章节 {index + 1} (Title: '{section_title}') 时发生异常: {str(e)}. 耗时: {total_time:.2f} 秒", exc_info=True)
            # 返回 None 表示此章节处理失败，同时返回处理时间
            return None, total_time

    # 获取请求数据
    data = request.get_json()
    topic = data.get('title', '未知标题') # 提供默认值
    purpose = data.get('purpose', '')
    reports_overview = data.get('reports_overview', '') # 提供默认值
    general_overview = data.get('general_overview', []) # 提供默认值
    logger.info(f"处理标题: {topic}")
    print(f"处理标题: {topic}") # 保留原有 print

    # 处理章节并返回UTF-8编码结果
    full_section_list = process_sections(reports_overview, general_overview, topic,purpose)
    logger.info(f"process_sections 返回 {len(full_section_list)} 个有效章节")
    
    # 修改返回方式，确保中文正确显示
    response_data = {"sections": full_section_list}
    logger.info("准备返回 JSON 响应")
    response = Response(
        json.dumps(response_data, ensure_ascii=False, cls=DateTimeEncoder), # 添加 DateTimeEncoder
        content_type='application/json; charset=utf-8'
    )
    request_end_time = time.time() # 记录请求结束时间
    logger.info(f"overview_v3 接口处理完成。总请求耗时: {request_end_time - request_start_time:.2f} 秒。")
    return response

@app.route('/overview_v3_no_refine', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def overview_v3_no_refine():
    """
    生成研报综合目录的API
    
    参数:
        reports_overview: 研报概览数据
        general_overview: 通用概览数据
        title: 输入标题
        
    返回:
        full_section_list: 处理后的完整章节列表的UTF-8编码
    """
    request_start_time = time.time() # 记录请求开始时间
    logger.info("进入 overview_v3_no_refine 接口") # 添加日志

    def process_sections(reports_overview, general_overview, topic,purpose):
        process_sections_start_time = time.time()
        logger.info(f"进入 process_sections，topic: {topic}") # 添加日志
        # 生成最终概览
        # write_log('开始生成最终概览') # 改用 logger
        logger.info('开始生成最终概览')
        overview_start_time = time.time()
        try: # 添加 try-except 块
            if isinstance(general_overview, list) and len(general_overview) >= 1: # 检查 general_overview 是否是列表且非空
                logger.info("使用 general_overview[0] 生成最终概览")
                final_overview, _ = overview_conclusion(reports_overview, general_overview[0], topic,purpose)
            else:
                logger.warning("general_overview 不是预期的列表格式或为空，将直接使用")
                final_overview, _ = overview_conclusion(reports_overview, general_overview, topic,purpose)
            overview_end_time = time.time()
            logger.info(f"最终概览生成完成, 耗时: {overview_end_time - overview_start_time:.2f} 秒") # 记录部分概览内容
            # logger.info(f"最终概览生成完成: {final_overview}") # 避免打印过长内容
        except Exception as e:
            overview_end_time = time.time()
            logger.error(f"生成最终概览时出错: {e}, 耗时: {overview_end_time - overview_start_time:.2f} 秒", exc_info=True) # 记录错误
            return [] # 返回空列表表示失败

        print(f"====="*10)
        print(f"完全目录：{final_overview}")
        print(f"=====" * 10)
        
        extract_start_time = time.time()
        try: # 添加 try-except 块
            # 提取章节内容
            content_json = extract_headlines(final_overview)
            section_list = generate_section_list(content_json)
            extract_end_time = time.time()
            logger.info(f"提取到 {len(section_list)} 个一级章节, 耗时: {extract_end_time - extract_start_time:.2f} 秒")
        except Exception as e:
            extract_end_time = time.time()
            logger.error(f"提取章节内容时出错: {e}, 耗时: {extract_end_time - extract_start_time:.2f} 秒", exc_info=True) # 记录错误
            return [] # 返回空列表表示失败

        full_section_list = [None] * len(section_list) # 预分配列表以保持顺序
        # write_log(f"开始处理章节，共 {len(section_list)} 个一级标题") # 改用 logger
        logger.info(f"开始检索章节相关信息，共 {len(section_list)} 个一级标题")
        
        # 使用线程池并行处理章节
        print(f"当前处理的主题是: {topic}")  # 打印当前处理的主题
        
        # 创建一个锁用于同步打印
        print_lock = threading.Lock()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有章节处理任务，并传入锁
            future_to_section = {executor.submit(process_single_section, section, i, topic, print_lock): i
                                for i, section in enumerate(section_list)}
            
            processed_count = 0
            # 收集处理结果
            for future in concurrent.futures.as_completed(future_to_section):
                section_index = future_to_section[future]
                section_process_start_time = time.time() # 记录单个 section 开始处理时间 (近似)
                try:
                    # result() 会阻塞直到任务完成
                    modified_content, process_time = future.result() # 获取函数返回的处理时间和结果
                    section_process_end_time = time.time() # 记录单个 section 完成时间
                    section_duration = section_process_end_time - section_process_start_time # 外部测量的实际执行时间（包括等待和线程调度）
                    # logger.info(f"Future for section {section_index+1} completed. Reported internal time: {process_time:.2f}s. Measured external time: {section_duration:.2f}s.")

                    if modified_content is not None: # 检查是否处理成功
                        # 确保结果为UTF-8编码 (虽然通常返回的是dict或str)
                        if isinstance(modified_content, bytes):
                            modified_content = modified_content.decode('utf-8')
                        full_section_list[section_index] = modified_content # 按原索引放入结果列表
                        # write_log(f"章节 {section_index+1} 处理完成") # 改用 logger
                        with print_lock:
                            logger.info(f"章节 {section_index+1} 处理成功。内部耗时: {process_time:.2f} 秒。外部测量总耗时: {section_duration:.2f} 秒。")
                        processed_count += 1
                    else:
                        with print_lock:
                            logger.warning(f"章节 {section_index+1} 处理失败 (返回 None)。内部耗时: {process_time:.2f} 秒。外部测量总耗时: {section_duration:.2f} 秒。")
                except Exception as e:
                    # 这个异常是 future.result() 抛出的，表示线程内部的错误
                    section_process_end_time = time.time()
                    section_duration = section_process_end_time - section_process_start_time
                    with print_lock:
                        logger.error(f"处理章节 {section_index+1} 的线程中发生严重错误: {str(e)}。外部测量总耗时: {section_duration:.2f} 秒。", exc_info=True)
                    # write_log(f"处理章节 {section_index+1} 时出错: {str(e)}") # 改用 logger
                    # full_section_list[section_index] 将保持 None
                    continue
        
        # 过滤掉处理失败的 None 值
        final_list = [item for item in full_section_list if item is not None]
        process_sections_end_time = time.time()
        logger.info(f"process_sections 完成，成功处理 {processed_count}/{len(section_list)} 个章节。总耗时: {process_sections_end_time - process_sections_start_time:.2f} 秒。")
        return final_list

    def process_single_section(section, index, topic=None, print_lock=None):
        """处理单个章节的独立函数，用于并行处理"""
        single_section_start_time = time.time()
        section_title = section.get('title', 'N/A')
        
        # 使用锁确保日志按顺序打印
        if print_lock:
            with print_lock:
                logger.info(f"开始处理章节 {index + 1}，topic: {topic}, section_title: '{section_title}'")
        else:
            logger.info(f"开始处理章节 {index + 1}，topic: {topic}, section_title: '{section_title}'")
            
        try:
            # 处理一级标题
            step_start_time = time.time()
            if print_lock:
                with print_lock:
                    logger.info(f"章节 {index + 1}: 调用 process_first_level_title_no_refine")
            else:
                logger.info(f"章节 {index + 1}: 调用 process_first_level_title_no_refine")
                
            _, processed_first_level = process_first_level_title_no_refine(section, index, topic)
            step_end_time = time.time()
            
            if print_lock:
                with print_lock:
                    logger.info(f"章节 {index + 1}: process_first_level_title_no_refine 完成, title: {processed_first_level.get('title')}, 耗时: {step_end_time - step_start_time:.2f} 秒")
            else:
                logger.info(f"章节 {index + 1}: process_first_level_title_no_refine 完成, title: {processed_first_level.get('title')}, 耗时: {step_end_time - step_start_time:.2f} 秒")

            # # 调整二级标题
            step_start_time = time.time()
            logger.info(f"章节 {index + 1}: 调用 modify_second_level_headers_stream")
            modified_content_second_headings = modify_second_level_headers_stream_no_refine(processed_first_level, topic)
            step_end_time = time.time()
            logger.info(f"章节 {index + 1}: modify_second_level_headers_stream 完成, 耗时: {step_end_time - step_start_time:.2f} 秒")

            # # 调整一级标题
            step_start_time = time.time()
            logger.info(f"章节 {index + 1}: 调用 modify_first_level_headers_stream")
            modified_content = modify_first_level_headers_stream_no_refine(modified_content_second_headings, topic)
            step_end_time = time.time()
            logger.info(f"章节 {index + 1}: modify_first_level_headers_stream 完成, 耗时: {step_end_time - step_start_time:.2f} 秒")


            # modified_content = processed_first_level
            single_section_end_time = time.time()
            total_time = single_section_end_time - single_section_start_time
            
            if print_lock:
                with print_lock:
                    logger.info(f"成功完成处理章节 {index + 1} ('{section_title}'). 总耗时: {total_time:.2f} 秒")
            else:
                logger.info(f"成功完成处理章节 {index + 1} ('{section_title}'). 总耗时: {total_time:.2f} 秒")
                
            # 返回结果和处理时间
            return modified_content, total_time
        except Exception as e:
            single_section_end_time = time.time()
            total_time = single_section_end_time - single_section_start_time
            
            if print_lock:
                with print_lock:
                    logger.error(f"处理章节 {index + 1} (Title: '{section_title}') 时发生异常: {str(e)}. 耗时: {total_time:.2f} 秒", exc_info=True)
            else:
                logger.error(f"处理章节 {index + 1} (Title: '{section_title}') 时发生异常: {str(e)}. 耗时: {total_time:.2f} 秒", exc_info=True)
                
            # 返回 None 表示此章节处理失败，同时返回处理时间
            return None, total_time

    # 获取请求数据
    data = request.get_json()
    topic = data.get('title', '未知标题') # 提供默认值
    purpose = data.get('purpose', '')
    reports_overview = data.get('reports_overview', '') # 提供默认值
    general_overview = data.get('general_overview', []) # 提供默认值
    logger.info(f"处理标题: {topic}")
    print(f"处理标题: {topic}") # 保留原有 print

    # 处理章节并返回UTF-8编码结果
    full_section_list = process_sections(reports_overview, general_overview, topic,purpose)
    logger.info(f"process_sections 返回 {len(full_section_list)} 个有效章节")
    
    # 修改返回方式，确保中文正确显示
    response_data = {"sections": full_section_list}
    logger.info("准备返回 JSON 响应")
    response = Response(
        json.dumps(response_data, ensure_ascii=False, cls=DateTimeEncoder), # 添加 DateTimeEncoder
        content_type='application/json; charset=utf-8'
    )
    request_end_time = time.time() # 记录请求结束时间
    logger.info(f"overview_v3_no_refine 接口处理完成。总请求耗时: {request_end_time - request_start_time:.2f} 秒。")
    return response



@app.route('/overview_v3_no_refine_v2', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def overview_v3_no_refine_v2():
    """
    生成研报综合目录的API（流式版本）
    """
    request_start_time = time.time()
    logger.info("进入 overview_v3_no_refine_v2 接口")

    # 在请求上下文中获取数据
    try:
        data = request.get_json()
        if not data:
            return Response(
                json.dumps({"error": "无效的请求数据"}, ensure_ascii=False),
                mimetype='text/event-stream',
                status=400
            )
        
        topic = data.get('title', '')
        purpose = data.get('purpose', '')
        reports_overview = data.get('reports_overview', '')
        general_overview = data.get('general_overview', [])
        
        if not topic:
            return Response(
                json.dumps({"error": "缺少必要参数 'title'"}, ensure_ascii=False),
                mimetype='text/event-stream',
                status=400
            )
    except Exception as e:
        logger.error(f"解析请求数据时出错: {str(e)}", exc_info=True)
        return Response(
            json.dumps({"error": f"解析请求数据时出错: {str(e)}"}, ensure_ascii=False),
            mimetype='text/event-stream',
            status=400
        )

    def process_single_section(section, index, topic=None, print_lock=None):
        """处理单个章节的独立函数，用于并行处理"""
        single_section_start_time = time.time()
        section_title = section.get('title', 'N/A')

        # 使用锁确保日志按顺序打印
        if print_lock:
            with print_lock:
                logger.info(f"开始处理章节 {index + 1}，topic: {topic}, section_title: '{section_title}'")
        else:
            logger.info(f"开始处理章节 {index + 1}，topic: {topic}, section_title: '{section_title}'")

        try:
            # 处理一级标题
            step_start_time = time.time()
            if print_lock:
                with print_lock:
                    logger.info(f"章节 {index + 1}: 调用 process_first_level_title_no_refine")
            else:
                logger.info(f"章节 {index + 1}: 调用 process_first_level_title_no_refine")

            _, processed_first_level = process_first_level_title_no_refine(section, index, topic)
            step_end_time = time.time()

            if print_lock:
                with print_lock:
                    logger.info(
                        f"章节 {index + 1}: process_first_level_title_no_refine 完成, title: {processed_first_level.get('title')}, 耗时: {step_end_time - step_start_time:.2f} 秒")
            else:
                logger.info(
                    f"章节 {index + 1}: process_first_level_title_no_refine 完成, title: {processed_first_level.get('title')}, 耗时: {step_end_time - step_start_time:.2f} 秒")

            # # 调整二级标题
            step_start_time = time.time()
            logger.info(f"章节 {index + 1}: 调用 modify_second_level_headers_stream")
            modified_content_second_headings = modify_second_level_headers_stream_no_refine(processed_first_level,
                                                                                            topic)
            step_end_time = time.time()
            logger.info(
                f"章节 {index + 1}: modify_second_level_headers_stream 完成, 耗时: {step_end_time - step_start_time:.2f} 秒")

            # # 调整一级标题
            step_start_time = time.time()
            logger.info(f"章节 {index + 1}: 调用 modify_first_level_headers_stream")
            modified_content = modify_first_level_headers_stream_no_refine(modified_content_second_headings, topic)
            step_end_time = time.time()
            logger.info(
                f"章节 {index + 1}: modify_first_level_headers_stream 完成, 耗时: {step_end_time - step_start_time:.2f} 秒")

            # modified_content = processed_first_level
            single_section_end_time = time.time()
            total_time = single_section_end_time - single_section_start_time

            if print_lock:
                with print_lock:
                    logger.info(f"成功完成处理章节 {index + 1} ('{section_title}'). 总耗时: {total_time:.2f} 秒")
            else:
                logger.info(f"成功完成处理章节 {index + 1} ('{section_title}'). 总耗时: {total_time:.2f} 秒")

            # 返回结果和处理时间
            return modified_content, total_time
        except Exception as e:
            single_section_end_time = time.time()
            total_time = single_section_end_time - single_section_start_time

            if print_lock:
                with print_lock:
                    logger.error(
                        f"处理章节 {index + 1} (Title: '{section_title}') 时发生异常: {str(e)}. 耗时: {total_time:.2f} 秒",
                        exc_info=True)
            else:
                logger.error(
                    f"处理章节 {index + 1} (Title: '{section_title}') 时发生异常: {str(e)}. 耗时: {total_time:.2f} 秒",
                    exc_info=True)

            # 返回 None 表示此章节处理失败，同时返回处理时间
            return None, total_time

    def generate(topic, purpose, reports_overview, general_overview):
        logger.info(f"开始处理标题: {topic}")
        
        # 用于存储最终概览内容
        final_overview = None
        
        # 第一阶段：流式生成目录
        try:
            if isinstance(general_overview, list) and len(general_overview) >= 1:
                logger.info("使用 general_overview[0] 生成最终概览")
                for chunk in overview_conclusion_stream(reports_overview, general_overview[0], topic, purpose):
                    if isinstance(chunk, dict):
                        chunk_type = chunk.get('type')
                        content = chunk.get('content')
                        
                        if chunk_type == 'error':
                            # 发送错误事件
                            yield f"event: error\ndata: {json.dumps({'error': content}, ensure_ascii=False)}\n\n"
                            return
                        elif chunk_type == 'final':
                            # 目录生成完成事件
                            final_overview = content
                            yield f"event: overview_complete\ndata: {json.dumps({'content': content}, ensure_ascii=False)}\n\n"
                        elif chunk_type == 'content':
                            # 目录生成进度事件
                            # 注意：如果content本身已经是字符串，可以直接发送，无需json.dumps
                            yield f"event: overview_progress\ndata: {json.dumps({'content': content}, ensure_ascii=False) if not isinstance(content, str) else content}\n\n"
            else:
                logger.warning("general_overview 格式不正确或为空")
                for chunk in overview_conclusion_stream(reports_overview, general_overview, topic, purpose):
                    if isinstance(chunk, dict):
                        chunk_type = chunk.get('type')
                        content = chunk.get('content')
                        
                        if chunk_type == 'error':
                            yield f"event: error\ndata: {json.dumps({'error': content}, ensure_ascii=False)}\n\n"
                            return
                        elif chunk_type == 'final':
                            final_overview = content
                            yield f"event: overview_complete\ndata: {json.dumps({'content': content}, ensure_ascii=False)}\n\n"
                        elif chunk_type == 'content':
                             yield f"event: overview_progress\ndata: {json.dumps({'content': content}, ensure_ascii=False) if not isinstance(content, str) else content}\n\n"


            if not final_overview:
                logger.error("未能生成最终概览")
                # 发送错误事件
                yield f"event: error\ndata: {json.dumps({'error': '未能生成最终概览'}, ensure_ascii=False)}\n\n"
                return

            # 第二阶段：处理生成的目录
            logger.info("开始处理生成的目录")
            # 发送状态更新事件
            yield f"event: status\ndata: {json.dumps({'message': '目录生成完成，开始处理章节...'}, ensure_ascii=False)}\n\n"

            # 提取章节内容
            content_json = extract_headlines(final_overview)
            section_list = generate_section_list(content_json)
            logger.info(f"提取到 {len(section_list)} 个一级章节")

            # 创建线程池处理章节
            full_section_list = [None] * len(section_list)
            print_lock = threading.Lock()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_section = {
                    executor.submit(process_single_section, section, i, topic, print_lock): i
                    for i, section in enumerate(section_list)
                }
                
                processed_count = 0
                for future in concurrent.futures.as_completed(future_to_section):
                    section_index = future_to_section[future]
                    try:
                        modified_content, process_time = future.result()
                        if modified_content is not None:
                            full_section_list[section_index] = modified_content
                            processed_count += 1
                            # 发送章节处理进度事件
                            progress_data = {
                                'message': f'完成章节 {section_index + 1}/{len(section_list)} 处理',
                                'current': section_index + 1,
                                'total': len(section_list)
                            }
                            yield f"event: section_progress\ndata: {json.dumps(progress_data, ensure_ascii=False)}\n\n"
                    except Exception as e:
                        logger.error(f"处理章节 {section_index + 1} 时发生错误: {str(e)}", exc_info=True)
                        # 可以选择发送一个章节处理错误事件
                        yield f"event: error\ndata: {json.dumps({'error': f'处理章节 {section_index + 1} 时发生错误: {str(e)}'}, ensure_ascii=False)}\n\n"
                        continue

            # 过滤掉处理失败的 None 值
            final_list = [item for item in full_section_list if item is not None]
            
            # 发送最终结果事件
            final_result_data = {
                'sections': final_list,
                'total_sections': len(section_list),
                'processed_sections': processed_count
            }
            yield f"event: final_result\ndata: {json.dumps(final_result_data, ensure_ascii=False)}\n\n"

        except Exception as e:
            logger.error(f"处理过程中发生错误: {str(e)}", exc_info=True)
            # 发送通用错误事件
            yield f"event: error\ndata: {json.dumps({'error': str(e)}, ensure_ascii=False)}\n\n"

    # 将数据作为参数传递给生成器函数
    return Response(
        generate(topic, purpose, reports_overview, general_overview),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )



@app.route('/augment_title', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def query_keywords_reports_policy():
    """
    第一步：研报标题语义增强API，打印思维链，以及返回语义增强后的标题和关键词

    功能:
    1. 对输入的研报标题进行语义增强,扩展标题内容
    2. 提取三个维度的关键词:
       - 核心关键词: 与主题直接相关的关键术语
       - 领域关键词: 相关行业、市场、技术的细分类别
       - 聚焦关键词: 研究方向、趋势、政策和产业链相关术语
    3. 流式输出思维链过程

    同时调用 函数，进行基础的语义增强，来检索并且返回相关政策（max：10）
    
    请求参数:
        title (str): 研报标题
        
    返回:
        Response: 流式响应,包含以下事件:
            - think: 思维链过程
            - title: 扩展后的标题
            - keyword: 关键词内容
            - result: 最终JSON结果
            -
    """
    data = request.get_json()
    title = data['title']
    purpose = data.get('purpose', None)
    
    def generate():
        #政策摘要
        for chunk in title_augement_stream(title, purpose):
            # 如果chunk是字典类型,说明是最终JSON结果
            if isinstance(chunk, dict):
                # 将政策列表和报告ID列表添加到结果中
                yield f"event: result\ndata: {json.dumps(chunk, ensure_ascii=False)}\n\n"
            # 如果是扩展标题内容
            elif chunk.startswith("\n\n扩展标题:"):
                yield f"event: title\ndata: {chunk}\n\n"
            # 如果是关键词内容
            elif chunk.startswith("\n关键词:") or chunk.startswith("- "):
                yield f"event: keyword\ndata: {chunk}\n\n"
            # 其他思维链内容
            else:
                yield f"event: think\ndata: {chunk}\n\n"
    
    return Response(stream_with_context(generate()), 
                   content_type='text/event-stream')


@app.route('/augment_title', methods=['POST'])
def report_overview_stage_0_augment_title_api():
    """
    研报标题语义增强API

    功能:
    1. 对输入的研报标题进行语义增强,扩展标题内容
    2. 提取三个维度的关键词:
       - 核心关键词: 与主题直接相关的关键术语
       - 领域关键词: 相关行业、市场、技术的细分类别
       - 聚焦关键词: 研究方向、趋势、政策和产业链相关术语
    3. 流式输出思维链过程

    请求参数:
        title (str): 研报标题

    返回:
        Response: 流式响应,包含以下事件:
            - think: 思维链过程
            - title: 扩展后的标题
            - keyword: 关键词内容
            - result: 最终JSON结果
    """
    try:
        data = request.get_json()
        if not data or 'title' not in data:
            return jsonify({'error': '输入要撰写的研报题目'}), 400

        title = data['title']

        def generate():
            for chunk in title_augement_stream(title):
                # 如果chunk是字典类型,说明是最终JSON结果
                if isinstance(chunk, dict):
                    yield f"event: result\ndata: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                # 如果是扩展标题内容
                elif chunk.startswith("\n\n扩展标题:"):
                    yield f"event: title\ndata: {chunk}\n\n"
                # 如果是关键词内容
                elif chunk.startswith("\n关键词:") or chunk.startswith("- "):
                    yield f"event: keyword\ndata: {chunk}\n\n"
                # 其他思维链内容
                else:
                    yield f"event: think\ndata: {chunk}\n\n"

        return Response(stream_with_context(generate()),
                       content_type='text/event-stream')

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/search', methods=['POST'])
@validate_json_request(['query'])
@error_handler
def search_api():
    """
    第二步：研报文本向量检索API，返回检索到的文档ID列表
    
    功能:
    1. 对输入的文本进行向量检索,返回最相似的文档ID列表
    2. 支持检索不同类型的向量库:
       - filename: 文件名向量库
       - header: 标题向量库  
       - content: 内容向量库(默认)
    3. 可配置返回结果数量
    
    请求参数:
        query (str): 待检索的文本
        index_type (str, 可选): 检索的向量库类型,默认为'content'
        top_k (int, 可选): 返回结果数量,默认为10
        
    返回:
        json: 包含检索到的文档ID列表
    """
    data = request.get_json()
    query = data.get('query')  # 检索文本
    index_type = data.get('index_type', 'filename')  # 默认检索content向量库
    top_k = data.get('top_k', 10)  # 默认返回10条结果

    # 调用search函数执行向量检索
    results = search(
        query=query,  # 检索文本
        index_type=index_type,  # 向量库类型
        top_k=top_k  # 返回结果数量
    )
    
    # 返回检索结果
    return jsonify({
        'report_ids_list': results  # 返回文档ID列表
    })

@app.route('/generate_toc_with_focus_points', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def report_overview_stage_0_focus_points_toc_api():
    """
    生成研报目录结构API
    
    功能:
    1. 根据研报标题和关注点生成三级目录结构
    2. 返回Markdown格式的目录文本
    
    请求参数:
        title (str): 研报标题
        focus_points (str): 关注点字符串
        
    返回:
        json: 包含生成的目录结构和API调用成本
    """
    data = request.get_json()
    title = data.get('title')
    focus_points, cost = match_focus_points(title)
    
    # 调用函数生成目录
    toc, cost = generate_toc_from_focus_points(
        title=title,
        focus_points=focus_points
    )
    
    # 返回结果
    return jsonify({
        'toc': toc,  # 目录结构
        'cost': cost  # API调用成本
    })

@app.route('/search_policy', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def search_policy_api():
    """
    搜索政策API
    
    功能:
    1. 对输入的研报标题进行语义增强
    2. 基于增强后的标题搜索相关政策，并且政策内容进行摘要总结
    
    请求参数:
        title (str): 研报标题
        size (int, 可选): 返回结果数量，默认为5
        
    返回:
        json: 包含政策搜索结果和摘要
    """
    data = request.get_json()
    title = data.get('title')
    size = data.get('size', 5)  # 默认返回5条结果
    
    # 第一步：调用语义增强函数
    augmented_data, enhancement_cost = title_semantic_enhancement_agent(title)
    
    # 第二步：搜索相关政策
    policy_results = search_es_policy_v2(title, augmented_data, size)
    
    # 返回结果
    return jsonify(policy_results)
@app.route('/build_overview_with_report', methods=['POST'])
def build_overview_with_report_api():
    """
    构建研报概览API
    
    功能:
    1. 对输入的研报标题进行语义增强
    2. 提取关键词
    3. 搜索相关研报
    
    请求参数:
        title (str): 研报标题
        purpose (str, 可选): 研报目的
        
    返回:
        json: 包含扩展后的标题、相关研报、关键词和处理时间
    """
    data = request.get_json()
    input_title = data.get('title')
    purpose = data.get('purpose')
    
    def generate():
        # 调用标题增强函数（流式输出思考过程）
        for chunk in title_augement_stream(input_title, purpose):
            if isinstance(chunk, dict):
                # 最终结果
                result_json = chunk
                new_title = result_json["new_title"]
                keywords = result_json["keywords"]
                
                # 提取所有关键词并合并
                all_keywords = []
                all_keywords.extend(keywords.get('core_keywords', []))
                all_keywords.extend(keywords.get('domain_keywords', []))
                all_keywords.extend(keywords.get('focus_keywords', []))
                
                # 去重并转换为字符串
                unique_keywords = list(set(all_keywords))
                
                # 搜索相关研报
                relative_reports = search(input_title, index_type='filename', top_k=10)
                
                # # 处理时间字段，确保可序列化
                # processing_time = result_json.get('time', 0)
                # if isinstance(processing_time, np.int64):
                #     processing_time = int(processing_time)
                #
                # 返回最终结果
                final_result = {
                    'new_title': new_title,
                    'relative_reports': relative_reports,
                    'keywords': keywords,
                    # 'processing_time': processing_time
                }
                yield f"event: result\ndata: {json.dumps(final_result, ensure_ascii=False, default=str)}\n\n"
            elif chunk.startswith("\n\n扩展标题:"):
                yield f"event: title\ndata: {chunk}\n\n"
            elif chunk.startswith("\n关键词:") or chunk.startswith("- "):
                yield f"event: keyword\ndata: {chunk}\n\n"
            else:
                yield f"event: think\ndata: {chunk}\n\n"
    
    return Response(stream_with_context(generate()), 
                   content_type='text/event-stream')

@app.route('/query_filenode_get_report_info', methods=['POST'])
@validate_json_request(['file_node_ids'])
@error_handler
def query_file_batch_api():
    """
    批量查询多个file_node_id的文件节点及其关联信息API
    
    请求参数:
        file_node_ids (list): 文件节点ID列表
        
    返回:
        JSON: 包含多个文件信息的字典列表，每个字典包含文件信息和headers_content
    """
    data = request.get_json()
    file_node_ids = data['file_node_ids']
    
    if not isinstance(file_node_ids, list):
        return jsonify({'error': 'file_node_ids必须是一个列表'}), 400
        
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 批量查询文件节点及其关联的AllHeaders节点
            result = session.run(
                """
                MATCH (f:File) 
                WHERE f.file_node_id IN $file_node_ids
                OPTIONAL MATCH (f)-[:HAS_ALL_HEADERS]->(ah:AllHeaders)
                RETURN f, ah.content as headers_content
                """,
                file_node_ids=file_node_ids
            )
            
            file_infos = []
            for record in result:
                file_info = dict(record["f"])
                # 添加headers_content到文件信息中
                file_info["headers_content"] = record["headers_content"] or ""
                file_infos.append(file_info)
                
            return jsonify({'data': file_infos})
            
    finally:
        driver.close()

@app.route('/generate_report_outline', methods=['POST'])
def generate_report_outline_api():
    """
    生成研报大纲的API接口
    
    请求参数:
        input_title (str): 研报标题
        relative_reports (list): 相关研报列表
        keywords (list): 关键词列表
        
    返回:
        JSON: 包含生成的研报大纲结构
    """
    start_time = time.time()
    data = request.get_json()
    
    input_title = data.get('input_title')
    relative_reports = data.get('relative_reports', [])
    keywords = data.get('keywords', [])
    topic = data.get('topic', '')
    # 生成综合目录
    # 使用异步方式生成综合目录和常规目录
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def get_reports_overview():
        return generate_comprehensive_toc_v2(input_title, relative_reports, keywords)
        
    async def get_general_overview():
        return generate_comprehensive_toc_with_focus_points(input_title, keywords)
        
    async def run_tasks():
        reports_task = loop.create_task(get_reports_overview())
        general_task = loop.create_task(get_general_overview())
        await asyncio.gather(reports_task, general_task)
        return reports_task.result(), general_task.result()
        
    try:
        (reports_overview, all_reports, reports_cost), (general_overview, focus_points) = loop.run_until_complete(run_tasks())
    finally:
        loop.close()

    # 基于历史研报目录和常规目录生成总目录
    final_overview, cost = overview_conclusion(reports_overview, general_overview[0], input_title)

    content_json = extract_headlines(final_overview)
    section_list = generate_section_list(content_json)

    # 设置合理的一级标题并行处理数量
    cpu_count = multiprocessing.cpu_count()
    first_level_max_workers = min(2, cpu_count // 2, len(section_list))

    # 初始化存储所有一级标题的列表
    full_section_list = [None] * len(section_list)

    # 使用线程池并行处理一级标题
    with ThreadPoolExecutor(max_workers=first_level_max_workers) as executor:
        # 提交所有一级标题处理任务，带上索引以便识别顺序
        first_level_futures = {executor.submit(process_first_level_title, section, i, topic): i 
                              for i, section in enumerate(section_list)}
        
        # 处理完成的一级标题结果
        for future in as_completed(first_level_futures):
            try:
                # 获取结果和原始索引
                original_index, processed_first_level = future.result()
                
                # 将结果放入对应位置
                full_section_list[original_index] = processed_first_level
                
            except Exception as e:
                logger.error(f"处理一级标题时出错: {str(e)}", exc_info=True)

    # 移除可能存在的None值（如果有一级标题处理失败）
    full_section_list = [section for section in full_section_list if section is not None]

    modified_content_first_headings = modify_first_level_headers(modify_second_level_headers(full_section_list))

    end_time = time.time()
    processing_time = end_time - start_time
    
    return jsonify({
        'success': True,
        'data': modified_content_first_headings,
        'processing_time': processing_time
    })


@app.route('/generate_report_outline_stream_trunk', methods=['POST'])
def generate_report_outline_api_format_trunks():
    """
    生成研报大纲的API接口

    请求参数:
        input_title (str): 研报标题
        relative_reports (list): 相关研报列表
        keywords (list): 关键词列表

    返回:
        JSON: 包含生成的研报大纲结构
    """
    start_time = time.time()
    data = request.get_json()

    input_title = data.get('input_title')
    relative_reports = data.get('relative_reports', [])
    keywords = data.get('keywords', [])

    # 生成综合目录
    # 使用异步方式生成综合目录和常规目录
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def get_reports_overview():
        return generate_comprehensive_toc_v2(input_title, relative_reports, keywords)

    async def get_general_overview():
        return generate_comprehensive_toc_with_focus_points(input_title, keywords)

    async def run_tasks():
        reports_task = loop.create_task(get_reports_overview())
        general_task = loop.create_task(get_general_overview())
        await asyncio.gather(reports_task, general_task)
        return reports_task.result(), general_task.result()

    try:
        (reports_overview, all_reports, reports_cost), (general_overview, focus_points) = loop.run_until_complete(
            run_tasks())
    finally:
        loop.close()

    

    # 基于历史研报目录和常规目录生成总目录
    final_overview, cost = overview_conclusion(reports_overview, general_overview[0], input_title)

    content_json = extract_headlines(final_overview)
    section_list = generate_section_list(content_json)

    # 设置合理的一级标题并行处理数量
    cpu_count = multiprocessing.cpu_count()


    full_section_list = []
    print(f"总共 {len(section_list)} 个一级标题需要处理")

    # 修改返回方式为流式输出
    def generate():
        # 发送初始进度消息
        yield f"data: {json.dumps({'progress': 0, 'message': '开始生成研报大纲'}, ensure_ascii=False)}\n\n"
        
        total_sections = len(section_list)
        for i, section in enumerate(section_list):
            try:
                # 计算当前进度百分比
                progress = int((i / total_sections) * 100)
                yield f"data: {json.dumps({'progress': progress, 'message': f'正在处理第 {i+1}/{total_sections} 个一级标题'}, ensure_ascii=False)}\n\n"
                
                index, processed_first_level = process_first_level_title(section, i)
                print('开始对当前的一级和二级标题进行调整。')
                modified_content = modify_first_level_headers_stream(
                    modify_second_level_headers_stream(processed_first_level)
                )
                full_section_list.append(modified_content)

                # 将每个标题结构转为流式事件输出
                yield f"data: {json.dumps({'type': 'content', 'data': modified_content}, ensure_ascii=False, cls=DateTimeEncoder)}\n\n"

            except Exception as e:
                print(f"处理一级标题时出错: {str(e)}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
        
        # 保存结果并发送完成消息
        with open('section_list_stream_trunks.json', 'w', encoding='utf-8') as f:
            json.dump(full_section_list, f, indent=4, ensure_ascii=False, cls=DateTimeEncoder)
        print("结果已保存到 section_list_stream_trunks.json")
        
        # 发送完成消息
        yield f"data: {json.dumps({'progress': 100, 'message': '研报大纲生成完成'}, ensure_ascii=False)}\n\n"

    # 返回流式响应
    return Response(generate(), mimetype='text/event-stream')



@app.route('/generate_report', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def generate_report_api():
    """
    生成研报API
    
    请求参数:
        title (str): 研报标题
        
    返回:
        流式响应: 包含生成的研报章节信息
    """
    data = request.get_json()
    input_title = data['title']
    
    # 获取研报数据
    title, reports_node, keywords, time_cost = build_overview_with_report(input_title)
    relative_reports = query_file_batch_nodes(reports_node)
    
    # 修改返回方式为流式输出
    def generate():
        # 生成目录
        reports_overview, all_reports, reports_cost = generate_comprehensive_toc_v2(input_title, relative_reports, keywords)
        general_overview, focus_points = generate_comprehensive_toc_with_focus_points(input_title, keywords)
        final_overview, cost = overview_conclusion(reports_overview, general_overview[0], input_title)
        
        # 处理章节
        content_json = extract_headlines(final_overview)
        section_list = generate_section_list(content_json)
        
        full_section_list = []
        print(f"总共 {len(section_list)} 个一级标题需要处理")

        for i, section in enumerate(section_list):
            try:
                index, processed_first_level = process_first_level_title(section, i)
                print('开始对当前的一级和二级标题进行调整。')
                modified_content = modify_first_level_headers_stream(
                    modify_second_level_headers_stream(processed_first_level)
                )
                full_section_list.append(modified_content)
                
                # 将每个标题结构转为流式事件输出
                yield f"data: {json.dumps(modified_content, ensure_ascii=False, cls=DateTimeEncoder)}\n\n"
                
                print(f"\n--- 一级标题 #{index+1}: {processed_first_level['title']} ---")
                print(json.dumps(processed_first_level, indent=2, ensure_ascii=False, cls=DateTimeEncoder))
                print(f"--- 一级标题 #{index+1} 结束 ---\n")
            except Exception as e:
                print(f"处理一级标题时出错: {str(e)}")
                yield f"data: {json.dumps({'error': str(e)}, ensure_ascii=False)}\n\n"

        # 保存结果
        with open('section_list_stream_trunks.json', 'w', encoding='utf-8') as f:
            json.dump(full_section_list, f, indent=4, ensure_ascii=False, cls=DateTimeEncoder)
        print("结果已保存到 section_list_stream_trunks.json")
        
        # 发送最终的元数据
        metadata = {
            'status': 'complete',
            'title': input_title,
            'time_cost': time_cost,
            'reports_cost': reports_cost,
            'total_cost': cost
        }
        yield f"data: {json.dumps(metadata, ensure_ascii=False, cls=DateTimeEncoder)}\n\n"

    # 返回流式响应
    return Response(generate(), mimetype='text/event-stream')


def query_file_batch_nodes(file_node_ids):
    """
    批量查询多个file_node_id的文件节点及其关联信息
    
    参数:
        file_node_ids (list): 文件节点ID列表
        
    返回:
        list: 包含多个文件信息的字典列表
    """
    if not isinstance(file_node_ids, list):
        raise ValueError('file_node_ids必须是一个列表')
        
    driver = get_neo4j_driver()
    
    try:
        with driver.session() as session:
            # 批量查询文件节点及其关联的AllHeaders节点
            result = session.run(
                """
                MATCH (f:File) 
                WHERE f.file_node_id IN $file_node_ids
                OPTIONAL MATCH (f)-[:HAS_ALL_HEADERS]->(ah:AllHeaders)
                RETURN f, ah.content as headers_content
                """,
                file_node_ids=file_node_ids
            )
            
            file_infos = []
            for record in result:
                file_info = dict(record["f"])
                # 添加headers_content到文件信息中
                file_info["headers_content"] = record["headers_content"] or ""
                file_infos.append(file_info)
                
            return file_infos
            
    finally:
        driver.close()




@app.route('/query_third_title_relative_info', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def query_title_info():
    """
    检索当前标题相关信息的API
    
    参数:
        first_level_title (str, 可选): 一级标题
        second_level_title (str, 可选): 二级标题
        title (str): 需要检索的标题
        instruction (str, 可选): 分析思路
        topic (str, 可选): 主题，用于更精确的相关性匹配
        
    返回:
        JSON: 包含标题相关的所有检索信息
    """
    data = request.get_json()
    title = data['title']
    first_level_title = data.get('first_level_title', '')
    second_level_title = data.get('second_level_title', '')
    instruction = data.get('instruction', None)
    input_title = data.get('topic')
    
    print(f"当前大标题：{input_title}")
    
    try:
        # 如果提供了一级和二级标题，则视为三级标题处理
        if first_level_title and second_level_title:
            third_level_section = {'title': title}
            result = process_third_level_title(first_level_title, second_level_title, third_level_section, instruction, input_title)
            print(f"result:{result}")
        else:
            # 单独处理标题
            year = 2024
            # year = year_extract_from_title(title)
            # 将一级标题、二级标题和三级标题拼接在一起
            combined_title = ""
            if first_level_title:
                combined_title += first_level_title
            if second_level_title:
                combined_title += " - " + second_level_title if combined_title else second_level_title
            combined_title += " - " + title if combined_title else title
            reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_relative_data_v3(year, combined_title, instruction, input_title)
            
            # 处理数据
            # ic_trends_analysis = process_ic_trends(ic_trends)
            ic_current = ic_current if isinstance(ic_current, str) else "无相关数据"
            instruction = instruction or "无具体写作指导"
            
            result = {
                "title": title,
                "relative_data": {
                    "writing_instructions": instruction,
                    "reference": {
                        "report_source": reports if isinstance(reports, list) else [],
                        "policy_source": policy if isinstance(policy, list) else [],
                        "industry_indicator_part_1": ic_trends if ic_trends else "",
                        "industry_indicator_part_1_analysis": analysis_results_ictrend_v2,
                        "industry_indicator_part_2": ic_current,
                        "industry_indicator_part_2_analysis": filtered_result_ic_current_rating,
                        "indicators": eco_indicators,
                        "indicators_sum": eco_indicators_sum,
                        "indicators_report": eco_indicators_report
                    }
                }
            }
        
        # 方法一：不使用jsonify，直接构建Response对象
        response = Response(
            json.dumps(result, ensure_ascii=False, cls=DateTimeEncoder),
            mimetype='application/json',
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )
        return response
        
    
    except Exception as e:
        logger.error(f"检索标题信息时出错: {str(e)}", exc_info=True)
        error_data = {
            "error": f"检索标题信息时出错: {str(e)}",
            "title": title,
            "relative_data": {
                "writing_instructions": "无法获取写作指导",
                "reference": {
                    "report_source": [],
                    "policy_source": [],
                    "industry_indicator_part_1": "无法获取行业指标数据",
                    "industry_indicator_part_1_analysis": {},
                    "industry_indicator_part_2": "无法获取行业指标数据",
                    "industry_indicator_part_2_analysis": {},
                    "indicators": [],
                    "indicators_sum": "",
                    "indicators_report": []
                }
            }
        }
        
        # 对错误响应也使用相同的处理方式
        error_response = Response(
            json.dumps(error_data, ensure_ascii=False, cls=DateTimeEncoder),
            mimetype='application/json',
            status=500,
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )
        return error_response


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return super().default(obj)



@app.route('/api/edit_second_level_title', methods=['POST'])
def edit_second_level_title_section():
    """
    编辑二级标题的API
    
    请求参数:
        first_level_title (str): 一级标题
        second_level_section (dict): 二级标题的JSON结构
        topic (str, 可选): 主题

    返回：
        JSON: 包含编辑后的二级标题的JSON结构
    """
    try:
        data = request.get_json()
        first_level_title = data.get('first_level_title', '')
        second_level_section = data.get('second_level_section', {})
        topic = data.get('topic', None)
        
        if not first_level_title or not second_level_section:
            return jsonify({
                "error": "缺少必要参数",
                "status": "failed"
            }), 400
            
        new_second_level = process_second_level_title_for_edit(first_level_title, second_level_section, topic)
        
        result = {
            "status": "success",
            "data": new_second_level
        }
        
        response = Response(
            json.dumps(result, ensure_ascii=False, cls=DateTimeEncoder),
            mimetype='application/json',
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )
        return response
        
    except Exception as e:
        logger.error(f"编辑二级标题时出错: {str(e)}", exc_info=True)
        error_data = {
            "error": f"编辑二级标题时出错: {str(e)}",
            "status": "failed"
        }
        
        error_response = Response(
            json.dumps(error_data, ensure_ascii=False, cls=DateTimeEncoder),
            mimetype='application/json',
            status=500,
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )
        return error_response



@app.route('/edit_second_level_title_section', methods=['POST'])
@error_handler
def edit_second_level_title_enrich_nested():
    """
    接收嵌套的JSON结构（包含second_level_section），
    为 second_level_section.subsections 中的每个条目查询并添加relative_data，
    保留原有的三级标题title和title_code，并返回与输入完全相同的结构。
    """
    input_data = request.get_json()

    # --- 验证输入结构 ---
    if not input_data or not isinstance(input_data, dict):
        return jsonify({"error": "请求体必须是JSON对象"}), 400

    second_level_section = input_data.get('second_level_section')
    if not second_level_section or not isinstance(second_level_section, dict):
        return jsonify({"error": "请求体必须包含 'second_level_section' 对象"}), 400

    subsections = second_level_section.get('subsections')
    if not isinstance(subsections, list):
        return jsonify({"error": "'second_level_section' 必须包含 'subsections' 列表"}), 400
    # --- 验证结束 ---

    # 从正确的位置获取 parent_title 和 topic
    parent_title = second_level_section.get('title', '')
    topic = input_data.get('topic', '') # topic 在顶层

    # 直接遍历嵌套的 subsections
    for third_level_section in subsections: # 使用从 second_level_section 获取的 subsections
        third_title = third_level_section.get('title')
        # 保留传入的 title_code
        third_title_code = third_level_section.get('title_code')
        third_ana_instruction = third_level_section.get('ana_instruction', '') # 使用传入的指令

        if not third_title:
            print(f"警告：跳过缺少 'title' 的 subsection: {third_level_section}")
            continue

        # 使用父标题和当前标题组合查询，获取年份
        combined_title = f"{parent_title} - {third_title}" if parent_title else third_title
        year = year_extract_from_title(combined_title)

        try:
            # 使用从 subsection 获取的信息进行查询
            query_result = query_relative_data_v3(year, combined_title, third_ana_instruction, topic)
            reports, policy, ic_trends, ic_current, writing_instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_result
        except Exception as e:
            print(f"错误：调用 query_relative_data_v3 时发生异常 (标题: '{third_title}'): {e}")
            # 在出错时填充错误信息或默认值
            reports, policy, ic_trends, ic_current, writing_instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = [], [], [], [], f"查询失败: {e}", [], {}, [], {}, {}

        # 构建 relative_data
        relative_data = {
            "reference": {
                "report_source": reports if isinstance(reports, list) else [],
                "policy_source": policy if isinstance(policy, list) else [],
                "industry_indicator_part_1": ic_trends if ic_trends else "",
                "industry_indicator_part_1_analysis": analysis_results_ictrend_v2,
                "industry_indicator_part_2": ic_current,
                "industry_indicator_part_2_analysis": filtered_result_ic_current_rating if isinstance(filtered_result_ic_current_rating, dict) else {},
                "indicators": eco_indicators,
                "indicators_sum": eco_indicators_sum,
                "indicators_report": eco_indicators_report
            },
            # 使用查询返回的 writing_instruction，如果查询失败则包含错误信息
            "writing_instruction": writing_instruction or "无具体分析思路"
        }

        # 将 relative_data 添加/更新到当前 third_level_section 中
        # 其他字段（如 title, title_code, previous_title 等）被保留
        third_level_section["relative_data"] = relative_data
        # 确保 title_code 确实被保留 (虽然它本来就在)
        third_level_section["title_code"] = third_title_code

    # 返回被修改后的原始输入数据结构
    # 修改返回方式，确保UTF-8编码
    response_data = {
        "status": "success",
        # 直接返回 input_data，因为里面的 subsections 已经被修改
        "data": input_data
    }
    return Response(
        json.dumps(response_data, ensure_ascii=False, cls=DateTimeEncoder), # 添加 DateTimeEncoder 以处理可能的日期时间对象
        mimetype='application/json',
        status=200,
        headers={'Content-Type': 'application/json; charset=utf-8'}
    )


@app.route('/edit_first_level_title', methods=['POST'])
@validate_json_request(['first_level_json'])
@error_handler
def edit_first_level_title():

    data = request.get_json()
    input_json = data.get('first_level_json', {})
    topic = data.get('topic',"")
    title_code = input_json.get("title_code", "")
    title = input_json.get("title", "")
    ana_instruction = input_json.get("ana_instruction", "")

    result = generate_second_level_titles(title, title_code, ana_instruction)
    formatted_result = format_third_level_result_to_json(title, title_code, ana_instruction, result)
    print(json.dumps(formatted_result, indent=4, ensure_ascii=False))

    # 定义处理单个三级标题的函数
    def process_third_level_section(index, third_level_section, parent_title, topic):
        print(f"third_level_section: {third_level_section}")
        original_instruction = third_level_section.get("ana_instruction", None)
        print(f"Original instruction from third_level_section: {original_instruction}")
        title_code = third_level_section.get("title_code", "")
        third_title = third_level_section.get("title", "")
        combined_title = parent_title + " - " + third_title
        year = year_extract_from_title(combined_title)
        try:
            query_result = query_relative_data_v3(year, combined_title, original_instruction, topic)
            reports, policy, ic_trends, ic_current, writing_instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_result
        except Exception as e:
            print(f"错误：调用 query_relative_data_v3 时发生异常: {e}")
            reports, policy, ic_trends, ic_current, writing_instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = [], [], [], [], "", [], {}, [], {}, {}

        writing_instruction = writing_instruction or "无具体分析思路"
        print(f"Instruction returned from query_relative_data_v3: {writing_instruction}")

        reference = {
            "report_source": reports if isinstance(reports, list) else [],
            "policy_source": policy if isinstance(policy, list) else [],
            "industry_indicator_part_1": ic_trends if ic_trends else "",
            "industry_indicator_part_1_analysis": analysis_results_ictrend_v2,
            "industry_indicator_part_2": ic_current,
            "industry_indicator_part_2_analysis": filtered_result_ic_current_rating if isinstance(
                filtered_result_ic_current_rating, dict) else {},
            "indicators": eco_indicators,
            "indicators_sum": eco_indicators_sum,
            "indicators_report": eco_indicators_report
        }

        # 添加相关数据到三级标题
        third_level_section["relative_data"] = {
            "reference": reference,
            "writing_instruction": writing_instruction
        }
        
        return third_level_section

    # 定义二级标题处理函数
    def process_second_level_section(section, first_level_title, topic):
        second_level_title = section.get("title", "")
        section_title_code = section.get("title_code", "")
        section_ana_instruction = section.get("ana_instruction", "")

        # 生成三级标题
        third_level_result = generate_third_level_titles(
            second_level_title,
            section_title_code,
            section_ana_instruction
        )

        formatted_third_level = format_third_level_result_to_json_v2(
            second_level_title,
            section_title_code,
            section_ana_instruction,
            third_level_result
        )

        print(f"formatted_third_level: {json.dumps(formatted_third_level, indent=4, ensure_ascii=False)}")
        # 处理formatted_third_level格式
        if isinstance(formatted_third_level, str):
            try:
                formatted_third_level = json.loads(formatted_third_level)
            except json.JSONDecodeError:
                print(f"无法将formatted_third_level解析为JSON: {formatted_third_level}")
                formatted_third_level = []
        if isinstance(formatted_third_level, dict):
            if "subsections" in formatted_third_level:
                formatted_third_level = formatted_third_level.get("subsections", [])
            else:
                formatted_third_level = [formatted_third_level]

        if not isinstance(formatted_third_level, list):
            print(f"formatted_third_level不是列表类型: {type(formatted_third_level)}")
            formatted_third_level = []

        # 处理每个三级标题 (并行)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_third_level = {}
            
            for index, third_level_item in enumerate(formatted_third_level):
                print(f"提交处理第{index}个三级标题: {third_level_item.get('title', '')}")
                future = executor.submit(
                    process_third_level_section, # 现在可以正确引用了
                    index,
                    third_level_item,
                    first_level_title, # 使用传入的一级标题
                    topic
                )
                future_to_third_level[future] = index
            
            for future in concurrent.futures.as_completed(future_to_third_level):
                index = future_to_third_level[future]
                try:
                    result_section = future.result()
                    formatted_third_level[index] = result_section
                    print(f"完成处理第{index}个三级标题")
                except Exception as e:
                    print(f"处理第{index}个三级标题时发生错误: {e}")
        
        # 生成整体分析思路
        all_third_titles = [item.get("title", "") for item in formatted_third_level if item.get("title")]
        print(f"all_third_titles: {all_third_titles}")
        new_ana_instruction = section_ana_instruction # 默认使用原有的
        if all_third_titles:
            combined_titles = "、".join(all_third_titles)
            new_ana_instruction = generate_ana_instruction(combined_titles) # 覆盖
            print(f"ana_instruction: {new_ana_instruction}")

        # 返回处理后的二级标题部分，包含更新后的三级标题和分析思路
        section["subsections"] = formatted_third_level
        section["ana_instruction"] = new_ana_instruction
        return section


    # 为每个二级标题生成三级标题并添加到结果中
    if "subsections" in formatted_result:
        # 使用多线程并行处理所有二级标题
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有二级标题的处理任务
            future_to_second_level = {
                executor.submit(
                    process_second_level_section, # 现在可以正确引用了
                    section,
                    formatted_result.get("title", ""), # 传递一级标题
                    topic
                ): section for section in formatted_result["subsections"]
            }
            
            # 处理完成的任务
            updated_subsections = [None] * len(formatted_result["subsections"])
            section_map = {id(s): i for i, s in enumerate(formatted_result["subsections"])}

            for future in concurrent.futures.as_completed(future_to_second_level):
                original_section = future_to_second_level[future]
                original_index = section_map[id(original_section)]
                try:
                    # 获取处理结果
                    processed_section = future.result()
                    updated_subsections[original_index] = processed_section
                except Exception as e:
                    print(f"处理二级标题 '{original_section.get('title', '')}' 时出错: {e}")
                    # 保留原始的section或标记为错误
                    updated_subsections[original_index] = original_section # 或者可以选择标记错误
            
            # 更新原始formatted_result中的subsections
            formatted_result["subsections"] = [s for s in updated_subsections if s is not None]


    # 保存结果为JSON文件
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    json_filename = f"final_formatted_result_{timestamp}.json"

    try:
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(formatted_result, f, ensure_ascii=False, indent=4)
        print(f"已成功将formatted_result保存为JSON文件: {json_filename}")
    except Exception as e:
        print(f"保存formatted_result为JSON文件时发生错误: {e}")

    return jsonify({
        "status": "success",
        "data": formatted_result
    }), 200

    # if __name__ == '__main__':
    #     app.run(host='0.0.0.0', port=5009, debug=False)  # 将端口改为5001以避免与AirPlay冲突



# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5009, debug=False)  # 将端口改为5001以避免与AirPlay冲突


@app.route('/query_relative_data', methods=['POST'])
@error_handler
def query_relative_data_api():
    """
    API接口，用于调用query_relative_data_v3函数获取相关数据
    
    请求体格式:
    {
        "year": "2023", // 可选，年份
        "title": "标题内容", // 必填，标题
        "instruction": "", // 可选，分析指令
        "topic": "" // 可选，主题
    }
    
    返回:
    {
        "status": "success",
        "data": {
            "reports": [...],
            "policy": [...],
            "ic_trends": [...],
            "ic_current": [...],
            "writing_instruction": "...",
            "eco_indicators": [...],
            "eco_indicators_sum": {...},
            "eco_indicators_report": [...],
            "analysis_results_ictrend_v2": {...},
            "filtered_result_ic_current_rating": {...}
        }
    }
    """
    try:
        data = request.get_json()
        
        # 获取请求参数
        year = data.get('year', '')
        title = data.get('title', '')
        instruction = data.get('instruction', '')
        topic = data.get('topic', '')
        
        # 验证必填参数
        if not title:
            return jsonify({
                "status": "failed",
                "error": "缺少必填参数'title'"
            }), 400
            
        # 调用query_relative_data_v3函数
        query_result = query_relative_data_v3(year, title, instruction, topic)
        reports, policy, ic_trends, ic_current, writing_instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_result
        
        # 构建返回数据
        result = {
            "status": "success",
            "data": {
                "reports": reports,
                "policy": policy,
                "ic_trends": ic_trends,
                "ic_current": ic_current,
                "writing_instruction": writing_instruction,
                "eco_indicators": eco_indicators,
                "eco_indicators_sum": eco_indicators_sum,
                "eco_indicators_report": eco_indicators_report,
                "analysis_results_ictrend_v2": analysis_results_ictrend_v2,
                "filtered_result_ic_current_rating": filtered_result_ic_current_rating
            }
        }
        
        # 返回JSON响应
        response = Response(
            json.dumps(result, ensure_ascii=False, cls=DateTimeEncoder),
            mimetype='application/json',
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )
        return response
        
    except Exception as e:
        logger.error(f"调用query_relative_data_v3时出错: {str(e)}", exc_info=True)
        error_data = {
            "status": "failed",
            "error": f"调用query_relative_data_v3时出错: {str(e)}"
        }
        
        error_response = Response(
            json.dumps(error_data, ensure_ascii=False, cls=DateTimeEncoder),
            mimetype='application/json',
            status=500,
            headers={'Content-Type': 'application/json; charset=utf-8'}
        )
        return error_response



@app.route('/overview_v3_serial', methods=['POST'])
@validate_json_request(['title'])
@error_handler
def overview_v3_serial():
    """
    生成研报综合目录的API（串行版本）
    
    参数:
        reports_overview: 研报概览数据
        general_overview: 通用概览数据
        title: 输入标题
        
    返回:
        full_section_list: 处理后的完整章节列表的UTF-8编码
    """
    request_start_time = time.time() # 记录请求开始时间
    logger.info("进入 overview_v3_serial 接口 (串行版本)")
    
    def process_sections_serial(reports_overview, general_overview, topic, purpose):
        process_sections_start_time = time.time()
        logger.info(f"进入 process_sections_serial，topic: {topic}")
        # 生成最终概览
        logger.info('开始生成最终概览')
        overview_start_time = time.time()
        try: # 添加 try-except 块
            if isinstance(general_overview, list) and len(general_overview) >= 1: # 检查 general_overview 是否是列表且非空
                logger.info("使用 general_overview[0] 生成最终概览")
                final_overview, _ = overview_conclusion(reports_overview, general_overview[0], topic, purpose)
            else:
                logger.warning("general_overview 不是预期的列表格式或为空，将直接使用")
                final_overview, _ = overview_conclusion(reports_overview, general_overview, topic, purpose)
            overview_end_time = time.time()
            logger.info(f"最终概览生成完成, 耗时: {overview_end_time - overview_start_time:.2f} 秒")
        except Exception as e:
            overview_end_time = time.time()
            logger.error(f"生成最终概览时出错: {e}, 耗时: {overview_end_time - overview_start_time:.2f} 秒", exc_info=True)
            return [] # 返回空列表表示失败

        print(f"====="*10)
        print(f"完全目录：{final_overview}")
        print(f"=====" * 10)
        
        extract_start_time = time.time()
        try: # 添加 try-except 块
            # 提取章节内容
            content_json = extract_headlines(final_overview)
            section_list = generate_section_list(content_json)
            extract_end_time = time.time()
            logger.info(f"提取到 {len(section_list)} 个一级章节, 耗时: {extract_end_time - extract_start_time:.2f} 秒")
        except Exception as e:
            extract_end_time = time.time()
            logger.error(f"提取章节内容时出错: {e}, 耗时: {extract_end_time - extract_start_time:.2f} 秒", exc_info=True)
            return [] # 返回空列表表示失败

        # 使用 milestone_4.py 中的串行处理函数，替代原有的章节处理逻辑
        logger.info(f"开始串行处理章节树，共 {len(section_list)} 个一级标题")
        tree_process_start_time = time.time()
        
        try:
            # 直接调用 process_section_tree_serial 函数，一次性处理所有章节
            full_section_list = process_section_tree_serial(section_list, topic)
            
            tree_process_end_time = time.time()
            logger.info(f"串行处理章节树完成，成功生成 {len(full_section_list)} 个章节。总耗时: {tree_process_end_time - tree_process_start_time:.2f} 秒")
        except Exception as e:
            tree_process_end_time = time.time()
            logger.error(f"串行处理章节树时出错: {e}, 耗时: {tree_process_end_time - tree_process_start_time:.2f} 秒", exc_info=True)
            # 如果使用串行处理失败，尝试使用原始的逐个处理方式作为备选
            logger.warning("尝试使用备选的逐个处理方法...")
            full_section_list = []
            for i, section in enumerate(section_list):
                try:
                    logger.info(f"处理章节 {i+1}/{len(section_list)}: {section.get('title', 'N/A')}")
                    section_start_time = time.time()
                    
                    # 使用串行方式处理一级标题
                    index, processed_first_level = process_first_level_title_serial(section, i, topic)
                    
                    # 调整二级和一级标题
                    modified_content_second_headings = modify_second_level_headers_stream(processed_first_level, topic)
                    modified_content = modify_first_level_headers_stream(modified_content_second_headings, topic)
                    
                    # 添加到结果列表
                    if modified_content:
                        full_section_list.append(modified_content)
                        section_end_time = time.time()
                        logger.info(f"章节 {i+1} 处理完成，耗时: {section_end_time - section_start_time:.2f} 秒")
                except Exception as section_error:
                    logger.error(f"处理章节 {i+1} 时出错: {section_error}", exc_info=True)
                    continue  # 继续处理下一个章节
        
        process_sections_end_time = time.time()
        logger.info(f"process_sections_serial 完成，总耗时: {process_sections_end_time - process_sections_start_time:.2f} 秒")
        return full_section_list

    # 获取请求数据
    data = request.get_json()
    topic = data.get('title', '未知标题') # 提供默认值
    purpose = data.get('purpose', '')
    reports_overview = data.get('reports_overview', '') # 提供默认值
    general_overview = data.get('general_overview', []) # 提供默认值
    logger.info(f"处理标题: {topic}")
    print(f"处理标题: {topic}") # 保留原有 print

    # 处理章节并返回UTF-8编码结果 - 使用串行处理函数
    full_section_list = process_sections_serial(reports_overview, general_overview, topic, purpose)
    logger.info(f"process_sections_serial 返回 {len(full_section_list)} 个有效章节")
    
    # 修改返回方式，确保中文正确显示
    response_data = {"sections": full_section_list}
    logger.info("准备返回 JSON 响应")
    response = Response(
        json.dumps(response_data, ensure_ascii=False, cls=DateTimeEncoder), # 添加 DateTimeEncoder
        content_type='application/json; charset=utf-8'
    )
    request_end_time = time.time() # 记录请求结束时间
    logger.info(f"overview_v3_serial 接口处理完成。总请求耗时: {request_end_time - request_start_time:.2f} 秒")
    return response

if __name__ == '__main__':
    # 配置日志系统 - 已在文件顶部配置，这里无需重复配置
    # handler = RotatingFileHandler(
    #     'app.log',
    #     maxBytes=10*1024*1024,  # 10MB
    #     backupCount=5,
    #     encoding='utf-8'
    # )
    
    # # 设置httpx日志级别为WARNING，避免打印过多请求日志
    # logging.getLogger("httpx").setLevel(logging.WARNING)
    
    # 解决控制台输出编码问题 (如果日志配置中已添加 StreamHandler，则可能不需要这个)
    # sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    # sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    # 打印所有路由
    logger.info("Flask 应用启动，打印路由信息...")
    with app.app_context(): # 需要 app context 才能访问 url_map
        logger.info(f"Available routes:\n{app.url_map}")

    # 运行应用
    logger.info("启动 Flask 应用，监听 0.0.0.0:5009")
    app.run(host='0.0.0.0', port=5009, debug=False)
