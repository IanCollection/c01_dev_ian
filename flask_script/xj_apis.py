import concurrent
import time
import os
import sys
import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)
from Agent.overview_agent_part2 import generate_third_level_titles, format_third_level_result_to_json, \
    generate_second_level_titles, format_third_level_result_to_json_v2
from scrpit.query_report_policy_ic_indicator import query_relative_data_v2
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
    title_augement_without_cot, generate_toc_from_focus_points_stream_no_title, 
    year_extract_from_title, generate_ana_instruction
)
from database.neo4j_query import get_neo4j_driver
from palyground import extract_headlines, generate_section_list
from scrpit.milestone_4 import (
    process_first_level_title, process_third_level_title, 
    process_ic_trends, process_second_level_title_for_edit
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

from scrpit.tune_second_level_headers import modify_second_level_headers, modify_first_level_headers, \
    modify_first_level_headers_stream, modify_second_level_headers_stream

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # 这个设置在某些情况下可能不够

# 自定义JSON编码器
class CustomJSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        kwargs['ensure_ascii'] = False
        super().__init__(*args, **kwargs)

app.json_encoder = CustomJSONEncoder

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
        for toc_chunk in generate_toc_from_focus_points_stream_no_title(enhanced_title, focus_points, keywords):
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
            new_title, relative_reports, keywords, time = build_overview_with_report(title, purpose)
            # 打印初步检索完成
            # yield f"event: toc_progress\ndata: {json.dumps({'status': '初步检索完成', 'title': new_title, 'keywords': keywords, 'time': time}, ensure_ascii=False)}\n\n"

            reports_node = search(title, index_type='filename', top_k=20)
            # 将reports_node转换为普通Python整数列表
            reports_node = [int(node) if isinstance(node, np.int64) else node for node in reports_node]
            # 将reports_node返回给前端
            yield f"event: reports_node\ndata: {json.dumps({'reports_node': reports_node}, ensure_ascii=False)}\n\n"
            yield f"event: toc_progress\ndata: {json.dumps({'status': 'node检索完成', 'title': new_title, 'keywords': keywords, 'time': time}, ensure_ascii=False)}\n\n"

            relative_reports = query_file_batch_nodes(reports_node)
            yield f"event: toc_progress\ndata: {json.dumps({'status': '相关研报搜索完成', 'title': new_title, 'keywords': keywords, 'time': time}, ensure_ascii=False)}\n\n"
            
            # 生成综合目录（流式）
            # 记录是否已发送目录内容
            # toc_content_sent = False
            
            for chunk in generate_comprehensive_toc_v2_stream_no_title(title, relative_reports, keywords):
                if isinstance(chunk, dict):
                    event_type = chunk['event']
                    event_data = chunk['data']
                    
                    # 如果是目录内容（不是状态更新），直接发送文本
                    if event_type == 'toc_progress' and not isinstance(event_data, dict):
                        yield f"event: {event_type}\ndata: {event_data}\n\n"
                    else:
                        # 其他类型的数据（如状态更新）仍然使用JSON格式
                        yield f"event: {event_type}\ndata: {json.dumps(event_data, ensure_ascii=False)}\n\n"

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
    def process_sections(reports_overview, general_overview, input_title):
        # 生成最终概览
        print('开始生成最终概览')
        if len(general_overview) >= 2:
            final_overview, _ = overview_conclusion(reports_overview, general_overview[0], input_title)
        else:
            final_overview, _ = overview_conclusion(reports_overview, general_overview, input_title)
        
        # 提取章节内容
        content_json = extract_headlines(final_overview)
        section_list = generate_section_list(content_json)
        
        full_section_list = []
        logger.info(f"开始处理章节，共 {len(section_list)} 个一级标题")
        
        # 使用线程池并行处理章节
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有章节处理任务
            future_to_section = {executor.submit(process_single_section, section, i): i
                                for i, section in enumerate(section_list)}
            
            # 收集处理结果
            for future in concurrent.futures.as_completed(future_to_section):
                section_index = future_to_section[future]
                try:
                    modified_content = future.result()
                    # 确保结果为UTF-8编码
                    if isinstance(modified_content, bytes):
                        modified_content = modified_content.decode('utf-8')
                    full_section_list.append(modified_content)
                    logger.info(f"章节 {section_index+1} 处理完成")
                except Exception as e:
                    logger.error(f"处理章节 {section_index+1} 时出错: {str(e)}")
                    continue
                
        return full_section_list

    def process_single_section(section, index):
        """处理单个章节的独立函数，用于并行处理"""
        # 处理一级标题
        _, processed_first_level = process_first_level_title(section, index)
        logger.debug(f"处理一级标题: {processed_first_level['title']}")
        
        # 调整二级标题
        modified_content_second_headings = modify_second_level_headers_stream(processed_first_level)
        logger.debug("二级标题调整完成")
        
        # 调整一级标题
        modified_content = modify_first_level_headers_stream(modified_content_second_headings)
        logger.debug("一级标题调整完成")
        
        return modified_content

    # 获取请求数据
    data = request.get_json()
    input_title = data['title']
    reports_overview = data['reports_overview']
    general_overview = data['general_overview']
    
    # 处理章节并返回UTF-8编码结果
    full_section_list = process_sections(reports_overview, general_overview, input_title)
    
    # 修改返回方式，确保中文正确显示
    response = Response(
        json.dumps({"sections": full_section_list}, ensure_ascii=False),
        content_type='application/json; charset=utf-8'
    )
    return response


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
        first_level_futures = {executor.submit(process_first_level_title, section, i): i 
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
        
    返回:
        JSON: 包含标题相关的所有检索信息
    """
    data = request.get_json()
    title = data['title']
    first_level_title = data.get('first_level_title', '')
    second_level_title = data.get('second_level_title', '')
    instruction = data.get('instruction', None)
    
    try:
        # 如果提供了一级和二级标题，则视为三级标题处理
        if first_level_title and second_level_title:
            third_level_section = {'title': title}
            result = process_third_level_title(first_level_title, second_level_title, third_level_section, instruction)
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
            reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_relative_data_v2(year, combined_title, instruction)
            
            # 处理数据
            ic_trends_analysis = process_ic_trends(ic_trends)
            ic_current = ic_current if isinstance(ic_current, str) else "无相关数据"
            instruction = instruction or "无具体写作指导"
            
            result = {
                "title": title,
                "relative_data": {
                    "writing_instructions": instruction,
                    "reference": {
                        "report_source": reports if isinstance(reports, list) else [],
                        "policy_source": policy if isinstance(policy, list) else [],
                        "industry_indicator_part_1": ic_trends_analysis,
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

    返回：
        JSON: 包含编辑后的二级标题的JSON结构
    """
    try:
        data = request.get_json()
        first_level_title = data.get('first_level_title', '')
        second_level_section = data.get('second_level_section', {})
        
        if not first_level_title or not second_level_section:
            return jsonify({
                "error": "缺少必要参数",
                "status": "failed"
            }), 400
            
        new_second_level = process_second_level_title_for_edit(first_level_title, second_level_section)
        
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


@app.route('/edit_second_level_title', methods=['POST'])
@validate_json_request(['second_level_json'])
@error_handler
def edit_second_level_title():
    data = request.get_json()
    title = data.get('title', '')
    title_code = data.get('title_code', '')
    ana_instruction = data.get('ana_instruction', '')

    result = generate_third_level_titles(title, title_code, ana_instruction)
    formatted_result = format_third_level_result_to_json(title, title_code, ana_instruction, result)

    if formatted_result.get("subsections") and formatted_result["subsections"][0].get("subsections"):
        for third_level_section in formatted_result["subsections"][0]["subsections"]:
            combined_title = f"{formatted_result.get('title', '')} - {third_level_section.get('title', '')}"
            year = year_extract_from_title(combined_title)

            try:
                query_result = query_relative_data_v2(year, combined_title,
                                                      third_level_section.get("ana_instruction", ""))
                reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_result
            except Exception as e:
                print(f"错误：调用 query_relative_data_v2 时发生异常: {e}")
                query_result = ([], [], [], [], "", [], {}, [], {}, {})
                reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_result

            third_level_section["relative_data"] = {
                "reference": {
                    "report_source": reports if isinstance(reports, list) else [],
                    "policy_source": policy if isinstance(policy, list) else [],
                    "industry_indicator_part_1": process_ic_trends(ic_trends),
                    "industry_indicator_part_1_analysis": analysis_results_ictrend_v2,
                    "industry_indicator_part_2": ic_current,
                    "industry_indicator_part_2_analysis": filtered_result_ic_current_rating if isinstance(
                        filtered_result_ic_current_rating, dict) else {},
                    "indicators": eco_indicators,
                    "indicators_sum": eco_indicators_sum,
                    "indicators_report": eco_indicators_report
                },
                "writing_instruction": instruction or "无具体分析思路"
            }
    # try:
    #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #     file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"third_level_titles_{timestamp}.json")
    #     with open(file_path, 'w', encoding='utf-8') as f:
    #         json.dump(formatted_result, f, ensure_ascii=False, indent=4)
    #     print(f"已成功将结果保存为JSON文件: {file_path}")
    # except Exception as e:
    #     print(f"保存JSON文件时出错: {e}")
    return jsonify({
        "status": "success",
        "data": formatted_result
    }), 200


@app.route('/edit_first_level_title', methods=['POST'])
@validate_json_request(['first_level_json'])
@error_handler
def edit_first_level_title():
    data = request.get_json()
    input_json = data.get('first_level_json', {})

    title_code = input_json.get("title_code", "")
    title = input_json.get("title", "")
    ana_instruction = input_json.get("ana_instruction", "")

    result = generate_second_level_titles(title, title_code, ana_instruction)
    formatted_result = format_third_level_result_to_json(title, title_code, ana_instruction, result)
    print(json.dumps(formatted_result, indent=4, ensure_ascii=False))

    # 为每个二级标题生成三级标题并添加到结果中
    if "subsections" in formatted_result:
        for section in formatted_result["subsections"]:
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

            # 处理每个三级标题
            for index, third_level_section in enumerate(formatted_third_level):
                print(f"third_level_section: {third_level_section}")

                instruction = third_level_section.get("ana_instruction", None)
                title_code = third_level_section.get("title_code", "")
                third_title = third_level_section.get("title", "")
                combined_title = formatted_result.get("title", "") + " - " + third_title
                year = year_extract_from_title(combined_title)

                try:
                    query_result = query_relative_data_v2(year, combined_title, instruction)
                    reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = query_result
                except Exception as e:
                    print(f"错误：调用 query_relative_data_v2 时发生异常: {e}")
                    reports, policy, ic_trends, ic_current, instruction, eco_indicators, eco_indicators_sum, eco_indicators_report, analysis_results_ictrend_v2, filtered_result_ic_current_rating = [], [], [], [], "", [], {}, [], {}, {}

                ic_trends_analysis = process_ic_trends(ic_trends)
                instruction = instruction or "无具体分析思路"
                print(f"current_instruction:{instruction}")

                reference = {
                    "report_source": reports if isinstance(reports, list) else [],
                    "policy_source": policy if isinstance(policy, list) else [],
                    "industry_indicator_part_1": ic_trends_analysis,
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
                    "writing_instruction": instruction
                }
                formatted_third_level[index]["relative_data"] = third_level_section["relative_data"]

            # 生成整体分析思路
            all_third_titles = [section.get("title", "") for section in formatted_third_level if section.get("title")]
            print(f"all_third_titles: {all_third_titles}")
            if all_third_titles:
                combined_titles = "、".join(all_third_titles)
                ana_instruction = generate_ana_instruction(combined_titles)
                print(f"ana_instruction: {ana_instruction}")

            section["subsections"] = formatted_third_level
            section["ana_instruction"] = ana_instruction

    # 保存结果为JSON文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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
if __name__ == '__main__':
    print(app.url_map)  # 打印所有路由
    app.run(host='0.0.0.0', port=5009, debug=False)