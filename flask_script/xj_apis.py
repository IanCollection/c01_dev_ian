from flask import Flask, request, jsonify, Response, stream_with_context
from Agent.Overview_agent import title_augement_stream, generate_toc_from_focus_points,match_focus_points,semantic_enhancement_agent 
from scrpit.policy_query import search_es_policy_v2
from database.faiss_query import search
from Agent.policy_agent import title_semantic_enhancement_agent



import json
app = Flask(__name__)


@app.route('/augment_title', methods=['POST'])
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
    """
    try:
        data = request.get_json()
        if not data or 'title' not in data:
            return jsonify({'error': '输入要撰写的研报题目'}), 400

        title = data['title']
        
        def generate():
            #政策摘要
            for chunk in title_augement_stream(title):
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

    except Exception as e:
        return jsonify({'error': str(e)}), 500



# @app.route('/augment_title', methods=['POST'])
# def report_overview_stage_0_augment_title_api():
#     """
#     研报标题语义增强API
    
#     功能:
#     1. 对输入的研报标题进行语义增强,扩展标题内容
#     2. 提取三个维度的关键词:
#        - 核心关键词: 与主题直接相关的关键术语
#        - 领域关键词: 相关行业、市场、技术的细分类别
#        - 聚焦关键词: 研究方向、趋势、政策和产业链相关术语
#     3. 流式输出思维链过程
    
#     请求参数:
#         title (str): 研报标题
        
#     返回:
#         Response: 流式响应,包含以下事件:
#             - think: 思维链过程
#             - title: 扩展后的标题
#             - keyword: 关键词内容
#             - result: 最终JSON结果
#     """
#     try:
#         data = request.get_json()
#         if not data or 'title' not in data:
#             return jsonify({'error': '输入要撰写的研报题目'}), 400

#         title = data['title']
        
#         def generate():
#             for chunk in title_augement_stream(title):
#                 # 如果chunk是字典类型,说明是最终JSON结果
#                 if isinstance(chunk, dict):
#                     yield f"event: result\ndata: {json.dumps(chunk, ensure_ascii=False)}\n\n"
#                 # 如果是扩展标题内容
#                 elif chunk.startswith("\n\n扩展标题:"):
#                     yield f"event: title\ndata: {chunk}\n\n"
#                 # 如果是关键词内容
#                 elif chunk.startswith("\n关键词:") or chunk.startswith("- "):
#                     yield f"event: keyword\ndata: {chunk}\n\n"
#                 # 其他思维链内容
#                 else:
#                     yield f"event: think\ndata: {chunk}\n\n"
        
#         return Response(stream_with_context(generate()), 
#                        content_type='text/event-stream')

#     except Exception as e:
#         return jsonify({'error': str(e)}), 500

@app.route('/search', methods=['POST'])
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
    try:
        # 获取请求数据
        data = request.get_json()
        if not data:
            return jsonify({'error': '请提供搜索参数'}), 400
            
        # 获取检索参数
        query = data.get('query')  # 检索文本
        index_type = data.get('index_type', 'filename')  # 默认检索content向量库
        top_k = data.get('top_k', 10)  # 默认返回10条结果
        
        # 检查必要参数
        if not query:
            return jsonify({'error': '请提供搜索关键词'}), 400
            
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
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/generate_toc_with_focus_points', methods=['POST'])
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
    try:
        # 获取请求数据
        data = request.get_json()
        if not data:
            return jsonify({'error': '请提供必要参数'}), 400
            
        # 获取必要参数
        title = data.get('title')
        focus_points,cost = match_focus_points(title)
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
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/search_policy', methods=['POST'])
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
    try:
        # 获取请求数据
        data = request.get_json()
        if not data:
            return jsonify({'error': '请提供必要参数'}), 400
            
        # 获取必要参数
        title = data.get('title')
        if not title:
            return jsonify({'error': '请提供研报标题'}), 400
            
        size = data.get('size', 5)  # 默认返回5条结果
        
        # 第一步：调用语义增强函数
        augmented_data, enhancement_cost = title_semantic_enhancement_agent(title)
        
        # 第二步：搜索相关政策
        policy_results = search_es_policy_v2(title, augmented_data, size)
        
        # 返回结果
        return jsonify(policy_results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)  # 将端口改为5001以避免与AirPlay冲突

