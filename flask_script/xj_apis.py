from flask import Flask, request, jsonify, Response, stream_with_context
from Agent.Overview_agent import title_augement_stream
from database.faiss_query import search
import json
app = Flask(__name__)

@app.route('/augment_title', methods=['POST'])
def report_overview_stage_0_augment_title_api():
    try:
        data = request.get_json()
        if not data or 'title' not in data:
            return jsonify({'error': '输入要撰写的研报题目'}), 400

        title = data['title']
        
        def generate():
            for chunk in title_augement_stream(title):
                # 如果chunk是字典类型,说明是JSON结果
                if isinstance(chunk, dict):
                    yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                # 如果是思维链内容
                elif chunk.startswith("\n\n扩展标题:"):
                    yield f"data: {chunk}\n\n"
                # 如果是关键词内容
                elif chunk.startswith("\n关键词:") or chunk.startswith("- "):
                    yield f"data: {chunk}\n\n"
                # 其他思维链内容
                else:
                    yield f"data: {chunk}\n\n"
        
        return Response(stream_with_context(generate()), 
                       content_type='text/event-stream')

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/search', methods=['POST'])
def search_api():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': '请提供搜索参数'}), 400
            
        # 获取请求参数
        query = data.get('query')
        index_type = data.get('index_type', 'content')  # 默认搜索内容
        top_k = data.get('top_k', 2)  # 默认返回2条结果
        with_details = data.get('with_details', True)  # 默认返回详细信息
        
        if not query:
            return jsonify({'error': '请提供搜索关键词'}), 400
            
        # 调用search函数执行检索
        results = search(
            query=query,
            index_type=index_type,
            top_k=top_k,
            with_details=with_details
        )
        
        return jsonify({
            'report_ids_list': results
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)  # 将端口改为5001以避免与AirPlay冲突

