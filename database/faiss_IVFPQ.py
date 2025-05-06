import faiss
import numpy as np
from utils.vector_generator import get_embeddings,get_embedding_single_text
import os
import json
import datetime
import time

# filename = {'286792841991028736': '2024年商业地产行业展望:强基固本向新而生', '286792850677432320': '主要观点', '286792856071307264': '重新稳固发展基础', '286792858629832704': '各位同仁:', '286792864627687424': '经济放缓隐忧进一步影响行业收入和支出', '286792869941870592': '调研方法', '286792875134418944': '各地区大部分受访者均预计2023年营业收入将降低', '286792886358376448': '市场预期发生结构性变化', '286792900811948032': '受访者更看好数字地产和独户租赁/建房出租类地产', '286792908072288256': '写字楼 ', '286792910458847232': '现状', '286792915231965184': '零售地产 ', '286792917765324800': '展望 ', '286792920156078080': '现状', '286792924937584640': '展望 ', '286792936501280768': '工业地产', '286792941534445568': '现状', '286792946466947072': '展望 ', '286792951437197312': '住宅', '286792953899253760': '现状', '286792961759379456': '展望 ', '286792966683492352': '酒店地产', '286792970265427968': '现状', '286792975860629504': '展望 ', '286792982370189312': '替代型资产:数字地产、养老地产和生命科学地产', '286792987919253504': '数字地产 ', '286792992788840448': '独户租赁/建房出租类地产', '286792998556008448': '养老地产 ', '286793004365119488': '生命科学地产', '286793009171791872': '行动建议', '286793014041378816': '保守型', '286793019615608832': '主动型', '286793024510361600': '行业领导者观点:选址和多功能区域的重要性', '286793029321228288': '制定可持续发展战略,推进低碳转型', '286793034136289280': '行业领导者观点:将可持续发展充分融入资产组合策略', '286793038947155968': '写字楼运营的可持续性', '286793043783188480': '全生命周期碳排放评估应成为标准', '286793048615026688': '许多房地产公司缺乏满足合规要求的内部控制', '286793053434281984': 'ESG法规与合规:房地产公司是否做好准备?', '286793062921797632': '行动建议', '286793070131806208': '厘清税惠政策,实现利润提升', '286793075018170368': '$\\bullet$ 绿色协议工业计划(The Green Deal', '286793081519341568': '全球最低税率实施', '286793083947843584': '展望 ', '286793092718133248': '行动建议', '286793097528999936': '混合办公与运营转型', '286793102562164736': '行业领导者观点:当今职场如何发挥领导力', '286793108316749824': 'Heitman全球投资研究总监MaryLudgin', '286793113115033600': '借职能外包提高效率', '286793115547729920': '变革驱动因素', '286793122812264448': '行业领导者观点:"赢在通勤" ', '286793127677657088': 'Seaforth Land首席执行官Tyler Goodwin ', '286793134866694144': '房地产公司的目标', '286793139715309568': '行业领导者观点:加速运营转型', '286793146786906112': '世邦魏理仕集团首席运营官Vikram Kohli', '286793155037102080': '行动建议', '286793162356162560': '提高房地产行业技术能力', '286793169062854656': '行业领导者观点:投资技术提升竞争力', '286793175366893568': '正视房地产行业技术债务', '286793181960339456': '挑战现状 ', '286793192156692480': '行动建议', '286793201992335360': '为未来商业地产奠定坚实基础', '286793209743409152': '尾注', '286793222913523712': '关于作者', '286793225300082688': 'Jeffrey Smith ', '286793227711807488': 'Renea Burns ', '286793232749166592': 'Kathy Feucht ', '286793238541500416': 'Tim Coy ', '286793243734048768': '致谢 ', '286793248599441408': '联系我们', '286793250994388992': '行业领导人', '286793254576324608': '德勤金融服务行业研究中心', '286793256954494976': '罗远江', '286793262918795264': 'Jim Eckenrode ', '286793267779993600': 'Jeffrey J. Smith ', '286793272662163456': 'Tim Coy ', '286793277535944704': 'Kathy Feucht ', '286793283181477888': 'Renea Burns ', '286793287967178752': '参与人员', '286793292782239744': '德勤中国联系人', '286793297597300736': '关于本刊物', '286793302429138944': '关于德勤'}

def gpu_memory_cleanup():
    """清理GPU内存"""
    try:
        import torch
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            print("已清理GPU缓存")
    except Exception as e:
        print(f"清理GPU缓存失败: {str(e)}")

def build_index_IVFPQ(texts_with_ids, type):
    """
    增强版索引构建函数，支持加载旧向量和重新训练
    
    Args:
        texts_with_ids (dict): 文本ID和内容的字典映射
        type (str): 索引类型，可选"header"或"content"
        
    Returns:
        faiss.IndexIDMap: 构建好的FAISS索引
    """
    # 确保存储目录存在
    save_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "faiss_index_sc")
    os.makedirs(save_dir, exist_ok=True)
    
    # 定义文件路径
    index_path = os.path.join(save_dir, f"{type}_index_IVFPQ.index")
    vectors_file = os.path.join(save_dir, f"{type}_vectors.npy")
    ids_file = os.path.join(save_dir, f"{type}_ids.npy")
    meta_file = os.path.join(save_dir, f"{type}_meta.json")

    # 加载现有数据（如果存在）
    existing_vectors = np.array([], dtype=np.float32)
    existing_ids = np.array([], dtype=np.int64)
    meta_data = {
        'total_vectors': 0,
        'last_trained_count': 0,
        'history': []
    }
    
    # 尝试加载旧数据
    if os.path.exists(vectors_file) and os.path.exists(ids_file):
        existing_vectors = np.load(vectors_file)
        existing_ids = np.load(ids_file)
        print(f"加载到现有向量 {len(existing_ids)} 条")
        
    if os.path.exists(meta_file):
        with open(meta_file, 'r') as f:
            meta_data = json.load(f)

    # 处理新数据
    text_ids = list(texts_with_ids.keys())
    texts = list(texts_with_ids.values())
    total_cost = 0
    
    # 获取新嵌入向量
    new_embeddings, cost = get_embeddings(texts, text_ids)
    total_cost += cost
    
    # 转换为数组
    new_vectors = np.array(list(new_embeddings.values()), dtype=np.float32)
    new_ids = np.array([int(k) for k in new_embeddings.keys()], dtype=np.int64)
    
    # 合并新旧数据
    if len(existing_vectors) > 0:
        all_vectors = np.vstack([existing_vectors, new_vectors])
        all_ids = np.concatenate([existing_ids, new_ids])
    else:
        all_vectors = new_vectors
        all_ids = new_ids
    
    # 计算是否需要重新训练
    need_retrain = should_retrain(
        current_total_count=len(all_ids),
        last_trained_count=meta_data.get('last_trained_count', 0)
    )
    
    try:
        # 创建/更新索引
        if need_retrain or not os.path.exists(index_path):
            print("执行全量训练...")
            params = calculate_optimal_ivfpq_params(len(all_ids), all_vectors.shape[1])
            
            # 检查向量数量是否满足聚类要求
            if len(all_ids) < params['nlist']:
                print(f"警告：总向量数({len(all_ids)})小于聚类数({params['nlist']})，使用Flat索引替代")
                return build_index_flat(texts_with_ids, type)
                
            # GPU加速配置
            use_gpu = False
            gpu_res = None
            if faiss.get_num_gpus() > 0:
                try:
                    gpu_res = faiss.StandardGpuResources()
                    print("检测到可用GPU，启用加速训练")
                    use_gpu = True
                except Exception as e:
                    print(f"GPU初始化失败: {str(e)}，回退到CPU")

            # 记录训练开始时间
            train_start = time.time()
            
            # 创建量化器
            quantizer = faiss.IndexFlatL2(all_vectors.shape[1])
            
            if use_gpu:
                # GPU版本
                print("使用GPU加速训练...")
                train_start = time.time()
                
                # 创建GPU配置选项
                gpu_options = faiss.GpuMultipleClonerOptions()
                gpu_options.useFloat16 = True  # 使用半精度以节省GPU内存
                
                # 将量化器移至GPU
                quantizer_gpu = faiss.index_cpu_to_gpu(gpu_res, 0, quantizer)
                
                # 创建GPU版IVFPQ索引
                ivfpq_index = faiss.GpuIndexIVFPQ(
                    gpu_res,
                    quantizer_gpu,
                    all_vectors.shape[1],
                    params['nlist'],
                    params['m'],
                    params['nbits'],
                    faiss.METRIC_L2
                )
                
                # 设置训练参数
                ivfpq_index.verbose = True  # 启用详细输出
                
                # 执行训练
                print(f"开始GPU训练，向量数量: {len(all_vectors)}")
                ivfpq_index.train(all_vectors)
                
                # 记录训练时间
                train_duration = time.time() - train_start
                print(f"GPU训练完成，耗时 {train_duration:.2f} 秒")
                
                # 将索引转回CPU保存
                print("将索引从GPU转回CPU...")
                ivfpq_index = faiss.index_gpu_to_cpu(ivfpq_index)
                index_with_ids = faiss.IndexIDMap(ivfpq_index)
                
                # 清理GPU内存
                gpu_memory_cleanup()
            else:
                # CPU版本
                ivfpq_index = faiss.IndexIVFPQ(
                    quantizer, 
                    all_vectors.shape[1],
                    params['nlist'],
                    params['m'],
                    params['nbits']
                )

            # 执行训练
            ivfpq_index.train(all_vectors)
            
            # 记录训练时间
            train_duration = time.time() - train_start
            print(f"训练完成，耗时 {train_duration:.2f} 秒")
            
            # 如果使用GPU，将索引转回CPU保存
            if use_gpu:
                ivfpq_index = faiss.index_gpu_to_cpu(ivfpq_index)
            
            index_with_ids = faiss.IndexIDMap(ivfpq_index)
            add_vectors_in_batches(index_with_ids, all_vectors, all_ids)
            
            # 保存训练数据
            np.save(vectors_file, all_vectors)
            np.save(ids_file, all_ids)
        else:
            print("增量添加新向量...")
            index = faiss.read_index(index_path)
            if isinstance(index, faiss.IndexIDMap):
                add_vectors_in_batches(index, new_vectors, new_ids)
            else:
                index_with_ids = faiss.IndexIDMap(index)
                add_vectors_in_batches(index_with_ids, new_vectors, new_ids)
                index = index_with_ids
            index_with_ids = index
            
            # 追加保存数据
            np.save(vectors_file, np.vstack([existing_vectors, new_vectors]))
            np.save(ids_file, np.concatenate([existing_ids, new_ids]))

        # 保存索引
        faiss.write_index(index_with_ids, index_path)
        
        # 更新元数据
        meta_data.update({
            'total_vectors': len(all_ids),
            'last_trained_count': len(all_ids) if need_retrain else meta_data['last_trained_count'],
            'last_updated': datetime.datetime.now().isoformat(),
            'parameters': params if need_retrain else meta_data.get('parameters', {}),
            'training_time_seconds': train_duration if need_retrain else None,
            'history': meta_data['history'] + [{
                'time': datetime.datetime.now().isoformat(),
                'operation': 'retrain' if need_retrain else 'add',
                'added_vectors': len(new_ids),
                'total_vectors': len(all_ids),
                'training_seconds': train_duration if need_retrain else None,
                'gpu_accelerated': use_gpu if need_retrain else None
            }]
        })
        
        with open(meta_file, 'w') as f:
            json.dump(meta_data, f, indent=2)

        print(f"索引更新完成，总计{len(all_ids)}个向量")
        if use_gpu:
            gpu_memory_cleanup()
        return index_with_ids, total_cost

    except Exception as e:
        print(f"索引构建失败: {str(e)}")
        print("尝试使用Flat索引作为备选")
        return build_index_flat(texts_with_ids, type)

def calculate_optimal_ivfpq_params(n_vectors, dim):
    """
    根据向量数量和维度计算最优的IVFPQ参数
    
    Args:
        n_vectors (int): 向量数量
        dim (int): 向量维度
        
    Returns:
        dict: 包含nlist, m, nbits的字典
    """
    # 默认参数
    params = {
        'nlist': 4,  # 聚类中心数量
        'm': 8,      # 子量化器数量
        'nbits': 8   # 每个子量化器的位数
    }
    
    # 根据数据量调整nlist (聚类中心数量)
    if n_vectors < 1000:
        params['nlist'] = max(4, int(np.sqrt(n_vectors)))
    elif n_vectors < 10000:
        params['nlist'] = max(16, int(np.sqrt(n_vectors) / 2))
    elif n_vectors < 100000:
        params['nlist'] = max(64, int(np.sqrt(n_vectors)))
    elif n_vectors < 1000000:
        params['nlist'] = max(256, int(np.sqrt(n_vectors) * 2))
    else:
        params['nlist'] = max(1024, int(np.sqrt(n_vectors) * 4))
    
    # 确保nlist不超过向量数量的10%
    params['nlist'] = min(params['nlist'], int(n_vectors * 0.1))
    
    # 调整m (子量化器数量)，通常是维度的因子
    # 对于512维向量，常见选择是8, 16, 32, 64
    if dim == 512:
        if n_vectors < 10000:
            params['m'] = 8
        elif n_vectors < 100000:
            params['m'] = 16
        elif n_vectors < 1000000:
            params['m'] = 32
        else:
            params['m'] = 64
    else:
        # 对于其他维度，尝试找到合适的因子
        factors = [i for i in [8, 16, 32, 64] if dim % i == 0]
        if factors:
            # 根据数据量选择合适的因子
            if n_vectors < 10000:
                params['m'] = factors[0]
            else:
                params['m'] = factors[min(len(factors)-1, int(np.log10(n_vectors))-3)]
        else:
            # 如果没有合适的因子，使用接近dim/64的值
            params['m'] = max(8, min(64, dim // 64 * 8))
    
    # nbits通常保持为8，但对于大数据集可以考虑减小以节省空间
    if n_vectors > 10000000:  # 超过1000万条数据
        params['nbits'] = 6
    
    return params

def calculate_optimal_nprobe(nlist, n_vectors):
    """
    根据聚类中心数量和向量数量计算最优的nprobe值
    
    Args:
        nlist (int): 聚类中心数量
        n_vectors (int): 向量数量
        
    Returns:
        int: 最优的nprobe值
    """
    # 基本策略：数据量越大，nprobe相对nlist的比例越小
    if n_vectors < 10000:
        # 小数据集，搜索更多聚类以提高准确率
        return max(1, min(nlist, int(nlist * 0.3)))
    elif n_vectors < 100000:
        return max(1, min(nlist, int(nlist * 0.2)))
    elif n_vectors < 1000000:
        return max(1, min(nlist, int(nlist * 0.1)))
    else:
        # 大数据集，限制nprobe以保持搜索速度
        return max(1, min(nlist, int(nlist * 0.05)))

def should_retrain(current_total_count, last_trained_count, retrain_ratio=0.3):
    """
    根据当前向量总数和上次训练时的数量决定是否需要重新训练

    Args:
        current_total_count (int): 索引中更新后的向量总数 (即 current_count + new_count)
        last_trained_count (int): 上次训练时的向量数量
        retrain_ratio (float): 触发重新训练的增长比例阈值，默认0.3(30%)

    Returns:
        bool: 是否需要重新训练
    """
    # 如果索引从未训练过，必须训练
    if last_trained_count == 0:
        print("索引从未训练过，需要进行首次训练。")
        return True

    # 如果上次训练的数量有效
    if last_trained_count > 0:
        # 计算当前总数相对于上次训练数量的增长比例
        # 确保 last_trained_count 不为零以避免除零错误
        growth_ratio = (current_total_count - last_trained_count) / last_trained_count
        print(f"当前总向量数: {current_total_count}, 上次训练时数量: {last_trained_count}, 增长比例: {growth_ratio:.2f}, 阈值: {retrain_ratio}")
        # 如果增长比例超过阈值，则需要重新训练
        if growth_ratio > retrain_ratio:
            print(f"增长比例 {growth_ratio:.2f} > 阈值 {retrain_ratio}，建议重新训练。")
            return True
        else:
            print(f"增长比例 {growth_ratio:.2f} <= 阈值 {retrain_ratio}，不进行重新训练。")
            return False
    else:
        # last_trained_count < 0 的异常情况或逻辑错误
        print(f"警告: last_trained_count ({last_trained_count}) 无效，默认需要训练。")
        return True


def add_small_batch(texts_with_ids, type, auto_retrain=True, retrain_ratio=0.3):
    """
    优化版小批量更新IVFPQ索引函数
    
    Args:
        texts_with_ids (dict): 文本ID和内容的字典映射
        type (str): 索引类型，可选"header"或"content"
        auto_retrain (bool): 是否自动重新训练，默认True
        retrain_ratio (float): 触发重新训练的比例阈值，默认0.3(30%)
        
    Returns:
        tuple: (成功标志, 成本, 是否重新训练)
    """
    text_ids = list(texts_with_ids.keys())
    texts = list(texts_with_ids.values())
    total_cost = 0
    retrained = False
    
    # 获取嵌入向量
    text_embeddings, cost = get_embeddings(texts, text_ids)
    total_cost += cost
    
    # 转换为向量数组和ID数组
    new_vectors = np.array(list(text_embeddings.values()), dtype=np.float32)
    new_ids = np.array([int(k) for k in text_embeddings.keys()], dtype=np.int64)
    new_count = len(new_ids)
    
    # 向量规范化处理
    new_vectors = np.nan_to_num(new_vectors, nan=0.0, posinf=0.0, neginf=0.0)
    
    print(f"处理{new_count}个新向量")
    
    # 确保存储目录存在
    save_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "faiss_index_sc")
    os.makedirs(save_dir, exist_ok=True)
    
    # 获取索引和向量文件路径
    if type == "filename":
        index_path = os.path.join(save_dir, "filename_index_flat.index")
    elif type == "header":
        index_path = os.path.join(save_dir, "header_index_IVFPQ.index")
    else:
        index_path = os.path.join(save_dir, "content_index_IVFPQ.index")
    
    vectors_file = os.path.join(save_dir, f"{type}_vectors.npy")
    ids_file = os.path.join(save_dir, f"{type}_ids.npy")
    meta_file = os.path.join(save_dir, f"{type}_meta.json")
    
    try:
        # 尝试使用GPU加速（如果可用）
        use_gpu = False
        gpu_res = None
        
        if faiss.get_num_gpus() > 0 and type != "filename":
            try:
                gpu_res, gpu_id = get_available_gpu_resources()
                if gpu_res:
                    print(f"使用GPU {gpu_id} 加速索引操作")
                    use_gpu = True
            except Exception as e:
                print(f"GPU初始化失败: {str(e)}，使用CPU模式")
        
        # 检查索引是否存在
        if not os.path.exists(index_path):
            print(f"索引文件不存在: {index_path}，将创建新索引")
            
            # 使用适当的参数创建新索引
            if type == "filename":
                # 对于filename使用Flat索引
                return build_index_flat(texts_with_ids, type)
            else:
                # 对于header和content使用IVFPQ
                vectors = new_vectors
                dim = vectors.shape[1]
                
                # 获取最优参数
                params = calculate_optimal_ivfpq_params(len(new_ids), dim)
                
                # 检查是否满足聚类要求
                if len(new_ids) < params['nlist']:
                    print(f"警告：向量数({len(new_ids)})小于聚类数({params['nlist']})，调整参数")
                    params['nlist'] = max(4, len(new_ids) // 5)
                
                # 创建索引
                quantizer = faiss.IndexFlatL2(dim)
                
                if use_gpu:
                    # GPU版本
                    quantizer_gpu = faiss.index_cpu_to_gpu(gpu_res, 0, quantizer)
                    ivfpq_index = faiss.GpuIndexIVFPQ(
                        gpu_res, 
                        quantizer_gpu,
                        dim,
                        params['nlist'],
                        params['m'],
                        params['nbits'],
                        faiss.METRIC_L2
                    )
                    
                    # 训练
                    ivfpq_index.train(vectors)
                    
                    # 转回CPU
                    ivfpq_index = faiss.index_gpu_to_cpu(ivfpq_index)
                    index_with_ids = faiss.IndexIDMap(ivfpq_index)
                else:
                    # CPU版本
                    ivfpq_index = faiss.IndexIVFPQ(
                        quantizer, 
                        dim,
                        params['nlist'],
                        params['m'],
                        params['nbits']
                    )
                    
                    # 训练
                    ivfpq_index.train(vectors)
                    index_with_ids = faiss.IndexIDMap(ivfpq_index)
                
                # 分批添加向量
                add_vectors_in_batches(index_with_ids, vectors, new_ids)
                
                # 保存索引和数据
                faiss.write_index(index_with_ids, index_path)
                np.save(vectors_file, vectors)
                np.save(ids_file, new_ids)
                
                # 创建元数据
                meta_data = {
                    'total_vectors': len(new_ids),
                    'last_trained_count': len(new_ids),
                    'last_updated': datetime.datetime.now().isoformat(),
                    'parameters': params,
                    'training_time_seconds': None,
                    'history': [{
                        'time': datetime.datetime.now().isoformat(),
                        'operation': 'initial_build',
                        'added_vectors': len(new_ids),
                        'total_vectors': len(new_ids),
                        'gpu_accelerated': use_gpu
                    }]
                }
                with open(meta_file, 'w') as f:
                    json.dump(meta_data, f, indent=2)
                    
                return True, total_cost, True
        
        # 索引存在，进行增量更新
        # 获取元数据
        meta_data = {}
        if os.path.exists(meta_file):
            with open(meta_file, 'r') as f:
                meta_data = json.load(f)
        
        # 获取上次训练时的向量数量
        last_trained_count = meta_data.get('last_trained_count', 0)
        
        # 加载现有索引
        index = faiss.read_index(index_path)
        current_count = index.ntotal
        
        print(f"当前索引包含{current_count}个向量，上次训练时有{last_trained_count}个向量")
        print(f"新增{new_count}个向量，占已训练向量的{new_count/max(1, last_trained_count)*100:.2f}%")
        
        # 加载现有向量数据（用于可能的重新训练）
        if os.path.exists(vectors_file) and os.path.exists(ids_file):
            existing_vectors = np.load(vectors_file)
            existing_ids = np.load(ids_file)
            
            # 合并新旧向量和ID
            all_vectors = np.vstack([existing_vectors, new_vectors])
            all_ids = np.concatenate([existing_ids, new_ids])
            
            # 保存更新后的向量数据
            np.save(vectors_file, all_vectors)
            np.save(ids_file, all_ids)
            print(f"更新向量数据文件，总计{len(all_ids)}个向量")
        else:
            # 如果向量文件不存在，直接保存新向量
            np.save(vectors_file, new_vectors)
            np.save(ids_file, new_ids)
            print(f"创建新的向量数据文件，包含{len(new_ids)}个向量")
            all_vectors = new_vectors
            all_ids = new_ids
        
        # 判断是否需要重新训练
        need_retrain = should_retrain(current_count + new_count, last_trained_count, retrain_ratio)
        
        # 如果需要且允许重新训练
        if need_retrain and auto_retrain and type != "filename":
            print(f"新增向量比例达到{retrain_ratio*100}%阈值 或 首次训练，执行重新训练...")
            
            # 根据数据量确定最优参数
            params = calculate_optimal_ivfpq_params(len(all_ids), all_vectors.shape[1])
            nlist = params['nlist']
            m = params['m']
            nbits = params['nbits']
            
            print(f"使用参数 nlist={nlist}, m={m}, nbits={nbits} 训练索引")
            
            # 训练新索引
            try:
                dim = all_vectors.shape[1]
                quantizer = faiss.IndexFlatL2(dim)
                
                # 记录训练开始时间
                train_start = time.time()
                
                if use_gpu:
                    # GPU版本
                    quantizer_gpu = faiss.index_cpu_to_gpu(gpu_res, 0, quantizer)
                    ivfpq_index = faiss.GpuIndexIVFPQ(
                        gpu_res,
                        quantizer_gpu,
                        dim,
                        nlist,
                        m,
                        nbits,
                        faiss.METRIC_L2
                    )
                    
                    # 分批训练（对于大数据集）
                    if len(all_vectors) > 500000:
                        # 对于大数据集，使用样本训练
                        sample_size = min(500000, len(all_vectors))
                        indices = np.random.choice(len(all_vectors), sample_size, replace=False)
                        sample_vectors = all_vectors[indices]
                        ivfpq_index.train(sample_vectors)
                    else:
                        ivfpq_index.train(all_vectors)
                    
                    # 转回CPU以便保存
                    ivfpq_index = faiss.index_gpu_to_cpu(ivfpq_index)
                    index_with_ids = faiss.IndexIDMap(ivfpq_index)
                    
                    # 记录训练时间
                    train_duration = time.time() - train_start
                else:
                    # CPU版本
                    ivfpq_index = faiss.IndexIVFPQ(
                        quantizer,
                        dim,
                        nlist,
                        m,
                        nbits
                    )
                    
                    # 分批训练（对于大数据集）
                    if len(all_vectors) > 500000:
                        # 对于大数据集，使用样本训练
                        sample_size = min(500000, len(all_vectors))
                        indices = np.random.choice(len(all_vectors), sample_size, replace=False)
                        sample_vectors = all_vectors[indices]
                        ivfpq_index.train(sample_vectors)
                    else:
                        ivfpq_index.train(all_vectors)
                        
                    index_with_ids = faiss.IndexIDMap(ivfpq_index)
                    train_duration = time.time() - train_start
                
                # 分批添加向量到索引
                add_vectors_in_batches(index_with_ids, all_vectors, all_ids)
                
                # 保存索引
                faiss.write_index(index_with_ids, index_path)
                print(f"重新训练完成，耗时 {train_duration:.2f} 秒，索引已保存")
                
                # 更新索引和重训标志
                index = index_with_ids
                retrained = True
                
                # 更新元数据
                meta_data['last_trained_count'] = len(all_ids)
                meta_data['parameters'] = params
                meta_data['training_time_seconds'] = train_duration
                
            except Exception as e:
                print(f"重新训练失败: {str(e)}，维持原有索引并添加新向量")
                # 失败时，仍然添加新向量到原索引
                if isinstance(index, faiss.IndexIDMap):
                    add_vectors_in_batches(index, new_vectors, new_ids)
                else:
                    index_with_ids = faiss.IndexIDMap(index)
                    add_vectors_in_batches(index_with_ids, new_vectors, new_ids)
                    index = index_with_ids
                
                # 保存更新后的索引
                faiss.write_index(index, index_path)
        
        else:
            # 不需要重新训练，直接添加新向量
            if not need_retrain:
                print(f"总向量数增长比例未达到重新训练阈值，直接添加到现有索引")
            elif not auto_retrain:
                print(f"需要重新训练，但auto_retrain=False，直接添加到现有索引")
            elif type == "filename":
                print(f"文件名索引使用Flat类型，不需要重新训练")
            
            # 添加新向量到索引
            if isinstance(index, faiss.IndexIDMap):
                add_vectors_in_batches(index, new_vectors, new_ids)
            else:
                index_with_ids = faiss.IndexIDMap(index)
                add_vectors_in_batches(index_with_ids, new_vectors, new_ids)
                index = index_with_ids
            
            # 保存更新后的索引
            faiss.write_index(index, index_path)
        
        # 更新元数据
        current_time = datetime.datetime.now().isoformat()
        if 'history' not in meta_data:
            meta_data['history'] = []
        
        # 添加操作记录
        meta_data['history'].append({
            'time': current_time,
            'operation': 'retrain' if retrained else 'add_small_batch',
            'added_vectors': new_count,
            'total_vectors': index.ntotal,
            'training_seconds': train_duration if retrained else None,
            'gpu_accelerated': use_gpu
        })
        
        # 更新总向量数
        meta_data['total_vectors'] = index.ntotal
        meta_data['last_updated'] = current_time
        
        # 保存元数据
        with open(meta_file, 'w') as f:
            json.dump(meta_data, f, indent=2)
        
        print(f"元数据更新完成")
        
        if use_gpu:
            gpu_memory_cleanup()
        
        return True, total_cost, retrained
        
    except Exception as e:
        print(f"处理向量批次失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False, total_cost, False

def build_index_flat(texts_with_ids, type):
    """
    构建或更新文件名的FAISS Flat索引（支持增量添加）

    Args:
        texts_with_ids (dict): 新增的文本ID和内容的字典映射
        type (str): 索引类型，应为 "filename"

    Returns:
        tuple: (构建/更新后的FAISS索引, 本次操作的成本)
    """
    if type != "filename":
        print(f"警告: build_index_flat 函数预期类型为 'filename', 但收到 '{type}'.")
        # 可以选择抛出错误或继续，但逻辑主要为filename设计
        # raise ValueError("build_index_flat is designed for type 'filename'")

    save_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "faiss_index_sc")
    os.makedirs(save_dir, exist_ok=True)

    index_path = os.path.join(save_dir, "filename_index_flat.index")
    ids_path = os.path.join(save_dir, "filename_index_flat_ids.json") # 注意，这里之前可能是 .npy，但Flat索引通常ID较少，json也可
    meta_file = os.path.join(save_dir, f"{type}_meta.json")

    dim = 512  # 假设向量维度为 512
    total_cost = 0
    start_time = time.time()
    operation_type = 'initial_build' # 默认为初始构建

    # --- 开始修改 ---
    existing_ids_list = []
    index_with_ids = None

    # 1. 检查并加载现有索引和ID
    if os.path.exists(index_path) and os.path.exists(ids_path):
        try:
            print(f"加载现有索引: {index_path}")
            index_read = faiss.read_index(index_path)
            # 确保加载的是 IndexIDMap，如果不是则包装
            if isinstance(index_read, faiss.IndexIDMap):
                 index_with_ids = index_read
            else:
                 # 如果之前保存的不是 IndexIDMap (虽然不太可能，但做个兼容)
                 print("警告：加载的索引不是 IndexIDMap，将尝试包装。")
                 index_with_ids = faiss.IndexIDMap(index_read)

            print(f"加载现有 IDs: {ids_path}")
            with open(ids_path, 'r', encoding='utf-8') as f:
                existing_ids_list = json.load(f)
            print(f"已加载 {len(existing_ids_list)} 个现有 IDs。")
            operation_type = 'update' # 标记为更新操作
        except Exception as e:
            print(f"加载现有索引或ID失败: {e}。将重新创建。")
            index_with_ids = None # 重置以确保创建新索引
            existing_ids_list = []

    # 2. 如果没有加载现有索引，则创建新索引
    if index_with_ids is None:
        print("创建新的 Flat 索引...")
        index_flat = faiss.IndexFlatL2(dim)
        index_with_ids = faiss.IndexIDMap(index_flat)
        existing_ids_list = [] # 确保 ID 列表为空

    # 3. 处理传入的新文本和ID
    new_text_ids_str = list(texts_with_ids.keys())
    # 过滤掉已经存在的 ID，避免重复添加
    new_text_ids_to_process = {
        text_id: text
        for text_id, text in texts_with_ids.items()
        if int(text_id) not in [int(existing_id) for existing_id in existing_ids_list]
    }

    added_vectors_count = 0
    if not new_text_ids_to_process:
        print("没有新的、唯一的文本ID需要处理。")
    else:
        print(f"准备处理 {len(new_text_ids_to_process)} 个新的文本ID...")
        new_texts = list(new_text_ids_to_process.values())
        new_ids_str = list(new_text_ids_to_process.keys())

        # 4. 批量获取新文本的嵌入向量
        new_embeddings, cost = get_embeddings(new_texts, new_ids_str)
        total_cost += cost

        # 5. 转换新数据为向量和ID数组 (确保ID是 int64)
        new_embedding_ids_str = list(new_embeddings.keys())
        new_vectors = np.array(list(new_embeddings.values()), dtype=np.float32)
        new_ids_int64 = np.array([int(k) for k in new_embedding_ids_str], dtype=np.int64)
        new_ids_list = [int(k) for k in new_embedding_ids_str] # 用于更新json列表

        # 确保新向量和ID数量匹配
        if new_vectors.shape[0] != len(new_ids_int64):
             print(f"警告: 新向量数量 ({new_vectors.shape[0]}) 与新 ID 数量 ({len(new_ids_int64)}) 不匹配。跳过添加。")
        elif new_vectors.shape[0] > 0:
            # 6. 添加新向量和ID到索引
            print(f"添加 {new_vectors.shape[0]} 个新向量到索引...")
            index_with_ids.add_with_ids(new_vectors, new_ids_int64)
            added_vectors_count = new_vectors.shape[0]

            # 7. 更新总的ID列表
            existing_ids_list.extend(new_ids_list)
        else:
            print("没有生成有效的嵌入向量来添加。")

    # --- 结束修改 ---

    print(f"索引中最终向量数量: {index_with_ids.ntotal}")

    # 8. 保存更新后的索引到文件
    try:
        faiss.write_index(index_with_ids, index_path)
        print(f"成功保存更新后的FAISS索引文件: {index_path}")

        # 9. 保存更新后的ID列表
        with open(ids_path, 'w', encoding='utf-8') as f:
            json.dump([int(id_val) for id_val in existing_ids_list], f)
        print(f"成功保存更新后的ID映射文件: {ids_path}")
    except Exception as e:
        print(f"保存索引或ID文件失败: {str(e)}")
        # 可以考虑在这里返回错误或之前的状态

    build_duration = time.time() - start_time
    print(f"{operation_type} Flat索引完成，耗时 {build_duration:.2f} 秒")

    # 10. 更新并保存元数据
    meta_data = {}
    if os.path.exists(meta_file):
        try:
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
            # 确保 history 字段存在且是列表
            if 'history' not in meta_data or not isinstance(meta_data.get('history'), list):
                 meta_data['history'] = []
            print(f"成功加载现有元数据: {meta_file}")
        except Exception as e:
            print(f"加载元数据文件失败: {e}。将创建新的元数据。")
            meta_data = {'history': []} # 创建空的元数据
    else:
        print(f"元数据文件不存在: {meta_file}。将创建新的元数据。")
        meta_data = {'history': []}

    meta_data.update({
        'total_vectors': index_with_ids.ntotal,
        'last_updated': datetime.datetime.now().isoformat(),
        'index_type': 'flat', # 明确索引类型
        # Flat 索引没有训练时间，但可以记录本次操作耗时
        'last_operation_seconds': build_duration
    })

    # 添加历史记录
    meta_data['history'].append({
        'time': datetime.datetime.now().isoformat(),
        'operation': operation_type, # 'initial_build' 或 'update'
        'added_vectors': added_vectors_count, # 本次实际添加的向量数
        'total_vectors': index_with_ids.ntotal,
        'operation_seconds': build_duration
    })

    try:
        with open(meta_file, 'w', encoding='utf-8') as f:
            json.dump(meta_data, f, indent=2, ensure_ascii=False)
        print(f"成功保存更新后的元数据文件: {meta_file}")
    except Exception as e:
        print(f"保存元数据文件失败: {e}")

    return index_with_ids, total_cost

def add_vectors_in_batches(index, vectors, ids, batch_size=10000):
    """分批添加向量到索引，避免内存峰值过高
    
    Args:
        index: FAISS索引对象
        vectors: 全部向量
        ids: 向量对应的ID
        batch_size: 每批处理的向量数量
    """
    for i in range(0, len(ids), batch_size):
        batch_vectors = vectors[i:i+batch_size]
        batch_ids = ids[i:i+batch_size]
        index.add_with_ids(batch_vectors, batch_ids)
        print(f"已添加 {i+len(batch_ids)}/{len(ids)} 个向量")

def get_available_gpu_resources():
    """获取可用GPU资源并返回最佳配置"""
    n_gpus = faiss.get_num_gpus()
    
    if n_gpus == 0:
        print("未检测到可用GPU")
        return None, False
        
    try:
        # 获取GPU内存信息
        import torch
        gpu_mem_free = []
        for i in range(n_gpus):
            free_mem = torch.cuda.get_device_properties(i).total_memory - torch.cuda.memory_allocated(i)
            total_mem = torch.cuda.get_device_properties(i).total_memory
            used_mem = torch.cuda.memory_allocated(i)
            gpu_mem_free.append((i, free_mem))
            print(f"GPU {i} 信息: 总内存={total_mem/(1024**3):.2f}GB, 已用={used_mem/(1024**3):.2f}GB, 可用={free_mem/(1024**3):.2f}GB")
            
        # 选择内存最大的GPU
        best_gpu_id, best_mem = max(gpu_mem_free, key=lambda x: x[1])
        print(f"选择GPU {best_gpu_id} 作为最佳设备 (可用内存: {best_mem/(1024**3):.2f}GB)")
        
        # 创建GPU资源
        gpu_options = faiss.GpuMultipleClonerOptions()
        gpu_options.useFloat16 = True  # 使用半精度浮点数以节省GPU内存
        res = faiss.StandardGpuResources()
        
        print(f"GPU资源初始化成功，使用设备ID: {best_gpu_id}")
        return res, best_gpu_id
    except Exception as e:
        print(f"获取GPU详细信息失败: {str(e)}")
        try:
            # 如果无法获取详细内存信息，使用第一个GPU
            print("尝试使用第一个可用GPU")
            res = faiss.StandardGpuResources()
            return res, 0
        except Exception as e2:
            print(f"GPU资源初始化失败: {str(e2)}")
            return None, False

def process_large_vector_set(vectors_file, ids_file, process_func, batch_size=10000):
    """分批处理大规模向量数据，避免一次性加载全部数据
    
    Args:
        vectors_file: 向量文件路径
        ids_file: ID文件路径
        process_func: 处理函数，接收(vectors, ids)参数
        batch_size: 每批处理数量
        
    Returns:
        处理结果
    """
    # 获取文件大小和总记录数
    vectors_shape = tuple(np.lib.format.read_array_header_1_0(open(vectors_file, 'rb'))[0])
    total_vectors = vectors_shape[0]
    
    results = []
    
    # 分批读取和处理
    for i in range(0, total_vectors, batch_size):
        # 内存映射方式读取
        mmap_mode = 'r'
        vectors_batch = np.load(vectors_file, mmap_mode=mmap_mode)[i:i+batch_size]
        ids_batch = np.load(ids_file, mmap_mode=mmap_mode)[i:i+batch_size]
        
        # 处理当前批次
        result = process_func(vectors_batch, ids_batch)
        results.append(result)
        
        # 显式释放内存
        del vectors_batch
        del ids_batch
        import gc
        gc.collect()
        
    return results

def auto_tune_ivfpq(vectors, ids, test_queries=None, search_k=10):
    """自动调整IVFPQ参数以获得最佳性能
    
    Args:
        vectors: 向量数据
        ids: 向量ID
        test_queries: 测试查询向量(可选)
        search_k: 搜索返回的向量数
        
    Returns:
        最优参数字典
    """
    # 如果没有提供测试查询，使用部分训练数据
    if test_queries is None and len(vectors) > 1000:
        test_queries = vectors[:100]
    
    # 候选参数组合
    nlist_candidates = [4, 16, 64, 256, 1024]
    m_candidates = [8, 16, 32, 64]
    nbits_candidates = [8]
    
    best_params = None
    best_score = -1
    best_time = float('inf')  # 记录最佳参数的训练时间
    
    # 创建基准索引用于比较
    benchmark_index = faiss.IndexFlatL2(vectors.shape[1])
    benchmark_index.add(vectors)
    
    # 测试各参数组合
    for nlist in nlist_candidates:
        # 跳过不适合的nlist
        if nlist > len(vectors) // 10:
            continue
            
        for m in m_candidates:
            # 跳过不适合的m
            if m > vectors.shape[1] // 8:
                continue
                
            for nbits in nbits_candidates:
                # 记录训练开始时间
                tune_start = time.time()
                
                # 构建测试索引
                quantizer = faiss.IndexFlatL2(vectors.shape[1])
                index = faiss.IndexIVFPQ(quantizer, vectors.shape[1], nlist, m, nbits)
                
                # 训练和添加向量
                index.train(vectors)
                index_with_ids = faiss.IndexIDMap(index)
                index_with_ids.add_with_ids(vectors, ids)
                
                # 记录训练时间
                tune_duration = time.time() - tune_start
                
                # 评估质量
                score = evaluate_index_quality(index_with_ids, benchmark_index, test_queries, search_k)
                
                if score > best_score or (score == best_score and tune_duration < best_time):
                    best_score = score
                    best_time = tune_duration
                    best_params = {
                        'nlist': nlist, 
                        'm': m, 
                        'nbits': nbits, 
                        'score': score,
                        'training_seconds': tune_duration
                    }
    
    return best_params

def evaluate_index_quality(test_index, benchmark_index, queries, k=10):
    """评估索引质量，比较与基准索引的结果相似度
    
    Args:
        test_index: 待评估的索引
        benchmark_index: 基准索引(通常是准确但慢的IndexFlatL2)
        queries: 查询向量
        k: 检索结果数量
        
    Returns:
        float: 0-1之间的质量分数
    """
    # 设置查询参数
    if hasattr(test_index, 'nprobe'):
        test_index.nprobe = min(test_index.nlist, 16)  # 对IVFPQ设置nprobe
        
    # 使用基准索引查询(准确结果)
    D_ref, I_ref = benchmark_index.search(queries, k)
    
    # 使用测试索引查询
    D_test, I_test = test_index.search(queries, k)
    
    # 计算结果重叠度
    overlap = 0
    for i in range(len(queries)):
        ref_ids = set(I_ref[i])
        test_ids = set(I_test[i])
        overlap += len(ref_ids.intersection(test_ids)) / k
        
    # 返回平均重叠率作为质量指标
    return overlap / len(queries)

def check_gpu_status():
    """检查GPU状态并打印信息"""
    try:
        n_gpus = faiss.get_num_gpus()
        print(f"检测到 {n_gpus} 个可用GPU")
        
        if n_gpus > 0:
            import torch
            for i in range(n_gpus):
                if torch.cuda.is_available():
                    device_name = torch.cuda.get_device_name(i)
                    total_mem = torch.cuda.get_device_properties(i).total_memory
                    used_mem = torch.cuda.memory_allocated(i)
                    free_mem = total_mem - used_mem
                    
                    print(f"GPU {i}: {device_name}")
                    print(f"  总内存: {total_mem/(1024**3):.2f} GB")
                    print(f"  已用内存: {used_mem/(1024**3):.2f} GB")
                    print(f"  可用内存: {free_mem/(1024**3):.2f} GB")
                    print(f"  使用率: {used_mem/total_mem*100:.2f}%")
            
            return True
        return False
    except Exception as e:
        print(f"检查GPU状态时出错: {str(e)}")
        return False

# 在主函数开始处调用
if __name__ == "__main__":
    # 检查GPU状态
    gpu_available = check_gpu_status()
    print(f"GPU加速{'可用' if gpu_available else '不可用'}")
    
    # ... 其余代码 ...
