import faiss
import numpy as np
from utils.vector_generator import get_embeddings,get_embedding_single_text
import os
import json
import datetime
import time

# filename = {'286792841991028736': '2024年商业地产行业展望:强基固本向新而生', '286792850677432320': '主要观点', '286792856071307264': '重新稳固发展基础', '286792858629832704': '各位同仁:', '286792864627687424': '经济放缓隐忧进一步影响行业收入和支出', '286792869941870592': '调研方法', '286792875134418944': '各地区大部分受访者均预计2023年营业收入将降低', '286792886358376448': '市场预期发生结构性变化', '286792900811948032': '受访者更看好数字地产和独户租赁/建房出租类地产', '286792908072288256': '写字楼 ', '286792910458847232': '现状', '286792915231965184': '零售地产 ', '286792917765324800': '展望 ', '286792920156078080': '现状', '286792924937584640': '展望 ', '286792936501280768': '工业地产', '286792941534445568': '现状', '286792946466947072': '展望 ', '286792951437197312': '住宅', '286792953899253760': '现状', '286792961759379456': '展望 ', '286792966683492352': '酒店地产', '286792970265427968': '现状', '286792975860629504': '展望 ', '286792982370189312': '替代型资产:数字地产、养老地产和生命科学地产', '286792987919253504': '数字地产 ', '286792992788840448': '独户租赁/建房出租类地产', '286792998556008448': '养老地产 ', '286793004365119488': '生命科学地产', '286793009171791872': '行动建议', '286793014041378816': '保守型', '286793019615608832': '主动型', '286793024510361600': '行业领导者观点:选址和多功能区域的重要性', '286793029321228288': '制定可持续发展战略,推进低碳转型', '286793034136289280': '行业领导者观点:将可持续发展充分融入资产组合策略', '286793038947155968': '写字楼运营的可持续性', '286793043783188480': '全生命周期碳排放评估应成为标准', '286793048615026688': '许多房地产公司缺乏满足合规要求的内部控制', '286793053434281984': 'ESG法规与合规:房地产公司是否做好准备?', '286793062921797632': '行动建议', '286793070131806208': '厘清税惠政策,实现利润提升', '286793075018170368': '$\\bullet$ 绿色协议工业计划(The Green Deal', '286793081519341568': '全球最低税率实施', '286793083947843584': '展望 ', '286793092718133248': '行动建议', '286793097528999936': '混合办公与运营转型', '286793102562164736': '行业领导者观点:当今职场如何发挥领导力', '286793108316749824': 'Heitman全球投资研究总监MaryLudgin', '286793113115033600': '借职能外包提高效率', '286793115547729920': '变革驱动因素', '286793122812264448': '行业领导者观点:"赢在通勤" ', '286793127677657088': 'Seaforth Land首席执行官Tyler Goodwin ', '286793134866694144': '房地产公司的目标', '286793139715309568': '行业领导者观点:加速运营转型', '286793146786906112': '世邦魏理仕集团首席运营官Vikram Kohli', '286793155037102080': '行动建议', '286793162356162560': '提高房地产行业技术能力', '286793169062854656': '行业领导者观点:投资技术提升竞争力', '286793175366893568': '正视房地产行业技术债务', '286793181960339456': '挑战现状 ', '286793192156692480': '行动建议', '286793201992335360': '为未来商业地产奠定坚实基础', '286793209743409152': '尾注', '286793222913523712': '关于作者', '286793225300082688': 'Jeffrey Smith ', '286793227711807488': 'Renea Burns ', '286793232749166592': 'Kathy Feucht ', '286793238541500416': 'Tim Coy ', '286793243734048768': '致谢 ', '286793248599441408': '联系我们', '286793250994388992': '行业领导人', '286793254576324608': '德勤金融服务行业研究中心', '286793256954494976': '罗远江', '286793262918795264': 'Jim Eckenrode ', '286793267779993600': 'Jeffrey J. Smith ', '286793272662163456': 'Tim Coy ', '286793277535944704': 'Kathy Feucht ', '286793283181477888': 'Renea Burns ', '286793287967178752': '参与人员', '286793292782239744': '德勤中国联系人', '286793297597300736': '关于本刊物', '286793302429138944': '关于德勤'}


def build_index_IVFPQ(texts_with_ids, type):
    """
    构建文件名的FAISS索引，自动调整IVFPQ参数
    
    Args:
        texts_with_ids (dict): 文本ID和内容的字典映射
        type (str): 索引类型，可选"header"或"content"
        
    Returns:
        faiss.IndexIDMap: 构建好的FAISS索引
    """
    # 问题：这里调用add_small_batch，但add_small_batch在索引不存在时会再次调用build_index_IVFPQ，形成循环依赖
    
    # 解决方案：直接从头构建索引，避免循环调用
    text_ids = list(texts_with_ids.keys())
    texts = list(texts_with_ids.values())
    total_cost = 0
    
    # 获取嵌入向量
    text_embeddings, cost = get_embeddings(texts, text_ids)
    total_cost += cost
    
    # 转换为向量数组和ID数组
    vectors = np.array(list(text_embeddings.values()), dtype=np.float32)
    ids = np.array([int(k) for k in text_embeddings.keys()], dtype=np.int64)
    
    # 确保存储目录存在
    save_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "faiss_index")
    os.makedirs(save_dir, exist_ok=True)
    
    # 根据数据量计算最优参数
    params = calculate_optimal_ivfpq_params(len(ids), vectors.shape[1])
    
    # 创建并训练索引
    try:
        quantizer = faiss.IndexFlatL2(vectors.shape[1])
        ivfpq_index = faiss.IndexIVFPQ(quantizer, vectors.shape[1], params['nlist'], params['m'], params['nbits'])
        
        if len(ids) < params['nlist']:
            print(f"警告：向量数量({len(ids)})小于聚类数({params['nlist']})，使用Flat索引替代")
            return build_index_flat(texts_with_ids, type)
            
        ivfpq_index.train(vectors)
        
        # 添加向量到索引
        index_with_ids = faiss.IndexIDMap(ivfpq_index)
        index_with_ids.add_with_ids(vectors, ids)
        
        # 保存索引
        if type == "header":
            index_path = os.path.join(save_dir, "header_index_IVFPQ.index") 
        else:
            index_path = os.path.join(save_dir, "content_index_IVFPQ.index")
            
        faiss.write_index(index_with_ids, index_path)
        
        # 保存向量数据
        vectors_file = os.path.join(save_dir, f"{type}_vectors.npy")
        ids_file = os.path.join(save_dir, f"{type}_ids.npy")
        np.save(vectors_file, vectors)
        np.save(ids_file, ids)
        
        # 保存元数据
        meta_file = os.path.join(save_dir, f"{type}_meta.json")
        meta_data = {
            'total_vectors': len(ids),
            'last_trained_count': len(ids),
            'last_updated': datetime.datetime.now().isoformat(),
            'parameters': params,
            'history': [{
                'time': datetime.datetime.now().isoformat(),
                'operation': 'initial_build',
                'added_vectors': len(ids),
                'total_vectors': len(ids)
            }]
        }
        with open(meta_file, 'w') as f:
            json.dump(meta_data, f, indent=2)
        
        print(f"成功构建并保存IVFPQ索引，包含{len(ids)}个向量")
        return index_with_ids, total_cost
    except Exception as e:
        print(f"构建IVFPQ索引失败: {str(e)}")
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

def should_retrain(current_count, new_count, last_trained_count, retrain_ratio=0.25):
    """
    根据当前向量数量和新增向量数量决定是否需要重新训练
    
    Args:
        current_count (int): 索引中当前的向量总数
        new_count (int): 新添加的向量数量
        last_trained_count (int): 上次训练时的向量数量
        retrain_ratio (float): 触发重新训练的比例阈值，默认0.3(30%)
        
    Returns:
        bool: 是否需要重新训练
    """
    # 如果索引从未训练过，必须训练
    if last_trained_count == 0:
        return True
    
    # 计算新增向量占已训练向量的比例
    ratio = new_count / last_trained_count
    
    # 如果新增向量超过已训练向量的一定比例，建议重新训练
    return ratio > retrain_ratio


def add_small_batch(texts_with_ids, type, auto_retrain=True, retrain_ratio=0.3):
    """
    小批量更新IVFPQ索引，根据向量数量比例自动决定是否重新训练
    
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
    
    print(f"处理{new_count}个新向量")
    
    # 确保存储目录存在
    save_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "faiss_index")
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
        # 检查索引是否存在
        if not os.path.exists(index_path):
            print(f"索引文件不存在: {index_path}，将创建新索引")
            # 修复：不要再调用build_index_IVFPQ，而是直接用传入的数据创建索引
            # 使用calculate_optimal_ivfpq_params获取最优参数
            vectors = np.array(list(text_embeddings.values()), dtype=np.float32)
            params = calculate_optimal_ivfpq_params(len(new_ids), vectors.shape[1])
            
            dim = vectors.shape[1]
            quantizer = faiss.IndexFlatL2(dim)
            ivfpq_index = faiss.IndexIVFPQ(quantizer, dim, params['nlist'], params['m'], params['nbits'])
            
            # 训练并添加向量
            ivfpq_index.train(vectors)
            index_with_ids = faiss.IndexIDMap(ivfpq_index)
            index_with_ids.add_with_ids(vectors, new_ids)
            
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
                'history': [{
                    'time': datetime.datetime.now().isoformat(),
                    'operation': 'initial_build',
                    'added_vectors': len(new_ids),
                    'total_vectors': len(new_ids)
                }]
            }
            with open(meta_file, 'w') as f:
                json.dump(meta_data, f, indent=2)
                
            return True, total_cost, True
        
        # 获取元数据
        meta_data = {}
        if os.path.exists(meta_file):
            with open(meta_file, 'r') as f:
                meta_data = json.load(f)
        
        # 获取上次训练时的向量数量
        last_trained_count = meta_data.get('last_trained_count', 0)
        
        # 加载现有索引以获取当前向量数量
        index = faiss.read_index(index_path)
        current_count = index.ntotal
        
        print(f"当前索引包含{current_count}个向量，上次训练时有{last_trained_count}个向量")
        print(f"新增{new_count}个向量，占已训练向量的{new_count/max(1, last_trained_count)*100:.2f}%")
        
        # 判断是否需要重新训练
        need_retrain = should_retrain(current_count, new_count, last_trained_count, retrain_ratio)
        
        # 更新向量数据文件(无论是否重新训练，都需要更新)
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
            # 如果没有向量文件，可能需要重新训练
            need_retrain = True
        
        # 如果需要且允许重新训练
        if need_retrain and auto_retrain:
            print(f"新增向量比例达到{retrain_ratio*100}%阈值，执行重新训练...")
            
            # 加载全部向量数据
            all_vectors = np.load(vectors_file)
            all_ids = np.load(ids_file)
            
            # 根据数据量确定最优参数
            params = calculate_optimal_ivfpq_params(len(all_ids), all_vectors.shape[1])
            nlist = params['nlist']
            m = params['m']
            nbits = params['nbits']
            
            print(f"使用参数 nlist={nlist}, m={m}, nbits={nbits} 训练索引")
            
            # 创建新索引并训练
            quantizer = faiss.IndexFlatL2(all_vectors.shape[1])
            ivfpq_index = faiss.IndexIVFPQ(quantizer, all_vectors.shape[1], nlist, m, nbits)
            ivfpq_index.train(all_vectors)
            
            # 添加向量到索引
            index_with_ids = faiss.IndexIDMap(ivfpq_index)
            index_with_ids.add_with_ids(all_vectors, all_ids)
            
            # 保存训练好的索引
            faiss.write_index(index_with_ids, index_path)
            print(f"重新训练完成，索引已保存: {index_path}")
            
            # 更新索引对象和训练状态
            index = index_with_ids
            retrained = True
            
            # 更新元数据中的训练记录
            meta_data['last_trained_count'] = len(all_ids)
        else:
            # 如果不需要重新训练，直接添加新向量到现有索引
            if not need_retrain:
                print(f"新增向量比例未达到重新训练阈值，直接添加到现有索引")
            elif not auto_retrain:
                print(f"需要重新训练，但auto_retrain=False，直接添加到现有索引")
                print(f"建议稍后手动调用force_retrain_IVFPQ_index('{type}')以获得更好性能")
            
            # 添加新向量到索引
            if isinstance(index, faiss.IndexIDMap):
                index.add_with_ids(new_vectors, new_ids)
            else:
                # 如果索引不支持ID，先包装为支持ID的索引
                index_with_ids = faiss.IndexIDMap(index)
                index_with_ids.add_with_ids(new_vectors, new_ids)
                index = index_with_ids
            
            # 保存更新后的索引
            faiss.write_index(index, index_path)
            print(f"成功添加{len(new_ids)}个新向量，索引现包含{index.ntotal}个向量")
        
        # 更新元数据
        current_time = datetime.datetime.now().isoformat()
        if 'history' not in meta_data:
            meta_data['history'] = []
        
        # 添加操作记录
        meta_data['history'].append({
            'time': current_time,
            'operation': 'retrain' if retrained else 'add_small_batch',
            'added_vectors': new_count,
            'total_vectors': index.ntotal
        })
        
        # 更新总向量数
        meta_data['total_vectors'] = index.ntotal
        meta_data['last_updated'] = current_time
        
        # 保存元数据
        with open(meta_file, 'w') as f:
            json.dump(meta_data, f, indent=2)
        
        print(f"元数据更新完成")
        
        return True, total_cost, retrained
        
    except Exception as e:
        print(f"处理向量批次失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False, total_cost, False

def build_index_flat(texts_with_ids, type):
    """
    构建文件名的FAISS Flat索引
    
    Args:
        texts_with_ids (dict): 文本ID和内容的字典映射
        type (str): 索引类型,可选"header"或"content"
        
    Returns:
        faiss.IndexIDMap: 构建好的FAISS索引
    """

    text_ids = list(texts_with_ids.keys())
    texts = list(texts_with_ids.values())
    total_cost = 0
    # 批量获取各部分的嵌入向量
    text_embeddings,cost = get_embeddings(texts, text_ids)
    total_cost+=cost
    # 转换字典为向量数组和ID数组
    vectors = np.array(list(text_embeddings.values()), dtype=np.float32)
    ids = np.array([int(k) for k in text_embeddings.keys()], dtype=np.int64)

    # 确保向量矩阵形状正确
    print(f"向量矩阵形状: {vectors.shape}")  # 应该是 (N, 512)
    print(f"ID数组形状: {ids.shape}")  # 应该是 (N,)

    # 1. 创建支持 ID 的索引
    dim = 512  # 向量维度
    index_flat = faiss.IndexFlatL2(dim)  # 创建Flat索引
    index_with_ids = faiss.IndexIDMap(index_flat)  # 包装为支持ID的索引

    # 2. 添加向量和ID到索引
    index_with_ids.add_with_ids(vectors, ids)
    print(f"索引中的向量数量: {index_with_ids.ntotal}")  # 应该等于 vectors.shape[0]

    # 保存索引到文件
    # 确保存储目录存在
    save_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "faiss_index")
    os.makedirs(save_dir, exist_ok=True)
    
    try:
        if type == "filename":
            index_path = os.path.join(save_dir, "filename_index_flat.index")
        elif type == "header":
            index_path = os.path.join(save_dir, "header_index_IVFPQ.index")
        else:
            index_path = os.path.join(save_dir, "content_index_IVFPQ.index")
        faiss.write_index(index_with_ids, index_path)
        print(f"成功保存FAISS索引文件: {index_path}")
        
        # 保存ID映射
        ids_list = ids.tolist()
        ids_path = index_path.replace(".index", "_ids.json")
        with open(ids_path, 'w', encoding='utf-8') as f:
            json.dump(ids_list, f)
        print(f"成功保存ID映射文件: {ids_path}")
    except Exception as e:
        print(f"保存索引文件失败: {str(e)}")

    return index_with_ids,total_cost
