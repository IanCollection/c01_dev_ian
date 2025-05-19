from elasticsearch import Elasticsearch
from llama_index.embeddings.ollama import OllamaEmbedding

import yaml

class HybridSearch:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # 初始化Elasticsearch客户端
        es_config = self.config['elasticsearch']
        es_url = f"http{'s' if es_config.get('use_ssl', False) else ''}://{es_config['host']}:{es_config['port']}"
        self.es = Elasticsearch(
            [es_url],
            basic_auth=(es_config.get('user'), es_config.get('password')),
            verify_certs=es_config.get('use_ssl', False)
        )
        
        # 初始化Ollama embedding模型
        ollama_config = self.config['ollama']
        self.embed_model = OllamaEmbedding(
            model_name=ollama_config['model'],
            base_url=f"{ollama_config['host']}:{ollama_config['port']}"
        )

    def search(self, table_name: str, query: str, vector_field: str = None, size: int = 10):
        index_name = f"{table_name}_index"
        
        if vector_field:
            # 生成查询文本的向量
            query_vector = self.embed_model.get_text_embedding(query)
            
            # 混合搜索查询
            search_query = {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "match": {
                                    "_all": {
                                        "query": query,
                                        "boost": 0.3
                                    }
                                }
                            },
                            {
                                "script_score": {
                                    "query": {"match_all": {}},
                                    "script": {
                                        "source": f"cosineSimilarity(params.query_vector, '{vector_field}_vector') + 1.0",
                                        "params": {
                                            "query_vector": query_vector
                                        }
                                    }
                                }
                            }
                        ]
                    }
                },
                "size": size
            }
        else:
            # 仅文本搜索
            search_query = {
                "query": {
                    "match": {
                        "_all": {
                            "query": query,
                            "fuzziness": "AUTO"
                        }
                    }
                },
                "size": size
            }
        
        try:
            results = self.es.search(index=index_name, body=search_query)
            return results['hits']['hits']
        except Exception as e:
            # print(f"搜索查询: {search_query}")
            raise e 

    def hybrid_search(self, table_name: str, query: str, vector_field: str, size: int = 10, 
                     text_boost: float = 0.3, vector_boost: float = 1.0, min_score: float = 0.0):
        """
        执行可自定义权重的混合检索，结合文本匹配和向量相似度
        
        Args:
            table_name (str): 要搜索的表名
            query (str): 查询文本
            vector_field (str): 用于向量搜索的字段
            size (int): 返回结果数量
            text_boost (float): 文本匹配的权重系数
            vector_boost (float): 向量相似度的权重系数
            min_score (float): 结果最低分数阈值
            
        Returns:
            list: 搜索结果
        """
        index_name = f"{table_name}_index"
        
        # 生成查询文本的向量
        query_vector = self.embed_model.get_text_embedding(query)
        
        # 构建混合搜索查询
        search_query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "_all": {
                                    "query": query,
                                    "boost": text_boost
                                }
                            }
                        },
                        {
                            "script_score": {
                                "query": {"match_all": {}},
                                "script": {
                                    "source": f"cosineSimilarity(params.query_vector, '{vector_field}_vector') * {vector_boost}",
                                    "params": {
                                        "query_vector": query_vector
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "min_score": min_score,
            "size": size
        }
        
        try:
            results = self.es.search(index=index_name, body=search_query)
            return results['hits']['hits']
        except Exception as e:
            print(f"混合检索出错: {str(e)}")
            return [] 