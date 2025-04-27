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