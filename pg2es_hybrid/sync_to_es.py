import yaml
from typing import List, Dict
from sqlalchemy import create_engine, MetaData, Table
from elasticsearch import Elasticsearch
from llama_index.embeddings.ollama import OllamaEmbedding

import pandas as pd
import numpy as np

class PostgresToElasticsearch:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # 初始化数据库连接
        db_config = self.config['database']
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(self.db_url)
        self.metadata = MetaData()
        
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

    def create_index_mapping(self, table_name: str, columns: List[Dict], vector_fields: List[str]) -> dict:
        properties = {}
        
        for col in columns:
            col_name = col['name']
            col_type = col['type']
            
            # 映射PostgreSQL数据类型到Elasticsearch数据类型
            if 'varchar' in col_type or 'text' in col_type:
                properties[col_name] = {"type": "text"}
            elif 'int' in col_type:
                properties[col_name] = {"type": "integer"}
            elif 'float' in col_type or 'double' in col_type:
                properties[col_name] = {"type": "float"}
            elif 'date' in col_type or 'timestamp' in col_type:
                properties[col_name] = {"type": "date"}
            elif 'bool' in col_type:
                properties[col_name] = {"type": "boolean"}
            
            # 为向量字段添加dense_vector映射
            if col_name in vector_fields:
                properties[f"{col_name}_vector"] = {
                    "type": "dense_vector",
                    "dims": 1024,  # bge-m3:567m模型的维度
                    "index": True,
                    "similarity": "cosine"
                }
        
        return {
            "mappings": {
                "properties": properties
            }
        }

    def sync_table(self, table_config: Dict):
        table_name = table_config['name']
        vector_fields = [f['name'] for f in table_config['vector_fields']]
        
        # 获取表结构
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        columns = [{"name": col.name, "type": str(col.type)} for col in table.columns]
        print(columns)
        
        # 创建或更新ES索引
        index_name = f"{table_name}_index"
        mapping = self.create_index_mapping(table_name, columns, vector_fields)
        
        if self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name)
        self.es.indices.create(index=index_name, body=mapping)
        
        # 读取数据并生成向量
        df = pd.read_sql_table(table_name, self.engine)
        
        # 批量处理数据
        batch_size = 100
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            bulk_data = []
            
            for _, row in batch.iterrows():
                doc = row.to_dict()
                
                # 为指定字段生成向量
                for field in vector_fields:
                    if pd.notna(doc.get(field)):
                        vector = self.embed_model.get_text_embedding(str(doc[field]))
                        doc[f"{field}_vector"] = vector
                
                bulk_data.append({
                    "index": {
                        "_index": index_name,
                        "_id": str(row.get('id', i))
                    }
                })
                bulk_data.append(doc)
            
            if bulk_data:
                self.es.bulk(body=bulk_data)
                print(f"Processed {i+len(batch)}/{len(df)} records in {table_name}")

    def sync_all(self):
        for table_config in self.config['tables']:
            print(f"Syncing table: {table_config['name']}")
            self.sync_table(table_config)
            print(f"Finished syncing table: {table_config['name']}")

if __name__ == "__main__":
    syncer = PostgresToElasticsearch('config.yaml')
    syncer.sync_all() 