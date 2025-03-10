import json
import jieba
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import defaultdict

class TitleMatcher:
    def __init__(self, json_file):
        # 加载结构化的关注点数据
        with open(json_file, 'r', encoding='utf-8') as f:
            self.structure = json.load(f)
        
        # 构建关键词索引
        self.keyword_index = self._build_keyword_index()
        
        # 初始化 TF-IDF 向量化器
        self.vectorizer = TfidfVectorizer(
            tokenizer=lambda x: jieba.cut(x, cut_all=False),
            analyzer='word'
        )
        
        # 准备训练数据
        self._prepare_vectors()
    
    def _build_keyword_index(self):
        """构建关键词索引，将详情中的关键信息提取出来"""
        index = defaultdict(list)
        for level1, content1 in self.structure.items():
            for level2, content2 in content1.items():
                # 提取详情中的关键词
                details = content2['详情']
                keywords = list(jieba.cut(details))
                
                # 存储关键词与标签的对应关系
                for keyword in keywords:
                    if len(keyword) > 1:  # 忽略单字词
                        index[keyword].append((level1, level2))
        return index
    
    def _prepare_vectors(self):
        """准备向量化的训练数据"""
        self.details_texts = []
        self.labels = []
        
        for level1, content1 in self.structure.items():
            for level2, content2 in content1.items():
                self.details_texts.append(content2['详情'])
                self.labels.append((level1, level2))
        
        # 转换为 TF-IDF 向量
        self.vectors = self.vectorizer.fit_transform(self.details_texts)
    
    def match_title(self, title, top_k=3):
        """匹配标题与最相关的标签"""
        # 对标题进行分词
        title_words = list(jieba.cut(title))
        
        # 方法1：关键词匹配
        keyword_matches = defaultdict(int)
        for word in title_words:
            if word in self.keyword_index:
                for level1, level2 in self.keyword_index[word]:
                    keyword_matches[(level1, level2)] += 1
        
        # 方法2：向量相似度匹配
        title_vector = self.vectorizer.transform([title])
        similarities = np.array((title_vector * self.vectors.T).toarray()[0])
        
        # 综合两种方法的结果
        combined_scores = defaultdict(float)
        
        # 添加关键词匹配分数
        max_keyword_score = max(keyword_matches.values()) if keyword_matches else 1
        for labels, score in keyword_matches.items():
            combined_scores[labels] += score / max_keyword_score * 0.5
        
        # 添加向量相似度分数
        for i, score in enumerate(similarities):
            labels = self.labels[i]
            combined_scores[labels] += score * 0.5
        
        # 获取最佳匹配
        best_matches = sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        
        return [
            {
                "一级关注": level1,
                "二级关注点": level2,
                "匹配度": round(score * 100, 2),
                "可用工具": self.structure[level1][level2]["工具"]
            }
            for (level1, level2), score in best_matches
        ]

def main():
    # 使用示例
    matcher = TitleMatcher('optimized_output.json')
    
    while True:
        title = input("\n请输入研报标题（输入 'q' 退出）：")
        if title.lower() == 'q':
            break
        
        matches = matcher.match_title(title)
        print("\n匹配结果：")
        for i, match in enumerate(matches, 1):
            print(f"\n{i}. 推荐标签：")
            print(f"   一级关注：{match['一级关注']}")
            print(f"   二级关注点：{match['二级关注点']}")
            print(f"   匹配度：{match['匹配度']}%")
            print("   可用工具：")
            for tool_type, tools in match['可用工具'].items():
                if tools:  # 只显示非空的工具列表
                    print(f"     {tool_type}：{', '.join(tools)}")

if __name__ == "__main__":
    main() 