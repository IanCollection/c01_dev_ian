import os
import sys

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)

import datetime
from decimal import Decimal

import pandas as pd

def analyze_industry_trends(data_list):
    # 将数据转换为DataFrame
    df = pd.DataFrame(data_list)
    df['date'] = pd.to_datetime(df['date'])

    # 将Decimal类型转换为float类型，仅转换存在的列
    score_columns = ['supply_demand_score', 'capital_market_score',
                    'policy_direction_score', 'horizontal_integration_score']
    for col in score_columns:
        if col in df.columns:  # 检查列是否存在
            df[col] = df[col].astype(float)

    # 按cics_id分组进行分析，只取前5个
    results = {}
    for cics_id in df['cics_id'].unique()[:5]:
        cics_data = df[df['cics_id'] == cics_id].sort_values('date')
        cics_name = cics_data['cics_name'].iloc[0]  # 获取行业名称

        dimensions = {
            'supply_demand': '供需',
            'capital_market': '资本市场',
            'policy_direction': '政策方向'
        }

        cics_analysis = {
            'cics_name': cics_name,  # 添加行业名称
            'time_range': f"{cics_data['date'].min().strftime('%Y-%m')} 至 {cics_data['date'].max().strftime('%Y-%m')}",
            'data_points': len(cics_data),
            'dimensions': {}
        }

        for dim, cn_name in dimensions.items():
            score_col = f'{dim}_score'
            grade_col = f'{dim}_grade'

            # 检查score列是否存在
            if score_col not in cics_data.columns:
                dim_stats = {'数据状态': '无相关数据'}
                cics_analysis['dimensions'][cn_name] = dim_stats
                continue

            valid_scores = cics_data[score_col].dropna()

            if len(valid_scores) >= 2:
                # 基础统计
                mean_score = round(float(valid_scores.mean()), 2)
                max_score = round(float(valid_scores.max()), 2)
                min_score = round(float(valid_scores.min()), 2)
                std_score = round(float(valid_scores.std()), 2)

                # 趋势分析
                first_score = float(valid_scores.iloc[0])
                last_score = float(valid_scores.iloc[-1])
                trend = '上升' if last_score > first_score else '下降'
                change_rate = round((last_score - first_score) / first_score * 100, 2)

                # 波动分析
                volatility = round(std_score / mean_score * 100, 2)  # 变异系数
                max_drawdown = round((min_score - max_score) / max_score * 100, 2)

                # 阶段分析
                grades = cics_data[grade_col].value_counts()
                main_grade = grades.index[0]
                grade_distribution = grades.to_dict()

                # 计算环比变化
                mom_changes = valid_scores.pct_change().dropna() * 100
                avg_mom = round(float(mom_changes.mean()), 2)

                # 添加季度分析
                cics_data['quarter'] = cics_data['date'].dt.quarter
                quarterly_avg = cics_data.groupby('quarter')[score_col].mean().round(2)
                best_quarter = quarterly_avg.idxmax()
                worst_quarter = quarterly_avg.idxmin()

                dim_stats = {
                    '基础统计': {
                        '平均分': mean_score,
                        '最高分': max_score,
                        '最低分': min_score,
                        '标准差': std_score
                    },
                    '趋势分析': {
                        '整体趋势': trend,
                        '变化幅度': f"{change_rate}%",
                        '期初值': round(first_score, 2),
                        '期末值': round(last_score, 2)
                    },
                    '波动分析': {
                        '波动系数': f"{volatility}%",
                        '最大回撤': f"{max_drawdown}%",
                        '平均环比变化': f"{avg_mom}%"
                    },
                    '季度表现': {
                        '最佳季度': f"Q{best_quarter} ({quarterly_avg[best_quarter]}分)",
                        '最差季度': f"Q{worst_quarter} ({quarterly_avg[worst_quarter]}分)",
                        '季度均值': quarterly_avg.to_dict()
                    },
                    '景气等级分布': grade_distribution,
                    '主要景气等级': main_grade
                }
            else:
                dim_stats = {'数据状态': '数据不足'}

            cics_analysis['dimensions'][cn_name] = dim_stats

        results[f'CICS_{cics_id}'] = cics_analysis

    return results

def get_analysis_summary(results):
    summary_data = {}

    for cics_id, analysis in results.items():
        report = {
            'cics_id': cics_id,
            'cics_name': analysis['cics_name'],
            'time_range': analysis['time_range'],
            'data_points': analysis['data_points'],
            'dimensions': {}
        }

        for dim_name, stats in analysis['dimensions'].items():
            dim_info = {}

            if '数据状态' in stats:
                dim_info['status'] = stats['数据状态']
            else:
                for category, details in stats.items():
                    if isinstance(details, dict):
                        dim_info[category] = details
                    else:
                        dim_info[category] = details

            report['dimensions'][dim_name] = dim_info

        summary_data[cics_id] = report
    return summary_data

def analyze_industry_trends_by_columns(data_list):
    # 将数据转换为DataFrame
    df = pd.DataFrame(data_list)
    df['date'] = pd.to_datetime(df['date'])

    # 将Decimal类型转换为float类型
    score_columns = ['supply_demand_score', 'capital_market_score',
                     'policy_direction_score']
    for col in score_columns:
        df[col] = df[col].astype(float)

    # 按cics_id分组进行分析，只取前5个
    results = {}
    for cics_id in df['cics_id'].unique()[:5]:
        cics_data = df[df['cics_id'] == cics_id].sort_values('date')
        cics_name = cics_data['cics_name'].iloc[0]  # 获取行业名称

        dimensions = {
            'supply_demand': '供需',
            'capital_market': '资本市场',
            'policy_direction': '政策方向',
        }

        cics_analysis = {
            'cics_name': cics_name,  # 添加行业名称
            'time_range': f"{cics_data['date'].min().strftime('%Y-%m')} 至 {cics_data['date'].max().strftime('%Y-%m')}",
            'data_points': len(cics_data),
            'dimensions': {}
        }

        # ... 其余分析代码保持不变 ...
        # 以下是原有的分析逻辑
        for dim, cn_name in dimensions.items():
            score_col = f'{dim}_score'
            grade_col = f'{dim}_grade'

            valid_scores = cics_data[score_col].dropna()

            if len(valid_scores) >= 2:
                # 基础统计
                mean_score = round(float(valid_scores.mean()), 2)
                max_score = round(float(valid_scores.max()), 2)
                min_score = round(float(valid_scores.min()), 2)
                std_score = round(float(valid_scores.std()), 2)

                # 趋势分析
                first_score = float(valid_scores.iloc[0])
                last_score = float(valid_scores.iloc[-1])
                trend = '上升' if last_score > first_score else '下降'
                change_rate = round((last_score - first_score) / first_score * 100, 2)

                # 波动分析
                volatility = round(std_score / mean_score * 100, 2)  # 变异系数
                max_drawdown = round((min_score - max_score) / max_score * 100, 2)

                # 阶段分析
                grades = cics_data[grade_col].value_counts()
                main_grade = grades.index[0]
                grade_distribution = grades.to_dict()

                # 计算环比变化
                mom_changes = valid_scores.pct_change().dropna() * 100
                avg_mom = round(float(mom_changes.mean()), 2)

                # 添加季度分析
                cics_data['quarter'] = cics_data['date'].dt.quarter
                quarterly_avg = cics_data.groupby('quarter')[score_col].mean().round(2)
                best_quarter = quarterly_avg.idxmax()
                worst_quarter = quarterly_avg.idxmin()

                dim_stats = {
                    '基础统计': {
                        '平均分': mean_score,
                        '最高分': max_score,
                        '最低分': min_score,
                        '标准差': std_score
                    },
                    '趋势分析': {
                        '整体趋势': trend,
                        '变化幅度': f"{change_rate}%",
                        '期初值': round(first_score, 2),
                        '期末值': round(last_score, 2)
                    },
                    '波动分析': {
                        '波动系数': f"{volatility}%",
                        '最大回撤': f"{max_drawdown}%",
                        '平均环比变化': f"{avg_mom}%"
                    },
                    '季度表现': {
                        '最佳季度': f"Q{best_quarter} ({quarterly_avg[best_quarter]}分)",
                        '最差季度': f"Q{worst_quarter} ({quarterly_avg[worst_quarter]}分)",
                        '季度均值': quarterly_avg.to_dict()
                    },
                    '景气等级分布': grade_distribution,
                    '主要景气等级': main_grade
                }
            else:
                dim_stats = {'数据状态': '数据不足'}

            cics_analysis['dimensions'][cn_name] = dim_stats

        results[f'CICS_{cics_id}'] = cics_analysis

    return results

def analyze_flexible_industry_trends(data_list):
    # 将数据转换为DataFrame
    df = pd.DataFrame(data_list)
    
    # 检查存在哪些分类字段
    categories = []
    if 'profitability_cat' in df.columns:
        categories.append(('profitability_cat', '盈利能力'))
    if 'financial_cat' in df.columns:
        categories.append(('financial_cat', '财务状况'))
    
    # 如果没有发现任何分类字段，返回基本信息
    if not categories:
        return {'error': '未找到可分析的分类字段'}
    
    # 添加日期列（基于年份和季度）
    if 'year' in df.columns and 'quarter' in df.columns:
        # 修复季度日期转换问题
        quarter_map = {'Q1': '01', 'Q2': '04', 'Q3': '07', 'Q4': '10'}
        df['date'] = df.apply(lambda x: f"{x['year']}-{quarter_map.get(x['quarter'], '01')}-01", axis=1)
        df['date'] = pd.to_datetime(df['date'])
    else:
        # 如果没有年份和季度，尝试使用其他可能的日期字段
        if 'date' not in df.columns:
            return {'error': '未找到可用于时间序列分析的日期字段'}
        df['date'] = pd.to_datetime(df['date'])
    
    # 按cics_id分组进行分析
    results = {}
    for cics_id in df['cics_id'].unique():
        cics_data = df[df['cics_id'] == cics_id].sort_values('date')
        cics_name = cics_data['cics_name'].iloc[0] if 'cics_name' in df.columns else f'行业ID {cics_id}'
        
        cics_analysis = {
            'cics_name': cics_name,
            'time_range': f"{cics_data['date'].min().strftime('%Y-%m')} 至 {cics_data['date'].max().strftime('%Y-%m')}",
            'data_points': len(cics_data),
            'categories_analysis': {}
        }
        
        # 对每个分类字段进行分析
        for cat_field, cat_name in categories:
            if cat_field in cics_data.columns:
                # 分类分布
                cat_dist = cics_data[cat_field].value_counts()
                main_category = cat_dist.index[0] if not cat_dist.empty else '无数据'
                
                # 季度分析（如果有季度数据）
                quarterly_data = {}
                if 'quarter' in cics_data.columns:
                    quarterly_data = cics_data.groupby('quarter')[cat_field].apply(list).to_dict()
                
                # 趋势分析
                trend = '稳定'
                if len(cics_data) > 1:
                    first_cat = cics_data[cat_field].iloc[0]
                    last_cat = cics_data[cat_field].iloc[-1]
                    if first_cat != last_cat:
                        # 定义分类等级（可根据实际情况调整）
                        cat_ranks = {'靠前': 3, '良好': 3, '中游': 2, '一般': 2, '靠后': 1, '较差': 1}
                        first_rank = cat_ranks.get(first_cat, 0)
                        last_rank = cat_ranks.get(last_cat, 0)
                        if last_rank > first_rank:
                            trend = '上升'
                        elif last_rank < first_rank:
                            trend = '下降'
                
                analysis_result = {
                    f'主要{cat_name}分类': main_category,
                    '分类分布': cat_dist.to_dict(),
                    '趋势分析': {
                        '整体趋势': trend,
                        '期初值': cics_data[cat_field].iloc[0],
                        '期末值': cics_data[cat_field].iloc[-1]
                    }
                }
                
                if quarterly_data:
                    analysis_result['季度表现'] = quarterly_data
                
                cics_analysis['categories_analysis'][cat_name] = analysis_result
        
        results[f'CICS_{cics_id}'] = cics_analysis
    
    return results

def get_flexible_summary(results):
    summary_data = {}
    
    for cics_id, analysis in results.items():
        if 'error' in analysis:
            return {'error': analysis['error']}
            
        report = {
            'cics_id': cics_id,
            'cics_name': analysis['cics_name'],
            'time_range': analysis['time_range'],
            'data_points': analysis['data_points'],
            'categories': {}
        }
        
        for cat_name, cat_analysis in analysis.get('categories_analysis', {}).items():
            report['categories'][cat_name] = {
                f'主要分类': cat_analysis.get(f'主要{cat_name}分类', '无数据'),
                '趋势': cat_analysis.get('趋势分析', {}).get('整体趋势', '无法确定'),
            }
            
            if '季度表现' in cat_analysis:
                report['categories'][cat_name]['季度表现'] = cat_analysis['季度表现']
        
        summary_data[cics_id] = report
    
    return summary_data


if __name__ == '__main__':
    print(1)