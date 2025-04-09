import pandas as pd
import numpy as np
from datetime import datetime


def analyze_eco_indicators(data_list):
    """
    分析经济指标数据
    :param data_list: 包含指标数据的列表
    :return: 分析结果列表
    """
    try:
        # 将列表数据转换为DataFrame
        df = pd.DataFrame(data_list)

        # 将period_date转换为datetime格式
        df['period_date'] = pd.to_datetime(df['period_date'])
        
        # 确保data_value列为数值类型，将None值转换为NaN
        df['data_value'] = pd.to_numeric(df['data_value'], errors='coerce')

        # 按name_cn分组进行分析
        grouped_data = df.groupby('name_cn')
        analysis_results = []

        for name, group in grouped_data:
            # 按时间排序
            group = group.sort_values('period_date')
            
            # 过滤掉NaN值
            valid_data = group.dropna(subset=['data_value'])
            
            # 如果没有有效数据，则跳过此指标
            if len(valid_data) == 0:
                continue
                
            # 安全获取第一个和最后一个值
            first_value = valid_data['data_value'].iloc[0] if len(valid_data) > 0 else np.nan
            last_value = valid_data['data_value'].iloc[-1] if len(valid_data) > 0 else np.nan
            
            # 计算趋势
            if pd.notna(first_value) and pd.notna(last_value):
                trend = '上升' if last_value > first_value else '下降'
            else:
                trend = '无法确定'
            
            # 安全计算年均增长率
            try:
                growth_rate = float(valid_data['data_value'].pct_change().mean() * 100)
            except:
                growth_rate = np.nan

            # 计算基本统计量
            stats = {
                '指标名称': name,
                '数据时间范围': f"{valid_data['period_date'].min().strftime('%Y-%m-%d')} 至 {valid_data['period_date'].max().strftime('%Y-%m-%d')}",
                '数据点数量': len(valid_data),
                '最新值': float(last_value),
                '平均值': float(valid_data['data_value'].mean()),
                '中位数': float(valid_data['data_value'].median()),
                '最大值': float(valid_data['data_value'].max()),
                '最小值': float(valid_data['data_value'].min()),
                '标准差': float(valid_data['data_value'].std()),
                '变化趋势': trend,
                '年均增长率': growth_rate
            }

            analysis_results.append(stats)

        return analysis_results

    except Exception as e:
        print(f"分析过程中出现错误: {str(e)}")
        return []


def generate_summary_report(analysis_results):
    """
    生成分析报告
    :param analysis_results: 分析结果列表
    :return: 格式化的报告字符串
    """
    if not analysis_results:
        return "暂无经济指标数据分析"
        
    summary_parts = []
    summary_parts.append("# 经济指标数据分析报告")
    summary_parts.append("\n## 总体情况")
    summary_parts.append(f"- 共分析了 {len(analysis_results)} 个不同指标")

    # 按指标类型分组
    for stats in analysis_results:
        summary_parts.append(f"\n### {stats['指标名称']}")
        summary_parts.append(f"- 数据期间：{stats['数据时间范围']}")
        summary_parts.append(f"- 最新数据：{stats['最新值']:.2f}")
        summary_parts.append(f"- 平均值：{stats['平均值']:.2f}")
        summary_parts.append(f"- 变化范围：{stats['最小值']:.2f} 至 {stats['最大值']:.2f}")
        summary_parts.append(f"- 年均增长率：{stats['年均增长率']:.2f}%")
        summary_parts.append(f"- 整体趋势：{stats['变化趋势']}")
        summary_parts.append(f"- 波动情况：标准差 {stats['标准差']:.2f}")

    return "\n".join(summary_parts)


# 使用示例
def process_eco_indicators(eco_indicators):
    """
    处理经济指标数据的入口函数
    :param eco_indicators: 原始数据列表
    :return: 分析结果和总结报告
    """
    try:
        results = analyze_eco_indicators(eco_indicators)
        if results:
            summary = generate_summary_report(results)
            return results, summary
        else:
            return None, "数据分析失败"
    except Exception as e:
        print(f"处理数据时出错: {str(e)}")
        return None, "数据处理出错"

# 使用方法：
# results, summary = process_eco_indicators(eco_indicators)
# print(summary)