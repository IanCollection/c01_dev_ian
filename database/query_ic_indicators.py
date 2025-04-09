import os
import sys
import datetime
from decimal import Decimal

import psycopg2

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # 假设当前文件在项目根目录的子目录中
sys.path.append(project_root)

import logging
import pymysql
from typing import Union, List

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_cics_id_by_name(cics_name: Union[str, List[str]]) -> Union[str, List[str]]:
    """
    根据CICS名称查询CICS ID
    
    Args:
        cics_name: 要查询的CICS名称,可以是单个名称或名称列表
    
    Returns:
        Union[str, List[str]]: 单个CICS ID或CICS ID列表
    """
    try:
        # 检查输入是否为空
        if not cics_name:
            logger.warning("输入的CICS名称为空")
            return []
            
        # 连接数据库
        # connection = pymysql.connect(
        #     host="120.24.20.49",
        #     port=33062,
        #     database="irsip_db",
        #     user="irsip_db",
        #     password="e7J5yzdNcVAgqBz6"
        # )
        # cursor = connection.cursor()
        #
        connection = psycopg2.connect(
            host="pgm-uf61ya3k69j587m2.pg.rds.aliyuncs.com",
            port=5432,
            database="deloitte_data",
            user="deloitte_data",
            password="R9henc1VDdtuUxBG"
        )
        cursor = connection.cursor()
        # 根据输入类型构建查询条件
        if isinstance(cics_name, str):
            cics_name = [cics_name]
        
        # 过滤空值
        cics_name = [name for name in cics_name if name]
        
        # 如果过滤后列表为空，返回空列表
        if not cics_name:
            logger.warning("过滤空值后的CICS名称列表为空")
            return []
            
        # 构建IN查询的参数占位符
        placeholders = ','.join(['%s'] * len(cics_name))
        
        # 构建查询语句 - 确保在SQL语句中处理placeholders为空的情况
        if placeholders:
            query = f"SELECT id FROM ic_cics WHERE name IN ({placeholders})"
            # 执行查询
            cursor.execute(query, cics_name)
            results = cursor.fetchall()
            
            # 返回结果
            if results:
                if len(results) == 1:
                    return results[0][0]
                return [result[0] for result in results]
            else:
                logger.warning(f"未找到CICS名称 {cics_name} 的ID")
                return []
        else:
            logger.warning("没有有效的CICS名称用于查询")
            return []
            
    except Exception as e:
        logger.error(f"查询CICS ID时发生错误: {str(e)}")
        return None  # 修改返回值为None，以便调用方可以区分空列表和错误情况
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()
    
def query_ic_trend_score(cics_ids, year: Union[str, List[str]]) -> List[dict]:
    """
    根据CICS ID查询ic_trend_score指标数据
    
    Args:
        cics_ids: 单个CICS ID或CICS ID列表
        year: 查询年份
        
    Returns:
        List[dict]: 包含指标数据的字典列表
    """
    try:
        # 检查输入是否为空
        if cics_ids is None:
            logger.warning("输入的CICS ID为None")
            return []
            
        if not cics_ids:
            logger.warning("输入的CICS ID为空")
            return []
            
        # 确保cics_ids是列表类型
        if not isinstance(cics_ids, (list, tuple)):
            cics_ids = [cics_ids]
            
        # 过滤空值和非法值
        cics_ids = [str(cid) for cid in cics_ids if cid is not None]
        
        # 如果过滤后列表为空，返回空列表
        if not cics_ids:
            logger.warning("过滤空值后的CICS ID列表为空")
            return []
            
        # 连接数据库
        connection = psycopg2.connect(
            host="pgm-uf61ya3k69j587m2.pg.rds.aliyuncs.com",
            port=5432,
            database="deloitte_data",
            user="deloitte_data",
            password="R9henc1VDdtuUxBG"
        )
        cursor = connection.cursor()
            
        # 构建IN查询的参数占位符
        placeholders = ','.join(['%s'] * len(cics_ids))
        
        # 构建查询语句
        query = f"""
            SELECT ts.*, c.name AS cics_name 
            FROM ic_trend_scores ts
            JOIN ic_cics c ON ts.cics_id = c.id
            WHERE ts.cics_id IN ({placeholders}) 
        """
        
        # 执行查询
        cursor.execute(query, cics_ids)
        results = cursor.fetchall()
        
        if not results:
            logger.warning(f"未找到CICS ID {cics_ids} 的指标数据")
            return []
            
        # 获取列名
        column_names = [desc[0] for desc in cursor.description]
        
        # 构建返回结果
        indicators_data = []
        for row in results:
            indicator = dict(zip(column_names, row))
            
            # 处理datetime和decimal类型
            for key, value in indicator.items():
                if isinstance(value, datetime.date):
                    indicator[key] = value.strftime('%Y-%m-%d')
                elif isinstance(value, datetime.datetime):
                    indicator[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(value, Decimal):
                    indicator[key] = float(value)
                    
            indicators_data.append(indicator)

        return indicators_data
        
    except Exception as e:
        logger.error(f"查询ic_trend_score指标数据时发生错误: {str(e)}")
        return []
        
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def query_ic_current_rating(cics_ids, year):
    """
    根据CICS ID和年份查询ic_current_rating表中的指标数据
    
    Args:
        cics_ids: 单个CICS ID或CICS ID列表
        year (int): 查询年份
        
    Returns:
        List[dict]: 包含指标数据的字典列表
    """
    try:
        # 检查输入是否为空或None
        if cics_ids is None:
            logger.warning("输入的CICS ID为None")
            return []
            
        # 确保cics_ids是列表类型
        if not isinstance(cics_ids, (list, tuple)):
            cics_ids = [cics_ids]
            
        # 过滤空值
        cics_ids = [str(cid) for cid in cics_ids if cid is not None]
        
        # 如果过滤后列表为空，返回空列表
        if not cics_ids:
            logger.warning("过滤空值后的CICS ID列表为空")
            return []
            
        # 连接数据库
        connection = psycopg2.connect(
            host="pgm-uf61ya3k69j587m2.pg.rds.aliyuncs.com",
            port=5432,
            database="deloitte_data",
            user="deloitte_data",
            password="R9henc1VDdtuUxBG"
        )
        cursor = connection.cursor()
            
        # 构建IN查询的参数占位符
        placeholders = ','.join(['%s'] * len(cics_ids))
        
        # 构建查询语句
        query = f"""
            SELECT 
                icr.id, 
                icr.cics_id, 
                icr.profitability_cat, 
                icr.financial_cat, 
                icr.year, 
                icr.quarter,
                icc.name AS cics_name
            FROM ic_current_rating icr
            JOIN ic_cics icc ON icr.cics_id = icc.id
            WHERE icr.cics_id IN ({placeholders}) AND icr.year = {year}
            LIMIT 10
        """
        
        # 执行查询
        cursor.execute(query, cics_ids)
        results = cursor.fetchall()
        
        if not results:
            logger.warning(f"未找到CICS ID {cics_ids} 的指标数据")
            return []
            
        # 获取列名
        column_names = [desc[0] for desc in cursor.description]
        
        # 构建返回结果
        indicators_data = []
        for row in results:
            indicator = dict(zip(column_names, row))
            
            # 处理datetime和decimal类型
            for key, value in indicator.items():
                if isinstance(value, datetime.date):
                    indicator[key] = value.strftime('%Y-%m-%d')
                elif isinstance(value, datetime.datetime):
                    indicator[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(value, Decimal):
                    indicator[key] = float(value)
                    
            indicators_data.append(indicator)
            
        return indicators_data
        
    except Exception as e:
        logger.error(f"查询ic_current_rating指标数据时发生错误: {str(e)}")
        return []
        
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def get_ic_current_rating():
    """
    查询ic_current_rating表的前10行数据

    Returns:
        List[dict]: 包含前10行数据的字典列表
    """
    try:
        # 连接数据库
        connection = psycopg2.connect(
            host="pgm-uf61ya3k69j587m2.pg.rds.aliyuncs.com",
            port=5432,
            database="deloitte_data",
            user="deloitte_data",
            password="R9henc1VDdtuUxBG"
        )
        cursor = connection.cursor()

        # 构建查询语句
        query = "SELECT * FROM ic_trend_scores limit 1"

        # 执行查询
        cursor.execute(query)
        results = cursor.fetchall()
        print(results)

        # 将查询结果转换为字典列表
        if results:
            data = [dict(row) for row in results]
            return data
        else:
            return []
    except Exception as e:
        return None
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()



if __name__ == "__main__":
    # 测试查询ic_trend_score指标数据
    year = 2024

    cics_name = ['土地与工程管理服务', '工程规划']
    cics_ids = get_cics_id_by_name(cics_name)
    # print(cics_ids)
    indicators_data_ic_trend_score = query_ic_trend_score(cics_ids, year)
    print(indicators_data_ic_trend_score)

    # indicators_data_ic_current_rating = query_ic_current_rating(cics_ids, year)
    # print(indicators_data_ic_current_rating)