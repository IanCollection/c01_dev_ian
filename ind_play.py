import psycopg2
import pymysql

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
        query = "select * from unified_eco_data_view limit 10"
        
        # 执行查询
        cursor.execute(query)
        results = cursor.fetchall()
        
        # 获取列名
        colnames = [desc[0] for desc in cursor.description]
        
        # 将查询结果转换为字典列表
        if results:
            data = [dict(zip(colnames, row)) for row in results]
            print(f"查询到 {len(data)} 条数据")  # 打印数据长度
            return data
        else:
            print("未查询到任何数据")
            return []
    except Exception as e:
        print(f"查询时发生错误: {str(e)}")
        return None
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

if __name__ == "__main__":
    data = get_ic_current_rating()
    print(data)
    if data is not None:
        print(f"获取到 {len(data)} 条数据")  # 打印最终数据长度
