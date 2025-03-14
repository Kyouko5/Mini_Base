'''
Author: Kyouko
Date: 2025-03-13 11:42:16
LastEditTime: 2025-03-14 08:13:56
Description: 测试schema的添加表功能: appendTable()
FilePath: /Database/Mini_Base/src/test_schema.py
'''
from schema import Schema

def test_append_table():
    """测试添加表功能"""
    # 1. 创建Schema实例
    schema = Schema()
    
    # 2. 构造测试数据
    # 表名
    table_name = "students"
    
    # 字段列表：(字段名, 字段类型, 字段长度)
    # 字段类型: 0->str, 1->varstr, 2->int, 3->bool
    fields = [
        ("id", 2, 4),           # 整型学号
        ("name", 0, 10),        # 定长字符串姓名
        ("age", 2, 4),          # 整型年龄
        ("active", 3, 1)        # 布尔型状态
    ]
    
    # 3. 添加表
    print("添加表")
    result = schema.appendTable(table_name.encode('utf-8'), fields)
    
    # 4. 验证结果
    if result is False:
        print("添加表失败")
    else:
        print(f"成功添加表 {table_name}")
        # 显示所有表名
        schema.viewTableNames()
        schema.viewtableStructure(table_name.encode('utf-8'))

if __name__ == "__main__":
    test_append_table()