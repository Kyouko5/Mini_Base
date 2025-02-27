'''
Author: Kyouko
Date: 2025-02-27 10:47:36
LastEditTime: 2025-02-27 11:13:06
Description: to process the schema data, which is stored in all.sch
             all.sch are divied into three parts,namely metaHead, tableNameHead and body
FilePath: /Database/Mini_Base/src/schema.py
'''

import ctypes
import struct

'''
文件存储结构
+------------+----------------+-----------+
| metaHead   | tableNameHead | body      |
| (12 bytes) | (表名部分)     | (字段信息) |
+------------+----------------+-----------+

struct metaHead {
    bool isStored;    // 是否有数据
    int tableNum;     // 表的数量
    int offset;       // body部分的起始位置
};

struct tableNameEntry {
    char tablename[10];    // 表名
    int numofFields;       // 字段数量
    int beginOffsetInBody; // body中的偏移量
};

struct fieldEntry {
    char field_name[10];   // 字段名
    int field_type;        // 字段类型(0:str,1:varstr,2:int,3:bool)
    int field_length;      // 字段长度
};
'''

# 1. Meta Head结构 (12字节)
META_HEAD_SIZE=12       #the First part in the schema file

# 2. Table Name Head结构常量
MAX_TABLE_NAME_LEN=10                                       # the maximum length of table name
MAX_TABLE_NUM=100                                           # the maximum number of tables in the all.sch
TABLE_NAME_ENTRY_LEN=MAX_TABLE_NAME_LEN+4+4                 # the length of one table name entry
TABLE_NAME_HEAD_SIZE=MAX_TABLE_NUM*TABLE_NAME_ENTRY_LEN     # the SECOND part in the schema file

# 3. Body部分常量
MAX_FIELD_NAME_LEN=10   # the maximum length of field name
MAX_FIELD_LEN=10+4+4    # the maximum length of one field
MAX_NUM_OF_FIELD_PER_TABLE=5    # the maximum number of fields in one table
FIELD_ENTRY_SIZE_PER_TABLE=MAX_FIELD_LEN*MAX_NUM_OF_FIELD_PER_TABLE
MAX_FIELD_SECTION_SIZE=FIELD_ENTRY_SIZE_PER_TABLE*MAX_TABLE_NUM     #the THIRD part in the schema file

# 4. 文件布局相关
BODY_BEGIN_INDEX=META_HEAD_SIZE+TABLE_NAME_HEAD_SIZE            # Intitially, where the field name, type and length are stored


# 5. 辅助函数
def fill_table_name(table_name, str):
     """填充表名至固定长度"""
     if len(table_name.strip()) < MAX_FIELD_NAME_LEN:
          table_name = (' ' * (MAX_FIELD_NAME_LEN-len(table_name.strip()))).encode('utf-8') + table_name.strip()
          return table_name
     
     
class Schema():
    '''
    description: 
    '''
    def __init__(self):
        # 1.打开/创建all.sch文件
        # 2.读取文件内容到内存
        # 3.初始化头部信息
        # 4.构建内存中的表结构


    def appendTable(self,tableName,fieldList):
        # 1. 验证输入
        # 2. 写入字段信息到body
        # 3. 写入表名信息到tableNameHead
        # 4. 更新内存中的header结构
        
    def delete_table_schema(self, table_name):
        # 1. 查找表
        # 2. 从内存结构中删除
        # 3. 重组剩余表的偏移量
        # 4. 更新文件