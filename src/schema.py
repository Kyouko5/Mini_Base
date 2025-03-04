'''
Author: Kyouko
Date: 2025-02-27 10:47:36
LastEditTime: 2025-03-04 09:01:43
Description: to process the schema data, which is stored in all.sch
             all.sch are divied into three parts,namely metaHead, tableNameHead and body
FilePath: /Database/Mini_Base/src/schema.py
'''

import ctypes
import struct
import head

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

    def __init__(self):
        # 1.打开/创建all.sch文件
        # 2.读取文件内容到内存
        # 3.初始化头部信息
        # 4.构建内存中的表结构

        self.filename = 'all.sch'
        self.table_dict = {}    # 存储表信息的字典结构

        try:
            # 1. 打开文件，如果不存在就创建
            self.file = open(self.filename, 'rb+')

            # 2. 读取metaHead信息（12B）
            meta_buf = self.file.read(META_HEAD_SIZE)
            if len(meta_buf) == META_HEAD_SIZE:
                # 解析metaHead
                self.is_stored,self.table_num,self.body_offset = struct.unpack('!?ii', meta_buf, 0)
                if self.is_stored:
                    print ("there is something  in the all.sch")
                    self._load_tables()
            else:
                self.init_meta_()

        except FileNotFoundError:
            # 如果文件不存在，创建新文件
            print ("there is nothing in the all.sch")
            self.file = open(self.filename, 'wb+')
            self.init_meta_()


    def init_meta_(self):
        """初始化metaHead信息"""
        print('init meta')
        self.is_stored = False
        self.table_num = 0
        self.body_offset = BODY_BEGIN_INDEX

        # 写入初始化的metaHead信息
        self.file.seek(0)
        meta_buf = struct.pack('!?ii', self.is_stored, self.table_num, self.body_offset)
        self.file.write(meta_buf)
        self.file.flush()

        # 初始化Header
        nameList = []
        fieldDict = {}
        self.head = head.Header(nameList, fieldDict, False, 0, self.body_offset)

        print ('metaHead of schema has been written to all.sch and the Header ojbect created')

    def _load_tables(self):
        """从文件中加载信息到Header"""
        # 1. 读取metaHead信息（12B）
        print ("tableNum in schema file is ", self.table_num)
        print ("isStored in schema file is ", self.is_stored)
        print ("offset of body in schema  file is ", self.body_offset)

        self.body_begin = self.body_offset
        nameList = []
        fieldsDict = {}

        self.file.seek(0)
        buf = self.file.read(META_HEAD_SIZE + TABLE_NAME_HEAD_SIZE + MAX_FIELD_SECTION_SIZE)
        # 2. 读取每个表的信息
        for i in range(self.table_num):
            # 2.1 读取表名信息
            tempName = struct.unpack('!10s', buf, 
                                     META_HEAD_SIZE + i * TABLE_NAME_ENTRY_LEN)
            print ("tablename is ", tempName)

            # 2.2 读取字段数量
            tempNum, = struct.unpack_from('!i', buf, 
                                          META_HEAD_SIZE + i * TABLE_NAME_ENTRY_LEN + 10)
            print ('number of fields of table ', tempName, ' is ', tempNum)

            # 2.3 读取该表在body中的偏移量
            tempPos, = struct.unpack_from('!i', buf,
                                          META_HEAD_SIZE + i * TABLE_NAME_ENTRY_LEN + 10 + struct.calcsize('i'))
            print ("tempPos in body is ", tempPos)

            tempnameitem = (tempName, tempNum, tempPos)
            nameList.append(tempnameitem)

            # fieldDict = (tablename, fieldList)
            # fieldList = (fieldname,fieldtype,fieldlength)
            # 获取该表的字段信息
            if tempNum > 0:
                fieldsList = []
                for j in range(tempNum):
                    # 获取该表的字段信息（字段名，字段类型，字段长度）
                    tempFieldName,tempFieldType,tempFieldLength = struct.unpack_from('!10sii',
                                                                                             buf, tempPos + j * MAX_FIELD_LEN)
                    print ("field name is ", tempFieldName)
                    print ("field type is ", tempFieldType)
                    print ("field length is ", tempFieldLength)

                    tempfielditem = (tempFieldName, tempFieldType, tempFieldLength)
                    fieldsList.append(tempfielditem)
                fieldsDict[tempName] = fieldsList
            
            self.head = head.Header(nameList,fieldsList, True, self.table_num, self.body_offset)




    def appendTable(self,tableName,fieldList):
        # 1. 验证输入
        # 2. 写入字段信息到body
        # 3. 写入表名信息到tableNameHead
        # 4. 更新内存中的header结构
        print('append table')
        
    def delete_table_schema(self, table_name):
        # 1. 查找表
        # 2. 从内存结构中删除
        # 3. 重组剩余表的偏移量
        # 4. 更新文件
        print('delete table schema')


schematest = Schema()
head.Header.showTables(schematest.head)