'''
Author: Kyouko
Date: 2025-02-27 14:56:22
LastEditTime: 2025-03-04 09:00:37
Description: 1. 保存表的元数据信息
             2. 所有数据库操作都通过 Schema 类进行
             3. Schema 类负责管理唯一的 Header 实例
FilePath: /Database/Mini_Base/src/head.py
'''

import struct

class Header(object):
    # TODO: 保存从文件中读取的数据信息
    # Schema.py 中调用 Header 的构造函数：
    # self.head = head.Header(nameList,fieldsList, True, self.table_num, self.body_offset)
    #
    #  namelist  : 表名列表，每个元素是一个三元组 (table_name, num_of_fields, offset_in_body)
    #  fieldDict : 所有表的字段字典，每个元素是 (tablename, fieldList)，
    #                其中 fieldList 是字段列表，每个字段是一个元组 (fieldname, fieldtype, fieldlength)
    #  inLen     : 表的数量
    #  off       : 在文件中 body 空间的开始位置
    def __init__(self,nameList,fieldDict,inistored, inLen, off):
        print ('__init__ of Header')
        self.isStored = inistored
        self.numsOfTable = inLen
        self.offsetOfBody = off
        self.tableNames = nameList
        self.tableFields = fieldDict

        print ("isStore is ",self.isStored," tableNum is ",self.numsOfTable," offset is ",self.offsetOfBody)


    def __del__(self):
        print ('del Header')


    '''
    description: 显示数据库中所有表的元数据信息
    '''
    def showTables(self):
        if self.numsOfTable > 0:
            print("There are", self.numsOfTable, "tables in the database.")
            for i in range(len(self.tableNames)):
                print(self.tableNames[i])
                print(self.tableFields[i])
