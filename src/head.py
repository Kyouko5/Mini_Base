'''
Author: Kyouko
Date: 2025-02-27 14:56:22
LastEditTime: 2025-02-27 15:02:57
Description: 1. 保存表的元数据信息
             2. 所有数据库操作都通过 Schema 类进行
             3. Schema 类负责管理唯一的 Header 实例
FilePath: /Database/Mini_Base/src/head.py
'''

import struct

class Header(object):
    