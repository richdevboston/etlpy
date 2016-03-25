# encoding: UTF-8
import etl;

import csv;
import sys

etl.LoadProject('project.xml');

# tool=etl.modules['数据清洗ETL-链家二手房'];

tool = etl.modules['大众点评门店'];
tool.AllETLTools[0].arglists=['1']  #修改城市，1为上海，2为北京，参考大众点评的网页定义
tool.AllETLTools[-1].NewTableName= 'D:\大众点评.txt'  #修改导出的文件
#tool.mThreadExecute();  #parallel

#the following is serial
datas = tool.QueryDatas(etlCount=100, execute=True)
i = 0;
for r in datas:
    try:
        print(r)
    except IOError:
        pass;


