# encoding: UTF-8
import etl;


etl.LoadProject('project.xml');
tool=etl.modules['数据清洗ETL-链家二手房'];
#tool=etl.modules['大众点评门店'];
#etlcount is used for debug
datas = tool.RefreshDatas(etlCount=100)
i = 0;
for r in datas:
    try:
        print(r)
    except:
        pass;
    i += 1;
    if i > 500:
        break;
