# encoding: UTF-8
from etl import ETLTool

tool = ETLTool();
tool.LoadProject('project.xml', '数据清洗ETL-链家二手房');
datas = tool.RefreshDatas();
i = 0;
for r in datas:
    try:
        print(r)
    except:
        pass;
    i += 1;
    if i > 200:
        break;
