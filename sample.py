import etl;

import extends
import time;

# import  json
#
# proj=etl.Project_LoadXml('all.xml');
#
# dic={};
# for t in proj.modules['数据清洗'].AllETLTools:
#     mytype=t.Type;
#     del t.__dict__['Type']
#     dic[mytype]=t;
#
# open('tools.json','w').write(json.dumps( dic, ensure_ascii=False, indent=2,default=extends.convert_to_builtin_type));
# json.dump(json.load(open('fuck.json')),open('desc.json','w'),ensure_ascii=False,indent=2)
# exit()

proj=etl.Project_LoadXml('../Hawk-projects/图片抓取/昵图网.xml');
lagou=proj.modules['昵图网'];
lagou.AllETLTools[-4].Format=True;
#print (etl.Project_DumpJson(proj));
print (etl.task_DumpLinq(lagou.AllETLTools))


for r in lagou.QueryDatas(etlCount=19,execute=False):
    print(r)
#     print(r)
exit();
from  distributed import  *
master =Master(proj,'风云时讯列表');
master.start();


#tool.mThreadExecute(canexecute=False);



#the following is serial
# datas = tool.QueryDatas(etlCount=100, execute=False)
# for r in datas:
#     print(r)
#
# tool = etl.modules['风云时讯列表'];
# tool.AllETLTools[-1].TableName='baidu_hotnews_'+ currtime;
# #the following is serial
# datas = tool.QueryDatas(etlCount=190, execute=True)
# for r in datas:
#     print(r)