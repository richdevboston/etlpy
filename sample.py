import etl;

import extends
import time;
path='/home/desert.zym/dev'

proj=etl.Project_LoadXml(path+'/Hawk-Projects/图片抓取/昵图网.xml');
lagou=proj.modules['昵图网'];
tools= lagou.AllETLTools;
tools[-12].Format="/cloud/usr/desert.zym/picture/昵图网/{1}/{0}.jpg";
tools[-1].Enabled=False;
tools[-9].Enabled=False;
#for r in lagou.QueryDatas(etlCount=19,execute=False):
#    print(r)
#     print(r)
from  distributed import  *
master =Master(proj,"昵图网");
master.start();


