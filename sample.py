import etl;

import extends
import time;
path='/home/desert.zym/dev'


proj=etl.Project_LoadXml('../Hawk-Projects/新浪新闻/微信头条.xml');
lagou=proj.modules['数据清洗'];
tools= lagou.AllETLTools;
#tools[-8].Format="picture/蚂蜂窝/{1}/{0}.jpg";

#tools[17].CrawlerSelector='瀑布流列表2'
#tools[-1].Enabled=False;
#tools[-9].Enabled=False;
for r in lagou.QueryDatas(etlCount=30,execute=False):
    print(r)
exit();
from  distributed import  *
master =Master(proj,"马蜂窝相册");
master.start();


