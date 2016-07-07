import etl;

import extends
import time;
path='/home/desert.zym/dev'


proj=etl.Project_LoadXml('../Hawk-Projects/图片抓取/蚂蜂窝.xml');
lagou=proj.modules['马蜂窝相册'];
tools= lagou.AllETLTools;
tools[-8].Format="picture/蚂蜂窝/{1}/{0}.jpg";

tools[17].CrawlerSelector='瀑布流列表2'
#tools[-1].Enabled=False;
#tools[-9].Enabled=False;
for r in lagou.QueryDatas(etlCount=30,execute=True):
    print(r)
exit();
from  distributed import  *
master =Master(proj,"马蜂窝相册");
master.start();


