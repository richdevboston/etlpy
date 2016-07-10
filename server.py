import etl;

import extends
import time;
import  sys
if __name__ == '__main__':
    projfile='../Hawk-Projects/新闻抓取/微信头条.xml';
    name='单公共号文章列表'
    argv=sys.argv;
    mode='keys'
    count=116;
    count1= 0;
    if len(argv)>1:
        projfile=argv[1];
    if len(argv)>2:
        name=argv[2];
    if len(argv)>3:
        mode= argv[3];
    if len(argv) > 4:
        count = int(argv[4])
    if len(argv)>5:
        count1= int(argv[5])
    try:
        proj=etl.Project_LoadXml(projfile);
    except Exception as e:
        print('load project failed:'+ str(e));
        exit();
    if name not in proj.modules:
        print('task name %s not in project'%(name));
        exit();
    task=proj.modules[name];
    #task.AllETLTools[0].Enabled=True;
    if mode=='master':
        from  distributed import *
        master = Master(proj, name);
        master.start(count1,count);
    elif mode =='display':
        print(etl.Task_DumpLinq(task.AllETLTools))
    elif mode in ['print','keys','exec']:
        resultcount=0;

        for r in task.QueryDatas(etlCount=count,execute=mode=='exec'):
            if mode =='print':
                print(r);
            elif mode=='keys':
                print(r.keys())
            resultcount+=1;
        print('task finished,total count is %d'%resultcount);






