import etl;

import extends
import time;
import  sys
if __name__ == '__main__':
    projfile='';
    name=''
    argv=sys.argv;
    mode='master'
    if len(argv)>1:
        projfile=argv[1];
    if len(argv)>2:
        name=argv[2];
    if len(argv)>3:
        mode= argv[3];
    try:
        proj=etl.Project_LoadXml(projfile);
    except Exception as e:
        print('load project failed:'+ str(e));
        exit();
    if name not in proj.modules:
        print('task name %s not in project'%(name));
        exit();
    task=proj.modules[name];
    if mode=='master':
        from  distributed import *
        master = Master(proj, name);
        master.start();
    elif mode in ['print','count','exec']:
        resultcount=0;
        count=int(argv[4]) if len(argv)>4 else 100;
        for r in task.QueryDatas(etlCount=count,execute=mode=='exec'):
            if mode =='print':
                print(r);
            elif mode=='count':
                print(len(r.keys()))
            resultcount+=1;
        print('task finished,total count is %d'%resultcount);

#




