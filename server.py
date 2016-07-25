# coding=utf-8
import etl;

import extends
import time;
import pprint;
import  sys
if __name__ == '__main__':
    projfile=u'../Hawk-Projects/新闻抓取/财经新闻.xml';
    name=u'新浪股吧清洗'
    argv=sys.argv;
    mode='master'
    count=10000;
    proj=etl.Project();
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
        if projfile.find('xml')>0:
            proj=proj.load_xml(projfile);
        else:
            proj=proj.load_json(projfile);
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
        print(etl)
    elif mode in ['pprint','print','keys','exec']:
        resultcount=0;

        for r in task.query(etl_count=count, execute=mode== 'exec'):
            if mode =='pprint':
                pprint.pprint(r);
            elif mode=='print':
                print(r);
            elif mode=='keys':
                print(r.keys())
            if resultcount%100==0:
                print(resultcount)
            resultcount+=1;
        print('task finished,total count is %d'%resultcount);






