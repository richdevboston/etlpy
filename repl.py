
import etl
import extends;
from distributed import *
proj=etl.Project();
cols=extends.EObject();

def html(text):
    from IPython.core.display import HTML, display
    display(HTML(text));

def new_spider(name='_crawler'):
    import spider;
    sp=spider.SmartCrawler();
    proj.modules[name]=sp;
    sp._proj=proj
    setattr(proj,name,sp);
    return sp;



def new_connector(name,connector):
    proj.connectors[name]=connector;
    return connector

def new_task(name='etl'):
    import etl;
    import inspect
    import extends;
    basetype= [etl.Filter,etl.Generator,etl.Executor,etl.Transformer];
    ignoreparas=['OneInput','IsMultiYield','Column','OneOutput'];
    task=etl.ETLTask();
    task._proj=proj;
    task.name=name;
    proj.modules[name]=task;
    setattr(proj,name,task);

    def set_attrs(val, dic):
        for k in val.__dict__:
            if k.lower() in dic:
                setattr(val, k, dic[k.lower()])
    def set_cols(datas):
        keys= extends.get_keys(datas)
        for key in keys:
            setattr(cols,key,key)

    dynaimc_method = '''def __%s(column='',%s):
        import etl
        newtool=etl.%s();
        newtool._proj=proj
        set_attrs(newtool,locals())
        task.tools.append(newtool);
        return task;
    '''

    def mergefunc(k, v):
        if isinstance(v, str):
            v = "'%s'" % (v);
        return '%s=%s' % (k.lower(), v);
    for name,tooltype in etl.__dict__.items():
        if not inspect.isclass(tooltype):
            continue;
        if  not issubclass(tooltype,etl.ETLTool) :
            continue
        if tooltype in basetype:
            continue;
        tool=tooltype();
        paras=[mergefunc(k,v) for k,v in tool.__dict__.items() if not k.startswith('_')  and k not in ignoreparas]
        paras.sort()
        paras=','.join(paras);
        method_str= dynaimc_method%(name,paras,name);
        locals()['proj']=proj
        locals()['cols']=cols;
        exec(method_str,locals());
        func= locals()['__'+name];
        setattr(task,name,func);
    return task;

def fuck(x):
    print(x['date']);

if __name__ == '__main__':
    from etl import *
    c= new_connector('c', MongoDBConnector())
    c.ConnectString='mongodb://10.101.167.107'
    c.DBName='ant_temp';
    t = new_task('xx')
    s = new_spider('sp')
    t.clear()
    datas= ('http://www.cnblogs.com/#p%s'%p for p in range(20))
    t.PythonGE('url',script=datas)
    t.ToListTF()
    t.CrawlerTF('url', selector='sp')
    t.XPathTF({'Content': 'content'}, gettext=True)
    t.DeleteTF('Content')
    t.DbEX(connector='c',tablename='haha')
    #t.execute()

    t.distribute()
    exit()

    t2=new_task()
    t2.clear() \
        .TextGE('text', content='1 2 3 4 5 6 7 8 9 100 101 102 103 104 105 106 107 108') \
        .ToListTF('text') \
        .MergeTF({'text':'url'},format='http://baijia.baidu.com/ajax/labellatestarticle?page=1&pagesize=20&labelid={0}&prevarticalid=533025' ) \
        .CrawlerTF('url', selector='网页采集器') \
        .JsonTF('Content', scriptworkmode=etl.GENERATE_NONE) \
        .PythonTF({'Content':'total'}, script="value['data']['total']" ) \
        .DeleteTF('Content') \
        .MergeTF({'text':'classurl'}, format='http://baijia.baidu.com/?tn=listarticle&labelid={0}') \
        .CrawlerTF('classurl', selector='网页采集器') \
        .XPathTF({'Content':'class'}, xpath='//*[@id="page_title"]/h1') \
        .DeleteTF('Content') \
        .PythonTF({'total':'p'}, script='int(value)/100+1') \
        .RangeGE('p', mergetype='Cross', maxvalue='[p]') \
        .ToListTF('p') \
        .MergeTF('p',format='http://baijia.baidu.com/ajax/labellatestarticle?page={0}&pagesize=100&labelid={1}&prevarticalid=533025',mergewith='text') \
        .CrawlerTF('p', selector='网页采集器') \
        .JsonTF('Content', scriptworkmode=etl.GENERATE_NONE) \
        .PythonTF('Content',ncol='class', script="value['data']['list']", scriptworkmode=etl.GENERATE_DOCS ) \
        .TnTF({'hotcount':'like'},rule= 'integer_int') \
        .RenameTF({'m_summary':'description','m_title':'title','m_create_time':'date'}) \
        .TnTF({'date':'timestamp'},rule='daterule')\
        .PythonFT('timestamp',script='get_time(datetime(2016,7,26,15,15))>timestamp>get_time(datetime(2016,7,20,12,15))',stopwhile=True)\
        .RenameTF({'m_writer_name': 'source','m_display_url':'url'}) \
        .CrawlerTF('url', selector='百度百家文章') \
        .TnTF({'date':'timestamp'}, rule='daterule' )
        #.get(format='key',etl_count=50,take=2)
    print(proj.dumps_yaml())
    print(t2)
    #print(buf);