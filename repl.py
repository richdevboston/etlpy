
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
    def _rename(module):
        repl={'TF':'','Python':'py'}
        for k,v in repl.items():
            module= module.replace(k,v)
        return module.lower();


    def set_cols(datas):
        keys= extends.get_keys(datas)
        for key in keys:
            setattr(cols,key,key)

    dynaimc_method = '''def __%s(column='',%s):
        import etl
        new_tool=etl.%s();
        new_tool._proj=proj
        set_attrs(new_tool,locals())
        task.tools.append(new_tool);
        return task;
    '''

    def mergefunc(k, v):
        if extends.is_str(v ):
            v = "'%s'" % (v);
        return '%s=%s' % (k.lower(), v);
    for name,tool_type in etl.__dict__.items():
        if not inspect.isclass(tool_type):
            continue;
        if  not issubclass(tool_type,etl.ETLTool) :
            continue
        if tool_type in basetype:
            continue;
        tool=tool_type();
        paras=[mergefunc(k,v) for k,v in tool.__dict__.items() if not k.startswith('_')  and k not in ignoreparas]
        paras.sort()
        paras=','.join(paras);
        new_name=_rename(name)
        method_str= dynaimc_method%(new_name,paras,name);
        locals()['proj']=proj
        locals()['cols']=cols;
        exec(method_str,locals());
        func= locals()['__'+ new_name];
        setattr(task,new_name,func);
    return task;


if __name__ == '__main__':
    from etl import *

    datas = open('/Users/zhaoyiming/Documents/stock.news').read().split('\001')
    datas = [json.loads(r) for r in datas if len(r) > 10]
    for r in datas:
        r['url'] = r['text'].split('"')[1]
        del r['text']
    c = new_connector('cc', MongoDBConnector())
    c.ConnectString='mongodb://10.101.167.107'
    c.DBName='ant_temp';

    s=new_spider('sp')
    t = new_task('xx')
    t.clear()
    t.pyge(script=datas)
    t.tolist(mountperthread=5)
    t.crawler('url', selector='sp')
    t.xpath({'Content': 'content'}, gettext=True)
    t.delete('Content')
    t.dbex(connector='cc', tablename='news')

    t.get();
    exit()

    t.distribute()
    exit()

    t2=new_task()
    t2.clear() \
        .textge('text', content='1 2 3 4 5 6 7 8 9 100 101 102 103 104 105 106 107 108') \
        .tolist('text') \
        .merge({'text':'url'},format='http://baijia.baidu.com/ajax/labellatestarticle?page=1&pagesize=20&labelid={0}&prevarticalid=533025' ) \
        .crawler('url', selector='spider') \
        .json('Content', scriptworkmode=etl.GENERATE_NONE) \
        .py({'Content':'total'}, script="value['data']['total']" ) \
        .delete('Content') \
        .merge({'text':'classurl'}, format='http://baijia.baidu.com/?tn=listarticle&labelid={0}') \
        .crawler('classurl', selector='spider') \
        .xpath({'Content':'class'}, xpath='//*[@id="page_title"]/h1') \
        .delete('Content') \
        .py({'total':'p'}, script='int(value)/100+1') \
        .rangege('p', mergetype='Cross', maxvalue='[p]') \
        .tolist('p') \
        .merge('p',format='http://baijia.baidu.com/ajax/labellatestarticle?page={0}&pagesize=100&labelid={1}&prevarticalid=533025',mergewith='text') \
        .crawler('p', selector='spider') \
        .json('Content', scriptworkmode=etl.GENERATE_NONE) \
        .py('Content',ncol='class', script="value['data']['list']", scriptworkmode=etl.GENERATE_DOCS ) \
        .tn({'hotcount':'like'},rule= 'integer_int') \
        .rename({'m_summary':'description','m_title':'title','m_create_time':'date'}) \
        .tn({'date':'timestamp'},rule='daterule')\
        .pyft('timestamp',script='get_time(datetime(2016,7,26,15,15))>timestamp>get_time(datetime(2016,7,20,12,15))',stopwhile=True)\
        .rename({'m_writer_name': 'source','m_display_url':'url'}) \
        .crawler('url', selector=u'baidubaijia') \
        .tn({'date':'timestamp'}, rule='daterule' )
        #.get(format='key',etl_count=50,take=2)
    print(proj.dumps_yaml())
    print(t2)
    #print(buf);