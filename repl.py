import etl
import extends;
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


def new_task(name='etl'):
    import etl;
    import inspect
    import extends;
    basetype= [etl.Filter,etl.Generator,etl.Executor,etl.Transformer,etl.ConnectorBase];
    ignoreparas=['OneInput','IsMultiYield','Column','OneOutput','Enabled'];
    task=etl.ETLTask();
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
    proj.load_xml('../Hawk-Projects/新闻抓取/百度新闻.xml')
    #print(proj.百度百家)
    buf=[]
    t2=new_task();
    t2.clear() \
        .TextGE('text', content='1 2 3 4 5 6 7 8 9 100 101 102 103 104 105 106 107 108') \
        .MergeTF('text',
                 format='http://baijia.baidu.com/ajax/labellatestarticle?page=1&pagesize=20&labelid={0}&prevarticalid=533025',
                 ncol='url') \
        .CrawlerTF('url', selector='网页采集器') \
        .JsonTF('Content', scriptworkmode='不进行转换') \
        .PythonTF('Content', script="value['data']['total']", ncol='total') \
        .DeleteTF('Content') \
        .MergeTF('text', format='http://baijia.baidu.com/?tn=listarticle&labelid={0}', ncol='classurl') \
        .CrawlerTF('classurl', selector='网页采集器') \
        .XPathTF('Content', xpath='//*[@id="page_title"]/h1', ncol='class') \
        .DeleteTF('Content') \
        .PythonTF('total', script='int(value)/100+1', ncol='p') \
        .RangeGE('p', minvalue=1.0, mergetype='Cross', maxvalue='[p]', interval=1.0) \
        .ToListTF('p') \
        .MergeTF('p',
                 format='http://baijia.baidu.com/ajax/labellatestarticle?page={0}&pagesize=100&labelid={1}&prevarticalid=533025',
                 mergewith='text') \
        .CrawlerTF('p', selector='网页采集器') \
        .JsonTF('Content', scriptworkmode='不进行转换') \
        .PythonTF('Content', script="value['data']['list']", scriptworkmode='文档列表', ncol='class') \
        .TnTF('hotcount',rule= 'integer_int', ncol='like') \
        .RenameTF('m_summary', ncol='description') \
        .RenameTF('m_title', ncol='title') \
        .RenameTF('m_create_time', ncol='date') \
        .TnTF('date',rule='daterule',ncol='timestamp')\
        .PythonFT('timestamp',script='get_time(datetime(2016,7,18))>timestamp>get_time(datetime(2016,7,16))',\
                  stopwhile=True)\
        .RenameTF('m_writer_name', ncol='source') \
        .RenameTF('m_display_url', ncol='url') \
        .CrawlerTF('url', selector='百度百家文章') \
        .TnTF('date', rule='daterule', ncol='timestamp') \
        .exec()

    print(buf);