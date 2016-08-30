# coding=utf-8
from src.distributed import *

proj= etl.Project();


def html(text):
    from IPython.core.display import HTML, display
    display(HTML(text));

def spider(name='_crawler'):
    from src import spider
    sp= spider.SmartCrawler();
    proj.modules[name]=sp;
    sp.name=name;
    sp._proj=proj
    setattr(proj,name,sp);
    return sp;


def quote(string):
    if extends.PY2:
        import urllib
        s= urllib.quote(string.encode('utf-8'))
        s=str (s);
        return s;
    else:
        import urllib
        return urllib.request.quote(string);

def get_default_connector():
    mongo = etl.MongoDBConnector();
    mongo.connect_str = 'mongodb://10.101.167.107'
    mongo.db = 'ant_temp'
    con = connector('mongo', mongo)
    return con

def connector(name,connector):
    proj.connectors[name]=connector;
    return connector

def task(name='etl'):
    import inspect
    from src import extends
    base_type= [etl.Filter, etl.Generator, etl.Executor, etl.Transformer];
    ignore_paras=['one_input','multi','column'];
    my_task= etl.ETLTask();
    my_task._proj=proj;
    my_task.name=name;
    proj.modules[name]=my_task;
    setattr(proj,name,my_task);

    def set_attr(val, dic):
        for k in val.__dict__:
            if k.lower() in dic:
                setattr(val, k, dic[k.lower()])
    def _rename(module):
        repl={'TF':'','Python':'py','Parallel':'pl'}
        for k,v in repl.items():
            module= module.replace(k,v)
        return module.lower();


    dynaimc_method = '''def __%s(column='',%s):
        import src.etl as etl
        new_tool=etl.%s();
        new_tool._proj=proj
        set_attr(new_tool,locals())
        my_task.tools.append(new_tool);
        new_tool._index= len(my_task.tools)
        return my_task;
    '''

    def merge_func(k, v):
        if extends.is_str(v):
            v = "'%s'" % (v);
        return '%s=%s' % (k.lower(), v);
    for name,tool_type in etl.__dict__.items():
        if not inspect.isclass(tool_type):
            continue;
        if  not issubclass(tool_type, etl.ETLTool) :
            continue
        if tool_type in base_type:
            continue;
        tool=tool_type();
        paras=[merge_func(k,v) for k,v in tool.__dict__.items() if not k.startswith('_')  and k not in ignore_paras]
        paras.sort()
        paras=','.join(paras);
        new_name=_rename(name)
        method_str= dynaimc_method%(new_name,paras,name);
        locals()['proj']=proj
        locals()['cols']=etl.cols;
        exec(method_str,locals());
        func= locals()['__'+ new_name];
        setattr(my_task,new_name,func);
    return my_task;




