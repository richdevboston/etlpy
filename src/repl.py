# coding=utf-8



from distributed import *
from extends import  *
proj= etl.Project();
import inspect
import extends

__base_type = [etl.ETLTool, etl.Filter, etl.EtlBase, etl.Generator, etl.Executor, etl.Transformer];
__ignore_paras = ['one_input', 'multi', 'column'];

tool_dict={}

def get_etl(dic):
    for name, tool_type in etl.__dict__.items():
        if not inspect.isclass(tool_type):
            continue;
        if not issubclass(tool_type, etl.ETLTool):
            continue
        if tool_type in __base_type:
            continue;
        dic[name]=tool_type


get_etl(tool_dict)
def html(text):
    from IPython.core.display import HTML, display
    display(HTML(text));

def spider(name='_crawler'):
    import spider
    sp= spider.SmartCrawler();
    proj.env[name]=sp;
    sp.name=name;
    sp._proj=proj
    return sp;

def set(key,value):
    proj.env[key]=value

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
    mongo.connect_str = 'mongodb://10.244.0.112'
    mongo.db = 'ant_temp'
    set('mongo',mongo)
    return mongo


def task(name='etl'):
    my_task= etl.ETLTask();
    my_task._proj=proj;
    my_task.name=name
    proj.env[name]=my_task

    def attr_filler(attr):
        if attr.startswith('_'):
            return  True
        if attr in __ignore_paras:
            return True
        return  False
    def set_attr(val, dic):
        default = type(val)().__dict__;
        for key in val.__dict__:
            if key.startswith('_'):
                continue
            dv=default[key]
            value = dic.get(key,dv)

            if value==dv:
                if key in para_dict:
                    key2=para_dict[key]
                    value= dic.get(key2,None)
            if value is not None:
                setattr(val, key, value)
        pos=val.column.find(":")
        column = val.column;
        column0=column
        if pos==0:
            column=my_task._last_column+val.column
            column0=column.split(':')[0]

        if column == '' and my_task._last_column != '' and not str(type(val)).lower().find('python') >= 0:
            val.column = my_task._last_column
        else:
            val.column=column
        if column0!='':
            my_task._last_column = column0
    def _rename(module):
        repl={'TF':'','Python':'py','Parallel':'pl'}
        for k,v in repl.items():
            module= module.replace(k,v)
        return module.lower();


    dynaimc_method = '''def __%s(column='',%s):
        import etl as etl
        new_tool=etl.%s();
        new_tool._proj=proj
        set_attr(new_tool,locals())
        my_task.tools.append(new_tool);
        return my_task;
    '''
    para_dict={'selector':'sl','script':'sc'}
    def merge_func(k, v):
        if extends.is_str(v):
            v = "'%s'" % (v);
        yield '%s=%s' % (k, v)
        if k in para_dict:
            k=para_dict[k]
            yield '%s=%s'% (k, v)

    for name,tool_type in etl.__dict__.items():
        if not inspect.isclass(tool_type):
            continue;
        if  not issubclass(tool_type, etl.ETLTool) :
            continue
        if tool_type in __base_type:
            continue;
        tool=tool_type();
        paras= to_list(concat((merge_func(k,v) for k,v in tool.__dict__.items() if not attr_filler(k))))
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




