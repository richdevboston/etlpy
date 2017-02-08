# coding=utf-8
from distributed import *
from extends import  *
proj= etl.Project()
import inspect
import extends

__base_type = [etl.ETLTool, etl.Filter, etl.CrawlerTF, etl.SubBase, etl.Generator, etl.Executor, etl.Transformer]
__ignore_paras = ['one_input', 'multi', 'column','p']

tool_dict={}

def get_etl(dic):
    for name, tool_type in etl.__dict__.items():
        if not inspect.isclass(tool_type):
            continue
        if not issubclass(tool_type, etl.ETLTool):
            continue
        if tool_type in __base_type:
            continue
        dic[name]=tool_type


get_etl(tool_dict)


def html(text):
    from IPython.core.display import HTML, display
    display(HTML(text))

def get_default_connector():
    mongo = etl.MongoDBConnector()
    mongo.connect_str = 'mongodb://10.244.0.112'
    mongo.db = 'ant_temp'
    proj.env['mongo']=mongo
    return mongo


def task(name='etl'):
    _task= etl.ETLTask()
    _task._proj=proj
    _task.name=name
    proj.env[name]=_task

    def attr_filler(attr):
        if attr.startswith('_'):
            return  True
        if attr in __ignore_paras:
            return True
        return  False
    def set_attr(val, dic):
        default = type(val)().__dict__
        for key in val.__dict__:
            if key.startswith('_'):
                continue
            dv=default[key]
            value = dic.get(key,dv)
            if key=='p' and value=='':
                continue
            if value is not None:
                setattr(val, key, value)

    def _rename(module):
        repl={'TF':'','Python':'py','Parallel':'pl','Remove':'rm','Move':'mv','Copy':'cp'}
        for k,v in repl.items():
            module= module.replace(k,v)
        return module.lower()


    dynaimc_method = '''def __%s(p='',%s):
        import etl as etl
        new_tool=etl.%s()
        new_tool._proj=proj
        _task.tools.append(new_tool)
        return _task
    '''
    def merge_func(k, v):
        if extends.is_str(v):
            v = "'%s'" % (v)
        yield '%s=%s' % (k, v)

    def etl_help():
        import inspect
        for k,v in _task.__dict__.items():
            if inspect.isfunction(v):
                doc= v.__doc__
                if doc is None:
                    doc='doc is invalid'
                else:
                    doc= doc.split('\n')
                    for d in doc:
                        d=d.strip()
                        if d!='':
                            doc=d
                            break
                print k+':\t'+doc

    for name,tool_type in etl.__dict__.items():
        if not inspect.isclass(tool_type):
            continue
        if  not issubclass(tool_type, etl.ETLTool) :
            continue
        if tool_type in __base_type:
            continue
        tool=tool_type()
        paras= to_list(concat((merge_func(k,v) for k,v in tool.__dict__.items() if not attr_filler(k))))
        paras.sort()
        paras=','.join(paras)
        new_name=_rename(name)
        method_str= dynaimc_method%(new_name,paras,name)
        locals()['proj']=proj
        exec(method_str,locals())
        func= locals()['__'+ new_name]
        func.__doc__= tool.__doc__
        setattr(_task,new_name,func)

    setattr(_task,'help',etl_help)
    return _task



