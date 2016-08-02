# encoding: UTF-8
import re;
def is_in_ipynb():
    try:
        cfg = get_ipython()
        return True
    except NameError:
        return False

is_ipynb=is_in_ipynb();
def get_mount(generator,take,skip=0):
    i=0;
    for r in generator:
        i += 1;
        if i<skip:
            continue;
        if i>take+skip:
            break;
        yield r;


def get(datas,format='print'):
    if is_ipynb or format == 'df':
        from  pandas import DataFrame
        return DataFrame(datas);
    if format == 'print':
        import pprint
        for d in datas:
            pprint.pprint(d);
    elif format == 'key':
        import pprint
        for d in datas:
            pprint.pprint(d.keys())
    else:
        return list(datas);


def get_keys(generator,s):
    count=0;
    for r in generator:
        count+=1;
        if count<5:
            for key in r.keys():
                if not key.startswith('_'):
                    setattr(s,key,key);
        yield r

def repl_long_space(txt):
    spacere = re.compile("[ ]{2,}");
    spacern = re.compile("(^\r\n?)|(\r\n?$)")
    r = spacere.subn(' ', txt)[0]
    r = spacern.subn('', r)[0]
    return r;


def merge(d1, d2):
    for r in d2:
        d1[r] = d2[r];
    return d1;


def merge_query(d1, d2, columns):
    if isinstance(columns, str) and columns.strip() != "":
        columns = columns.split(' ');
    if columns is None:
        return d1;
    for r in columns:
        if r in d2:
            d1[r] = d2[r];
    return d1;


def first_or_default(generator):
    for r in generator:
        return r;
    return None;

def query(data, key):
    if data is None:
        return key;
    if isinstance(key, str) and key.startswith('[') and key.endswith(']'):
        key = key[1:-1];
        return data[key];
    return key;



def variance(n_list):
    sum1=0.0
    sum2=0.0
    N=len(n_list);
    for i in range(N):
        sum1+=n_list[i]
        sum2+= n_list[i] ** 2
    mean=sum1/N
    var=sum2/N-mean**2
    return var;


def find_any(iter, filter):
    for r in iter:
        if filter(r):
            return True;
    return False;


def get_index(iter, filter):
    for r in range(len(iter)):
        if filter(iter[r]):
            return r;
    return -1;

def get_indexs(iter, filter):
    res=[]
    for r in range(len(iter)):
        if filter(iter[r]):
            res.append(r);
    return res

def cross(a, gene_func):
    for r1 in a:
        r1=dict.copy(r1);
        for r2 in gene_func(r1):
            for key in r2:
                r1[key] = r2[key]
            yield dict.copy(r1);



def merge_all(a, b):
    while True:
        t1 = a.__next__()
        if t1 is None:
            return;
        t2 = b.__next__()
        if t2 is not None:
            for t in t2:
                t1[t] = t2[t];
        yield t1;


def append(a, b):
    for r in a:
        yield r;
    for r in b:
        yield r;

def get_type_name(obj):
    import inspect
    if inspect.isclass(obj):
        s=str(obj);
    else:
        s=str(obj.__class__);
    p=s.find('.');
    r= s[p+1:].split('\'')[0]
    return r;


class EObject(object):
    pass;



def convert_to_builtin_type(obj):
    d=  { key:value for key,value in obj.__dict__.items() if isinstance(value,(str,int,float,list,dict,tuple,EObject) or value is None)};
    return d

def dict_to_poco_type(obj):
    if isinstance(obj,dict):
        result=  EObject();
        for key in obj:
            v= obj[key]
            setattr(result,key,dict_to_poco_type(v))
        return result
    elif isinstance(obj,list):
        for i in range(len(obj)):
            obj[i]=dict_to_poco_type(obj[i]);

    return obj;


def dict_copy_poco(obj,dic):
    for key,value in obj.__dict__.items():
        if key in dic:
            if isinstance(dic[key], (str,int,float)):
                setattr(obj,key,dic[key])



def group_by_mount(generator, group_count=10, take=9999999, skip=0):
    tasks = [];
    task_id=0

    while True:
        task = next(generator, None);
        if task is None:
            yield tasks[:]
            return
        tasks.append(task)
        if len(tasks) >= group_count:
            yield tasks[:];
            task_id = task_id + 1
            tasks.clear()
        if task_id < skip:
            continue
        if task_id > take:
            break;

if __name__ == '__main__':
    res= is_in_ipynb();
    print(res)
