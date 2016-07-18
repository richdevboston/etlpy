# encoding: UTF-8
import re;

def get_mount(generator,take,skip=0):
    i=0;
    for r in generator:
        i += 1;
        if i<skip:
            continue;
        if i>take+skip:
            break;

        yield r;


def get(datas,format='df'):
    if format == 'print':
        import pprint
        for d in datas:
            pprint.pprint(d);
    elif format == 'key':
        import pprint
        for d in datas:
            pprint.pprint(d.keys())
    elif format == 'df':
        from  pandas import DataFrame
        return DataFrame(datas);
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



def variance(nlist):
    sum1=0.0
    sum2=0.0
    N=len(nlist);
    for i in range(N):
        sum1+=nlist[i]
        sum2+=nlist[i]**2
    mean=sum1/N
    var=sum2/N-mean**2
    return var;


def find_any(iteral, filter):
    for r in iteral:
        if filter(r):
            return True;
    return False;


def get_index(iteral, filter):
    for r in range(len(iteral)):
        if filter(iteral[r]):
            return r;
    return -1;

def cross(a, genefunc):
    for r1 in a:
        r1=dict.copy(r1);
        for r2 in genefunc(r1):
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
