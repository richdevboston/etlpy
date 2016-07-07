# encoding: UTF-8
import re;

spacere = re.compile("[ ]{2,}");
spacern = re.compile("(^\r\n?)|(\r\n?$)")


def getkeys(generator):
    count=0;
    s=set();
    for r in generator:
        s=s|r.keys();
        count+=1;
        if count>=20:
            return list(s);
    return list(s)

def ReplaceLongSpace(txt):
    r = spacere.subn(' ', txt)[0]
    r = spacern.subn('', r)[0]
    return r;


def Merge(d1, d2):
    for r in d2:
        d1[r] = d2[r];
    return d1;


def MergeQuery(d1, d2, columns):
    if isinstance(columns, str) and columns.strip() != "":
        columns = columns.split(' ');
    for r in columns:
        if r in d2:
            d1[r] = d2[r];
    return d1;




def Query(data, key):
    if data is None:
        return key;
    if isinstance(key, str) and key.startswith('[') and key.endswith(']'):
        key = key[1:-1];
        return data[key];
    return key;





def findany(iteral, func):
    for r in iteral:
        if func(r):
            return True;
    return False;


def getindex(iteral, func):
    for r in range(len(iteral)):
        if func(iteral[r]):
            return r;
    return -1;

def Cross(a, genefunc):

    for r1 in a:
        r1=dict.copy(r1);
        for r2 in genefunc(r1):
            for key in r2:
                r1[key] = r2[key]
            yield dict.copy(r1);


def MergeAll(a, b):
    while True:
        t1 = a.__next__()
        if t1 is None:
            return;
        t2 = b.__next__()
        if t2 is not None:
            for t in t2:
                t1[t] = t2[t];
        yield t1;


def Append(a, b):
    for r in a:
        yield r;
    for r in b:
        yield r;

def get_type_name(obj):
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
