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

def Cross(a, genefunc, tool):

    for r1 in a:
        for r2 in genefunc(tool, r1):
            for key in r1:
                r2[key] = r1[key]
            yield r2;


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
