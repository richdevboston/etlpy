# encoding: UTF-8
import re;

spacere = re.compile("[ ]{2,}");
spacern = re.compile("(^\r\n?)|(\r\n?$)")


def SaveFile(name, lines):
    f = open(name, 'w', encoding= 'utf-8');
    if isinstance(lines, list) == True:
        for r in lines:
            f.write(str(r) + '\r');
    else:
        f.write(lines);
    f.close();


def ReadFile(filename):
    f = open(filename, 'r',encoding= 'utf-8');
    r = f.read();
    f.close();
    return r;


def ReplaceLongSpace(txt):
    r = spacere.subn(' ', txt)[0]
    r = spacern.subn('', r)[0]
    return r;

def Merge(d1,d2):
    for r in d2:
        d1[r]=d2[r];
    return d1;

def MergeQuery(d1, d2, columns):
    if isinstance(columns, str):
        columns = columns.split(' ');
    for r in columns:
        if r in d2:
            d1[r] = d2[r];
    return d1;


def GetTableRows(datas):
    if datas is None:
        return {};
    result = {};
    i = 0;
    for data in datas:
        for r in data:
            value = ReplaceLongSpace(data[r])
            if i == 0:
                result[r] = [value];
            else:
                result[r].append(value);
        i += 1;
    return result;


def Query(data, key):
    if data is None:
        return key;
    if isinstance(key, str) and key.startswith('[') and key.endswith(']'):
        key = key[1:-1];
        return data[key];
    return key;


def GetMaxSameCount(datas):
    dic = {};
    for t in datas:
        if t in dic:
            dic[t] += 1;
        else:
            dic[t] = 1;
    if len(dic) == 0:
        return 0;
    maxkey, maxvalue = None, -1;
    for key in dic:
        if dic[key] > maxvalue:
            maxvalue = dic[key];
            maxkey = key;
    return (maxkey, maxvalue);


def GetChildNode(self, roots, name):
    for etool in roots:
        if etool.get('Name') == name or etool.tag == name:
            return etool;
    return None;


def GetVariance(data):
    sum1 = 0.0
    sum2 = 0.0
    l = len(data);
    for i in range(l):
        sum1 += data[i]
        sum2 += data[i] ** 2
    mean = sum1 / l
    var = sum2 / l - mean ** 2
    return var;


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



def Cross(a,genefunc,tool):
    for r1 in a:
        for r2 in genefunc(tool,r1):
            for r3 in r2:
                r1[r3]=r2[r3]
            yield r1;



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


'''获取几个序列开头共同几个部分
[30, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0]
[30, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 2, 0, 0]
[30, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 2, 0, 0]
[30, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 2, 0, 0]
例如，结果就是
30, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0
'''


def GetTopPublics(items):
    le = len(items);
    maxid = len(items[0]);
    for i in range(le - 1):
        j = 0;
        while (j < maxid):
            if (items[i][j] == items[i + 1][j]):
                j += 1;
            else:
                maxid = j;
    return items[0][:maxid];


def Add(a, b):
    return a + b + "haha";


def WriteHTMLHeader(file):
    file.write('''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
	<title>{title}}</title>
</head>
<body>''');


def WriteHTMLEnd(file):
    file.write('''</body>
</html>''');

