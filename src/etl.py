# coding=utf-8
import csv
import json;
import os;
import time;
import traceback
import urllib
import xml.etree.ElementTree as ET

import spider
import extends
from extends import *

if PY2:
    pass;
else:
    import html

MERGE_APPEND= 'append';
MERGE_CROSS= 'cross'
MERGE_MERGE= 'merge'
MERGE_MIX= 'mix'

GENERATE_DOCS='docs'
GENERATE_DOC= 'doc'
GENERATE_NONE='none';

CONV_ENCODE='encode'
CONV_DECODE='decode'
cols=EObject()

GET_HTML='html'
GET_NODE='node'
GET_TEXT='text'
GET_COUNT='count'
GET_ARRAY='list'


def __get_match_counts(mat):
    return mat.lastindex if mat.lastindex is not None else 1;

class ETLTool(EObject):
    def __init__(self):
        super(ETLTool, self).__init__()
        self.enabled=True;
        self.column = ''
        self.debug = False

    def query(self,data):
        for k,v in self.__dict__.items():
            if not k.startswith('_'):
                v2=query(data,v)
                setattr(self,k,v2)
    def process(self, data):
        return data
    def init(self):
        pass;

    def _is_mode(self,mode):
        if not hasattr(self,'mode'):
            return False
        return mode in self.mode.split('|')
    def _eval_script(self, global_para=None, local_para=None):
        if self.script == '':
            return True
        if not is_str(self.script):
            return self.script(self)
        result=None;
        from datetime import datetime

        from time import mktime,strptime,strftime
        def get_time(mtime):
            from pytz import utc
            if mtime == '':
                mtime = datetime.now()
            ts = mktime(utc.localize(mtime).utctimetuple())
            return int(ts);
        try:
            if global_para is not None:
                result = eval(self.script,global_para,locals())

            else:
                result=eval(self.script);
        except Exception as e:
            traceback.print_exc()
        return result

class Transformer(ETLTool):
    def __init__(self):
        super(Transformer, self).__init__()
        self._m_yield=False
        self.one_input = False;
        self.new_col=None
        self._m_process=False
    def transform(self,data):
        pass;

    def m_process(self,data):
        for r in data:
            yield r

    def _process(self,data,column,transform_func):
        def edit_data(col, ncol=None):
            ncol = ncol if ncol != '' and ncol is not None  else col;
            if col != '' and  col not in data:
                return
            if self.one_input:
                res = transform_func(data[col]);
                data[ncol] = res;
            else:
                ncol = ncol if ncol != '' and ncol is not None else col;
                try:
                    transform_func(data, col, ncol);
                except Exception as e:
                    if extends.debug_level>0:
                        print e
                    pass;
        if is_str(column):
            if column.find(u':') >= 0:
                column = para_to_dict(self.column, ',', ':')
            elif self.column.find(' ') > 0:
                column = [r.strip() for r in self.column.split(' ')]
        if isinstance(column, dict):
            for k, v in column.items():
                edit_data(k, v);
        elif isinstance(column, (list, set)):
            for k in column:
                edit_data(k)
        else:
            edit_data(column, self.new_col)
    def process(self,data,_m_process=True):
        if self._m_process==True and _m_process:
            for r  in self.m_process(data):
                yield r;
            return
        if self._m_yield:  # one to many
            for r in data:
                try:
                    datas=self.m_transform(r, self.column);
                    for p in datas:
                        yield p
                except Exception as e:
                    traceback.print_exc()
            return;
        for d in data:
            if d is None:
                continue
            column=self.column;
            self._process(d,column,self.transform);
            yield d;

class Executor(ETLTool):
    def __init__(self):
        super(Executor, self).__init__()

    def execute(self,data):
        pass;
    def process(self,data):
        for r in data:
            yield self.execute(r);
    def init(self):
        pass


class Filter(ETLTool):
    def __init__(self):
        super(Filter, self).__init__()
        self.revert=False;
        self.stop_while=False;
        self.one_input=True;
    def filter(self,data):
        return True;
    def process(self, data):
        error=0;
        for r in data:
            item = None;
            if self.one_input:
                if self.column in r:
                    item = r[self.column];
                if item is None and self.__class__ != NullFT:
                    continue;
            else:
                item=r;
            result = self.filter( item)
            if result == True and self.revert == False:
                yield r;
            elif result == False and self.revert == True:
                yield r;
            else:
                error+=1
                if self.stop_while==False:
                    continue
                elif  self.stop_while==True:
                    sys.stdout.write('stop iter \n')
                    break;
                elif isinstance(self.stop_while, int) and error >= self.stop_while:
                    sys.stdout.write('stop iter \n')
                    break

class Generator(ETLTool):
    def __init__(self):
        super(Generator, self).__init__()
        self.mode= 'append'
        self.pos= 0;
    def generate(self,generator):
        pass;

    def process(self, generator):
        if generator is None:
            return  self.generate(None);
        else:
            if self.mode== MERGE_APPEND:
                return append(generator, self.process(None));
            elif self.mode==MERGE_MERGE:
                return merge(generator, self.process(None));
            elif self.mode==MERGE_CROSS:
                return cross(generator, self.generate)
            else:
                return mix(generator, self.process(None))


EXECUTE_INSERT='insert'
EXECUTE_SAVE='save'
EXECUTE_UPDATE='update'


class MongoDBConnector(EObject):
    def __init__(self):
        super(MongoDBConnector, self).__init__()
        self.connect_str='';
        self.db=''

    def init(self):
        import pymongo
        client = pymongo.MongoClient(self.connect_str);
        self._db = client[self.db];



class FileConnector(EObject):
    def __init__(self,path=None,encoding='utf-8',work_type='r'):
        super(FileConnector, self).__init__()
        self.path=path;
        self.encoding= encoding;
        self.work_type=work_type;

    def init(self):
        path=self.path;
        self._file = open(path, self.work_type, encoding=self.encoding)

class CsvConnector(FileConnector):
    def __init__(self, path=None, encoding='utf-8',sp=','):
        super(FileConnector, self).__init__()
        self.sp=sp;

    def write(self, datas):
        field = get_keys(datas);
        self._writer = csv.DictWriter(self._file, field, delimiter=self.sp, lineterminator='\n')
        self._writer.writeheader()
        for data in datas:
            self._writer.writerow(data);
        self._file.close();

    def read(self):
        reader = csv.DictReader(self._file, delimiter=self.sp)
        for r in reader:
            yield r;

class JsonConnector(FileConnector):

    def write(self,datas):
        self._file.write('[')
        for data in datas:
            json.dump(data, self.file, ensure_ascii=False)
            self._file.write(',');
            yield data;
        self._file.write(']');
        self._file.close()

    def read(self):
        items = json.load(self._file);
        for r in items:
            yield r;


class DBBase(ETLTool):
    def __init__(self):
        super(DBBase, self).__init__();
        self.selector = '';
        self.table = '';

    def init(self):
        self._connector = self._proj.env[self.selector];
        self._connector.init()
        self._table = self._connector._db[self.table];



class DbEX(Executor,DBBase):
    def __init__(self):
        super(DbEX, self).__init__();
        self.mode = EXECUTE_INSERT

    def init(self):
        DBBase.init(self)
    def process(self,datas):
        etype = self.mode;
        work = None
        init=False
        for data in datas:
            if not init:
                table=  self._connector._db[query(data,self.table)];
                work={EXECUTE_INSERT: lambda d: table.save(d), EXECUTE_UPDATE: lambda d: table.save(d)};
                init=True
            ndata= data.copy()
            work[etype](ndata);
            yield data;


class DbGE(Generator,DBBase):
    def __init__(self):
        super(DbGE, self).__init__();

    def generate(self,data):
        for data in self._table.find():
            yield data;


class JoinDBTF(Transformer,DBBase):
    def __init__(self):
        super(JoinDBTF, self).__init__();
        self.mode = 'doc'
        self.script=''
        self._m_yield=True

    def init(self):
        super(JoinDBTF, self).init();


    def m_transform(self, data, col):
        if self._table==None:
            yield data;
        key = data[col]
        dic= para_to_dict(self.script,',',':')
        values = {r: 1 for r in  dic.keys()}
        def db_filter(d):
            if '_id' in d:
                del d['_id']

        if self.mode == 'docs':
            result = self._table.find({col: key}, values)
            for r in result:
                db_filter(r)
                r=conv_dict(r,dic)
                yield r;
        else:
            result = self._table.find_one({col: key}, values)
            if result is not None:
                db_filter(result)
                yield merge(data.copy(),conv_dict(result,dic))
            else:
                yield data



def set_value(data, value, col, ncol=None):
    if ncol!='' and ncol is not None:
        data[ncol]=value;
    else:
        data[col]=value;

class MatchFT(Filter):
    def __init__(self):
        super(MatchFT, self).__init__();
        self.script=''
        self.mode='str'
    def init(self):
        if self.mode=='re':
            self.regex = re.compile(self.script);
        self.count=1;

    def filter(self,data):
        if self.mode=='str':
            v= [data[i:].startswith(self.script) for i in range(len(data))]
            if v==0:
                return False;
        else:
            v = self.regex.findall(data);
            if v is None:
                return False;

        return self.count <= len(v)

class RangeFT(Filter):
    def __init__(self):
        super(RangeFT, self).__init__();
        self.min=0;
        self.max=100;
    def filter(self,item):
        f = float(item)
        return self.min <= f <= self.max;

class RepeatFT(Filter):
    def __init__(self):
        super(RepeatFT, self).__init__()
    def init(self):
        self.set=set();
    def filter(self,data):
        if data in self.set:
            return False;
        else:
            self.set.add(data);
            return True;

class NullFT(Filter):
    def __init__(self):
        super(NullFT, self).__init__()
    def filter(self,data):
        if data is None:
            return False;
        if is_str(data):
            return data.strip() != '';
        return True;



class RequestTF(Transformer):
    def __init__(self):
        super(RequestTF, self).__init__()
        self.url=''

    def transform(self, data, col, ncol):
        sub_dict= data[col];
        for k,v in sub_dict.items():
            if PY2 and isinstance(v,unicode):
                v= v.encode('utf-8')
                sub_dict[k]=v
        url_p=urllib.urlencode(sub_dict);
        url= query(data,self.url)
        data[ncol]=url+'?'+ url_p


class AddNewTF(Transformer):
    def __init__(self):
        super(AddNewTF, self).__init__()
        self.script= ''


    def m_transform(self, data, col):
        data[col] = self.script;
        yield data;





class AutoIndexTF(Transformer):
    def init(self):
        super(AutoIndexTF, self).__init__()
        self._index = 0;
    def transform(self, data):
        self._index += 1;
        return self._index;


class RenameTF(Transformer):
    def __init__(self):
        super(RenameTF, self).__init__()
        self.script=None;
    def transform(self, data, col, ncol):
        data[ncol]=data[col];
        if col !=ncol:
            del data[col]
class DeleteTF(Transformer):
    def __init__(self):
        super(DeleteTF, self).__init__()
        self.script= ''

    def init(self):
        self._m_yield= self.script != ''
        if self.script!= '':
            self._re=re.compile(self.script)

    def m_transform(self, data, col):
        for k,v in data.items():
            if self._re.match(k):
                del data[k]
        yield data

    def transform(self, data,col,ncol ):
        del data[col];


class KeepTF(Transformer):
    def __init__(self):
        super(KeepTF, self).__init__()
        self._m_yield=True
        self.script= ''

    def init(self):
        if self.script!= '':
            self._re = re.compile(self.script)

    def m_transform(self, data, col):
        doc = {}
        if col.find(':')>0:
            col= para_to_dict(col,',',':');
            for k, v in data.items():
                if k in col:
                    doc[col[k]] = v
            yield doc
        else:
            col=col.split(' ');

            for k,v in data.items():
                if  self.script== '':
                    if k in col:
                        doc[k]=v
                else:
                    if self._re.match(k):
                        doc[k]=v
            yield doc


class EscapeTF(Transformer):
    def __init__(self):
        super(EscapeTF, self).__init__()
        self.one_input = True;
        self.mode = 'decode'

    def transform(self, data):
        if self.mode == 'decode':
            if PY2:
                return data.encode('utf-8').decode('string_escape').decode('utf-8')
            else:
                return data.decode('unicode_escape')
        return data;

class HtmlTF(Transformer):
    def __init__(self):
        super(HtmlTF, self).__init__()
        self.one_input=True;
        self.mode='decode'
    def transform(self, data):
        if PY2:

            import HTMLParser
            html_parser = HTMLParser.HTMLParser()
            if self.mode=='decode':
                return html_parser.unescape(data)
            else:
                import cgi
                return  cgi.escape(data)
        else:
            return html.escape(data) if self.mode == 'encode' else html.unescape(data);


class UrlTF(Transformer):
    def __init__(self):
        super(UrlTF, self).__init__()
        self.one_input = True;
        self.mode = 'decode'
    def transform(self, data):
        if self.mode == 'encode':
            url = data.encode('utf-8');
            return urllib.parse.quote(url);
        else:
            return urllib.parse.unquote(data);


class RegexSplitTF(Transformer):
    def transform(self, data):
        items = re.split(self.regex, data)
        if len(items) <= self.index:
            return data;
        if not self.FromBack:
            return items[self.index];
        else:
            index = len(items) - self.index - 1;
            if index < 0:
                return data;
            else:
                return items[index];
        return items[index];


class MergeTF(Transformer):
    def __init__(self):
        super(MergeTF, self).__init__()
        self.script= '{0}'
        self.merge_with=''
    def transform(self, data, col, ncol=None):
        def get_value(data,r):
            if r in data:
                return data[r]
            else:
                return r;
        if self.merge_with == '':
            columns = [];
        else:
            columns = [to_str(get_value(data,r)) for r in self.merge_with.split(' ')]
        columns.insert(0, data[col] if col in data else '');
        res = self.script;
        res= format(res,columns)
        col = ncol if ncol is not None else col;
        data[col]=res;

class RegexTF(Transformer):
    def __init__(self):
        super(RegexTF, self).__init__()
        self.script = '';
        self.one_input = True;
        self.index=0;

    def init(self):
        self.regex = re.compile(self.script);
    def transform(self, data):
        item = re.findall(self.regex, to_str(data));
        if self.index < 0:
            return '';
        if len(item) <= self.index:
            return '';
        else:
            r = item[self.index];
            return r if is_str(r) else r[0];

class LastTF(Transformer):
    def __init__(self):
        super(LastTF,self).__init__()
        self.count=1
        self._m_process=True

    def m_process(self, data):
        r0 = None
        while True:
            try:
                # 获得下一个值:
                x = next(data)
                r0=x
            except StopIteration:
                # 遇到StopIteration就退出循环
                break
        yield r0



class AggTF(Transformer):
    def __init__(self):
        super(AggTF, self).__init__()
        self._m_process = True
        self.script=''
    def m_process(self,data):
        r0=None
        import inspect
        for m in data:

            if self.column!='':
                r=m[self.column]
            if r0==None:
                r0=r
                yield m
                continue
            if is_str(self.script):
                r2=self._eval_script(global_para={'a': r0, 'b': r})

            elif inspect.isfunction(self.script):
                r2 = self.script(r0,r)
            if r2 != None:
                r =r2
            if self.column!='':
                m[self.column]=r;
            else:
                m=r
            yield m
            r0=r



class ReplaceTF(RegexTF):
    def __init__(self):
        super(ReplaceTF, self).__init__()
        self.new_value = '';
        self.mode='str';
        self.one_input=False

    def transform(self, data,col,ncol):
        ndata=data[col]
        if ndata is None:
            return
        new_value= query(data,self.new_value)
        if self.mode=='re':
            result= re.sub(self.regex, new_value, ndata);
        else:
            if ndata is None:
                result= None
            else:
                result=ndata.replace(self.script,new_value);
        data[ncol]=result
class NumberTF(Transformer):
    def __init__(self):
        super(NumberTF, self).__init__()
        self.one_input=True;
        self.index=0
    def init(self):
        self.regex=  re.compile('\d+');
        self.index=int(self.index)
    def transform(self, data):
        item = re.findall(self.regex, to_str(data));
        if self.index < 0:
            return '';
        if len(item) <= self.index:
            return '';
        else:
            r = item[self.index];
            return r if is_str(r) else r[0];

class SplitTF(Transformer):
    def __init__(self):
        super(SplitTF, self).__init__()
        self.script= '';
        self.one_input = True;
        self.split_blank=False;
        self.split_null=False;

    def init(self):
        self.splits = self.script.split(' ');
        if '' in self.splits:
            self.splits.remove('')
        if self.split_blank==True:
            self.splits.append(' ');
        if self.split_null==True:
            self.splits.append('\n')
    def transform(self, data):
        if len(self.splits)==0:
            return data;
        for i in self.splits:
            data = data.replace(i, '\001');
        r=data.split('\001');
        return r
class TrimTF(Transformer):
    def __init__(self):
        super(TrimTF, self).__init__()
        self.one_input = True;

    def transform(self, data):
        return data.strip();

class StrExtractTF(Transformer):
    def __init__(self):
        super(StrExtractTF, self).__init__()
        self.has_margin=False;
        self.start= ''
        self.one_input=True;
        self.end= ''

    def transform(self, data):
        start = data.find(self.start);
        if start == -1:
            return
        end = data.find(self.end, start);
        if end == -1:
            return;
        if self.has_margin:
            end += len(self.end);
        if not self.has_margin:
            start += len(self.start);
        return data[start:end];

class PythonTF(Transformer):
    def __init__(self):
        super(PythonTF, self).__init__()
        self.script='script'


    def _get_data(self, data, col):
        if col=='':
            if is_str(self.script):
                dic = merge({'data': data}, data)
                self._eval_script(dic);
            else:
                self.script(data);
            return None
        else:
            value = data[col]
            if is_str(self.script ):
                dic = merge({'value': value, 'data': data}, data)
                result = self._eval_script(dic);
            else:
                result = self.script(value);
        return result;

    def transform(self, data,col,ncol):
        js = self._get_data( data,col)
        data[ncol]=js

class PythonGE(Generator):
    def __init__(self):
        super(PythonGE, self).__init__()
        self.script='xrange(1,20,1)'
    def can_dump(self):
        return  is_str(self.script);
    def generate(self,generator):
        import inspect;
        if is_str(self.script):
            result = self._eval_script();
        elif inspect.isfunction(self.script):
            result= self.script()
        else:
            result= self.script;
        for r in result:
            if self.column!= '':
                if self.column== 'id' and r==8:
                    break;
                yield {self.column:r};
            else:
                yield r;

class PythonFT(Filter):
    def __init__(self):
        super(PythonFT, self).__init__()
        self.script='True';
        self.one_input=False;
    def can_dump(self):
        return  is_str(self.script);
    def filter(self, data):
        import inspect
        data=data.copy();

        col=self.column
        value = data[col] if col in data else '';
        if is_str(self.script):
            dic=merge({'value': value, 'data': data},data)
            result = self._eval_script(dic);
        elif inspect.isfunction(self.script):
            result = self.script(data)
        if result==None:
            return False
        return result;

class GreatTF(Transformer):
    def __init__(self):
        super(GreatTF, self).__init__()
        self.index=0;
        self.attr=False
        self.script=''

    def init(self):
        self._m_yield=True

    def m_transform(self, data, col):
        from spider import search_properties,get_datas
        root = data[col]
        if root is None:
            return

        if is_str(root):
            from lxml import etree
            root = spider._get_etree(root)
        tree = etree.ElementTree(root);
        if self.script!='':
            xpaths= spider.get_diff_nodes(tree,root,self.script,self.attr)
            root_path=self.script
        else:
            result = first_or_default(get_mount(search_properties(root, None, self.attr), take=1, skip=self.index))
            if result is None:
                print ('great hand failed')
                yield data
                return
            root_path, xpaths = result
        for r in xpaths:
            r.path= spider.get_sub_xpath(root_path,r.path);

        code0=  '\n.'.join((u"xpath(u':{col}',sc='/{path}')\\" .format(col=r.name,path=r.path,sample=r.sample) for r in xpaths))
        code= u".xpath(sc='%s',mode='html|list').list().tree()\\\n.%s" %(root_path,code0 );
        print code
        code2= '\n'.join(u"#{key} : #{value}".format(key=r.name,value=r.sample.strip()) for r in xpaths);
        print code2
        datas = get_datas(root, xpaths, True, root_path=root_path)
        for r in datas:
            yield r

class CacheTF(Transformer):
    def __init__(self):
        super(CacheTF, self).__init__()
        self.refresh = False
        self.max=100
        self._m_process = True


    def init(self):
        if self.refresh:
            self._proj.__cache=[]
    def m_process(self,data):
        i=0
        while i<len(self._proj.__cache):
            yield self._proj.__cache[i]
            i+=1
        self._proj.__cache=[]
        for r in get_mount(data):
            if self.max>len(self._proj.__cache):
                self._proj.__cache.append(r)
            yield r;



class CrawlerTF(Transformer):
    def __init__(self):
        super(CrawlerTF, self).__init__()
        self.headers = ''
        self.encoding = 'utf-8'
        self.script= ''
        self.mode='get'
        self.pl_count=1

    def m_process(self,data):
        for g in group_by_mount(data,self.pl_count):
            if self._m_yield:
                col=self.column
                reqs=((d,col) for  d in g)
                for raw,result in self._get_data(reqs):
                    for p in result:
                        my = merge_query(raw, p, self.new_col);
                    yield my;
            else:
                reqs=[]
                new_col=[]
                datas=[]
                def transform(data, col, ncol):
                    reqs.append((data,col))
                    new_col.append(ncol)
                    datas.append(data)
                for d in g:
                    self._process(d, self.column, transform);
                index=0
                for data,result in self._get_data(reqs,new_col[index]):
                    index+=1
                    if result is not None:
                        for k, v in result.items():
                            data[k] = v;
                    yield data

    def init(self):
        if self.pl_count>1:
            self._m_process=True


    def crawls(self, req_datas, exception_handler=None):
        import requests
        from spider import get_encoding
        def get_request_para(req):
            url, data = req;
            paras = {};
            if self.headers in self._proj.env:
                paras['headers'] = self._proj.env[self.headers]
            if data != '':
                key= 'data' if self.mode=='post' else 'params';
                paras[key] = data
            paras['url'] = url;
            return paras

        if self.pl_count>1:
            import grequests
            def get_requests(req):
                paras = get_request_para(req)
                if self.mode=='get':
                    r = grequests.get(**paras)
                else:
                    r = grequests.post(**paras)
                return r

            res = grequests.map((get_requests(re) for re in req_datas), exception_handler=exception_handler);
        else:

            res = []
            for req in req_datas:
                paras = get_request_para(req)
                if self.mode=='get':
                    r = requests.get(**paras)
                else:
                    r = requests.post(**paras)
            res.append(r)
        for response in res:
            if response is not None:
                data = get_encoding(response.content)
                yield data;


    def _get_data(self,reqs):
        def _get_request_info(req):
            data = req[0]
            col = req[1]
            url = data[col];
            request_data= query(data, self.script);
            if self.debug:
                print(url)
            if request_data is None:
                request_data = ''
            return url,request_data,data

        reqs2=[]
        datas=[]
        for req in reqs:
            url,request_data,data = _get_request_info(req)
            reqs2.append((url,request_data))
            datas.append(data)
        for req,data,result in  zip(reqs2,datas,self.crawls(reqs2)):
            yield data,result;

    def transform(self, data, col,ncol):
        for r in  self._get_data([(data, col)]):
            data[ncol]=r[1]
            return



class TreeTF(Transformer):
    def __init__(self):
        super(TreeTF, self).__init__()
        self.one_input = True;


    def transform(self, data):
        from lxml import etree
        root = spider._get_etree(data);
        #tree = etree.ElementTree(root)
        return root


class SearchTF(Transformer):
    def __init__(self):
        super(SearchTF, self).__init__()
        self.script = ''
        self._m_yield=False;
        self.one_input=True;
        self.mode='str'
    def transform(self, data):
        from spider import search_xpath
        if data is None:
            return None;
        if is_str(data):
            from lxml import etree
            tree = spider._get_etree(data);
        result = search_xpath(tree, self.script,self.mode, True);
        print result
        return result

class ListTF(Transformer):
    def __init__(self):
        super(ListTF, self).__init__()
        self.mode = CONV_DECODE
        self._m_yield=True
        self.script=''

    def m_transform(self, data, col):
        root = data[col];
        if self.mode== CONV_DECODE:
            for r in root:
                my = merge_query(r, data, self.script);
                yield {col:my}


class XPathTF(Transformer):
    def __init__(self):
        super(XPathTF, self).__init__()
        self.script= ''
        self.mode= GET_TEXT

    def _trans(self,data):
        from lxml import etree
        if isinstance(data, (str, unicode)):
            root = spider._get_etree(data);
            tree = etree.ElementTree(root)
        else:
            tree = data
        return tree;

    def _transform(self, nodes):
        if len(nodes) < 1:
            return
        node = nodes[0]

        def get_data(node):
            res = None
            if self._is_mode(GET_NODE):
                res = node
            elif self._is_mode(GET_TEXT):
                if hasattr(node, 'text'):
                    res = spider.get_node_text(node);
                else:
                    res = to_str(node)
            elif self._is_mode(GET_HTML):
                res = spider.get_node_html(node)
            return res

        if self._is_mode(GET_ARRAY):
            res = [get_data(r) for r in nodes]
        else:
            res = get_data(node)
        return res

    def transform(self,data,col,ncol):
        tree= self._trans(data[col]);
        if tree is None:
            return
        node_path = query(data, self.script);
        if node_path is None:
            return
        if node_path == '' or node_path is None:
            return
        nodes = tree.xpath(self.script);
        if len(nodes)<1:
            return
        data[ncol]= self._transform(nodes)




class PyQTF(XPathTF):
    def __init__(self):
        super(PyQTF, self).__init__()
        self.script = ''
        self.mode = GET_HTML

    def transform(self, data, col,ncol):
        from pyquery import PyQuery as pyq
        root = pyq(data[col]);
        if root is None:
            return;
        node_path = query(data, self.script);
        if node_path is None:
            return
        if node_path == '' or node_path is None:
            return
        nodes = root(node_path);
        data[ncol]= self._transform(nodes)


class AtTF(Transformer):
    def __init__(self):
        super(AtTF, self).__init__()
        self.one_input = True;
        self.script=''

    def init(self):
        def get_int(x, default=0):
            try:
                return int(x)
            except:
                return default;
        index= get_int(self.script,None)
        if index!=None:
            self.script=index


    def transform(self, data):
        return data[self.script];

class TnTF(Transformer):

    def __init__(self):
        super(TnTF,self).__init__()
        self.rule=None;
        self.one_input=True;

    def init(self):
        import codecs
        from tn_py.tnpy import tn
        import tn_py.tnnlp as tnnlp
        import tn_py.custom as custom
        TnTF.log=False
        if not hasattr(TnTF,'core'):
            TnTF.core = tn()
            core=TnTF.core

            core.init_tn_rule('src/tn_py/cnext', True)
            core.init_py_rule(tnnlp)
            core.init_py_rule(custom)
            if TnTF.log:
                core.log_file= codecs.open("../test/log.txt", 'w', encoding='utf-8')
            core.rebuild()
    def transform(self,data):
        core=TnTF.core;
        data=to_str(data)
        result=core.extract(data, entities=[self.rule]);
        if any(result):
            result= result[0]['#rewrite'];
            return result
        if TnTF.log:
            core.log_file.flush()
        return '';


class ParallelTF(Transformer):
    def __init__(self):
        super(ParallelTF, self).__init__()
        self.count_per_thread=1;
        self._m_yield=True;
    def m_transform(self, data,col):
        yield data;


class JsonTF(Transformer):
    def __init__(self):
        super(JsonTF, self).__init__()
        self.mode=  CONV_ENCODE
        self.one_input=True

    def transform(self, data):
        if self.mode== CONV_DECODE:
            return json.loads(data);
        else:
            return json.dumps(data)



class RangeGE(Generator):
    def __init__(self):
        super(RangeGE, self).__init__()
        self.interval='1'
        self.max='1'
        self.min='1'

    def generate(self,generator):
        def get_int(x,default=0):
            try:
                return int(x)
            except:
                return default;
        interval= get_int(query(generator, self.interval),1)
        max_value= get_int(query(generator, self.max),1)
        min_value= get_int(query(generator, self.min),1)
        if min_value==max_value:
            yield {self.column:min_value}
            return
        if interval>0:
            values=range(min_value,max_value,interval);
        else:
            values=range(max_value,min_value,interval)
        for i in values:

            item= {self.column:i}
            yield item;






class EtlBase(ETLTool):
    def __init__(self):
        super(EtlBase, self).__init__()
        self.selector=''
        self.range = '0:100'
        self.new_col=''

    def _get_task(self, data):
        selector = query(data, self.selector);
        if isinstance(self.selector,ETLTask):
            return self.selector
        if selector not in self._proj.env:
            sys.stderr.write('sub task %s  not in current project' % selector);
        sub_etl = self._proj.env[selector];
        return sub_etl;


    def _get_tools(self,data):

        sub_etl = self._get_task(data)
        buf = [r for r in self.range.split(':') if r.strip()!='']
        start = int(buf[0])
        if len(buf)>1:
            end = int(buf[1])
        else:
            end=20000;
        tools= sub_etl.tools[start:end]
        for tool in tools:
            if tool==self:
                continue
            tool.init()
        return tools

    def _generate(self, data,execute=False,init=False):
        doc=None
        if spider.is_none(self.new_col):
            if data is not None:
                doc = data.copy();
        else:
            doc={}
            merge_query(doc, data, self.column+" "+self.new_col);
        generator=[doc] if doc is not None else None
        for r in ex_generate(self._get_tools(doc),generator,execute=execute,init=init):
            yield r;



class EtlGE(Generator,EtlBase):
    def __init__(self):
        super(EtlGE, self).__init__()


    def generate(self, data):
        return self._generate(data)

class EtlEX(Executor, EtlBase):
    def __init__(self):
        super(EtlEX, self).__init__()


    def process(self,data):
        for d in data:
            if self.debug == True:
                for r in self._generate(d, False):
                    yield r;
            else:
                yield self.execute(d)

    def execute(self,data):

        count=0
        try:
            for r in self._generate( data,self.enabled):
                count+=1;
        except Exception as e:
            sys.stderr.write('subtask fail')
        print('subtask:'+to_str(count))
        return data;

class EtlTF(Transformer, EtlBase):

    def __init__(self):
        super(EtlTF,self).__init__()
        self.cycle=False;
        self._m_yield=True
        self.mode=GENERATE_DOCS

    def init(self):
        self._m_yield=self.mode==GENERATE_DOCS;


    def m_transform(self,data,col):
        if self.cycle:
            new_data={}
            result = first_or_default(self._generate(data))
            if result is None:
                return
            yield result.copy();
        else:
            for r in self._generate(data):
                yield r
    def transform(self,data,col,ncol):
        if self.mode==GENERATE_DOC:
            for r in self._generate(data):
                merge(data, r);
                break
        else:
            buf=[];
            for r in self._generate(data):
                buf.append(r)
            data[ncol]=buf


class TextGE(Generator):
    def __init__(self):
        super(TextGE, self).__init__()
        self.script= '';
    def init(self):
        value=self.script.replace('\n', '\001').replace(' ', '\001')
        self._arglists= [r.strip() for r in value.split('\001')];
    def generate(self,data):
        for i in range(int(self.pos), len(self._arglists)):
            yield {self.column: self._arglists[i]}


class TextTF(XPathTF):
    def __init__(self):
        super(TextTF, self).__init__()


    def init(self):
        self._m_yield = False
        self.script='//*[0]'
        self.mode='text'

class SampleTF(Transformer):
    def __init__(self):
        super(SampleTF, self).__init__();
        self.count=1
        self._m_process=True

    def m_process(self,data):
        count=0
        for r in data:
            count+=1
            if self.count>0 and count% self.count==0:
                yield r

class RotateTF(Transformer):
    def __init__(self):
        super(RotateTF, self).__init__();
        self.sc = '';
        self._m_process = True


    def m_process(self, datas):
        result={}
        for data in  datas:
            key= data.get(self.column,None)
            if key is None:
                continue
            value = query(data,self.sc);
            result[key]=value
        yield result


class DictTF(Transformer):
    def __init__(self):
        super(DictTF, self).__init__();
        self.mode= CONV_DECODE
        self.script=''
        self._m_yield = True;
    def init(self):
        if self.script!='':
            self.mode=CONV_ENCODE

    def m_transform(self, data, col):
        if self.mode==CONV_ENCODE:
            doc={}
            merge_query(doc,data,self.script);
            data[col]=doc;
            yield data;
        else:
            col_data=data[col]
            yield merge(data.copy(),col_data);

class FileExistFT(Transformer):
    def __init__(self):
        super(FileExistFT,self).__init__();
        self.script = '';
        self.one_input = True;
    def transform(self,data):
        import os;
        return to_str(os.path.exists(data));


class TakeTF(Transformer):
    def __init__(self):
        super(TakeTF, self).__init__();
        self.skip=0;
        self.take=''
    def process(self,data):
        for r in get_mount(data,self.take,self.skip):
            yield r;



class DelayTF(Transformer):
    def __init__(self):
        super(DelayTF, self).__init__();
        self.delay=100;
        self._m_yield=True;

    def m_transform(self, data, col):
        import time
        delay = query(data, self.delay);
        time.sleep(float(delay)/1000.0)
        yield data;


class SaveFileEX(Executor):
    def __init__(self):
        super(SaveFileEX, self).__init__()
        self.path= '';


    def execute(self,data):
        import requests
        save_path = query(data, self.path);
        (folder,file)=os.path.split(save_path);
        if not os.path.exists(folder):
            os.makedirs(folder);
        urlpath= data[self.column];
        newfile= open(save_path,'wb');
        req = requests.get(urlpath)
        if req is None:
            return
        newfile.write(req.content);
        newfile.close();
        return data



class Project(EObject):
    def __init__(self):
        self.env={};
        self.__cache=[]
        self.desc="edit project description here";

    def clear(self):
        self.env.clear()
        return self;

    def dumps_json(self):
        dic = convert_dict(self )
        return json.dumps(dic, ensure_ascii=False, indent=2)

    def dumps_yaml(self):
        import yaml
        dic = convert_dict(self)
        return yaml.dump(dic)

    def dump_yaml(self, path):
        with extends.open(path, 'w', encoding='utf-8') as f:
            f.write(self.dumps_yaml());
    def load_yaml(self, path):
        import yaml
        with open(path, 'r', encoding='utf-8') as f:
            d = yaml.load(f);
            return self.load_dict(d)


    def loads_json(self, js):
        d = json.loads(js);
        return self.load_dict(d)

    def dump_json(self,path):
        with extends.open(path,'w',encoding='utf-8') as f:
            f.write(self.dumps_json());

    def load_json(self,path):
        with extends.open(path,'r',encoding='utf-8') as f:
            js= f.read();
            return self.loads_json(js)


    def load_dict(self,dic):
        items =dic.get('env',{});
        for key, item in items.items():
            if 'Type' in item:
                obj_type = item['Type']
                task = eval('%s()' % obj_type);
                if obj_type == 'ETLTask':
                    for r in item['tools']:
                        etl = eval('%s()' % r['Type']);
                        for attr, value in r.items():
                            if attr in ['Type']:
                                continue;
                            setattr(etl, attr, value);
                        etl._proj = self;
                        task.tools.append(etl)
                else:
                    dict_copy_poco(task,item);
            self.env[key]=task
        return self;




def convert_dict(obj):
    if not isinstance(obj, ( int, float, list, dict, tuple, EObject)) and not is_str(obj):
        return None
    if isinstance(obj, EObject):
        d={}
        obj_type= type(obj);
        typename= get_type_name(obj)
        default= obj_type().__dict__;
        for key, value in obj.__dict__.items():
            if value== default.get(key,None):
                    continue;
            if key.startswith('_'):
                continue;
            p =convert_dict(value)
            if p is not None:
                d[key]=p
        d['Type']= typename;
        return d;

    elif isinstance(obj, list):
       return [convert_dict(r) for r in obj];
    elif isinstance(obj,dict):
        return {key: convert_dict(value) for key,value in obj.items()}
    return obj;


def ex_generate(tools, generator=None, init=True, execute=False, enabled=True):
    mapper, reducer, tolist = parallel_map(tools)
    if init:
        for tool in tools:
            tool.init();
    for r in generate(mapper, generator, init=False, execute=execute):
        if reducer is None:
            yield r;
        else:
            if isinstance(r, dict):
                r = [r]
            for p in ex_generate(reducer, r, init=False, execute=execute):
                yield p;

def generate(tools, generator=None, init=True, execute=False, enabled=True):
    if tools is None:
        return generator;
    for tool in tools:
        if tool.enabled == False and enabled == True:
            continue

        if isinstance(tool,Executor):
            if execute==False and tool.debug==False:
                continue;
        if init:
            tool.init();
        generator = tool.process(generator)
    return generator;


def count(generator):
    if isinstance(generator,list):
        return len(generator);
    if isinstance(generator,Generator):
        if hasattr(generator,'_mount'):
            return generator._mount;
    return 100;



def parallel_map(tools):
    index= get_index(tools, lambda x:isinstance(x, ParallelTF))
    if index==-1:
        return tools,None,None;
    mapper = tools[:index]
    reducer=tools[index+1:]
    parameter= tools[index];
    return mapper,reducer,parameter;




class ETLTask(EObject):
    def __init__(self):
        self.tools = [];
        self.name=''
        self._master=None;
        self._last_column=''
    def clear(self):
        self.tools=[]
        return self;

    def to_json(self):
        dic = convert_dict(self)
        return json.dumps(dic, ensure_ascii=False, indent=2)

    def to_yaml(self):
        import yaml
        dic = convert_dict(self)
        return yaml.dump(dic)

    def to_graph(self,path='etl.jpg'):
        import pygraphviz as pgv
        A = pgv.AGraph(directed=True, strict=True);
        last = "root"
        A.add_node(last)
        for tool in self.tools:
            m_type=str(tool.__class__)
            A.add_node(m_type)


            A.add_edge(last,tool,tool.column)
            last=tool

        A.graph_attr['epsilon'] = '0.001'
        A.layout('dot')  # layout with dot
        A.draw(path)  # write to file

    def check(self,count=10):
        c=  force_generate(foreach(self.tools, lambda x:x.init()))
        for i in range(1,len(self.tools)):
            attr=EObject()
            tool=self.tools[i];
            title= get_type_name(tool).replace('etl.','')+' '+tool.column;
            list_datas =  to_list(progress_indicator(get_keys(get_mount(ex_generate(self.tools[:i],init=False),take=count), attr), count=count,title=title));
            keys= ','.join(attr.__dict__.keys())
            print('%s, %s, %s'%(str(i),title,keys))

    def distribute(self ,take=90999999,skip=0,port= None,monitor_connector_name=None,table_name=None):
        import distributed
        self._master= distributed.Master(
        self._master.start_project(self._proj, self.name,take,skip,port,monitor_connector_name,table_name))

    def _pl_generator(self,take=-1,skip=0):
        mapper, reducer, parallel = parallel_map(self.tools)
        if parallel is None:
            print 'this script do not support pl...'
            return

        count_per_group = parallel.count_per_thread if parallel is not None else 1;
        task_generator = extends.group_by_mount(ex_generate(mapper), count_per_group, take, skip);
        task_generator = extends.progress_indicator(task_generator, 'Task Dispatcher', count(mapper))
        for r in task_generator:
            yield r

    def _get_related_tasks(self,tasks):
        for r in self.tools:
            if isinstance(r, EtlBase) and r not in tasks:
                tasks.add(r.name)
                r._get_related_tasks(tasks)
    def get_related_tasks(self):
        tasks=set()
        self._get_related_tasks(tasks)
        return tasks

    def rpc(self,method='finished',server='127.0.0.1',port=60007,take=-1,skip=0):
        import requests

        if method in ['finished','dispatched','clean']:
            url="http://%s:%s/task/query/%s"%(server,port,method);
            data= json.loads(requests.get(url).content)
            print 'remain: %s'%(data['remain'])
            return get(data[method],count=1000000)
        elif method=='insert':
            url="http://%s:%s/task/%s"%(server,port,method);
            tasks=self.get_related_tasks()
            n_proj=convert_dict(self._proj);
            for k,v in n_proj.items():
                if  isinstance(v,dict) and v.get('Type',None)=='ETLTask' and v['name'] not in tasks:
                    del n_proj[k]
            id=0;
            for task in self._pl_generator(take,skip):
                job={'proj':n_proj,'name':self.name,'tasks':task,'id':id}
                id+=1
                res=requests.post(url,json=job)
            print 'total push tasks: %s'%(id);



    def stop_server(self):
        if self._master is None:
            return 'server is not exist';
        self._master.manager.shutdown();
    def __str__(self):
        def conv_value(value):
            if is_str(value):
                value= value.replace('\n',' ').replace('\r','');
                sp="'"
                if value.find("'")>=0:
                    if value.find('"')>=0:
                        sp="'''"
                    else:
                        sp='"'
                return "%s%s%s"%(sp,value,sp);
            return value;
        array = [];
        array.append('##task name:%s'%self.name);
        array.append('.clear()')
        for t in self.tools:
            typename = get_type_name(t);
            s = ".%s(%s" % (typename, conv_value(t.column));
            attrs = [];
            defaultdict = type(t)().__dict__;
            for att in t.__dict__:
                value = t.__dict__[att];
                if att in ['one_input','OneOutput', 'column', '_m_yield']:
                    continue
                if not isinstance(value, ( int, bool, float)) and not is_str(value):
                    continue;
                if value is None or att not in defaultdict or defaultdict[att] == value:
                    continue;
                attrs.append(',%s=%s' % (att.lower(), conv_value(value)))
            if any(attrs):
                s += ''.join(attrs)
            s+=')\\'
            array.append(s)
        return '\n'.join(array);

    def query(self, etl_count=100, execute=False):
        tools=self.tools[:etl_count];
        for r in ex_generate(tools,execute=execute):
            yield r;

    def execute(self, etl_count=100, execute=True):
        count= force_generate(progress_indicator( self.query(etl_count,execute)))
        print('task finished,total count is %s'%(count))



    def get(self,take=10, format='print', etl=100, skip=0,execute=False,paras=None):
        datas= get_keys(get_mount(self.query(etl,execute=execute), take, skip),cols);
        return get(datas,format,count=take,paras=paras);

    def m_execute(self, thread_count=10, execute=True, take=999999, skip=0):
        import threadpool
        pool = threadpool.ThreadPool(thread_count)
        mapper,reducer,parallel= parallel_map(self.tools);
        count_per_group = parallel.count_per_thread if parallel is not None else 1;
        mapper_generator= generate(mapper,execute=execute);
        def function(item):
            def get_task_name():
                if parallel is None:
                    return 'sub task';
                return query(item,parallel.column);
            generator= generate(reducer, item, execute);
            count = force_generate(progress_indicator(generator,title=get_task_name()))

        requests = threadpool.makeRequests(function, to_list(group_by_mount(mapper_generator, count_per_group, skip=skip, take=take)));
        [pool.putRequest(req) for req in requests]
        pool.wait()






