# coding=utf-8
import csv
import json;
import os;
import time;
import traceback
import urllib
import xml.etree.ElementTree as ET

import spider
import xspider
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
GET_ARRAY='array'


def __get_match_counts(mat):
    return mat.lastindex if mat.lastindex is not None else 1;

class ETLTool(EObject):
    def __init__(self):
        super(ETLTool, self).__init__()
        self.enabled=True;
        self.column = ''
    def process(self, data):
        return data
    def init(self):
        pass;

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
                        my=merge_query(p, r, self.new_col);
                        yield my;
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
        self.debug = False
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
        self._connector = self._proj.connectors[self.selector];
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
        self._m_yield=True
        self.mode=GENERATE_NONE


    def m_transform(self, data, col):
        if self.mode==GENERATE_NONE:
            data[col] = self.script;
            yield data;
        elif self.mode==GENERATE_DOC:
            merge(data, self.script)
            yield data;
        else:
            for d in self.script:
                yield merge(data.copy(),d)





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

class ReplaceTF(RegexTF):
    def __init__(self):
        super(ReplaceTF, self).__init__()
        self.new_value = '';
        self.one_input=True
        self.mode='str';

    def transform(self, data):
        if self.mode=='re':
            return re.sub(self.regex, self.new_value, data);
        else:
            if data is None:
                return None
            return data.replace(self.script,self.new_value);
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
        self.index=0;

    def init(self):
        self.splits = self.script.split(' ');
        if '' in self.splits:
            self.splits.remove('')
        if self.split_blank==True:
            self.splits.append(' ');
        if self.split_null==True:
            self.splits.append('\n')
        self.index = int(self.index)
    def transform(self, data):
        if len(self.splits)==0:
            return data;
        for i in self.splits:
            data = data.replace(i, '\001');
        r=data.split('\001');
        if len(r) <= self.index:
            return data;
        return r[self.index];

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
        self.mode=GENERATE_NONE;

    def init(self):
        self._m_yield= self.mode == GENERATE_DOCS;

    def _get_data(self, data, col):
        if col=='':
            if is_str(self.script):
                self._eval_script({'data': data}, data);
            else:
                self.script(data);
            return None
        else:
            value = data[col]
            if is_str(self.script ):
                result = self._eval_script({'value': value,'data':data}, data);
            else:
                result = self.script(value);
        return result;
    def m_transform(self,data,col):
        js= self._get_data(data,col)
        if isinstance(js,list):
            for j in js:
                yield j;
    def transform(self, data,col,ncol):
        js = self._get_data( data,col)
        if self.mode == GENERATE_DOC:
            merge(data, js);
        else:
            if col=='':
                return
            col= ncol if ncol is not None else col;
            data[col]=js;


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
            result = self._eval_script({'value': value, 'data': data}, data);
        elif inspect.isfunction(self.script):
            result = self.script(data)
        if result==None:
            return False
        return result;



class CrawlerTF(Transformer):
    def __init__(self):
        super(CrawlerTF, self).__init__()
        self.selector='';
        self.is_regex=False
        self.post_data=''
        self.refresh=False
        self.debug=False
        self.__buff = {};
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
        if is_str(self.selector):
            dic= self._proj.modules;
            if self.selector in dic:
                self._crawler= dic[self.selector]
            elif self.selector!='':
                sys.stderr.write('crawler {name} can not be found in project'.format(name=self.selector))
            else:
                self._crawler=spider.SmartCrawler();
        else:
            self._crawler=self.selector;
        if self.refresh:
            self.__buff={}
        if self.pl_count>1:
            self._m_process=True

        self._m_yield = self._crawler.multi   and len(self._crawler.xpaths)>0;


    def _get_data(self,reqs,default_key='Content'):
        buff = self.__buff;
        def _get_request_info(req):
            data = req[0]
            col = req[1]
            url = data[col];
            post_data = query(data, self.post_data);
            if self.debug:
                print(url)
            if post_data is None:
                post_data = ''
            return url,post_data,data

        def _filter(url,post_data):
            key = url + str(post_data)
            if key in buff:
                data = buff[key];
            else:
                data=False
            return data

        reqs2=[]
        datas=[]
        for req in reqs:
            url, post_data,data = _get_request_info(req)
            result= _filter(url,post_data)
            if result==False:
                reqs2.append((url,post_data))
                datas.append(data)
            else:
                yield data,result
        for req,data,result in  zip(reqs2,datas,self._crawler.crawls(reqs2,async= self.pl_count>1,get_data=True,default_key=default_key)):
            url,post=req
            key=url+str(post)
            if len(buff) < 100:
                buff[key] = result
            yield data,result;

    def m_transform(self,data,col):
        res=self._get_data([(data,col)]);
        for d in res:
            raw, datas = d
            for r in datas:
                yield r;
    def transform(self, data, col,ncol):
        for r in  self._get_data([(data, col)],default_key=ncol):
            for k,v in r[1].items():
                data[k]=v;
            return

class PyQueryTF(Transformer):
    def __init__(self):
        super(PyQueryTF, self).__init__()
        self.script = ''
        self._m_yield = True;
        self.mode = GET_TEXT
        self.m_yield = True;


    def init(self):
        self._m_yield = self.m_yield;

    def m_transform(self, data, col):
        from pyquery import PyQuery as pyq
        root = pyq(data[col]);
        if root is None:
            yield data;
            return;
        nodes =  root(self.script);
        for node in nodes:
            ext = {'text': spider.get_node_text( node) , 'html': node};
            yield ext;

    def transform(self, data, col, ncol):
        d = data[col]
        if d is None or d == '':
            return None
        from pyquery import PyQuery as pyq
        root = pyq(data[col]);
        if root is None:
            return None;
        mode = self.mode;
        nodes = root(self.script);
        if len(nodes) < 1:
            return
        node = nodes[0]
        if mode == GET_TEXT:
            res= spider.get_node_text(node)
        elif mode == GET_HTML:
            res=to_str(node);
        elif mode== GET_COUNT:
            res = len(nodes);
        else:
            res= nodes;
        data[ncol] = res;

class TreeTF(Transformer):
    def __init__(self):
        super(TreeTF, self).__init__()
        self.one_input = True;


    def transform(self, data):
        from lxml import etree
        root = spider._get_etree(data);
        tree = etree.ElementTree(root)
        return tree


class XPathTF(Transformer):
    def __init__(self):
        super(XPathTF, self).__init__()
        self.script= ''
        self._m_yield = True;
        self.mode= GET_TEXT
        self.m_yield=False;

    def init(self):
        self._m_yield=self.m_yield;

    def _trans(self,data):
        from lxml import etree
        if isinstance(data, (str, unicode)):
            root = spider._get_etree(data);
            tree = etree.ElementTree(root)
        else:
            tree = data
        return tree;
    def m_transform(self,data,col):
        from lxml import etree
        tree= self._trans(data[col]);
        if tree is None:
            yield data;
            return;
        nodes = tree.xpath(self.script);
        for node in nodes:
            html = etree.tostring(node).decode('utf-8');
            ext = {'text': spider.get_node_text(node), 'html': html};
            yield ext;

    def transform(self, data,col,ncol):
        from lxml import etree
        tree = self._trans(data[col]);
        if tree is None:
            return None;
        mode=self.mode;
        node_path = query(data, self.script);
        if node_path is None:
            return;
        if node_path== '' or node_path is None:
            return
        nodes = tree.xpath(node_path);
        if len(nodes) < 1:
            return
        node = nodes[0]
        if mode== GET_TEXT:
            if hasattr(node, 'text'):
                res = spider.get_node_text(node);
            else:
                res = to_str(node)
        elif mode==GET_HTML:
            res =  etree.tostring(node).decode('utf-8');
        elif mode==GET_NODE:
            res=node
        else:
            res=len(nodes);

        data[ncol]=res;

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
        self.mode= GENERATE_NONE
        self.conv_mode=CONV_DECODE

    def init(self):
        self._m_yield= self.mode == GENERATE_DOCS;

    def m_transform(self,data,col):
        js=json.loads(data[col]);
        for j in js:
            yield j;

    def transform(self, data,col,ncol):
        if self.conv_mode== CONV_DECODE:

            js = json.loads(data[col]);
            if self.mode==GENERATE_DOC:
                merge(data, js);
            else:
                data[ncol]=js
        else:
            data[ncol]=json.dumps(data[col])

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
        if selector not in self._proj.modules:
            sys.stderr.write('sub task %s  not in current project' % selector);
        sub_etl = self._proj.modules[selector];
        return sub_etl;


    def _get_tools(self,data):
        if data is not None:
            data=data.copy()
        sub_etl = self._get_task( data)
        buf = self.range.split(':');
        start = int(buf[0])
        end = int(buf[1])
        tools= sub_etl.tools[start:end]
        for tool in tools:
            tool.init()
        return tools

    def _generate(self, data,execute=False,init=False):
        doc=None
        if spider.is_none(self.new_col):
            if data is not None:
                doc = data.copy();
        else:
            doc={}
            merge_query(doc, data, self.new_col + " " + self.column);
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
            while new_data[col] != '':
                result = first_or_default(self._generate(data))
                if result is None:
                    break
                yield result.copy();
                new_data = result;
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





class RepeatTF(Transformer):
    pass;


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
        self.mode= CONV_ENCODE
        self.script=''
        self._m_yield = True;

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

class MergeRepeatTF(Transformer):
    pass;

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
        save_path = query(data, self.path);
        (folder,file)=os.path.split(save_path);
        if not os.path.exists(folder):
            os.makedirs(folder);
        urlpath= data[self.column];
        newfile= open(save_path,'wb');
        newdata= spider.get_web_file(urlpath);
        if newdata is None:
            return
        newfile.write(newdata);
        newfile.close();
        return data








class Project(EObject):
    def __init__(self):
        self.modules={};
        self.connectors={};
        self.edit_time= time.time();
        self.desc="edit project description here";
        self.author='desert';

    def clear(self):
        self.modules.clear()
        self.connectors.clear()
        return self;
    def pop(self,name):
        self.modules.pop(name)
        return self;

    def dumps_json(self):
        dic = convert_dict(self )
        return json.dumps(dic, ensure_ascii=False, indent=2)

    def dumps_yaml(self):
        import yaml
        dic = convert_dict(self)
        return yaml.dump(dic)

    def dump_yaml(self, path):
        with open(path, 'w', encoding='utf-8') as f:
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
        with open(path,'w',encoding='utf-8') as f:
            f.write(self.dumps_json());

    def load_json(self,path):
        with open(path,'r',encoding='utf-8') as f:
            js= f.read();
            return self.loads_json(js)


    def load_dict(self,dic):
        connectors =dic.get('connectors',{});
        for key, item in connectors.items():
            conn = eval('%s()' % item['Type']);
            dict_copy_poco(conn,item);
            self.connectors[key] = conn

        modules =dic.get('modules',{});
        for key, module in modules.items():
            obj_type = module['Type']

            if obj_type=='ETLTask':
                crawler = ETLTask();
                for r in module['tools']:
                    etl =eval('%s()'%r['Type']);
                    for attr, value in r.items():
                        if attr in ['Type']:
                            continue;
                        setattr(etl, attr, value);
                    etl._proj = self;
                    crawler.tools.append(etl)
            elif obj_type=='SmartCrawler':
                crawler = spider.SmartCrawler();
                dict_copy_poco(crawler, module);
                paths = module.get('xpaths',{});
                for r in paths:
                    xpath = spider.XPath()
                    dict_copy_poco(xpath, r);
                    crawler.xpaths.append(xpath)
            setattr(self,key,crawler);
            if crawler is not None:
                self.modules[key] = crawler;
        #print('load project success')
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
    def clear(self):
        self.tools=[]
        return self;

    def pop(self,i):
        self.tools.pop(i);
        return self;

    def check(self,count=10):
        c=  force_generate(foreach(self.tools,lambda x:x.init()))
        for i in range(1,len(self.tools)):
            attr=EObject()
            tool=self.tools[i];
            title= get_type_name(tool).replace('etl.','')+' '+tool.column;
            list_datas =  to_list(progress_indicator(get_keys(get_mount(ex_generate(self.tools[:i],init=False),take=count), attr), count=count,title=title));
            keys= ','.join(attr.__dict__.keys())
            print('%s, %s, %s'%(str(i),title,keys))

    def distribute(self ,take=90999999,skip=0,port= None,monitor_connector_name=None,table_name=None):
        import distributed
        self._master= distributed.Master(self._proj, self.name,monitor_connector_name,table_name)
        self._master.start(take,skip,port)
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
            s = ".%s(%s" % (typename, conv_value(t.Column));
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



    def get(self,take=10, format='print', etl_count=100, skip=0,execute=False):
        datas= get_keys(get_mount(self.query(etl_count,execute=execute), take, skip),cols);
        return get(datas,format,count=take);

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






