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
MERGE_TYPE_CROSS='cross'
MERGE_TYPE_MERGE='merge'

GENERATE_DOCS='docs'
GENERATE_DOC= 'doc'
GENERATE_NONE='none';
cols=EObject()

GET_HTML='html'
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
        from pytz import utc
        from time import mktime
        def get_time(mtime):
            ts = mktime(utc.localize(mtime).utctimetuple())
            return ts;
        result=None;
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
    def transform(self,data):
        pass;
    def process(self,data):
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
            def edit_data(col, ncol=None):
                ncol = ncol if ncol != '' and  ncol is  not None  else col;
                if col!='' and  col not in d:
                    return
                if self.one_input:
                    res = self.transform(d[col]);
                    d[ncol]=res;
                else:
                    ncol = ncol if ncol != '' and  ncol is  not None else col;
                    self.transform(d,col,ncol);
            column=self.column;
            if is_str(self.column):
                if self.column.find(':')>0:
                    column=para_to_dict(self.column,',',':')
                elif self.column.find(' ')>0:
                    column=[r.strip() for r in self.column.split(' ')]
            if isinstance(column, dict):
                for k,v in column.items():
                    edit_data(k,v);
            elif isinstance(column, (list, set)):
                for k in column:
                    edit_data(k)
            else:
                edit_data(column, self.new_col)
            yield d;

class Executor(ETLTool):
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
                if self.stop_while:
                    break;
class Generator(ETLTool):
    def __init__(self):
        super(Generator, self).__init__()
        self.mode= 'Append'
        self.pos= 0;
    def generate(self,generator):
        pass;

    def process(self, generator):
        if generator is None:
            return  self.generate(None);
        else:
            if self.mode== MERGE_APPEND:
                return append(generator, self.process(None));
            elif self.mode==MERGE_TYPE_MERGE:
                return merge(generator, self.process(None));
            else:
                return cross(generator, self.generate)


EXECUTE_INSERT='OnlyInsert'
EXECUTE_SAVE='Save'
EXECUTE_UPDATE='Update'


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




class DbEX(Executor):
    def __init__(self):
        super(DbEX, self).__init__();
        self.connector = '';
        self.mode = EXECUTE_INSERT
        self.table = '';

    def init(self):
        self._connector = self._proj.connectors[self.connector];
        self._connector.init()
        self._table = self._connector._db[self.table];


    def process(self,datas):
        etype = self.mode;
        table = self._table;
        work = {'OnlyInsert': lambda d: table.save(d),'InsertOrUpdate':lambda d: table.save(d)};
        for data in datas:
            ndata= data.copy()
            work[etype](ndata);
            yield data;



class DbGE(Generator):
    def __init__(self):
        super(Generator, self).__init__();
        self.connector = '';
        self.table = '';

    def init(self):
        self._connector = self._proj.connectors[self.connector];
        self._connector.init()
        self._table = self._connector._db[self.table];
    def generate(self,data):
        for data in self._table.find():
            yield data;



def set_value(data, value, col, ncol=None):
    if ncol!='' and ncol is not None:
        data[ncol]=value;
    else:
        data[col]=value;

class RegexFT(Filter):
    def __init__(self):
        super(RegexFT, self).__init__();
        self.script=''
    def init(self):
        self.regex = re.compile(self.script);
        self.count=1;

    def filter(self,data):
        v = self.regex.findall(data);
        if v is None:
            return False;
        else:
            return self.count <= len(v)

class RangeFT(Filter):
    def __init__(self):
        super(RangeFT, self).__init__();
        self.Min=0;
        self.Max=100;
    def filter(self,item):
        f = float(item)
        return self.Min <= f <= self.Max;

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


class AddNewTF(Transformer):
    def __init__(self):
        super(AddNewTF, self).__init__()
        self.value=''
        self._m_yield=True
    def m_transform(self, data, col):
        data[col] = self.value;
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
        self.script=None;
    def transform(self, data,col,ncol ):
        del data[col];

class HtmlTF(Transformer):
    def __init__(self):
        super(HtmlTF, self).__init__()
        self.one_input=True;
        self.mode='decode'
    def transform(self, data):
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
        self.re=False;

    def transform(self, data):
        if self.re:
            return re.sub(self.regex, self.new_value, data);
        else:
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
        self.split_char= '';
        self.one_input = True;
        self.split_blank=False;
        self.split_null=False;
        self.index=0;

    def init(self):
        self.splits = self.split_char.split(' ');
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
        if len(r) < self.index:
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
        self.script='value'
        self.mode=GENERATE_NONE;

    def init(self):
        self._m_yield= self.mode == GENERATE_DOCS;

    def _get_data(self, data, col):
        value = data[col] if col in data else '';
        if is_str(self.script ):
            result = self._eval_script({'value': value,'data':data}, data);
        else:
            result = self.script(data);
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
        if is_str(self.script):
            result = self._eval_script(data);
        elif inspect.isfunction(self.script):
            result = self.script(data)
        if result==None:
            return True;
        return result;

class CrawlerTF(Transformer):
    def __init__(self):
        super(CrawlerTF, self).__init__()
        self.selector='';
        self.max_try=1;
        self.is_regex=False
        self.post_data=''

    def init(self):
        if is_str(self.selector):
            dic= self._proj.modules;
            if self.selector in dic:
                self._crawler= dic[self.selector]
            else:
                sys.stderr.write('crawler {name} can not be found in project'.format(name=self.selector))
        else:
            self._crawler=self.selector;
        self.__buff = {};
        self._m_yield = self._crawler.multi   and len(self._crawler.xpaths)>0;
    def _get_data(self,url,post_data=''):
        crawler = self._crawler;
        buff = self.__buff;
        if post_data is None:
            post_data=''
        key=url+post_data;
        if key in buff:
            data = buff[key];
        else:
            data = crawler.crawl(url,post_data);
            if len(buff) < 100:
                buff[key] = data;
        return data;
    def m_transform(self,data,col):
        url = data[col];
        post_data= query(data,self.post_data);
        datas = self._get_data(url,post_data)
        for d in datas:
            yield d;
    def transform(self, data, col,ncol):

        post_data= query(data,self.post_data);
        ndata=self._get_data(data[col],post_data)
        for k,v in ndata.items():
            data[k]=v;

class PyQueryTF(Transformer):
    def __init__(self):
        super(PyQueryTF, self).__init__()
        self.script = ''
        self._m_yield = True;
        self.mode = GET_TEXT
        self.m_yield = False;

    def m_transform(self, data, col):
        from pyquery import PyQuery as pyq
        root = pyq(data[col]);
        if root is None:
            yield data;
            return;
        nodes =  root(self.script);
        for node in nodes:
            ext = {'text': node.text() , 'html': to_str(node)};
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
            res= node.text()
        elif mode == GET_HTML:
            res=to_str(node);
        elif mode== GET_COUNT:
            res = len(nodes);
        else:
            res= nodes;
        data[ncol] = res;


class XPathTF(Transformer):
    def __init__(self):
        super(XPathTF, self).__init__()
        self.script= ''
        self._m_yield = True;
        self.mode= GET_TEXT
        self.m_yield=False;

    def init(self):
        self._m_yield=self.m_yield;

    def m_transform(self,data,col):
        from lxml import etree
        root = spider._get_etree(data[col]);
        tree = etree.ElementTree(root)
        if tree is None:
            yield data;
            return;
        nodes = tree.search_xpath(self.script);
        for node in nodes:
            html = etree.tostring(node).decode('utf-8');
            ext = {'Text': spider.get_node_text(node), 'HTML': html};
            yield ext;

    def transform(self, data,col,ncol):
        from lxml import etree
        d= data[col]
        if d is None or d=='':
            return None
        root = spider._get_etree(data[col]);
        tree = etree.ElementTree(root)
        if tree is None:
            return None;
        mode=self.mode;
        if self.script== '' or self.script is None:
            node_path = xspider.search_text_root(tree, root);
        else:
            node_path=self.script;
        if node_path is None:
            return;
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
        else:
            res=len(nodes);

        data[ncol]=res;

class TnTF(Transformer):

    def __init__(self):
        super(TnTF,self).__init__()
        self.rule=None;
        self.one_input=True;
    def init(self):
        import tn
        if not hasattr(self,'_core'):
            self._core = tn.core;
        self._core.init_py_rule(tn)
        self._core.rebuild()
    def transform(self,data):
        result=self._core.extract(data, entities=[self.rule]);
        if any(result):
            return result[0]['#rewrite'];
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

    def init(self):
        self._m_yield= self.mode == GENERATE_DOCS;

    def m_transform(self,data,col):
        js=json.loads(data[col]);
        for j in js:
            yield j;

    def transform(self, data,col,ncol):
        js = json.loads(data[col]);
        if self.mode==GENERATE_DOC:
            merge(data, js);
        else:
            data[ncol]=js

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
        if interval>0:
            values=range(min_value,max_value,interval);
        else:
            values=range(max_value,min_value,interval)
        for i in values:
            item= {self.column:round(i, 5)}
            yield item;






class SubEtlBase(object):
    def __init__(self):
        super(SubEtlBase, self).__init__()
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
        data=data.copy()
        sub_etl = self._get_task( data)
        buf = self.range.split(':');
        start = int(buf[0])
        end = int(buf[1])
        return sub_etl.tools[start:end]


class EtlGE(Generator, SubEtlBase):
    def __init__(self):
        super(EtlGE, self).__init__()
    def generate(self,data):
        for r in generate(self._get_tools(data)):
            yield r;


class EtlEX(Executor, SubEtlBase):
    def __init__(self):
        super(EtlEX, self).__init__()
    def execute(self,data):
        if spider.is_none(self.new_col):
            doc = data.copy();
        else:
            doc = {};
            merge_query(doc, data, self.new_col + " " + self.column);

        tools=self._get_tools(doc)
        count=0;
        for r in generate(tools, [doc],execute=self.enabled):
            count+=1;
        print('subtask:'+to_str(count))
        return data;

class EtlTF(Transformer, SubEtlBase):

    def __init__(self):
        super(EtlTF,self).__init__()
        self.cycle=False;
        self._m_yield=True

    def m_transform(self,data,col):
        new_data = data;
        if self.cycle:
            while new_data[col] != '':
                result = first_or_default(generate(self._get_tools(new_data), [new_data.copy()]))
                if result is None:
                    break
                yield result.copy();
                new_data = result;
        else:
            for r in generate(self._get_tools(data), [data]):
                yield r
    def transform(self,data,col,ncol):
        for r in generate(self._get_tools(data),[data]):
            merge(data, r);
            break



class TextGE(Generator):
    def __init__(self):
        super(TextGE, self).__init__()
        self.content='';
    def init(self):
        value=self.content.replace('\n','\001').replace(' ','\001')
        self._arglists= [r.strip() for r in value.split('\001')];
    def generate(self,data):
        for i in range(int(self.pos), len(self._arglists)):
            yield {self.column: self._arglists[i]}



class BaiduLocation(Transformer):
    pass;


class GetIPLocation(Transformer):
    pass;

class GetRoute(Transformer):
    pass;

class NearbySearch(Transformer):
    pass;

class NlpTF(Transformer):
    pass;

class TransTF(Transformer):
    pass;
class JoinDBTF(Transformer):
    pass;

class RepeatTF(Transformer):
    pass;
class ResponseTF(Transformer):
    pass;

class Time2StrTF(Transformer):
    pass;

class BfsGE(Generator):
    pass;

class DictTF(Transformer):
    pass;

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
        self.take=None;
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
        self.SavePath='';


    def execute(self,data):
        save_path = query(data, self.SavePath);
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


    def load_xml(self,path):


        tree = ET.parse(path);

        root = tree.getroot();
        root = root.find('Doc');

        def init_from_httpitem(config, item):
            attrib = config.attrib;
            paras = spider.para_to_dict(attrib['Parameters'], '\n', ':');
            item.Headers = paras;
            item.Url = attrib['URL'];
            post = 'Postdata';
            if post in attrib:
                item.postdata = attrib[post];
            else:
                item.postdata = None;
        def get_child_node(roots, name):
            for etool in roots:
                if etool.get('name') == name or etool.tag == name:
                    return etool;
            return None;

        def set_attr(etl, key, value):
            value=value.strip()
            if key in ['Group', 'Type']:
                return
            intattrs = re.compile('Max|Min|count|index|interval|pos');
            if value in ['True', 'False']:
                setattr(etl, key, True if value == 'True' else False);
            elif intattrs.search(key) is not None:
                try:
                    t = float(value);
                    setattr(etl, key, t);
                except ValueError:
                    print('it is a ValueError')
                    setattr(etl, key, value);

            else:
                setattr(etl, key, value);

        def conv_key(key):
            if key.find('selector') >= 0:
                return  'selector';
            elif key=='NewColumn':
                return 'new_col';
            return key;
        for etool in root:
            if etool.tag == 'children':
                etype = etool.get('Type');
                name = etool.get('name');
                if etype == 'SmartETLTool':
                    tool = ETLTask()
                    tool.name=name;
                    for m in etool:
                        if m.tag == 'children':
                            type = m.attrib['Type']
                            etl =  eval(type+'()');
                            etl._proj =self
                            for key in m.attrib:
                                value=m.attrib[key];
                                set_attr(etl, conv_key(key), value );
                            tool.tools.append(etl);
                elif etype == 'SmartCrawler':
                    tool = spider.SmartCrawler();
                    tool.requests = spider.Requests()
                    tool.name = etool.attrib['name'];
                    tool.multi = etool.attrib['multi']
                    tool.root = etool.attrib['root']
                    httpconfig = get_child_node(etool, 'HttpSet');
                    init_from_httpitem(httpconfig, tool.requests);
                    login = get_child_node(etool, 'login');
                    if login is not None:
                        tool.login = spider.Requests()
                        init_from_httpitem(login, tool.login);
                    tool.xpaths = [];
                    for child in etool:
                        if child.tag == 'children':
                            xpath = spider.xpath();
                            xpath.name = child.attrib['name'];
                            xpath.search_xpath = child.attrib['script'];
                            xpath.IsHtml = child.attrib['IsHtml'] == 'True'
                            tool.xpaths.append(xpath);

                self.modules[name] = tool;
                setattr(self, name, tool);
            elif etool.tag == 'DBConnections':
                for tool in etool:
                    if tool.tag == 'children':
                        connector = EObject();
                        for key in tool.attrib:
                            set_attr(connector, key, tool.attrib[key]);
                        self.connectors[connector.name] = connector;

        #print('load project success')
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
                crawler.requests = spider.Requests()
                dict_copy_poco(crawler.requests, module['requests'])
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



def generate(tools, generator=None, init=True, execute=False, enabled=True):
    if tools is None:
        return generator;
    for tool in tools:
        if tool.enabled == False and enabled == True:
            continue
        if isinstance(tool,Executor) and execute==False:
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
        if isinstance(tools[0],Generator):
            return tools[:1],tools[1:],None;
        else:
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
            list_datas =  to_list(progress_indicator(get_keys(get_mount(generate(self.tools[:i],init=False),take=count), attr), count=count,title=title));
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
        mapper,reducer,tolist= parallel_map(tools)
        for tool in tools:
            tool.init();
        for r in generate(mapper,None,init=False, execute=execute):
            if  isinstance(r,dict):
                r=[r]
            for p in  generate(reducer, r, init=False,execute= execute):
                yield p;


    def execute(self, etl_count=100, execute=True):
        count= force_generate(progress_indicator( self.query(etl_count,execute)))
        print('task finished,total count is %s'%(count))



    def get(self, format='print', etl_count=100, take=10, skip=0):
        datas= get_keys(get_mount(self.query(etl_count), take, skip),cols);
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






