# coding=utf-8
import time;
import re;
import extends
import urllib
import spider;
import json;
import xml.etree.ElementTree as ET
import csv
import xspider;
import os;
import  sys;
import traceback

if extends.PY2:
    pass;
else:
    import html

MERGE_APPEND= 'Append';
MERGE_TYPE_CROSS='Cross'
MERGE_TYPE_MERGE='Merge'

GENERATE_DOCS=u'文档列表'
GENERATE_DOC= u'单文档'
GENERATE_NONE=u'不进行转换';
cols=extends.EObject()

GET_HTML=0
GET_TEXT=1
GET_COUNT=2


def __get_match_counts(mat):
    return mat.lastindex if mat.lastindex is not None else 1;

class ETLTool(extends.EObject):
    def __init__(self):
        self.enabled=True;
        self.column = ''
    def process(self, data):
        return data
    def init(self):
        pass;

    def _eval_script(self, global_para=None, local_para=None):
        if self.script == '':
            return True
        if not extends.is_str(self.script):
            return self.script(self)
        import time;
        from pytz import utc, timezone
        from datetime import datetime
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
                        my=extends.merge_query(p, r, self.new_col);
                        yield my;
                except Exception as e:
                    traceback.print_exc()
            return;
        for d in data:
            def edit_data(col, ncol=None):
                if col not in d:
                    return
                if self.one_input:
                    res = self.transform(d[col]);
                    ncol = ncol if ncol != '' and  ncol is  not None  else col;
                    d[ncol]=res;
                else:
                    ncol = ncol if ncol != '' and  ncol is  not None else col;
                    self.transform(d,col,ncol);
            if isinstance(self.column, dict):
                for k,v in self.column.items():
                    edit_data(k,v);
            elif isinstance(self.column, (list, set)):
                for k in self.column:
                    edit_data(k)
            else:
                edit_data(self.column, self.new_col)
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
                return extends.append(generator, self.process(None));
            elif self.mode==MERGE_TYPE_MERGE:
                return extends.merge(generator, self.process(None));
            else:
                return extends.cross(generator, self.generate)


EXECUTE_INSERT='OnlyInsert'
EXECUTE_SAVE='Save'
EXECUTE_UPDATE='Update'


class MongoDBConnector(extends.EObject):
    def __init__(self):
        super(MongoDBConnector, self).__init__()
        self.connect_str='';
        self.db=''

    def init(self):
        import pymongo
        client = pymongo.MongoClient(self.connect_str);
        self._db = client[self.db];



class FileConnector(extends.EObject):
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
        field = extends.get_keys(datas);
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
        if extends.is_str(data):
            return data.strip() != '';
        return True;


class AddNewTF(Transformer):
    def __init__(self):
        super(AddNewTF, self).__init__()
        self.value=''

    def transform(self,data):
        return self.value;

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

    def transform(self, data):
        return html.escape(data) if self.ConvertType == 'Encode' else html.unescape(data);


class UrlTF(Transformer):
    def __init__(self):
        super(UrlTF, self).__init__()
        self.one_input = True;
    def transform(self, data):
        if self.ConvertType == 'Encode':
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
            columns = [extends.to_str(get_value(data,r)) for r in self.merge_with.split(' ')]
        columns.insert(0, data[col] if col in data else '');
        res = self.script;
        for i in range(len(columns)):
            res = res.replace('{' +str(i) + '}', extends.to_str(columns[i]))
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
        item = re.findall(self.regex, extends.to_str(data));
        if self.index < 0:
            return '';
        if len(item) <= self.index:
            return '';
        else:
            r = item[self.index];
            return r if extends.is_str(r) else r[0];

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
        item = re.findall(self.regex, extends.to_str(data));
        if self.index < 0:
            return '';
        if len(item) <= self.index:
            return '';
        else:
            r = item[self.index];
            return r if extends.is_str(r) else r[0];

class SplitTF(Transformer):
    def __init__(self):
        super(SplitTF, self).__init__()
        self.split_char= '';
        self.one_input = True;
        self.from_back=False;
        self.split_blank=False;
        self.split_null=False;

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
        if self.from_back:
            return r[-self.index-1];
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
        if extends.is_str(self.script ):
            result = self._eval_script({'value': value}, data);
        else:
            result = self.script(data);
        return result;
    def m_transform(self,data,col):
        js= self._get_data(data,col)
        for j in js:
            yield j;
    def transform(self, data,col,ncol):
        js = self._get_data( data,col)
        if self.mode == GENERATE_DOC:
            extends.merge(data, js);
        else:
            col= ncol if ncol is not None else col;
            data[col]=js;


class PythonGE(Generator):
    def __init__(self):
        super(PythonGE, self).__init__()
        self.script='xrange(1,20,1)'
    def can_dump(self):
        return  extends.is_str(self.script);
    def generate(self,generator):
        import inspect;
        if extends.is_str(self.script):
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
        return  extends.is_str(self.script);
    def filter(self, data):
        import inspect
        data=data.copy();
        if extends.is_str(self.script):
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


    def init(self):
        if extends.is_str(self.selector):
            dic= self._proj.modules;
            if self.selector in dic:
                self._crawler= dic[self.selector]
            else:
                sys.stderr.write('crawler {name} can not be found in project'.format(name=self.selector))
        else:
            self._crawler=self.selector;
        self.__buff = {};
        self._m_yield = self._crawler.multi   and len(self._crawler.xpaths)>0;
    def _get_data(self,url):
        crawler = self._crawler;
        buff = self.__buff;
        if url in buff:
            data = buff[url];
        else:
            data = crawler.crawl(url);
            if len(buff) < 100:
                buff[url] = data;
        return data;
    def m_transform(self,data,col):
        url = data[col];
        datas = self._get_data(url)
        for d in datas:
            yield d;
    def transform(self, data, col,ncol):
        ndata=self._get_data(data[col])
        for k,v in ndata.items():
            data[k]=v;


class XPathTF(Transformer):
    def __init__(self):
        super(XPathTF, self).__init__()
        self.xpath=''
        self._m_yield = True;
        self.mode=False;
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
        nodes = tree.xpath(self.xpath);
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
        if self.xpath=='' or self.xpath is None:
            node_path = xspider.search_text_root(tree, root);
        else:
            node_path=self.xpath;
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
                res = extends.to_str(node)
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
        import tn;
        self._core =tn.core;
        self._core.InitPyRule(tn)
        self._core.RebuildEntity()
    def transform(self,data):
        result=self._core.Extract(data, entities=[self.rule]);
        if any(result):
            return result[0]['#rewrite'];
        return '';


class ToListTF(Transformer):
    def __init__(self):
        super(ToListTF, self).__init__()
        self.count_per_thread=1;
        self._m_yield=True;

    def m_transform(self, data,col):
        yield data;

class JsonTF(Transformer):
    def __init__(self):
        super(JsonTF, self).__init__()
        self.mode=GENERATE_DOCS

    def init(self):
        self._m_yield= self.mode == GENERATE_DOCS;

    def m_transform(self,data,col):
        js=json.loads(data[col]);
        for j in js:
            yield j;

    def transform(self, data,col,ncol):
        js = json.loads(data[col]);
        if self.mode==GENERATE_DOC:
            extends.merge(data, js);
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
        interval= get_int(extends.query(generator, self.interval),1)
        max_value= get_int(extends.query(generator, self.max),1)
        min_value= get_int(extends.query(generator, self.min),1)
        if interval>0:
            values=range(min_value,max_value,interval);
        else:
            values=range(max_value,min_value,interval)
        for i in values:
            item= {self.column:round(i, 5)}
            yield item;




def _get_task(task, data):
    selector = extends.query(data, task.selector);
    if selector not in task._proj.modules:
        sys.stderr.write('sub task %s  not in current project' % selector);
    sub_etl = task._proj.modules[selector];
    return sub_etl;


class EtlGE(Generator):
    def __init__(self):
        super(EtlGE, self).__init__()
        self.selector=''
    def generate(self,data):
        sub_etl= _get_task(self, data);
        for r in generate(sub_etl.tools):
            yield r;


class EtlEX(Executor):
    def __init__(self):
        super(EtlEX, self).__init__()
        self.selector = ''
    def execute(self,data):
        sub_etl= _get_task(self, data);
        if spider.is_none(self.new_col):
            doc = data.copy();
        else:
            doc = {};
            extends.merge_query(doc, data, self.new_col + " " + self.column);
        result=(r for r in generate(sub_etl.tools, [doc]))
        count=0;
        for r in result:
            count+=1;
        print('subtask:'+extends.to_str(count))
        return data;

class EtlTF(Transformer):

    def __init__(self):
        self.cycle=False;
        self.selector = ''

    def m_transform(self,data,col):
        sub_etl = _get_task(self, data);
        newdata = data;
        if self.cycle:
            while newdata[col] != '':
                result = extends.first_or_default(generate(sub_etl.tools, [newdata.copy()]))
                if result is None:
                    break
                yield result.copy();
                newdata = result;
        else:
            doc = data.copy();
            for r in generate(sub_etl.tools, [doc]):
                yield r
    def transform(self,data,col,ncol):
        sub_etl = _get_task(self, data);
        for r in generate(sub_etl.tools,[data.copy()]):
            return extends.merge(data, r);




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
        return extends.to_str(os.path.exists(data));

class MergeRepeatTF(Transformer):
    pass;

class NumRangeFT(Transformer):
    def __init__(self):
        super(NumRangeFT, self).__init__();
        self.skip=0;
        self.take=1;
    def process(self,data):
        for r in extends.get_mount(data,self.take,self.skip):
            yield r;


class DelayTF(Transformer):
    def __init__(self):
        super(DelayTF, self).__init__();
        self.delay=100;
        self._m_yield=True;

    def m_transform(self,data):
        import time
        delay = extends.query(data, self.delay);
        time.sleep(int(delay))
        yield data;


class ReadFileTextTF(Transformer):
    pass;

class ReadFileTF(Transformer):
    pass;

class WriteFileTextTF(Transformer):
    pass;
class FolderGE(Generator):
    pass;

class TableGE(Generator):
    def __init__(self):
        super(TableGE, self).__init__()
        self.table = None;

    def can_dump(self):
        return False;
    def generate(self,generator):
        for r in self.table:
            yield r;

class TableEX(Executor):
    def __init__(self):
        super(TableEX, self).__init__()
        self.table = None;

    def can_dump(self):
        return False;

    def execute(self, data):
        table = self.table;
        table.append(data);

class FileDataTF(Transformer):
    pass;


class SaveFileEX(Executor):
    def __init__(self):
        super(SaveFileEX, self).__init__()
        self.SavePath='';


    def execute(self,data):
        save_path = extends.query(data, self.SavePath);
        (folder,file)=os.path.split(save_path);
        if not os.path.exists(folder):
            os.makedirs(folder);
        urlpath= data[self.column];
        newfile= open(save_path,'wb');
        newdata=spider.get_web_file(urlpath);
        newfile.write(newdata);
        newfile.close();








class Project(extends.EObject):
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
                    import spider;
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
                            xpath.xpath = child.attrib['xpath'];
                            xpath.IsHtml = child.attrib['IsHtml'] == 'True'
                            tool.xpaths.append(xpath);

                self.modules[name] = tool;
                setattr(self, name, tool);
            elif etool.tag == 'DBConnections':
                for tool in etool:
                    if tool.tag == 'children':
                        connector = extends.EObject();
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
            extends.dict_copy_poco(conn,item);
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
                extends.dict_copy_poco(crawler, module);
                paths = module.get('xpaths',{});
                for r in paths:
                    xpath = spider.XPath()
                    extends.dict_copy_poco(xpath, r);
                    crawler.xpaths.append(xpath)
                crawler.requests = spider.Requests()
                extends.dict_copy_poco(crawler.requests, module['requests'])
            setattr(self,key,crawler);
            if crawler is not None:
                self.modules[key] = crawler;
        #print('load project success')
        return self;




def convert_dict(obj):
    if not isinstance(obj, ( int, float, list, dict, tuple, extends.EObject)) and not extends.is_str(obj):
        return None
    if isinstance(obj, extends.EObject):
        d={}
        obj_type= type(obj);
        typename= extends.get_type_name(obj)
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



def parallel_map(tools):
    index= extends.get_index(tools,lambda x:isinstance(x,ToListTF))
    if index==-1:
        return tools,None,None;
    mapper = tools[:index]
    reducer=tools[index+1:]
    parameter= tools[index];
    return mapper,reducer,parameter;




class ETLTask(extends.EObject):
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

    def distribute(self ,take=90999999,skip=0,port= None):
        import distributed
        self._master= distributed.Master(self._proj,self.name);
        self._master.start(take,skip,port)
    def stop_server(self):
        if self._master is None:
            return 'server is not exist';
        self._master.manager.shutdown();
    def __str__(self):
        def conv_value(value):
            if extends.is_str(value):
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
            typename = extends.get_type_name(t);
            s = ".%s(%s" % (typename, conv_value(t.Column));
            attrs = [];
            defaultdict = type(t)().__dict__;
            for att in t.__dict__:
                value = t.__dict__[att];
                if att in ['one_input','OneOutput', 'column', '_m_yield']:
                    continue
                if not isinstance(value, ( int, bool, float)) and not extends.is_str(value):
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


    def execute(self, etl_count=100, execute=True, notify_count=100):
        c=0;
        for r in self.query(etl_count,execute):
            c+=1;
            if c%notify_count==0:
                print(c);
        print('task finish');



    def get(self, format='print', etl_count=100, take=10, skip=0):
        datas= extends.get_keys(extends.get_mount(self.query(etl_count), take, skip),cols);
        return extends.get(datas,format);

    def m_exec(self, thread_count=10, can_execute=True,take=999999,skip=0):
        import threadpool
        pool = threadpool.ThreadPool(thread_count)

        mapper,reducer,parameter= parallel_map(self.tools, can_execute);
        def funcs(item):
            task= generate(reducer, item, can_execute);
            print('total_count: %d'%len([r for r in task]));
            print('finish' + extends.to_str(item));

        requests = threadpool.makeRequests(funcs, extends.group_by_mount(mapper,parameter.count_per_thread,group_skip=skip,group_take=take));
        [pool.putRequest(req) for req in requests]
        pool.wait()


