# coding=utf-8
import time;
import re;
import extends
import urllib
import spider;
import json;
import html
import xml.etree.ElementTree as ET
import csv
import xspider;
import os;
import  sys;



MERGE_APPEND= 'Append';
MERGE_TYPE_CROSS='Cross'
MERGE_TYPE_MERGE='Merge'

GENERATE_DOCS='文档列表'
GENERATE_DOC= '单文档'
GENERATE_NONE='不进行转换';
cols=extends.EObject()



def __get_match_counts(mat):
    return mat.lastindex if mat.lastindex is not None else 1;

class ETLTool(extends.EObject):
    def __init__(self):
        self.Enabled=True;
        self.Column = ''
    def process(self, data):
        return data
    def init(self):
        pass;

    def _eval_script(self, global_para=None, local_para=None):
        if self.Script == '':
            return True
        if not isinstance(self.Script,str):
            return self.Script(self)
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
                result = eval(self.Script,global_para,locals())

            else:
                result=eval(self.Script);
        except Exception as e:
            sys.stderr.write(str(e))
        return result

class Transformer(ETLTool):
    def __init__(self):
        super(Transformer, self).__init__()
        self.IsMultiYield=False
        self.OneInput = False;
        self.nCol=None
    def transform(self,data):
        pass;
    def process(self,data):
        if self.IsMultiYield:  # one to many
            for r in data:
                try:
                    datas=self.mtransform(r,self.Column);
                    for p in datas:
                        my=extends.merge_query(p, r, self.nCol);
                        yield my;
                except Exception as e:
                    sys.stderr.write(str(e));

            return;
        for d in data:
            def edit_data(col, ncol=None):
                if col not in d:
                    return
                if self.OneInput:
                    res = self.transform(d[col]);
                    ncol = ncol if ncol != '' and  ncol is  not None  else col;
                    d[ncol]=res;
                else:
                    ncol = ncol if ncol != '' and  ncol is  not None else col;
                    self.transform(d,col,ncol);
            if isinstance(self.Column,dict):
                for k,v in self.Column.items():
                    edit_data(k,v);
            elif isinstance(self.Column,(list,set)):
                for k in self.Column:
                    edit_data(k)
            else:
                edit_data(self.Column,self.nCol)
            yield d;

class Executor(ETLTool):
    def execute(self,data):
        pass;
    def process(self,data):
        for r in data:
            yield self.execute(r);



class Filter(ETLTool):
    def __init__(self):
        super(Filter, self).__init__()
        self.Revert=False;
        self.StopWhile=False;
        self.OneInput=True;
    def filter(self,data):
        return True;
    def process(self, data):
        for r in data:
            item = None;
            if self.OneInput:
                if self.Column in r:
                    item = r[self.Column];
                if item is None and self.__class__ != NullFT:
                    continue;
            else:
                item=r;
            result = self.filter( item)
            if result == True and self.Revert == False:
                yield r;
            elif result == False and self.Revert == True:
                yield r;
            else:
                if self.StopWhile:
                    break;
class Generator(ETLTool):
    def __init__(self):
        super(Generator, self).__init__()
        self.MergeType='Append'
        self.Position= 0;
    def generate(self,generator):
        pass;

    def process(self, generator):
        if generator is None:
            return  self.generate(None);
        else:
            if self.MergeType== MERGE_APPEND:
                return extends.append(generator, self.process(None));
            elif self.MergeType==MERGE_TYPE_MERGE:
                return extends.merge(generator, self.process(None));
            else:
                return extends.cross(generator, self.generate)



class ConnectorBase(ETLTool):
    def __init__(self):
        super(ConnectorBase, self).__init__()
        self.Connector = '';
        self.ExecuteType = 'OnlyInsert'
        self.filetype = '';

    def init(self):
        self._connector= self._proj.connectors[self.Connector];
        if self._connector.TypeName=='MongoDBConnector':
            import pymongo
            client = pymongo.MongoClient(self._connector.ConnectString);
            db = client[self._connector.DBName];
            self._Table = db[self.TableName];
        else:
            path = self.TableName;
            filetype = path.split('.')[-1].lower();
            encode = 'utf-8';
            self._file = open(path, type, encoding=encode)
            self._filetype = filetype;



class DbEX(ConnectorBase):
    def __init__(self):
        super(DbEX, self).__init__()
        self.TableName=''

    def process(self,datas):
        if self._connector.TypeName == 'MongoDBConnector':
            etype = self.ExecuteType;
            table = self._Table;
            work = {'OnlyInsert': lambda d: table.save(d),'InsertOrUpdate':lambda d: table.save(d)};
            for data in datas:
                work[etype](data);
                yield data;
        else:

            if self.filetype in ['csv', 'txt']:
                field = extends.get_keys(datas);
                self._writer = csv.DictWriter(self.file, field, delimiter=sp, lineterminator='\n')
                self._writer.writeheader()
                for data in datas:
                    self._writer.writerow(data);
                    yield data;
            elif self.filetype == 'json':
                self._file.write('[')
                for data in datas:
                    json.dump(data, self.file, ensure_ascii=False)
                    self._file.write(',');
                    yield data;
                self._file.write(']')
            self._file.close();


class DBGE(ConnectorBase):

    def generate(self,data):
        if self.Connector=='MongoDBConnector':
            for data in self._Table.find():
                yield data;
        else:
            if self.filetype in ['csv', 'txt']:
                sp = ',' if self.filetype == 'csv' else '\t';
                reader = csv.DictReader(self._file, delimiter=sp)
                for r in reader:
                    yield r;
            elif self.filetype == 'json':
                items = json.load(self._file);
                for r in items:
                    yield r;

    def process(self, generator):
        if generator is None:
            return self.generate(None);
        else:
            if self.MergeType == MERGE_APPEND:
                return extends.append(generator, self.process(None));
            elif self.MergeType == MERGE_TYPE_MERGE:
                return extends.merge(generator, self.process(None));
            else:
                return extends.cross(generator, self.generate)


def set_value(data, value, col, ncol=None):
    if ncol!='' and ncol is not None:
        data[ncol]=value;
    else:
        data[col]=value;

class RegexFT(Filter):
    def __init__(self):
        super(RegexFT, self).__init__();
        self.Script=''
    def init(self):
        self.Regex = re.compile(self.Script);
        self.Count=1;

    def filter(self,data):
        v = self.Regex.findall(data);
        if v is None:
            return False;
        else:
            return self.Count <= len(v)

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
        if isinstance(data, str):
            return data.strip() != '';
        return True;


class AddNewTF(Transformer):
    def __init__(self):
        super(AddNewTF, self).__init__()
        self.NewValue=''

    def transform(self,data):
        data[self.nCol]=self.NewValue;

class AutoIndexTF(Transformer):
    def init(self):
        super(AutoIndexTF, self).__init__()
        self.currindex = 0;
    def transform(self, data):
        self.currindex += 1;
        return self.currindex;


class RenameTF(Transformer):

    def __init__(self):
        super(RenameTF, self).__init__()
        self.Script=None;
    def transform(self, data, col, ncol):
        data[ncol]=data[col];
        if col !=ncol:
            del data[col]
class DeleteTF(Transformer):
    def __init__(self):
        super(DeleteTF, self).__init__()
        self.Script=None;
    def transform(self, data,col,ncol ):
        del data[col];

class HtmlTF(Transformer):
    def __init__(self):
        super(HtmlTF, self).__init__()
        self.OneInput=True;

    def transform(self, data):
        return html.escape(data) if self.ConvertType == 'Encode' else html.unescape(data);


class UrlTF(Transformer):
    def __init__(self):
        super(UrlTF, self).__init__()
        self.OneInput = True;
    def transform(self, data):
        if self.ConvertType == 'Encode':
            url = data.encode('utf-8');
            return urllib.parse.quote(url);
        else:
            return urllib.parse.unquote(data);


class RegexSplitTF(Transformer):
    def transform(self, data):
        items = re.split(self.Regex, data)
        if len(items) <= self.Index:
            return data;
        if not self.FromBack:
            return items[self.Index];
        else:
            index = len(items) - self.Index - 1;
            if index < 0:
                return data;
            else:
                return items[index];
        return items[index];

class MergeTF(Transformer):
    def __init__(self):
        super(MergeTF, self).__init__()
        self.Format='{0}'
        self.MergeWith=''
    def transform(self, data, col, ncol=None):
        if self.MergeWith == '':
            columns = [];
        else:
            columns = [str(data[r]) for r in self.MergeWith.split(' ')]
        columns.insert(0, data[col] if col in data else '');
        res = self.Format;
        for i in range(len(columns)):
            res = res.replace('{' + str(i) + '}', str(columns[i]))
        col = ncol if ncol is not None else col;
        data[col]=res;



class RegexTF(Transformer):
    def __init__(self):
        super(RegexTF, self).__init__()
        self.Script = '';
        self.OneInput = True;
        self.Index=0;
    def init(self):
        self.Regex = re.compile(self.Script);
    def transform(self, data):
        item = re.findall(self.Regex, str(data));
        if self.Index < 0:
            return '';
        if len(item) <= self.Index:
            return '';
        else:
            r = item[self.Index];
            return r if isinstance(r, str) else r[0];

class ReReplaceTF(RegexTF):

    def transform(self, data):
        return re.sub(self.Regex, self.ReplaceText, data);

class NumberTF(Transformer):
    def __init__(self):
        super(NumberTF, self).__init__()
        self.OneInput=True;
        self.Index=0
    def init(self):
        self.Regex=  re.compile('\d+');
        self.Index=int(self.Index)
    def transform(self, data):
        item = re.findall(self.Regex, str(data));
        if self.Index < 0:
            return '';
        if len(item) <= self.Index:
            return '';
        else:
            r = item[self.Index];
            return r if isinstance(r, str) else r[0];

class SplitTF(Transformer):
    def __init__(self):
        super(SplitTF, self).__init__()
        self.SplitChar='';
        self.OneInput = True;
        self.FromBack=False;
        self.SplitPause=False;
        self.SplitNull=False;

    def init(self):
        self.splits = self.SplitChar.split(' ');
        if '' in self.splits:
            self.splits.remove('')
        if str(self.SplitPause)=='True':
            self.splits.append(' ');
        if str(self.SplitNull)=='True':
            self.splits.append('\n')
        self.FromBack=str(self.FromBack)=='True'
        self.Index = int(self.Index)
    def transform(self, data):
        if len(self.splits)==0:
            return data;
        for i in self.splits:
            data = data.replace(i, '\001');
        r=data.split('\001');
        if len(r) < self.Index:
            return data;
        if self.FromBack:
            return r[-self.Index-1];
        return r[self.Index];

class TrimTF(Transformer):
    def __init__(self):
        super(TrimTF, self).__init__()
        self.OneInput = True;

    def transform(self, data):
        return data.strip();

class StrExtractTF(Transformer):
    def __init__(self):
        super(StrExtractTF, self).__init__()
        self.HaveStartEnd=False;
        self.Start=''
        self.OneInput=True;
        self.End=''

    def transform(self, data):
        start = data.find(self.Former);
        if start == -1:
            return
        end = data.find(self.End, start);
        if end == -1:
            return;
        if self.HaveStartEnd:
            end += len(self.End);
        if not self.HaveStartEnd:
            start += len(self.Former);
        return data[start:end];

class PythonTF(Transformer):
    def __init__(self):
        super(PythonTF, self).__init__()
        self.Script='value'
        self.ScriptWorkMode=GENERATE_NONE;

    def init(self):
        self.IsMultiYield=self.ScriptWorkMode==GENERATE_DOCS;

    def _get_data(self, data, col):
        value = data[col] if col in data else '';
        if isinstance(self.Script, str):
            result = self._eval_script({'value': value}, data);
        else:
            result = self.Script(data);
        return result;
    def mtransform(self,data,col):
        js= self._get_data(data,col)
        for j in js:
            yield j;
    def transform(self, data,col,ncol):
        js = self._get_data( data,col)
        if self.ScriptWorkMode == GENERATE_DOC:
            extends.merge(data, js);
        else:
            col= ncol if ncol is not None else col;
            data[col]=js;


class PythonGE(Generator):
    def __init__(self):
        super(PythonGE, self).__init__()
        self.Script='xrange(1,20,1)'
    def can_dump(self):
        return  isinstance(self.Script,str);
    def generate(self,generator):
        import inspect;
        if isinstance(self.Script,str):
            result = self._eval_script();
        elif inspect.isfunction(self.Script):
            result= self.Script()
        else:
            result= self.Script;
        for r in result:
            if self.Column!='':
                if self.Column=='id' and r==8:
                    break;
                yield {self.Column:r};
            else:
                yield r;

class PythonFT(Filter):
    def __init__(self):
        super(PythonFT, self).__init__()
        self.Script='True';
        self.OneInput=False;
    def can_dump(self):
        return  isinstance(self.Script,str);
    def filter(self, data):
        import inspect
        data=data.copy();
        if isinstance(self.Script, str):
            result = self._eval_script(data);
        elif inspect.isfunction(self.Script):
            result = self.Script(data)
        if result==None:
            return True;
        return result;

class CrawlerTF(Transformer):
    def __init__(self):
        super(CrawlerTF, self).__init__()
        self.Selector='';
        self.MaxTryCount=1;
        self.IsRegex=False
        self.nCol=None;


    def init(self):
        if isinstance(self.Selector,str):
            dic= self._proj.modules;
            if self.Selector in dic:
                self._crawler= dic[self.Selector]
        else:
            self._crawler=self.Selector;
        self.__buff = {};
        self.IsMultiYield = self._crawler.IsMultiData  == 'List' and any(self._crawler.xpaths);
    def _get_data(self,url):
        crawler = self._crawler;
        buff = self.__buff;
        # print(url)
        if url in buff:
            datas = buff[url];
        else:
            datas = crawler.crawl(url);
            if len(buff) < 100:
                buff[url] = datas;
        return datas;
    def mtransform(self,data,col):
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
        self.XPath=''
        self.IsMultiYield = True;
        self.GetTextHtml=False;
        self.GetText=False;
        self.GetCount=False;
        self.IsManyData=False;

    def init(self):
        self.IsMultiYield=self.IsManyData;

    def mtransform(self,data,col):
        from lxml import etree
        root = spider._get_etree(data[col]);
        tree = etree.ElementTree(root)
        if tree is None:
            yield data;
            return;
        nodes = tree.xpath(self.XPath);
        for node in nodes:
            html = etree.tostring(node).decode('utf-8');
            ext = {'Text': spider.get_node_text(node), 'HTML': html};
            ext['OHTML'] = ext['HTML']
            yield extends.merge_query(ext, data, self.nCol);


    def transform(self, data,col,ncol):
        from lxml import etree
        root = spider._get_etree(data[col]);
        tree = etree.ElementTree(root)
        if tree is None:
            return ;
        if self.GetTextHtml or self.GetText:
            nodepath=xspider.search_text_root(tree, root);
        else:
            nodepath=self.XPath;
        if nodepath is None:
            return
        nodes = tree.xpath(nodepath);
        if len(nodes)<1:
            return
        node=nodes[0]
        if self.GetTextHtml:
            res= self, etree.tostring(node).decode('utf-8');
        else:
            if hasattr(node,'text'):
                res =spider.get_node_text(node);
            else:
                res= str(node)
        data[ncol]=res;

class TnTF(Transformer):

    def __init__(self):
        super(TnTF,self).__init__()
        self.Rule=None;
        self.OneInput=True;
    def init(self):
        import tn;
        self._core =tn.core;
        self._core.InitPyRule(tn)
        self._core.RebuildEntity()
    def transform(self,data):
        result=self._core.Extract(data,entities=[self.Rule]);
        if any(result):
            return result[0]['#rewrite'];
        return '';


class ToListTF(Transformer):
    def __init__(self):
        super(ToListTF, self).__init__()
        self.MountPerThread=1;
        self.IsMultiYield=True;

    def mtransform(self, data,col):
        yield data;

class JsonTF(Transformer):
    def __init__(self):
        super(JsonTF, self).__init__()
        self.ScriptWorkMode=GENERATE_DOCS

    def init(self):
        self.IsMultiYield= self.ScriptWorkMode==GENERATE_DOC;

    def mtransform(self,data,col):
        js=json.loads(data[col]);
        for j in js:
            yield j;

    def transform(self, data,col,ncol):
        js = json.loads(data[col]);
        if self.ScriptWorkMode==GENERATE_DOC:
            extends.merge(data, js);
        else:
            data[ncol]=js

class RangeGE(Generator):
    def __init__(self):
        super(RangeGE, self).__init__()
        self.Interval='1'
        self.MaxValue='1'
        self.MinValue='1'
    def generate(self,generator):
        interval= int(extends.query(generator, self.Interval))
        maxvalue= int(extends.query(generator, self.MaxValue))
        minvalue= int(extends.query(generator, self.MinValue))
        if interval>0:
            values=range(minvalue,maxvalue,interval);
        else:
            values=range(maxvalue,minvalue,interval)
        for i in values:
            item= {self.Column:round(i,5)}
            yield item;




def __gettask(task, data):
    etlselector = extends.query(data, task.ETLSelector);
    if etlselector not in task._proj.modules:
        sys.stderr.write('sub task %s  not in current project' % etlselector);
    subetl = task._proj.modules[etlselector];
    return subetl;


class EtlGE(Generator):
    def __init__(self):
        super(EtlGE, self).__init__()
        self.ETLSelector=''
    def generate(self,data):
        subetl= __gettask(self, data);
        for r in generate(subetl.tools):
            yield r;


class EtlEX(Executor):
    def __init__(self):
        super(EtlEX, self).__init__()
        self.Selector = ''
    def execute(self,data):
        subetl= __gettask(self, data);
        if spider.is_none(self.nCol):
            doc = data.copy();
        else:
            doc = {};
            extends.merge_query(doc, data, self.nCol + " " + self.Column);
        result=(r for r in generate(subetl.tools, [doc]))
        count=0;
        for r in result:
            count+=1;
        print('subtask:'+str(count))
        return data;

class EtlTF(Transformer):

    def __init__(self):
        self.IsCycle=False;
        self.Selector = ''

    def mtransform(self,data,col):
        subetl = __gettask(self, data);
        newdata = data;
        if self.IsCycle:
            while newdata[col] != '':
                result = extends.first_or_default(generate(subetl.tools, [newdata.copy()]))
                if result is None:
                    break
                yield result.copy();
                newdata = result;
        else:
            doc = data.copy();
            for r in generate(subetl.tools, [doc]):
                yield r
    def transform(self,data,col,ncol):
        subetl = __gettask(self, data);
        for r in generate(subetl.tools,[data.copy()]):
            return extends.merge(data, r);




class TextGE(Generator):
    def __init__(self):
        super(TextGE, self).__init__()
        self.Content='';
    def init(self):
        value=self.Content.replace('\n','\001').replace(' ','\001')
        self._arglists= [r.strip() for r in value.split('\001')];
    def generate(self,data):
        for i in range(int(self.Position), len(self._arglists)):
            yield {self.Column: self._arglists[i]}



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
        self.Script = '';
        self.OneInput = True;
    def transform(self,data):
        import os;
        return str(os.path.exists(data));

class MergeRepeatTF(Transformer):
    pass;

class NumRangeFT(Transformer):
    def __init__(self):
        super(NumRangeFT, self).__init__();
        self.Skip=0;
        self.Take=1;
    def process(self,data):
        for r in extends.get_mount(data,self.Take,self.Skip):
            yield r;


class DelayTF(Transformer):
    def __init__(self):
        super(DelayTF, self).__init__();
        self.DelayTime=100;

    def transform(self,data):
        import time
        delay = extends.query(data,self.DelayTime);
        time.sleep(int(delay))
        return data;


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
        self.Table = None;

    def can_dump(self):
        return False;
    def generate(self,generator):
        for r in self.Table:
            yield r;

class TableEX(Executor):
    def __init__(self):
        super(TableEX, self).__init__()
        self.Table = None;

    def can_dump(self):
        return False;

    def execute(self, data):
        table = self.Table;
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
        urlpath= data[self.Column];
        newfile= open(save_path,'wb');
        newdata=spider.get_web_file(urlpath);
        newfile.write(newdata);
        newfile.close();








class Project(extends.EObject):
    def __init__(self):
        self.modules={};
        self.connectors={};
        self.edittime= time.time();
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
            httprib = config.attrib;
            paras = spider.para_to_dict(httprib['Parameters'], '\n', ':');
            item.Headers = paras;
            item.Url = httprib['URL'];
            post = 'Postdata';
            if post in httprib:
                item.postdata = httprib[post];
            else:
                item.postdata = None;
        def get_child_node(roots, name):
            for etool in roots:
                if etool.get('Name') == name or etool.tag == name:
                    return etool;
            return None;

        def set_attr(etl, key, value):
            value=value.strip()
            if key in ['Group', 'Type']:
                return
            intattrs = re.compile('Max|Min|Count|Index|Interval|Position');
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
            if key.find('Selector') >= 0:
                return  'Selector';
            elif key=='NewColumn':
                return 'nCol';
            return key;
        for etool in root:
            if etool.tag == 'Children':
                etype = etool.get('Type');
                name = etool.get('Name');
                if etype == 'SmartETLTool':
                    tool = ETLTask()
                    tool.Name=name;
                    for m in etool:
                        if m.tag == 'Children':
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
                    tool.Name = etool.attrib['Name'];
                    tool.IsMultiData = etool.attrib['IsMultiData']
                    tool.RootXPath = etool.attrib['RootXPath']
                    httpconfig = get_child_node(etool, 'HttpSet');
                    init_from_httpitem(httpconfig, tool.requests);
                    login = get_child_node(etool, 'Login');
                    if login is not None:
                        tool.Login = spider.Requests()
                        init_from_httpitem(login, tool.Login);
                    tool.xpaths = [];
                    for child in etool:
                        if child.tag == 'Children':
                            xpath = spider.XPath();
                            xpath.Name = child.attrib['Name'];
                            xpath.XPath = child.attrib['XPath'];
                            xpath.IsHtml = child.attrib['IsHtml'] == 'True'
                            tool.xpaths.append(xpath);

                self.modules[name] = tool;
                setattr(self, name, tool);
            elif etool.tag == 'DBConnections':
                for tool in etool:
                    if tool.tag == 'Children':
                        connector = extends.EObject();
                        for key in tool.attrib:
                            set_attr(connector, key, tool.attrib[key]);
                        self.connectors[connector.Name] = connector;

        print('load project success')
        return self;

    def dumps_json(self):
        dic = convert_dict(self )
        return json.dumps(dic, ensure_ascii=False, indent=2)

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
        for key, connector in connectors.items():
            self.connectors[key] = extends.dict_to_poco_type(connector);

        modules =dic.get('modules',{});
        for key, module in modules.items():
            crawler = None
            if 'tools' in module:
                crawler = ETLTask();
                for r in module['tools']:
                    etl =eval('%s()'%r['Type']);
                    for attr, value in r.items():
                        if attr in ['Type']:
                            continue;
                        setattr(etl, attr, value);
                    etl._proj = self;
                    crawler.tools.append(etl)
            elif 'RootXPath' in module:
                crawler = spider.SmartCrawler();
                extends.dict_copy_poco(crawler, module);
                paths = module.get('xpaths',{});
                for r in paths:
                    xpath = spider.XPath()
                    extends.dict_copy_poco(xpath, r);
                    crawler.xpaths.append(xpath)
                crawler.requests = spider.Requests()
                extends.dict_copy_poco(crawler.requests, module['requests'])
                crawler.requests.Headers = module['requests']["Headers"];
            setattr(self,key,crawler);
            if crawler is not None:
                self.modules[key] = crawler;
        print('load project success')
        return self;




def convert_dict(obj):
    if not isinstance(obj, (str, int, float, list, dict, tuple, extends.EObject)):
        return None
    if isinstance(obj, extends.EObject):
        d={}
        objtype= type(obj);
        typename= extends.get_type_name(obj)
        default= objtype().__dict__;
        for key, value in obj.__dict__.items():
            if value== default.get(key,None):
                    continue;
            if key.startswith('_'):
                continue;
            p =convert_dict(value)
            if p is not None:
                d[key]=p
        if isinstance(obj,ETLTool):
            d['Type']= typename;
        return d;

    elif isinstance(obj, list):
       return [convert_dict(r) for r in obj];
    elif isinstance(obj,dict):
        return {key: convert_dict(value) for key,value in obj.items()}
    return obj;



def generate(tools, generator=None, execute=False, enabledFilter=True):
    if tools is None:
        return generator;
    for tool in tools:
        if tool.Enabled == False and enabledFilter == True:
            continue
        if isinstance(tool,Executor) and execute==False:
            continue;
        generator = tool.process(generator)
    return generator;



def parallel_map(tools):
    index= extends.get_index(tools,lambda x:isinstance(x,ToListTF))
    if index==-1:
        return tools,None;
    first_tools = tools[:index]
    other_tools=tools[index+1:]
    return first_tools,other_tools;




class ETLTask(extends.EObject):
    def __init__(self):
        self.tools = [];
        self.Name=''

    def clear(self):
        self.tools.clear();
        return self;

    def pop(self,i):
        self.tools.pop(i);
        return self;


    def __str__(self):
        def conv_value(value):
            if isinstance(value,str):
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
        array.append('##task name:%s'%self.Name);
        array.append('.clear()')
        for t in self.tools:
            typename = extends.get_type_name(t);
            s = ".%s(%s" % (typename, conv_value(t.Column));
            attrs = [];
            defaultdict = type(t)().__dict__;
            for att in t.__dict__:
                value = t.__dict__[att];
                if att in ['OneInput','OneOutput', 'Column', 'IsMultiYield']:
                    continue
                if not isinstance(value, (str, int, bool, float)):
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
        part1,part2= parallel_map(tools)
        for tool in tools:
            tool.init();
        for r in generate(part1,None,execute=execute):
            if  isinstance(r,dict):
                r=[r]
            for p in  generate(part2, r,execute= execute):
                yield p;


    def execute(self, etl_count=100, execute=True, notify_count=100):
        c=0;
        for r in self.query(etl_count,execute):
            c+=1;
            if c%notify_count==0:
                print(c);
        print('task finish');



    def get(self, format='df', etl_count=100, take=10, skip=0):
        datas= extends.get_keys(extends.get_mount(self.query(etl_count), take, skip),cols);
        return extends.get(datas,format);

    def m_exec(self, thread_count=10, can_execute=True):
        import threadpool
        pool = threadpool.ThreadPool(thread_count)

        seed= parallel_map(self, can_execute);
        def Funcs(item):
            task= parallel_reduce(self, [item], can_execute);
            print('totalcount: %d'%len([r for r in task]));
            print('finish' + str(item));

        requests = threadpool.makeRequests(Funcs, seed);
        [pool.putRequest(req) for req in requests]
        pool.wait()


