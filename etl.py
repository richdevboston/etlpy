# coding=utf-8
__author__ = 'zhaoyiming'
import re;
import extends
import urllib
import spider;
import json;
import html
import xml.etree.ElementTree as ET
import csv

import os;

intattrs = re.compile('Max|Min|Count|Index|Interval|Position');
boolre = re.compile('^(One|Can|Is)|Enable|Should|Have|Revert');
rescript = re.compile('Regex|Number')


def SetAttr(etl, key, value):
    if key in ['Group','Type']:
        return

    if intattrs.search(key) is not None:
        try:
            t = int(value);
            setattr(etl, key, t);
        except ValueError:
            print('it is a ValueError')
            setattr(etl, key, value);
    elif boolre.search(key) is not None:
        setattr(etl, key, True if value == 'True' else False);
    else:
        setattr(etl, key, value);

def getMatchCount(mat):
    return mat.lastindex if mat.lastindex is not None else 1;

class ETLTool(extends.EObject):
    def __init__(self):
        self.Enabled=True;
        self.Column = ''
    def process(self, data):
        return data
    def init(self):
        pass;

class Transformer(ETLTool):
    def __init__(self):
        super(Transformer, self).__init__()
        self.IsMultiYield=False
        self.NewColumn='';
        self.OneOutput=True;
        self.OneInput = False;

    def transform(self,data):
        pass;
    def process(self,data):
        if self.IsMultiYield:  # one to many
            for r in data:
                for p in self.transform( r):
                    yield extends.MergeQuery(p, r,self.NewColumn);
            return;
        for d in data:  # one to one
            if self.OneOutput:
                if self.Column not in d or self.Column not in d:
                    yield d;
                    continue;
                item = d[self.Column] if self.OneInput else d;
                res = self.transform(item)
                key= self.NewColumn if self.NewColumn!='' else self.Column;
                d[key]=res;
            else:
                self.transform( d)
            yield d;

class Executor(ETLTool):
    def execute(self,data):
        pass;
    def process(self,data):
        for r in data:
            self.execute(r);
            yield r;


class Filter(ETLTool):
    def __init__(self):
        super(Filter, self).__init__()
        self.Revert=False;
    def filter(self,data):

        return True;

    def process(self, data):
        for r in data:
            item = None;
            if self.Column in r:
                item = r[self.Column];
            if item is None and self.__class__ != NullFT:
                continue;
            result = self.filter( item)
            if result == True and self.Revert == False:
                yield r;
            elif result == False and self.Revert == True:
                yield r;

class Generator(ETLTool):
    def __init__(self):
        super(Generator, self).__init__()
        self.MergeType='Append'
        self.Position=0;
    def generate(self,generator):
        pass;

    def process(self, generator):
        if generator is None:
            return  self.generate(None);
        else:
            if self.MergeType=='Append':
                return extends.Append(generator,self.process(None));
            elif self.MergeType=='Merge':
                return extends.Merge(generator, self.process(None));
            else:
                return extends.Cross(generator,self.generate)



class ConnectorBase(ETLTool):
    def __init__(self):
        super(ConnectorBase, self).__init__()
        self.Connector = '';
        self.ExecuteType = 'OnlyInsert'
        self.filetype = '';

    def init(self):
        self.connector= self.__proj__.connectors[self.Connector];
        if self.connector.TypeName=='MongoDBConnector':
            import pymongo
            client = pymongo.MongoClient(self.connector.ConnectString);
            db = client[self.connector.DBName];
            self.Table = db[self.TableName];
        else:
            path = self.TableName;
            filetype = path.split('.')[-1].lower();
            encode = 'utf-8';
            self.file = open(path, type, encoding=encode)
            self.filetype = filetype;


class DbEX(ConnectorBase):
    def __init__(self):
        super(DbEX, self).__init__()
        self.TableName=''

    def execute(self,datas):
        if self.connector.TypeName == 'MongoDBConnector':
            etype = self.ExecuteType;
            table = self.Table;
            work = {'OnlyInsert': lambda d: table.save(d)};
            for data in datas:
                work[etype](data);
                yield data;
        else:

            if self.filetype in ['csv', 'txt']:
                field = extends.getkeys(datas);
                self.writer = csv.DictWriter(self.file, field, delimiter=sp, lineterminator='\n')
                self.writer.writeheader()
                for data in datas:
                    self.writer.writerow(data);
                    yield data;
            elif self.filetype == 'json':
                self.file.write('[')
                for data in datas:
                    json.dump(data, self.file, ensure_ascii=False)
                    self.file.write(',');
                    yield data;
                self.file.write(']')
            self.file.close();


class DBGE(ConnectorBase):

    def generate(self,data):
        if self.Connector=='MongoDBConnector':
            for data in self.Table.find():
                yield data;
        else:
            if self.filetype in ['csv', 'txt']:
                sp = ',' if self.filetype == 'csv' else '\t';
                reader = csv.DictReader(self.file, delimiter=sp)
                for r in reader:
                    yield r;
            elif self.filetype == 'json':
                items = json.load(self.file);
                for r in items:
                    yield r;


def setValue(data,etl,value):
    if etl.NewColumn!='':
        data[etl.NewColumn]=value;
    else:
        data[etl.Column]=value;

class RegexFT(Filter):

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

    def filter(self,item):
        f = float(item)
        return self.Min <= f <= self.Max;

class RepeatFT(Filter):

    def filter(self,data):
        if data in self.set:
            return False;
        else:
            self.set.append(data);
            return True;

class NullFT(Filter):

    def filter(self,data):
        if data is None:
            return False;
        if isinstance(data, str):
            return data.strip() != '';
        return True;


class AddNewTF(Transformer):

    def transform(self,data):
        return self.NewValue;


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
        self.OneOutput = False;
    def transform(self, data):
        if not self.Column in data:
            return;
        item = data[self.Column];
        del data[self.Column];
        if self.NewColumn != "":
            data[self.NewColumn] = item;

class DeleteTF(Transformer):
    def __init__(self):
        super(DeleteTF, self).__init__()
        self.OneOutput = False;
    def transform(self, data):
        if self.Column in data:
            del data[self.Column];

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
    def transform(self, data):
        if self.MergeWith == '':
            columns = [];
        else:
            columns = [str(data[r]) for r in self.MergeWith.split(' ')]
        columns.insert(0, data[self.Column] if self.Column in data else '');
        res = self.Format;
        for i in range(len(columns)):
            res = res.replace('{' + str(i) + '}', str(columns[i]))
        return res;




class RegexTF(Transformer):
    def __init__(self):
        super(RegexTF, self).__init__()
        self.Script = '';
        self.OneInput = True;

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

class NumberTF(RegexTF):
    def __init__(self):
        super(NumberTF, self).__init__()
        self.Script=''  #TODO

    def transform(self, data):
        t = super(NumberTF,self).transform( data);
        if t is not None and t != '':
            return int(t);
        return t;

class SplitTF(Transformer):
    def __init__(self):
        super(SplitTF, self).__init__()
        self.SplitChar='';
        self.OneInput = True;


    def transform(self, data):
        splits = self.SplitChar.split(' ');
        sp = splits[0]
        if sp == '':
            return data;

        r = data.split(splits[0]);
        if len(r) > self.Index:
            return r[self.Index];
        return '';

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
        self.OneOutput=False
        self.Script='value'
        self.ScriptWorkMode='不进行转换'
    def transform(self, data):
        result = eval(self.Script, {'value': data[self.Column]}, data);
        if result is not None and self.IsMultiYield == False:
            key = self.NewColumn if self.NewColumn != '' else self.Column;
            data[key] = result;
        return result;

class CrawlerTF(Transformer):
    def __init__(self):
        super(CrawlerTF, self).__init__()
        self.CrawlerSelector='';
        self.MaxTryCount=1;
        self.IsRegex=False
        self.OneOutput=False;
    def init(self):
        self.IsMultiYield = True;
        self.crawler = self.__proj__.modules.get(self.CrawlerSelector, None);
        self.buff = {};
    def transform(self, data):
        crawler = self.crawler;
        url = data[self.Column];
        buff = self.buff;
        if url in buff:
            datas = buff[url];
        else:
            datas = crawler.CrawData(url);
            if len(buff) < 100:
                buff[url] = datas;
        if self.crawler.IsMultiData == 'List':
            for d in datas:
                res = extends.MergeQuery(d, data, self.NewColumn);
                yield res;
        else:
            data = extends.Merge(data, datas);
            yield data;


class XPathTF(Transformer):
    def __init__(self):
        super(XPathTF, self).__init__()
        self.XPath=''
        self.IsMultiYield = True;
        self.OneOutput=False;

    def init(self):
        self.IsMultiYield=True;
        self.OneOutput = False;
    def transform(self, data):
        from lxml import etree
        if self.IsManyData:
            tree = spider.GetHtmlTree(data[self.Column]);
            nodes = tree.xpath(self.XPath);
            for node in nodes:
                ext = {'Text': spider.getnodetext(node), 'HTML': etree.tostring(node).decode('utf-8')};
                ext['OHTML'] = ext['HTML']
                yield extends.MergeQuery(ext, data, self.NewColumn);
        else:
            tree = spider.GetHtmlTree(data[self.Column]);
            nodes = tree.xpath(self.XPath);
            node=nodes[0]
            if hasattr(node,'text'):
                setValue(data, self, node.text);
            else:
                setValue(data,self,str(node))
            yield data;


class ToListTF(Transformer):
    def transform(self, data):
        yield data;

class JsonTF(Transformer):
    def __init__(self):
        super(JsonTF, self).__init__()
        self.OneOutput=False
        self.ScriptWorkMode='文档列表';

    def init(self):
        self.IsMultiYield= self.ScriptWorkMode=='文档列表';

    def transform(self, data):
        js = json.loads(data[self.Column]);
        if isinstance(js, list):
            for j in js:
                yield j;
        else:
            yield js;

class RangeGE(Generator):
    def __init__(self):
        super(RangeGE, self).__init__()
        self.Interval='1'
        self.MaxValue='1'
        self.MinValue='1'
    def generate(self,generator):
        interval= int(extends.Query(generator,self.Interval))
        maxvalue= int(extends.Query(generator,self.MaxValue))
        minvalue= int(extends.Query(generator,self.MinValue))
        for i in range(minvalue,maxvalue,interval):
            item= {self.Column:round(i,5)}
            yield item;

class RangeTF(Transformer):
    def __init__(self):
        super(RangeTF, self).__init__()
        self.Skip=0;
        self.Take=9999999;
    def transform(self, data):
        skip = int(extends.Query(data, self.Skip));
        take = int(extends.Query(data, self.Take));
        i = 0;
        for r in data:
            if i < skip:
                continue;
            if i >= take:
                break;
            i += 1;
            yield r;


class EtlGE(Generator):
    def generate(self,data):
        subetl = self.__proj__.modules[self.ETLSelector];
        for r in generate(subetl.AllETLTools):
            yield r;

class EtlEX(Executor):
    def execute(self,datas):
        subetl = self.__proj__.modules[self.ETLSelector];
        for data in datas:
            if spider.IsNone(self.NewColumn):
                doc = data.copy();
            else:
                doc = {};
                extends.MergeQuery(doc, data, self.NewColumn + " " + self.Column);
            result=(r for r in generate(subetl.AllETLTools, [doc]))
            count=0;
            for r in result:
                count+=1;
                print(r);
            print(count)
            yield data;

class EtlTF(Transformer):
    def transform(self,datas):
        subetl = self.__proj__.modules[self.ETLSelector];
        if self.IsMultiYield:

            for data in datas:
                doc = data.copy();
                for r in subetl.__generate__(subetl.AllETLTools, [doc]):
                    yield extends.MergeQuery(r, data, self.NewColumn);
        else:
            yield None;  # TODO



class TextGE(Generator):
    def __init__(self):
        super(TextGE, self).__init__()
        self.Content='';
    def init(self):
        self.arglists= [r.strip() for r in self.Content.split('\n')];
    def generate(self,data):
        for i in range(self.Position, len(self.arglists)):
            yield {self.Column: self.arglists[i]}






class TableEX(Executor):
    def __init__(self):
        super(TableEX, self).__init__()
        self.Table = 'Table';
    def execute(self,data):
        tables= self.__proj__.tables;
        tname = self.Table;
        if tname not in tables:
            tables[tname] = [];
        for r in data:
            tables[tname].append(r);
            yield r;







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

class NumRangeFT(Filter):
    pass;

class DelayTF(Transformer):
    pass;

class ReadFileTextTF(Transformer):
    pass;

class WriteFileTextTF(Transformer):
    pass;
class FolderGE(Generator):
    pass;

class TableGE(Generator):
    pass;
class FileDataTF(Transformer):
    pass;



class SaveFileEX(Executor):
    def __init__(self):
        super(SaveFileEX, self).__init__()
        self.SavePath='';

    def execute(self,data):

        save_path = extends.Query(data, self.SavePath);
        (folder,file)=os.path.split(save_path);
        if not os.path.exists(folder):
            os.makedirs(folder);
        urllib.request.urlretrieve(data[self.Column], save_path)


def GetChildNode(roots, name):
    for etool in roots:
        if etool.get('Name') == name or etool.tag == name:
            return etool;
    return None;


def InitFromHttpItem(config, item):
    httprib = config.attrib;
    paras = spider.Para2Dict(httprib['Parameters'], '\n', ':');
    item.Headers = paras;
    item.Url = httprib['URL'];
    post = 'Postdata';
    if post in httprib:
        item.postdata = httprib[post];
    else:
        item.postdata = None;




class Project(extends.EObject):
    def __init__(self):
        self.modules={};
        self.tables={}
        self.connectors={};
        self.__defaultdict__={};


def LoadProject_dict(dic):
    proj = Project();
    for key,connector in dic['connectors'].items():
        proj.connectors[key]= extends.dict_to_poco_type(connector);
    for key,module in dic['modules'].items():
        task =None;
        if 'AllETLTools' in  module:
            task = etl_factory(ETLTask(),proj);
            for r in module['AllETLTools']:
                etl= etl_factory(r['Type'],proj);
                for attr,value in r.items():
                    if attr in ['Type']:
                        continue;
                    setattr(etl,attr,value);
                etl.__proj__=proj;
                task.AllETLTools.append(etl)
        elif 'CrawItems' in module:
            task=etl_factory(spider.SmartCrawler(),proj);
            task.CrawItems=[];
            extends.dict_copy_poco(task,module);
            for r in module['CrawItems']:
                crawlitem= etl_factory(spider.CrawItem(),proj)
                extends.dict_copy_poco(crawlitem,r);
                task.CrawItems.append(crawlitem)
            task.HttpItem= etl_factory(spider.HTTPItem(),proj)
            extends.dict_copy_poco(task.HttpItem,module['HttpItem'])
            task.HttpItem.Headers=module['HttpItem']["Headers"];
        if task is not  None:
            proj.modules[key]=task;

    print('load project success')
    return proj;


def task_DumpLinq(tools):
    array=[];
    for t in tools:
        typename= extends.get_type_name(t);
        newcolumn=getattr(t,'NewColumn','');
        s='%s,%s'%(typename,t.Column);
        s+='=>%s,'%newcolumn if newcolumn!='' else ',';
        attrs=[];
        defaultdict= t.__proj__.__defaultdict__[typename];
        for att in t.__dict__:
            value=t.__dict__[att];
            if att in ['NewColumn','Column','IsMultiYield']:
                continue
            if not isinstance(value,(str,int,bool,float)):
                continue;
            if value is None  or att not in defaultdict or  defaultdict[att]==value:
                continue;
            attrs.append('%s=%s'%(att,value));
        s+=','.join(attrs)
        array.append(s)
    return '\n'.join(array);

def convert_dict(obj,defaultdict):
    if not isinstance(obj, (str, int, float, list, dict, tuple, extends.EObject)):
        return None
#    if isinstance(obj,)
    if isinstance(obj, extends.EObject):
        d={}
        typename = extends.get_type_name(obj);

        for key, value in obj.__dict__.items():
            if typename in defaultdict:
                default = defaultdict[typename];
                if value== default.get(key,None):
                    continue;
            if key.startswith('__'):
                continue;

            p =convert_dict(value,defaultdict)
            if p is not None:
                d[key]=p
        if isinstance(obj,ETLTool):
            d['Type']= typename;
        return d;

    elif isinstance(obj, list):
       return [convert_dict(r,defaultdict) for r in obj];
    elif isinstance(obj,dict):
        return {key: convert_dict(value,defaultdict) for key,value in obj.items()}
    return obj;




    return d

def Project_DumpJson(proj):
    dic=  convert_dict(proj,proj.__defaultdict__)
    return  json.dumps(dic, ensure_ascii=False, indent=2)


def Project_LoadJson(js):
    d=json.loads(js);
    return LoadProject_dict(d)

def etl_factory(item,proj):
    if isinstance(item,str):
        item=eval('%s()'%item);
    else:
        item=item;
    import copy
    name = extends.get_type_name(item)
    if name not in proj.__defaultdict__:
        proj.__defaultdict__[name]=copy.copy(  item.__dict__);
    return item;


def Project_LoadXml(path):
    tree = ET.parse(path);
    proj=Project();
    def factory(obj):
        return  etl_factory(obj,proj);
    root = tree.getroot();
    root = root.find('Doc');
    for etool in root:
        if etool.tag == 'Children':
            etype = etool.get('Type');
            name = etool.get('Name');
            if etype == 'SmartETLTool':
                etltool = factory(ETLTask());
                for m in etool:
                    if m.tag == 'Children':
                        type= m.attrib['Type']
                        etl = factory(type);
                        etl.__proj__=proj
                        for att in m.attrib:
                            SetAttr(etl, att, m.attrib[att]);
                        etltool.AllETLTools.append(etl);
                proj.modules[name] = etltool;
            elif etype == 'SmartCrawler':
                import spider;
                crawler =factory(spider.SmartCrawler());
                crawler.HttpItem= factory(spider.HTTPItem())
                crawler.Name = etool.attrib['Name'];
                crawler.IsMultiData = etool.attrib['IsMultiData']
                crawler.RootXPath= etool.attrib['RootXPath']
                httpconfig = GetChildNode(etool, 'HttpSet');
                InitFromHttpItem(httpconfig, crawler.HttpItem);
                login = GetChildNode(etool, 'Login');
                if login is not None:
                    crawler.Login = factory(spider.HTTPItem());
                    InitFromHttpItem(login, crawler.Login);
                crawler.CrawItems = [];
                for child in etool:
                    if child.tag == 'Children':
                        crawitem= factory(spider.CrawItem());
                        crawitem.Name=child.attrib['Name'];
                        crawitem.XPath = child.attrib['XPath'];
                        crawler.CrawItems.append(crawitem);

                proj.modules[name] = crawler;
        elif etool.tag == 'DBConnections':
            for tool in etool:
                if tool.tag == 'Children':
                    connector = extends.EObject();
                    for att in tool.attrib:
                        SetAttr(connector, att, tool.attrib[att]);
                    proj.connectors[connector.Name] = connector;

    print('load project success')
    return proj;


def generate(tools, generator=None, execute=False, enabledFilter=True):
    for tool in tools:
        if tool.Enabled == False and enabledFilter == True:
            continue
        tool.init();
        if isinstance(tool,Executor) and execute==False:
            continue;

        generator = tool.process(generator)
    return generator;

def parallel_map(task, execute=True):
    tools = task.AllETLTools;
    index = extends.getindex(tools, lambda d: isinstance(d,  ToListTF));
    if index == -1:
        index = 0;
        tool = tools[index];
        generator = tool.process(None);
    else:
        generator = generate(tools[:index],None, execute=execute);
    return generator;

def parallel_reduce(task,generator=None, execute=True):
    tools = task.AllETLTools;
    index = extends.getindex(tools, lambda d: isinstance(d,ToListTF));
    index =0 if index==-1 else index;
    generator = generate(tools[index + 1:], generator, execute);
    return generator;






class ETLTask(extends.EObject):
    def __init__(self):
        self.AllETLTools = [];



    def QueryDatas(self,  etlCount=100, execute=False):
        return generate((tool for tool in self.AllETLTools[:etlCount]), None, execute);

    def Close(self):
        for tool in self.AllETLTools:
            if tool.Type in ['DbGE', 'DbEX']:
                if tool.connector.TypeName == 'FileManager':
                    if tool.filetype == 'json':
                        tool.file.write('{}]');
                    tool.file.close();


    def mThreadExecute(self, threadcount=10,canexecute=True):
        import threadpool
        pool = threadpool.ThreadPool(threadcount)

        seed= parallel_map(self,canexecute);
        def Funcs(item):
            task= parallel_reduce(self,[item],canexecute);
            print('totalcount: %d'%len([r for r in task]));
            print('finish' + str(item));

        requests = threadpool.makeRequests(Funcs, seed);
        [pool.putRequest(req) for req in requests]
        pool.wait()
        # self.__close__()


