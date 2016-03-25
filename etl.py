# coding=utf-8
__author__ = 'zhaoyiming'
import re;
import extends
import urllib
import spider;
import json;
import html
import xml.etree.ElementTree as ET

modules = {};
tables = {};
connectors = {}


# translate Int/string
intattrs = re.compile('Max|Min|Count|Index|Interval|Position');
boolre = re.compile('^(One|Can|Is)|Enable|Should|Have');
rescript = re.compile('Regex|Number')


def getMatchCount(mat):
    return mat.lastindex if mat.lastindex is not None else 1;


def RegexFT(etl, data):
    v = etl.Regex.findall(data);
    if v is None:
        return False;
    else:
        return etl.Count >= len(v)


def RangeFT(etl, item):
    f = float(item)
    return etl.Min <= f <= etl.Max;


def RepeatFT(etl, data):
    if data in etl.set:
        return False;
    else:
        etl.set.append(data);
        return True;


def NullFT(etl, data):
    if data is None:
        return False;
    if isinstance(data, str):
        return data.strip() != '';
    return True;


def AddNewTF(etl, data):
    return etl.NewValue;


def AutoIndexTF(etl, data):
    etl.currindex += 1;
    return etl.currindex;


def RenameTF(etl, data):
    if not etl.Column in data:
        return;
    item = data[etl.Column];
    del data[etl.Column];
    if etl.NewColumn != "":
        data[etl.NewColumn] = item;


def DeleteTF(etl, data):
    if etl.Column in data:
        del data[etl.Column];


def HtmlTF(etl, data):
    return html.escape(data) if etl.ConvertType == 'Encode' else html.unescape(data);


def UrlTF(etl, data):
    if etl.ConvertType == 'Encode':
        url = data.encode('utf-8');
        return urllib.parse.quote(url);
    else:
        return urllib.parse.unquote(data);


def RegexSplitTF(etl, data):
    items = re.split(etl.Regex, data)
    if len(items) <= etl.Index:
        return data;
    if not etl.FromBack:
        return items[etl.Index];
    else:
        index = len(items) - etl.Index - 1;
        if index < 0:
            return data;
        else:
            return items[index];


def MergeTF(etl, data):
    if etl.MergeWith == '':
        columns = [];
    else:
        columns = [str(data[r]) for r in etl.MergeWith.split(' ')]
    columns.insert(0, data[etl.Column] if etl.Column in data else '');
    res = etl.Format;
    for i in range(len(columns)):
        res = res.replace('{' + str(i) + '}', str(columns[i]))
    return res;


def ReReplaceTF(etl, data):
    return re.sub(etl.Regex, etl.ReplaceText, data);


def RegexTF(etl, data):
    item = re.findall(etl.Regex, str(data));
    if etl.Index < 0:
        return '';
    if len(item) <= etl.Index:
        return '';
    else:
        r = item[etl.Index];
        return r if isinstance(r, str) else r[0];


def NumberTF(etl, data):
    t = RegexTF(etl, data);
    if t is not None and t != '':
        return int(t);
    return t;


def SplitTF(etl, data):
    splits = etl.SplitChar.split(' ');
    sp = splits[0]
    if sp == '':
        return data;
    r = data.split(splits[0])[etl.Index];
    return r;


def TrimTF(etl, data):
    return data.strip();


def StrExtractTF(etl, data):
    start = data.find(etl.Former);
    if start == -1:
        return
    end = data.find(etl.End, start);
    if end == -1:
        return;
    if etl.HaveStartEnd:
        end += len(etl.End);
    if not etl.HaveStartEnd:
        start += len(etl.Former);
    return data[start:end];


def PythonTF(etl, data):
    if len(etl.Script.split('\n')) == 1:
        result = eval(etl.Script, {'value': data[etl.Column]}, data);
        if result is not None:
            key = etl.NewColumn if etl.NewColumn != '' else etl.Column;
            data[key] = result;
    else:
        exec(etl.Script, None, data);


def CrawlerTF(etl, data):
    crawler = etl.crawler;
    url = data[etl.Column];
    buff = etl.buff;
    if url in buff:
        datas = buff[url];
    else:
        datas = crawler.CrawData(url);
        if len(buff) < 100:
            buff[url] = datas;
    if etl.crawler.IsMultiData == 'List':
        for d in datas:
            res = extends.MergeQuery(d, data, etl.NewColumn);
            yield res;
    else:
        data = extends.Merge(data, datas);
        yield data;


def XPathTF(etl, data):
    from lxml import etree
    if etl.IsManyData:
        tree = spider.GetHtmlTree(data[etl.Column]);
        nodes = tree.xpath(etl.XPath);
        for node in nodes:
            ext = {'Text': spider.getnodetext(node), 'HTML': etree.tostring(node).decode('utf-8')};
            ext['OHTML'] = ext['HTML']
            yield extends.MergeQuery(ext, data, etl.NewColumn);
    else:
        tree = spider.GetHtmlTree(data[etl.Column]);
        nodes = tree.xpath(etl.XPath);
        data[etl.NewColumn] = nodes[0].text;
        yield data;


def ToListTF(etl, data):
    yield data;


def JsonTF(etl, data):
    js = json.loads(data[etl.Column]);
    if isinstance(js, list):
        for j in js:
            yield j;
    else:
        yield js;


def RangeFT(etl, data):
    skip = int(extends.Query(data, etl.Skip));
    take = int(extends.Query(data, etl.Take));
    i = 0;
    for r in data:
        if i < skip:
            continue;
        if i >= take:
            break;
        i += 1;
        yield r;


def EtlGE(etl, data):
    subetl = modules[etl.ETLSelector];
    index = etl.Tool.AllETLTools.index(etl)

    def checkname(item, name):
        if hasattr(item, "Name") and item.Name == name:
            return True;
        return False;

    tools = extends.Append((r for r in etl.Tool.AllETLTools[:index] if checkname(r, etl.Insert)),
                           (r for r in subetl.AllETLTools if not checkname(r, etl.Insert)))
    for r in subetl.__generate__(tools):
        yield r;


def RangeGE(etl, data):
    interval = int(extends.Query(data, etl.Interval));
    maxvalue = int(extends.Query(data, etl.MaxValue));
    minvalue = int(extends.Query(data, etl.MinValue));
    repeat = int(extends.Query(data, etl.RepeatCount));
    for i in range(minvalue, maxvalue, interval):
        j = repeat;
        while j > 0:
            item = {etl.Column: round(i, 5)};
            yield item;
            j -= 1;


def TextGE(etl, data):
    for i in range(etl.Position, len(etl.arglists)):
        yield {etl.Column: etl.arglists[i]}


def MongoConnect(etl, datas, type):
    import pymongo
    if not etl.Init:
        client = pymongo.MongoClient('localhost', 27017);
        db = client[etl.connector.DBName];
        if etl.ExecuteType == 'OnlyInsert':
            etl.Table = db[etl.NewTaleName];
        else:
            etl.Table = db[etl.TableName];
        etl.Init = True
    else:
        etype = etl.ExecuteType;
        table = etl.Table;
        if type == 'r':
            work = {'OnlyInsert': lambda d: table.insert(d)};
            for data in datas:
                work[etype](data);
                yield data;
        else:
            for data in table.find():
                yield data;


def FileConnect(etl, datas, type):
    path = etl.NewTableName;
    filetype = path.split('.')[-1].lower();
    encode = 'utf-8';
    if not etl.Init:
        file=open(path, type, encoding=encode)
        etl.file = file;
        etl.filetype = filetype;
    if filetype in ['csv', 'txt']:
        sp = ',' if filetype == 'csv' else '\t';
        if not etl.Init:
            import csv
            if type == 'w':
                field = extends.getkeys(datas);
                etl.writer = csv.DictWriter(file, field, delimiter=sp, lineterminator='\n')
                etl.writer.writeheader()
            etl.Init = True;
        if type == 'r':
            reader = csv.DictReader(file, delimiter=sp)
            for r in reader:
                yield r;
        else:
            for data in datas:
                etl.writer.writerow(data);
                yield data;
    elif filetype == 'json':
        if type == 'r':
            items = json.load(file);
            for r in items:
                yield r;
        else:
            file.write('[')
            for data in datas:
                json.dump(data, file, ensure_ascii=False)
                file.write(',');
                yield data;
    etl.Init = True


def DbConnect(etl, datas, type):
    etl.connector = connectors[etl.Connector];
    if etl.connector.TypeName == 'MongoDB':
        return MongoConnect(etl, datas, type);
    else:

        return FileConnect(etl, datas, type);


def TableEX(etl, datas):
    tname = etl.NewTableName;
    if tname not in tables:
        tables[tname] = [];
    for r in datas:
        tables[tname].append(r);
        yield r;


def SaveFileExe(etl, data):
    save_path = extends.Query(data, etl.SavePath);
    urllib.request.urlretrieve(data[etl.Column], save_path)


def filter(tool, data):
    for r in data:
        item = None;
        if tool.Column in r:
            item = r[tool.Column];
        if item is None and tool.Func != NullFT:
            continue;
        result = tool.Func(tool, item)
        if result == True and tool.Revert == 'False':
            yield r;
        elif result == False and tool.Revert == 'True':
            yield r;


def transform(tool, data):
    func = tool.Func;
    if tool.IsMultiYield:  # one to many
        for r in data:
            for p in func(tool, r):
                yield p;
        return;
    for d in data:  # one to one
        if tool.OneOutput:
            if tool.Column not in d or tool.Column not in d:
                yield d;
                continue;
            item = d[tool.Column] if tool.OneInput else d;
            res = func(tool, item)
            if tool.NewColumn != '':
                d[tool.NewColumn] = res
            else:
                d[tool.Column] = res
        else:
            func(tool, d)
        yield d;


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


def SetAttr(etl, key, value):
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


class EObject(object):
    pass;


def LoadProject(path):
    tree = ET.parse(path);
    root = tree.getroot();
    root = root.find('Doc');
    for etool in root:
        if etool.tag == 'Children':
            etype = etool.get('Type');
            name = etool.get('Name');
            if etype == 'SmartETLTool':
                etltool = ETLTool();
                for m in etool:
                    if m.tag == 'Children':
                        etl = EObject();
                        for att in m.attrib:
                            SetAttr(etl, att, m.attrib[att]);
                        etltool.AllETLTools.append(etl);
                modules[name] = etltool;
            elif etype == 'SmartCrawler':
                import spider;
                crawler = spider.SmartCrawler();
                crawler.Name = etool.attrib['Name'];
                crawler.IsMultiData = etool.attrib['IsMultiData']
                httpconfig = GetChildNode(etool, 'HttpSet');
                InitFromHttpItem(httpconfig, crawler.HttpItem);
                login = GetChildNode(etool, 'Login');
                if login is not None:
                    crawler.Login = spider.HTTPItem();
                    InitFromHttpItem(login, crawler.Login);
                crawler.CrawItems = [];
                for child in etool:
                    if child.tag == 'Children':
                        crawitem = spider.CrawItem(child.attrib['Name']);
                        crawitem.XPath = str(spider.XPath(spider.XPath(child.attrib['XPath'])[1:]))
                        crawler.CrawItems.append(crawitem);
                if crawler.IsMultiData == 'List':
                    crawler.CrawItems = spider.CompileCrawItems(crawler.CrawItems);
                modules[name] = crawler;
        elif etool.tag == 'DBConnections':
            for tool in etool:
                if tool.tag == 'Children':
                    connector = EObject();
                    for att in tool.attrib:
                        SetAttr(connector, att, tool.attrib[att]);
                    connectors[connector.Name] = connector;

    for name in modules:
        module = modules[name];
        if not isinstance(module, ETLTool):
            continue
        for tool in module.AllETLTools:
            module.ETLInit(tool)
    print('load project success')


class ETLTool(object):
    def __init__(self):
        self.AllETLTools = [];

    def ETLInit(self, etl):
        etl.Tool = self;
        if etl.Type == 'DbGE':
            etl.Func = lambda tool, data: DbConnect(tool, data, 'r');
            etl.Init = False;
        elif etl.Type == 'DbEX':
            etl.Func = lambda tool, data: DbConnect(tool, data, 'w');
            etl.Init = False;
        else:
            etl.Func = eval(etl.Type);
        if rescript.match(etl.Type):
            etl.Regex = re.compile(etl.Script);
        if etl.Func == RepeatFT:
            etl.set = [];
        elif etl.Func == AutoIndexTF:
            etl.currindex = 0;
        elif etl.Func in [CrawlerTF, XPathTF]:
            etl.IsMultiYield = True;
        elif etl.Func == TextGE:
            etl.arglists = [r.strip() for r in etl.Content.split('\n')];
        if etl.Func == CrawlerTF:
            etl.crawler = modules[etl.CrawlerSelector];
            etl.buff = {};
        if etl.Func in [RegexTF, NumberTF, TrimTF, UrlTF, RegexFT, SplitTF, HtmlTF]:
            etl.OneInput = True;
        else:
            etl.OneInput = False;

    def __generate__(self, tools, generator=None, execute=False):
        for tool in tools:
            if tool.Group == 'Generator':
                if generator is None:
                    generator = tool.Func(tool, None);
                else:
                    if tool.MergeType == 'Append':
                        generator = extends.Append(generator, tool.Func(tool, None));
                    elif tool.MergeType == 'Merge':
                        generator = extends.MergeAll(generator, tool.Func(tool, None));
                    elif tool.MergeType == 'Cross':
                        generator = extends.Cross(generator, tool.Func, tool)
            elif tool.Group == 'Transformer':
                generator = transform(tool, generator);
            elif tool.Group == 'Filter':
                generator = filter(tool, generator);
            elif tool.Group == 'Executor' and execute:
                generator = tool.Func(tool, generator);

        return generator;

    def QueryDatas(self, etlCount=100, execute=False):
        return self.__generate__((tool for tool in self.AllETLTools[:etlCount] if tool.Enabled), None, execute);

    def Close(self):
        for tool in self.AllETLTools:
            if tool.Type in ['DbGE', 'DbEX']:
                if tool.connector.TypeName == 'FileManager':
                    if tool.filetype == 'json':
                        tool.file.write('{}]');
                    tool.file.close();

    def mThreadExecute(self, threadcount=10):
        import threadpool
        pool = threadpool.ThreadPool(threadcount)
        tools = [tool for tool in self.AllETLTools if tool.Enabled];
        index = extends.getindex(tools, lambda d: d.Type == 'ToListTF');
        if index == -1:
            index = 0;
            tool = tools[index];
            generator = tool.Func(tool, None);
        else:
            generator = self.__generate__(tools[:index]);

        def Funcs(item):
            mgenerator = self.__generate__(tools[index + 1:], (r for r in [item]), True);
            for r in mgenerator:
                pass;
            print('finish' + str(item));

        requests = threadpool.makeRequests(Funcs, generator);
        [pool.putRequest(req) for req in requests]
        pool.wait()
        # self.__close__()
