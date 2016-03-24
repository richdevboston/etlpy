# coding=utf-8
__author__ = 'zhaoyiming-laptop'
import re;
import extends
import urllib
import json;
import html
import spider;
import xml.etree.ElementTree as ET


class ETLItem(object):
    def __init__(self):
        pass;

    def __str__(self):
        return '%s:%s'(self.Name, self.Column);


# 识别变量名字，然后改成Int/string
intattrs = re.compile('Max|Min|Count|Index|Interval|Position');
boolre = re.compile('^(One|Can|Is)|Enable|Should|Have');
rescript = re.compile('正则|提取数字')


def getMatchCount(mat):
    return mat.lastindex if mat.lastindex is not None else 1;


# ('正则过滤器','过滤')
def regexfilter(etl, data):
    v = etl.Regex.findall(data);
    if v is None:
        return False;
    else:
        if etl.Count < len(v):
            return False;
        return True;


# ('数值范围过滤器','过滤')
def rangefilter(etl, item):
    f = float(item)
    return etl.Min <= f <= etl.Max;


# ('重复项过滤','过滤')
def repeatfilter(etl, data):
    if data in etl.set:
        return False;
    else:
        etl.set.append(data);
        return True;


# ('空对象过滤器','过滤')
def nullfilter(etl, data):
    if data is None:
        return False;
    if isinstance(data, str):
        return data.strip() != '';
    return True;


# ('添加新列','转换')
def AddNewcolumn(etl, data):
    return etl.NewValue;


# ('自增键生成','转换')
def AutoIndex(etl, data):
    etl.currindex += 1;
    return etl.currindex;


# ('列名修改器','转换')
def columnTransformer(etl, data):
    if not etl.Column in data:
        return;
    item = data[etl.Column];
    del data[etl.Column];
    if etl.NewColumn != "":
        data[etl.NewColumn] = item;


# ('删除该列','转换')
def Deletecolumn(etl, data):
    if etl.Column in data:
        del data[etl.Column];


# ('类型转换器','转换')
def DataFormat(etl, data):
    if etl.TargetDataType == 'INT':
        return int(data);
    if etl.TargetDataType == 'STRING':
        return str(data);
    if etl.TargetDataType == 'DOUBLE':
        return float(data);


# ('HTML字符转义','转换')
def HtmlConvert(etl, data):
    if etl.ConvertType == 'Encode':
        return html.escape(data);
    else:
        import html.parser as h
        html_parser = h.HTMLParser()
        return html_parser.unescape(data);


# ('URL字符转义','转换')
def URLConvert(etl, data):
    if etl.ConvertType == 'Encode':
        url = data.encode('utf-8');
        return urllib.parse.quote(url);
    else:
        return urllib.parse.unquote(data);


# ('正则分割','转换')
def RegexSplit(etl, data):
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


# ('合并多列','转换')
def Merge(etl, data):
    if etl.MergeWith == '':
        columns = [];
    else:
        columns = [str(data[r]) for r in etl.MergeWith.split(' ')]
    columns.insert(0, data[etl.Column] if etl.Column in data else '');
    res = etl.Format;
    for i in range(len(columns)):
        res = res.replace('{' + str(i) + '}', str(columns[i]))
    return res;


# ('正则替换','转换')
def RegexReplace(etl, data):
    return re.sub(etl.Regex, etl.ReplaceText, data);


# ('正则转换器','转换')
def RegexTransform(etl, data):
    item = re.findall(etl.Regex, data);
    if etl.Index < 0:
        return '';
    if len(item) <= etl.Index:
        return '';
    else:
        r = item[etl.Index];
        if isinstance(r, str):
            return r;
        return r[0];


# ('提取数字','转换')
def SelectNumber(etl, data):
    t = RegexTransform(etl, data);
    if t is not None and t != '':
        return int(t);
    return t;


def SplitByArray(etl, data):
    splits = etl.SplitChar.split(' ');
    sp = splits[0]
    if sp == '':
        return data;
    r = data.split(splits[0])[etl.Index];
    return r;


def TrimData(etl, data):
    return data.strip();


# ('字符首尾抽取','转换')
def StringRange(etl, data):
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


# ('脚本引擎转换器')
def PythonScript(etl, data):
    value = data[etl.Column];
    if len(etl.Script.split('\n'))==1:
        result = eval(etl.Script,None,data);
        if result is not None:
            key = etl.NewColumn if etl.NewColumn != '' else etl.Column;
            data[key] = result;
    else:
        exec(etl.Script,None,data);


# 网页爬虫抓取器
def CrawlHTML(etl, data):
    crawler = etl.crawler;
    url = data[etl.Column];
    datas = crawler.CrawData(url);
    if etl.crawler.IsMultiData == 'List':
        for d in datas:
            res = extends.MergeQuery(d, data, etl.NewColumn);
            yield res;
    else:
        data = extends.Merge(data, datas);
        yield data;


def XPathTransformer(etl, data):
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


def tolist(etl, data):
    yield data;


# json转换器
def JsonTrans(etl, data):
    js = json.loads(data[etl.Column]);
    if isinstance(js, list):
        for j in js:
            yield j;
    else:
        yield js;


# 数量范围过滤
def RangeFilter(etl, data):
    skip = int(extends.Query(data, etl.Skip));
    take = int(extends.Query(data, etl.Take));
    i = 0;
    for r in data:
        if i < skip:
            continue;
        if i >= take:
            break;
        yield r;


def ETLGene(etl, data):
    subetl = modules[etl.ETLSelector];

    def checkname(item, name):
        if hasattr(item, "Name") and item.Name == name:
            return True;
        return False;

    tools = extends.Append((r for r in etl.Tool.AllETLTools if checkname(r, etl.Insert)),
                           (r for r in subetl.AllETLTools if not checkname(r, etl.Insert)))
    for r in subetl.RefreshDatas2(tools):
        yield r;


# 区间范围生成
def RangeGene(etl, data):
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


# 从文本生成
def TextGene(etl, data):
    for i in range(etl.Position, len(etl.arglists)):
        yield {etl.Column: etl.arglists[i]}


# 从文件读写，CSV,XLSX,
def FileOper(etl, data, type):
    path = etl.FilePath;
    filetype = path.split('.')[-1].lower();
    encode = 'utf-8' if etl.EncodingType == 'UTF8'  else 'ascii';
    if filetype in ['csv', 'txt']:
        import csv
        file = open(etl.FilePath, type, encoding=encode);
        sp = ',' if filetype == 'csv' else '\t';
        if type == 'r':
            reader = csv.DictReader(file, delimiter=sp)
            for r in reader:
                yield r;
        else:
            writer = csv.DictWriter(file, delimiter=sp)
            start = False;
            for r in data:
                if not start:
                    field = r.keys;
                    writer.fieldnames = field;
                    writer.writerow(dict(zip(field, field)))
                    start = True;
                writer.writer(r)
                yield r;
        file.close();
    elif filetype == 'xlsx':
        pass;
    elif filetype == 'xml' and type == 'r':
        tree = ET.parse(path);
        root = tree.getroot();
        root = root.findall('Doc');
        for etool in root:
            p = {r: etool.attrib[r] for r in etool.attrib};
            yield p;
    elif filetype == 'xml' and type == 'w':
        pass;
    elif filetype == 'json':
        if type == 'r':
            items = json.load(open(path, encoding=encode));
            for r in items:
                yield r;
        else:
            json.open(path);
            for r in data:
                json.write(r)
                yield r;
            json.close()
            json.dump([r for r in data], open(path, type, encode));
            # json.dumps()


# 从数据库读取,MONGODB,SQL...
def ConnectorGene(etl, data, type):
    pass;


# 保存超链接文件
def SaveFileExe(etl, data):
    save_path = extends.Query(data, etl.SavePath);
    urllib.request.urlretrieve(data[etl.Column], save_path)


filterdict = {'正则筛选器': regexfilter, '数量范围选择': RangeFilter, '数值范围过滤器': rangefilter, '重复项过滤': repeatfilter,
              '空对象过滤器': nullfilter};
transformdict = {'添加新列': AddNewcolumn, '自增键生成': AutoIndex,
                 '列名修改器': columnTransformer, '删除该列': Deletecolumn, '类型转换器': DataFormat, 'HTML字符转义': HtmlConvert,
                 'URL字符转义': URLConvert, '正则分割': RegexSplit, '合并多列': Merge, '正则替换': RegexReplace,
                 '正则转换器': RegexTransform, '提取数字': SelectNumber,
                 '清除空白符': TrimData, '字符串分割': SplitByArray, 'XPath筛选器': XPathTransformer, '列表实例化': tolist,
                 '字符首尾抽取': StringRange, 'Python转换器': PythonScript, '转换为Json': JsonTrans, '从爬虫转换': CrawlHTML};
genedict = {'生成区间数': RangeGene, '从文本生成': TextGene, '从ETL生成': ETLGene,
            '从文件中读取': lambda etl, data: FileOper(etl, data, 'r')};


def filter(tool, data):
    for r in data:
        item = None;
        if tool.Column in r:
            item = r[tool.Column];
        if item is None and tool.Type != '空对象过滤器':
            continue;

        result = filterdict[tool.Type](tool, item)
        if result == True and tool.Revert == 'False':
            yield r;
        elif result == False and tool.Revert == 'True':
            yield r;


def transform(tool, data):
    func = transformdict[tool.Type];
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


def generate(tool, data):
    func = genedict[tool.Type];
    return func(tool, data);


def GetChildNode(roots, name):
    for etool in roots:
        if etool.get('Name') == name or etool.tag == name:
            return etool;
    return None;


def InitFromHttpItem(config, item):
    httprib = config.attrib;
    paras = spider.Para2Dict(httprib['Parameters'], '\n', ':');
    # cookie = 'Cookie';
    # if cookie in paras:
    #     item.Cookie = paras[cookie];
    #     del paras[cookie];
    item.Headers = paras;
    item.Url = httprib['URL'];
    post = 'Postdata';
    if post in httprib:
        item.postdata = httprib[post];
    else:
        item.postdata = None;


modules = {};


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


def LoadProject(path):
    tree = ET.parse(path);
    root = tree.getroot();
    root = root.find('Doc');
    for etool in root:
        if etool.tag == 'Children':
            etype = etool.get('Type');
            name = etool.get('Name');
            if etype == '数据清洗ETL':
                etltool = ETLTool();
                for m in etool:
                    if m.tag == 'Children':
                        etl = ETLItem();
                        for att in m.attrib:
                            SetAttr(etl, att, m.attrib[att]);
                        etltool.AllETLTools.append(etl);
                modules[name] = etltool;
            elif etype == '网页采集器':
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
        if rescript.match(etl.Type):
            etl.Regex = re.compile(etl.Script);
        if etl.Type == '删除重复项':
            etl.set = [];
        elif etl.Type == '自增键生成':
            etl.currindex = 0;
        elif etl.Type == '批量删除列':
            etl.columns = etl.Editcolumn.split(' ');
        elif etl.Type in ['从爬虫转换', 'XPath筛选器']:
            etl.IsMultiYield = True;
        elif etl.Type == '从文本生成':
            etl.arglists = [r.strip() for r in etl.Content.split('\n')];
        if etl.Type == '从爬虫转换':
            etl.crawler = modules[etl.CrawlerSelector];
        if etl.Type in ['正则转换器', '提取数字', '清除空白符', 'URL字符转义', '正则过滤器', '字符串分割', 'HTML字符转义']:
            etl.OneInput = True;
        else:
            etl.OneInput = False;

    def RefreshDatas2(self, tools):
        generator = None;
        for tool in tools:
            if tool.Group == '生成':
                if generator is None:
                    generator = generate(tool, None);
                else:
                    if tool.MergeType == 'Append':
                        generator = extends.Append(generator, generate(tool, None))
                    elif tool.MergeType == 'Merge':
                        generator = extends.MergeAll(generator, generate(tool, None))
                    elif tool.MergeType == 'Cross':
                        generator = extends.Cross(generator, genedict[tool.Type], tool)
            elif tool.Group == '转换':
                generator = transform(tool, generator);
            elif tool.Group == '过滤':
                generator = filter(tool, generator);
            elif tool.Group == '执行':
                pass;
            elif tool.Group == '排序':
                pass;

        return generator;

    def RefreshDatas(self, etlCount=100):
        return self.RefreshDatas2((tool for tool in self.AllETLTools[:etlCount] if tool.Enabled));
