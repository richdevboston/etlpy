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
        return '%s:%s'(self.Name, self.ColumnName);


#识别变量名字，然后改成Int/string
intattrs = re.compile('Max|Min|Count|Index|Interval|Position');
boolre = re.compile('^(Can|Is)|Enable|Should|Have');
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
    data[etl.NewColumnName] = etl.NewValue;


# ('自增键生成','转换')
def Array2Multicolumn(etl, data):
    v = data[etl.ColumnName];
    if not isinstance(v, list):
        return False;
    for i in range(min(len(v), etl.Maxcolumn)):
        data[etl.NewColumnName + str(i)] = v[i];


# ('自增键生成','转换')
def AutoIndex(etl, data):
    etl.currindex += 1;
    return etl.currindex;


# ('批量删除列','转换')
def BatchDelete(etl, data):
    for r in etl.columns:
        del data[r];


# ('列名修改器','转换')
def columnTransformer(etl, data):
    if not etl.ColumnName in data:
        return;
    item = data[etl.ColumnName];
    del data[etl.ColumnName];
    if etl.NewColumnName != "":
        data[etl.NewColumnName] = item;


# ('删除该列','转换')
def Deletecolumn(etl, data):
    if etl.ColumnName in data:
        del data[etl.ColumnName];


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
    if etl.TargetDataType == 'ARRAY':
        data[etl.NewColumnName] = items;
    else:
        if len(items) <= etl.Index:
            return "";
        if not etl.FromBack:
            return items[etl.Index];
        else:
            index = len(items) - etl.Index - 1;
            if index < 0:
                return "";
            else:
                return items[index];


# ('合并多列','转换')
def Merge(etl, data):
    if etl.MergeWith == '':
        columns = [];
    else:
        columns = [str(data[r]) for r in etl.MergeWith.split(' ')]
    columns.insert(0, data[etl.ColumnName] if etl.ColumnName in data else '');
    res = etl.Format;
    for i in range(len(columns)):
        res = res.replace('{' + str(i) + '}', str(columns[i]))
    key = etl.NewColumnName if etl.NewColumnName != '' else etl.ColumnName;
    data[key] = res;


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
        r = r[0]
        return r;


# ('提取数字','转换')
def SelectNumber(etl, data):
    t = RegexTransform(etl, data);
    if t is not None and t != '':
        return int(t);
    return t;


# ('获取长度','转换')
def GetLength(etl, data):
    return len(data);


# ('按字符分割','转换')
def SplitByChar(etl, data):
    return [r for r in data];


def SplitByArray(etl, data):
    splits = etl.SplitChar.split(' ');
    r = data.split(splits[0])[etl.Index];
    return r;


# ('字符首尾抽取','转换')
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


# ('脚本引擎转换器','转换')
def PythonScript(etl, data):
    value = data[etl.ColumnName];
    key = etl.NewColumnName if etl.NewColumnName != '' else etl.ColumnName;
    data[key] = eval(etl.Script);


# 网页爬虫抓取器
def CrawlHTML(etl, data):
    crawler = etl.crawler;
    url = data[etl.ColumnName];
    datas = crawler.CrawData(url);
    if etl.crawler.IsMultiData == 'List':
        for d in datas:
            res = extends.MergeQuery(d, data, etl.NewColumnName);
            yield res;
    else:
        data = extends.Merge(data, datas);
        yield data;


def XPathTransformer(etl, datas):
    if etl.IsManyData:
        for data in datas:
            tree = spider.GetHtmlTree(data[etl.ColumnName]);
            nodes = tree.xpath(etl.XPath);
            for node in nodes:
                ext = {'Text': node.text, 'HTML': node.html, 'OHTML': node.parent.html};
                yield extends.MergeQuery(data, ext, etl.NewColumn);
    else:
        tree = spider.GetHtmlTree(datas[etl.ColumnName]);
        nodes = tree.xpath(etl.XPath);
        datas[etl.NewColumnName] = nodes[0].text;
        yield datas;


def tolist(etl, data):
    yield data;


# json转换器
def JsonTrans(etl, data):
    js = json.loads(data[etl.ColumnName]);
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


# 区间范围生成
def RangeGene(etl, data):
    interval = int(extends.Query(data, etl.Interval));
    maxvalue = int(extends.Query(data, etl.MaxValue));
    minvalue = int(extends.Query(data, etl.MinValue));
    repeat = int(extends.Query(data, etl.RepeatCount));
    for i in range(minvalue, maxvalue, interval):
        j = repeat;
        while j > 0:
            item = {etl.ColumnName: round(i, 5)};
            yield item;
            j -= 1;


# 从文本生成
def TextGene(etl, data):
    for i in range(etl.Position, len(etl.arglists)):
        yield {etl.ColumnName: etl.arglists[i]}


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
    elif filetype=='xml' and type=='w':
        pass;
    elif filetype=='json':
        if type=='r':
            items= json.load(open(path,encoding=encode));
            for r in items:
                yield r;
        else:
            json.open(path);
            for r in data:
                json.write(r)
                yield r;
            json.close()
            json.dump([r for r in data],open(path,type,encode));
            #json.dumps()

# 从数据库读取,MONGODB,SQL...
def ConnectorGene(etl, data, type):
    pass;


# 保存超链接文件
def SaveFileExe(etl, data):
    save_path = extends.Query(data, etl.SavePath);
    urllib.request.urlretrieve(data[etl.ColumnName], save_path)


filterdict = {'正则筛选器': regexfilter, '数量范围选择': RangeFilter, '数值范围过滤器': rangefilter, '重复项过滤': repeatfilter,
              '空对象过滤器': nullfilter};
transformdict = {'添加新列': AddNewcolumn, '数组转多列': Array2Multicolumn, '自增键生成': AutoIndex, '批量删除列': BatchDelete,
                 '列名修改器': columnTransformer, '删除该列': Deletecolumn, '类型转换器': DataFormat, 'HTML字符转义': HtmlConvert,
                 'URL字符转义': URLConvert, '正则分割': RegexSplit, '合并多列': Merge, '正则替换': RegexReplace,
                 '正则转换器': RegexTransform, '提取数字': SelectNumber, '获取长度': GetLength, '按字符分割': SplitByChar,
                 '清除空白符': TrimData, '数列分割': SplitByArray, 'XPath筛选器': XPathTransformer, '列表实例化': tolist,
                 '字符首尾抽取': StringRange, '脚本引擎转换器': PythonScript, '转换为Json': JsonTrans, '从爬虫转换': CrawlHTML};
genedict = {'生成区间数': RangeGene, '从文本生成': TextGene, '从文件中读取': lambda etl, data: FileOper(etl, data, 'r')};


def filter(tool, data):
    for r in data:
        item = None;
        if tool.ColumnName in r:
            item = r[tool.ColumnName];
        if item is None and tool.Type != '空对象过滤器':
            continue;

        result = filterdict[tool.Type](tool, item)
        if result == True and tool.Revert == 'False':
            yield r;
        elif result == False and tool.Revert == 'True':
            yield r;


def transform(tool, data):
    func = transformdict[tool.Type];
    if tool.IsMultiData:  # one to many
        for r in data:
            for p in func(tool, r):
                yield p;
        return;
    for d in data:  # one to one
        if tool.ShouldContainName:
            if tool.ColumnName not in d:
                yield d;
                continue;
            item = d[tool.ColumnName];
            if item is None:
                yield d;
                continue;
            res = func(tool, item);
            if tool.NewColumnName != '':
                d[tool.NewColumnName] = res;
            else:
                d[tool.ColumnName] = res;
        else:
            func(tool, d);
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


def InitFromCrawler(etl, root):
    import spider;
    etl.crawler = spider.SmartCrawler();
    crawltask = GetChildNode(root, etl.CrawlerSelector);
    etl.crawler.Name = crawltask.attrib['Name'];
    etl.crawler.IsMultiData = crawltask.attrib['IsMultiData']
    httpconfig = GetChildNode(crawltask, 'HttpSet');
    InitFromHttpItem(httpconfig, etl.crawler.HttpItem);
    login = GetChildNode(crawltask, 'Login');
    if login is not None:
        etl.crawler.Login = spider.HTTPItem();
        InitFromHttpItem(login, etl.crawler.Login);

    etl.crawler.CrawItems = [];
    for child in crawltask:
        if child.tag == 'Children':
            crawitem = spider.CrawItem(child.attrib['Name']);
            crawitem.XPath = str(spider.XPath(spider.XPath(child.attrib['XPath'])[1:]))
            etl.crawler.CrawItems.append(crawitem);
    if etl.crawler.IsMultiData == 'List':
        etl.crawler.CrawItems = spider.CompileCrawItems(etl.crawler.CrawItems);


class ETLTool(object):
    def __init__(self):
        self.AllETLTools = [];

    def LoadProject(self, path, name):
        tree = ET.parse(path);
        root = tree.getroot();
        root = root.find('Doc');
        for etool in root:
            if etool.tag == 'Children':
                if etool.get('Type') == '数据清洗ETL' and etool.get('Name') == name:
                    for m in etool:
                        if m.tag == 'Children':
                            etl = ETLItem();
                            for att in m.attrib:
                                self.SetAttr(etl, att, m.attrib[att]);
                            self.ETLInit(etl, root);
                            self.AllETLTools.append(etl);
                    break;
        print('load project success')

    def SetAttr(self, etl, key, value):
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

    def ETLInit(self, etl, root):
        if rescript.match(etl.Type):
            etl.Regex = re.compile(etl.ScriptCode);
        if etl.Type == '删除重复项':
            etl.set = [];
        elif etl.Type == '自增键生成':
            etl.currindex = 0;
        elif etl.Type == '批量删除列':
            etl.columns = etl.Editcolumn.split(' ');
        elif etl.Type in ['正则转换器', '提取数字', '清除空白符', 'URL字符转义', '正则过滤器']:
            etl.ShouldContainName = True;
        elif etl.Type in ['合并多列', '删除该列', '列名修改器', '脚本引擎转换器']:
            etl.ShouldContainName = False;
        elif etl.Type in ['从爬虫转换', 'XPath筛选器', '列表实例化']:
            etl.IsMultiData = True;
        elif etl.Type == '从文本生成':
            etl.arglists = [r.strip() for r in etl.Content.split('\n')];
        if etl.Type == '从爬虫转换':
            InitFromCrawler(etl, root);

    def GetAllDatas(self):
        return [r for r in self.RefreshDatas()]

    def RefreshDatas(self, etlCount=100):
        index = 0;
        generator = None;
        for tool in self.AllETLTools[:etlCount]:
            if not tool.Enabled:
                index += 1;
                continue
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
            index += 1;
        return generator;


if __name__ == '__main__':

    tool = ETLTool();
    tool.LoadProject('D:\我的工程.xml', '数据清洗ETL-大众点评');
    datas = tool.RefreshDatas();
    for r in datas:
        print(r)
