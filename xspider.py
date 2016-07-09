import sys
import os;
import time;
import html.parser as h
import importlib
import extends
import spider;
from spider import  *

importlib.reload(sys)

PM25 = 2.4;
html_parser = h.HTMLParser()
ignoretag = re.compile('script|style');
boxRegex = re.compile(r"\[\d{1,3}\]");

def GetDataFromRoot(root, type):
    properies = SearchPropertiesSmart(root);
    return GetDataFromCrawItems(tree, properies, type);


def IsContainKeyword(text, keyword):
    if text is None: return 0;
    keyword = keyword.strip();
    items = [r.strip() for r in re.split('\t|\r|\n', text)];
    for r in items:
        if r.find(keyword) >= 0:
            return 1;
    return 0;


def GetLeafNodeCount(node):
    count = 0;
    if node is None: return;
    nodes = [r for r in node.iterchildren()];
    c = len(nodes);
    if c == 0: count += 1;
    for node in nodes:
        count += GetLeafNodeCount(node);
    return count;


def IsSameXPath(p1, p2, publicPath):
    path1 = XPath(p1).takeoff(publicPath);
    path2 = XPath(p2).takeoff(publicPath);
    return str(path1) == str(path2);

class ParaClass(object):
    def __init__(self):
        self.tlen=0;
        self.path=''

def GetTextRootProbability(tree, node):
    para=ParaClass();
    __GetTextRootProbability(tree,node,para);
    if para.path=='':
        return None;
    path=para.path.split('/');
    if len(path)<2:
        return para.path;
    path='/'.join(path[:-1])
    return path;


def __GetTextRootProbability(tree, node,para):
    if  hasattr(node,'tag')  and  isinstance(node.tag,str) and node.tag.lower() not in ['script','style','comment']:
        childnodes=[n for n in node.iterchildren()];
        if len(childnodes)>0:
            for child in childnodes:
                __GetTextRootProbability(tree,child,para);
        else:
            text = node.text;
            if text ==None:
                return ;
            tlen = len(text)
            if tlen > para.tlen:
                para.tlen = tlen;
                para.path = tree.getpath(node);


def __GetTableRootProbability(tree, nodes, dict, haschild):
    if nodes is None:
        return;
    if len(nodes) == 0:
        return None;
    node = nodes[0];
    if haschild:
        for rnode in nodes:
            childs = query(rnode.iterchildren()).where(lambda x: str(x.__class__).find("Element") > 0).group_by(
                lambda x: x.tag).to_list();
            for item in childs:
                l = list(item);
                __GetTableRootProbability(tree, l, dict, haschild);
    avanodes = list(tNode for tNode in nodes);  # filter(lambda x:not x.tag.startswith("#"),nodes);
    childCount = float(len(avanodes));
    if 2 > childCount:
        return;
    sameNameCount = len([x for x in avanodes if x.tag == avanodes[1].tag]);
    if sameNameCount < childCount * 0.7: return;
    childCounts = [];
    for n in avanodes:
        childCounts.append(len(list(r for r in n.iterchildren())));
    v = extends.GetVariance(childCounts);
    if v > 2: return;
    leafCount = GetLeafNodeCount(avanodes[0]);
    value = childCount * PM25 + leafCount;
    xpath = str(XPath(tree.getpath(node)).RemoveFinalNum());
    dict[xpath] = value;


def GetTableRootProbability(root, haschild):
    d = {};
    tree = etree.ElementTree(root);
    __GetTableRootProbability(tree, root, d, haschild);
    return d;


def SearchXPath(tree, node, keyword, hasAttr=False):
    if node is None or keyword is None: return;
    nodes = node.iterchildren();
    for node in nodes:
        if str(node.__class__).find("Element") > 0:
            path = SearchXPath(tree, node, keyword, hasAttr);
            if path is not None:
                return path;
            if node.text is not None and IsContainKeyword(node.text, keyword):
                xpath = tree.getpath(node)
                return xpath;
            if hasAttr:
                for r in node.attrib:
                    if IsContainKeyword(node.attrib[r], keyword):
                        xpath = tree.getpath(node);
                        return xpath;
    return None;


def SearchXPaths(tree, node, crawItems, hasAttr=False):
    if node is None or any(crawItems) == False: return;
    nodes = node.iterchildren();
    for node in nodes:
        if str(node.__class__).find("Element") > 0:
            SearchXPaths(tree, node, crawItems, hasAttr);
            for crawItem in crawItems:
                if crawItem.XPath is not None:
                    continue;
                if node.text is not None and IsContainKeyword(node.text, crawItem.Sample):
                    xpath = tree.getpath(node);
                    crawItem.XPath = xpath;
                if hasAttr is True:
                    for r in node.attrib:
                        if IsContainKeyword(node.attrib[r], crawItem.Sample):
                            xpath = tree.getpath(node);
                            crawItem.XPath = xpath + '/@' + r;
                            break;


def SearchXPathFromURL(url, keyword):
    urldata = GetHTML(url);
    root = etree.HTML(urldata)
    tree = etree.ElementTree(root);
    return SearchXPath(tree, root, keyword);


def __GetDiffNodes(tree, nodes, results, isAttrEnabled):
    ischildcontailInfo = False;
    node1 = nodes[0]
    node2 = nodes[1];
    tree1 = etree.ElementTree(node1);
    node1path = tree.getpath(node1);
    for childnode1 in node1.iterchildren():
        path = str(XPath(tree1.getpath(childnode1))[2:]);
        nodechild2 = [];
        for r in nodes:
            l = r.xpath(path);
            if len(l) == 0:
                break;
            nodechild2.append(l[0]);
        if len(nodechild2) <= 1:
            continue;
        ischildcontailInfo |= __GetDiffNodes(tree, nodechild2, results, isAttrEnabled);
    if ischildcontailInfo == False:
        for r in nodes:
            if r.text != node1.text:
                propertyname = SearchPropertyName(r, results);
                crawitem = CrawItem(propertyname);
                crawitem.Sample = node1.text;
                crawitem.XPath = node1path if len(results) % 2 == 0 else tree.getpath(r);
                results.append(crawitem);
                ischildcontailInfo = True;
                break;
    if not isAttrEnabled:
        return ischildcontailInfo;
    for r in node1.attrib:
        v = node1.attrib[r];
        for node in nodes:
            value = node.attrib.get(r, None);
            if value is None:
                break;
            if node.attrib[r] != v:
                crawitem = CrawItem(SearchPropertyName(r, results) + "_" + r);
                crawitem.XPath = node1path if len(results) % 2 == 0 else tree.getpath(node);
                crawitem.XPath += '/@' + r;
                crawitem.Sample = v;
                results.append(crawitem);
                break;
    return ischildcontailInfo;


def GetDiffNodes(tree, root, shortv, isAttrEnabled, exists=None):
    crawlItems = [];
    nodes = [r for r in root.xpath(shortv)]
    count = len(nodes);
    if count > 1:
        __GetDiffNodes(tree, nodes, crawlItems, isAttrEnabled);
    if exists is not None:
        for r in exists:
            for p in crawlItems:
                shortpath = XPath(p.XPath).takeoff(shortv);
                if r.XPath == str(shortpath):
                    p.Name = r.Name;
                    break;
    return crawlItems;


def __SearchPropertyName(node, crawitems):
    attrkey = "class";
    name = node.attrib.get(attrkey, None);
    if name is None:
        return None;
    for c in crawitems:
        if c.Name == name:
            name2 = node.getparent().attrib.get(attrkey, None);
            if name2 is None:
                return None;
            else:
                name = name2 + '_' + name;
    return name.replace(' ', '_');


def SearchPropertyName(node, crawitems):
    r = None;  # __SearchPropertyName(node, crawitems);
    if r is None:
        return u"Property" + str(len(crawitems));
    return r;


def SearchPropertiesSmart(root, existItems=None, isAttrEnabled=False):
    if existItems == None: existItems = [];
    tree = etree.ElementTree(root);
    existLen = len(existItems);
    if existLen > 1:
        mode = CompileCrawItems(existItems);
        return GetDiffNodes(tree, root, mode.XPath, isAttrEnabled, existItems);

    elif existLen == 1:
        realPath = XPath(existItems[0].XPath);
        cdict = {};
        for r in realPath.itersub():
            __GetTableRootProbability(root.xpath(str(r)), cdict, False);
        maxp = 0;
        path = None;
        for r in cdict:
            if cdict[r] > maxp:
                path = r;
            maxp = cdict[r];
        if path is not None:
            return GetDiffNodes(tree, root, path, isAttrEnabled, existItems);

    else:
        cdict = {};
        __GetTableRootProbability(tree, [r for r in root.iterchildren()], cdict, True);
        cdict = sorted(cdict, key=lambda d: cdict[d], reverse=True);
        for k in cdict:
            items = GetDiffNodes(tree, root, k, isAttrEnabled, existItems);
            if len(items) > 1:
                return items;



def search(url):
    html = GetHTML(url);
    root = etree.HTML(html);
    crawitems = SearchPropertiesSmart(root, None, False);
    return CompileCrawItems(crawitems);


def craw(url, mode):
    html = GetHTML(url);
    root = etree.HTML(html);
    tree = etree.ElementTree(root);
    datas = GetDataFromCrawItems(tree, mode);
    return datas;


class WebPageVisitor(object):
    def __init__(self):
        self.StartURL = None;
        self.Prefix = None;
        self.Folder = '';
        self.URLHash = [];
        self.CrawItems = [];
        self.Encoding = None;
        self.ShouldStop = False;
        self.Queue = Queue.Queue();
        self.ProcessFunc = None
        self.IsMultiThread = False;
        self.hashFile = 'URLHash.txt';


    def ReadHash(self):
        filename = self.Folder + self.hashFile;
        if os.path.exists(filename) == False:
            return;
        f = open(filename, 'r');
        texts = f.readlines();

        for t in texts:
            self.URLHash.append(int(t));

    def SearchCrawItems(self, url):
        html = GetHTML(url, self.Encoding)
        root = etree.HTML(html)
        tree = etree.ElementTree(root);
        SearchXPaths(tree, root, self.CrawItems);

    def Generate(self):
        pass;


class WebPageListVisitor(WebPageVisitor):
    def __init__(self):
        super(WebPageListVisitor, self).__init__()
        self.ListCrawItems = [];
        self.VisitMethod = None;

    def __newTask(self, currentURL):
        print(currentURL);
        html = GetHTML(currentURL, self.Encoding)
        root = etree.HTML(html)
        tree = etree.ElementTree(root);
        if self.ProcessFunc != None:
            self.ProcessFunc(currentURL, tree);

    def Generate(self):
        visit = self.VisitMethod();
        for currentURL in visit:
            if self.ShouldStop == False:
                urlhash = str(abs(hash(currentURL)));
                if self.StartURL is not currentURL and urlhash in self.URLHash:
                    continue;
                self.URLHash.append(urlhash);
                extends.SaveFile(self.Folder + self.hashFile, urlhash);

                try:

                    self.__newTask(currentURL);
                except   e:
                    print(e);
                finally:
                    pass
        time.sleep(120)


class WebPageBFSVisitor(WebPageVisitor):
    def __init__(self):
        super(WebPageBFSVisitor, self).__init__()
        self.Hello = 1;

    def __newTask(self, currentURL):
        print(currentURL);
        html = GetHTML(currentURL, self.Encoding)
        root = etree.HTML(html)
        tree = etree.ElementTree(root);
        # let's get it hrefs;
        others = tree.xpath("//@href");
        allurls = [];
        for url in others:
            surl = str(url);
            if surl.startswith("http"):
                if surl.startswith(self.Prefix):
                    allurls.append(surl);
                else:
                    continue;
            else:
                allurls.append(self.Prefix + surl);
        for url in allurls:
            thash = hash(url);
            if (thash not in self.URLHash):
                self.Queue.put(url);
        # hook function
        if self.ProcessFunc != None:
            self.ProcessFunc(currentURL, tree);

    def Generate(self):
        self.Queue.put(self.StartURL);

        while not self.ShouldStop:
            if self.Queue.empty():
                time.sleep(1);
                print("queue empty,let us wait 1 sec")
                extends.SaveFile(self.Folder + self.hashFile, self.URLHash);
            currentURL = self.Queue.get();
            urlhash = str(abs(hash(currentURL)));
            if self.StartURL is not currentURL and urlhash in self.URLHash:
                continue;
            self.URLHash.append(urlhash);
            if (self.IsMultiThread):
                threadpool.GlobalThreadPool._instance.addTask(self.__newTask, (currentURL,));
            else:
                self.__newTask(currentURL);



