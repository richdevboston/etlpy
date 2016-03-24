# coding=utf-8
import gzip
from io import StringIO

import re
import urllib.request
import socket

from lxml import etree
from urllib.parse import urlparse

boxRegex = re.compile(r"\[\d{1,3}\]");


class CrawItem(object):
    def __init__(self, name, sample=None, ismust=False, isHTMLorText=True, xpath=None):
        super(CrawItem, self).__init__()
        self.XPath = xpath;
        self.Sample = sample;
        self.Name = name;
        self.IsMust = ismust;
        self.IsHTMLorText = isHTMLorText;
        self.Children = [];

    def __str__(self):
        return "%s %s %s" % (self.Name, self.XPath, self.Sample);


class XPath(object):
    def __init__(self, items=None):
        super(XPath, self).__init__()
        self.Paths = [];
        if items is not None:
            if type(items) == type('u'):
                items = items.split('/');
            for x in items:
                if len(x) != 0:
                    self.Paths.append(x);

    def __len__(self):
        return len(self.Paths);

    def __getitem__(self, key):
        if isinstance(key, slice):
            return XPath(self.Paths[key.start:key.stop]);

        return self.Paths[key];

    def __str__(self):
        if len(self) == 0:
            return "";
        s = self.Paths[0];
        l = len(self.Paths);
        for r in range(1, l):
            s += '/' + self.Paths[r];
        return s;

    def takeoff(self, fullpath):
        if fullpath == "" or fullpath is None: return self;
        temp = XPath(fullpath);
        return self[len(temp):len(self)];

    def RemoveFinalNum(self):
        v = self.Paths[-1];
        m = boxRegex.search(v);
        if m is None:
            return self;
        s = m.group(0);
        self.Paths[-1] = v.replace(s, "");
        return self;

    def itersub(self):
        for r in range(1, len(self.Paths)):
            yield XPath(self.Paths[0: r]).RemoveFinalNum();


def GetMaxCompareXPath(items):
    xpaths = [XPath(r) for r in items];
    minlen = min(len(r) for r in xpaths);
    c = None;
    for i in range(minlen):
        for index in range(len(xpaths)):
            path = xpaths[index];
            if index == 0:
                c = path[i];
            elif c != path[i]:
                first = path[0:i + 1];
                return first.RemoveFinalNum();


def GetDataFromXPath(node, path):
    p = node.xpath(str(path));
    if p is None:
        return None;
    if len(p) == 0:
        return None;
    if path.find('@') >= 0:
        return str(p[0]);
    return p[0].text;


def GetDataFromCrawItems(tree, crawItems):
    documents = [];
    if isinstance(crawItems, list):
        document = {};
        for r in crawItems:
            data = GetDataFromXPath(tree, r.XPath);
            if data is not None:
                document[r.Name] = data;
            else:
                document[r.Name] = "";
        documents.append(document);
        return documents;
    else:
        nodes = tree.xpath(crawItems.XPath)
        if nodes is not None:
            for node in nodes:
                document = {};
                for r in crawItems.Children:
                    data = GetDataFromXPath(node, r.XPath);
                    if data is not None:
                        document[r.Name] = data;
                if len(document) == 0:
                    continue;
                documents.append(document);
            return documents;


def CompileCrawItems(crawitems, name='List'):
    if len(crawitems) == 0:
        return crawitems;
    crs = [r for r in crawitems];
    available = [r.XPath for r in crs if r.XPath is not None];
    shortv = GetMaxCompareXPath(available);
    craw = CrawItem(name);
    for r in crawitems:
        if r.XPath is not None:
            r.XPath = str(XPath(r.XPath).takeoff(shortv));
            craw.Children.append(r);
    craw.XPath = str(shortv);
    return craw;


def GetImage(addr, fname):
    u = urllib.urlopen(addr)
    data = u.read()
    f = open(fname, 'wb')
    f.write(data)
    f.close()


extract = re.compile('\[(\w+)\]');


class HTTPItem(object):
    def __init__(self):
        self.Url = ''
        self.Cookie = '';
        self.Headers = None;
        self.Timeout = 30;
        self.opener = None;

    def PraseURL(self, url):
        u = Para2Dict(urlparse(self.Url).query, '&', '=');

        for r in extract.findall(url):
            url = url.replace('[' + r + ']', u[r])
        return url;

    def GetHTML(self, destUrl=None):
        if destUrl is None:
            destUrl = self.Url;
        destUrl = self.PraseURL(destUrl);
        socket.setdefaulttimeout(self.Timeout);

        if self.opener is None:
            i_headers = self.Headers;
            req = urllib.request.Request(url=destUrl, headers=i_headers)
            page = urllib.request.urlopen(req)
        else:
            page = self.opener.open(destUrl);

        html = page.read()
        if page.info().get('Content-Encoding') == 'gzip':
            html = gzip.decompress(html).decode("utf-8")
        return html;


# 解压函数
def ungzip(data):
    data = gzip.decompress(data)
    return data;


def __getnodetext__(node, arrs):
    t=node.text;
    if t is not None:
        s = t.strip();
        if s != '':
            arrs.append(s)
    for sub in node.iterchildren():
        __getnodetext__(sub,arrs)

def getnodetext(node):
    if node is None:
        return ""
    arrs=[];
    __getnodetext__(node,arrs);
    return ' '.join(arrs);


class SmartCrawler(object):
    def __init__(self):
        self.IsMultiData = None;
        self.HttpItem = HTTPItem();
        self.Name = None;
        self.CrawItems = None;
        self.Login = None;
        self.haslogin = False;

    def autologin(self, loginItem):
        if loginItem.postdata is None:
            return;
        import http
        import http.cookiejar
        cj = http.cookiejar.CookieJar()
        pro = urllib.request.HTTPCookieProcessor(cj)
        opener = urllib.request.build_opener(pro)
        t = [(r, loginItem.Headers[r]) for r in loginItem.Headers];
        opener.addheaders = t;
        binary_data = loginItem.postdata.encode('utf-8')
        op = opener.open(loginItem.Url, binary_data)
        data = op.read().decode('utf-8')
        print(data)
        self.HttpItem.Url = op.url;
        return opener;

    def CrawData(self, url):

        if self.Login is not None and self.haslogin == False:
            self.HttpItem.opener = self.autologin(self.Login);
            self.haslogin = True;
        html = self.HttpItem.GetHTML(url);
        root = etree.HTML(html);
        tree = etree.ElementTree(root);
        if isinstance(self.CrawItems, list) and len(self.CrawItems) == 0:
            return {'Content': html};
        return GetDataFromCrawItems(tree, self.CrawItems);


def Para2Dict(para, split1, split2):
    r = {};
    for s in para.split(split1):
        rs = s.split(split2);
        if len(rs) < 2:
            continue;
        key = rs[0];
        value = s[len(key) + 1:];
        r[rs[0]] = value;

    return r;


def GetHTML(url, code=None):
    url = url.strip();
    if not url.startswith('http'):
        url = 'http://' + url;
        print("auto transform %s" % (url));
    socket.setdefaulttimeout(30)
    i_headers = {"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9.1) Gecko/20090624 Firefox/3.5",
                 "Accept": "text/plain"}
    req = urllib.request.Request(url=url, headers=i_headers)
    page = urllib.request.urlopen(req)
    html = page.read()
    return html;


def GetHTMLFromFile(fname):
    f = open(fname, 'r', 'utf-8');
    r = f.read();
    return r;


def GetCrawNode(craws, name, tree):
    for r in craws:
        if r.Name == name:
            return tree.xpath(r.XPath);
    return None;


def GetImageFormat(name):
    if name is None:
        return None, None;
    p = name.split('.');
    if len(p) != 2:
        return name, 'jpg';

    back = p[-1];
    if back == "jpg" or back == "png" or back == "gif":  # back=="png"  ignore because png is so big!
        return p[-2], back;
    return None, None;


def GetCrawData(crawitems, tree):
    doc = {};
    for crawItem in crawitems:
        node = tree.xpath(crawItem.XPath);
        if len(node) == 0:
            if crawItem.IsMust:
                return;
        if crawItem.IsHTMLorText is False:
            text = node[0].text;
        else:
            text = etree.tostring(node[0]);
        doc[crawItem.Name] = text;
    return doc;


def GetHtmlTree(html):
    root = etree.HTML(html);
    tree = etree.ElementTree(root);
    return tree;
