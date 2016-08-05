# coding=utf-8
import etl;
import copy
import sys;

import extends;
if extends.PY2:
    import urllib2
    from urlparse import urlparse
    from urlparse import urlunparse
    import cookielib
else:
    import http.cookiejar
    from urllib.request import quote
    from urllib.parse import urlparse, urlunparse
    import urllib.request

from lxml import etree


import socket

from xspider import *
import random;
boxRegex = re.compile(r"\[\d{1,3}\]");

agent_list = []
with open('agent.list.d') as f:
    for line_data in f:
        agent_list.append(line_data.strip())


class XPath(extends.EObject):
    def __init__(self, name=None,  xpath=None, ishtml =False,sample=None, ismust=False):
        self.XPath = xpath;
        self.Sample = sample;
        self.Name = name;
        self.IsMust = ismust;
        self.IsHtml = ishtml;
        self.Children = [];

    def __str__(self):
        return "%s %s %s" % (self.Name, self.XPath, self.Sample);





def get_common_xpath(xpaths):
    paths = [r.XPath.split('/') for r in xpaths];
    minlen = min(len(r) for r in paths);
    c = None;
    for i in range(minlen):
        for index in range(len(paths)):
            path = paths[index];
            if index == 0:
                c = path[i];
            elif c != path[i]:
                first = path[0:i + 1];
                return remove_last_xpath_num(first);


attrsplit=re.compile('@|\[');

def get_xpath_data(node, path, ishtml=False):
    p = node.xpath(path);
    if p is None:
        return None;
    if len(p) == 0:
        return None;
    paths = path.split('/');
    last = paths[-1];
    if last.find('@')>=0 and last.find('[1]')>=0:
        return p[0];
    if ishtml:
        return etree.tostring(p[0]).decode('utf-8');
    return  get_node_text(p[0]);


extract = re.compile('\[(\w+)\]');
charset = re.compile('<meta[^>]*?charset="?(\\w+)[\\W]*?>');
charset = re.compile('charset="?([A-Za-z0-9-]+)"?>');


default_encodings=['utf-8','gbk'];

class Requests(extends.EObject):
    '''
    save request parameters and can query html from certain url
    '''
    def __init__(self):
        self.Url = ''
        self.Cookie = '';
        self.Headers = {};
        self.Timeout = 30;
        self.opener = "";
        self.postdata=''
        self.best_encoding= 'utf-8'

    def parse_url(self, url):
        u = para_to_dict(urlparse(self.Url).query, '&', '=');
        for r in extract.findall(url):
            url = url.replace('[' + r + ']', u[r])
        return url;

    def get_page(self, url=None):
        def irl_to_url(iri):
            parts = urlparse(iri)
            pp = [(parti, part) for parti, part in enumerate(parts)]
            res = [];
            for p in pp:
                res.append(p[1] if p[0] != 4 else quote(p[1]))
            return urlunparse(res);

        if url is None:
            url = self.Url;
        url = self.parse_url(url);
        self.Headers['User-Agent'] = random.choice(agent_list)
        socket.setdefaulttimeout(self.Timeout);
        if extends.PY2:
            cookie_support = urllib2.HTTPCookieProcessor(cookielib.CookieJar())
            opener = urllib2.build_opener(cookie_support, urllib2.HTTPHandler)
            urllib2.install_opener(opener)
        else:

            cj = http.cookiejar.CookieJar()
            pro = urllib.request.HTTPCookieProcessor(cj)
            opener = urllib.request.build_opener(pro)

        t = [(r.strip(), self.Headers[r]) for r in self.Headers];
        opener.addheaders = t;
        binary_data = self.postdata.encode('utf-8')
        try:
            url.encode('ascii')
        except UnicodeEncodeError:
            url =  irl_to_url(url)

        try:
            if self.postdata=='':
                page=opener.open(url);
            else:
                page =opener.open(url, binary_data)
            return  page;
        except Exception as e:
            sys.stderr.write(str(e));
            return None;

    def _decoding(self,html,except_encoding):
        try:
            result = html.decode(except_encoding)
            return result;
        except UnicodeDecodeError as e:
            pass;

        for en in default_encodings:
            if en== except_encoding:
                continue;
            try:
                result = html.decode(en)
                return result;
            except UnicodeDecodeError as e:
                continue;
        sys.stderr.write(str(e) + '\n');
        import chardet
        en = chardet.detect(html)['encoding']
        result = html.decode(en, errors='ignore');
        return result


    def get_html(self, url=None):
        import gzip
        page = self.get_page(url);
        if page is None:
            return "";
        html=page.read();
        if page.info().get('Content-Encoding') == 'gzip':
            html = gzip.decompress(html)
        encoding = charset.search(str(html))
        if encoding is not None:
            encoding = encoding.group(1);
        if encoding is None:
            encoding = self.best_encoding

        return self._decoding(html,encoding);


def is_none(data):
    return  data is  None or data=='';

def __get_node_text(node, arrs):
    if  hasattr(node,'tag')  and  isinstance(node.tag,str) and node.tag.lower() not in ['script','style','comment']:
        t = node.text;
        if t is not None:
            t=t.strip()
            if t != '':
                arrs.append(t)
        t = node.tail;
        if t is not None:
            t = t.strip()
            if t != '':
                arrs.append(t)
        for sub in node.iterchildren():
            __get_node_text(sub, arrs)

def get_node_text(node):
    if node is None:
        return ""
    arrs=[];
    __get_node_text(node, arrs);
    return ' '.join(arrs);


def _get_etree( html):
    root = None
    if html != '':
        try:
            root = etree.HTML(html);
        except Exception as e:
            sys.stderr.write('html format error'+str(e))
    return root;

class SmartCrawler(extends.EObject):
    '''
    A _crawler with httpitem and parse html to structured data by search & xpath
    '''
    def __init__(self):
        self.IsMultiData = "List";
        self.requests = Requests()
        self.Name = None;
        self.Login = '';
        self.haslogin = False;
        self.clear();
        self.url='http://wwww.cnblogs.com';
    def auto_login(self, loginItem):
        if loginItem.postdata is None:
            return;
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
        self.requests.Url = op.url;
        return opener;

    def great_hand(self,has_attr=False):
        root=self.__root;
        self.__stage=2;
        if self.IsMultiData is not 'List':
            print('great hand can only be used in list')
            return self;
        root_path,xpaths=search_properties(root,self.xpaths,has_attr);
        datas= self._get_datas(root,xpaths, None)
        self._datas= datas;
        self._xpaths= xpaths;
        return self;

    def set_paras(self,is_list=True,rootxpath=None):
        self.IsMultiData=is_list;
        if rootxpath is not None:
            self.RootXPath=rootxpath;
        return self;


    def test(self):
        paths= self.xpaths if  self.__stage==1 else self._xpaths;
        rootpath= self.RootXPath if  self.__stage==1 else self.__RootXPath;
        self.__stage = 3;
        self._datas=self.__get_data_from_html(self.__html, paths, rootpath)
        return self;

    def crawl(self,url):
        if   self.Login !="" and  self.haslogin == False:
            self.requests.opener = self.auto_login(self.Login);
            self.haslogin = True;
        html='';
        try:
            html = self.requests.get_html(url);
        except Exception as e:
            sys.stderr.write('url %s get error, %s'%(url,str(e)));
        return self.__get_data_from_html(html,self.xpaths,self.RootXPath);

    def __get_data_from_html(self, html, xpaths, rootpath):
        if isinstance(xpaths, list) and len(xpaths) == 0:
            return {'Content': html};
        tree = _get_etree(html);
        if tree is None:
            return {} if self.IsMultiData == 'One' else [];
        return self._get_datas(tree, xpaths, rootpath);

    def add_xpath(self, name, xpath, ishtml=False):
        xpath = XPath(name, xpath, ishtml);
        self.xpaths.append(xpath);''
        return self;

    def search(self, tree, keyword, has_attr=False):
        return search_xpath(tree , keyword, has_attr);

    def visit(self, url=None):
        if url is not None:
           self.url=url;
        else:
            url= self.url;
        self.__stage=1;
        html = self.requests.get_html(url);
        self.__html= html;
        self.__root= _get_etree(html);
        return self;

    def clear(self):
        self.__stage=0;
        self.xpaths=[];
        self._xpaths=[];
        self.RootXPath=None;
        self._datas=None;
        self.__RootXPath=None;
        return self;
    def print_xpaths(self,is_test=True):
        if is_test:
            paths=self._xpaths;
        else:
            paths= self.xpaths;
        if paths is None:
            print( 'xpaths is None');
        if len(paths)==0:
            print('xpath  is empty')
        buf=[];
        if self.RootXPath is not None:
            rpath=self.RootXPath if not is_test else self.__RootXPath;
            buf.append('root:'+ rpath);
        for r in paths:
            buf.append('%s\t%s\tishtml: %s' % (r.Name, r.XPath, r.IsHtml))
        result= '\n'.join(buf);
        return result;

    def __str__(self):
        return self.print_xpaths(False);

    def accept(self,set_root_xpath=False):
        self.__stage=4;
        if any(self._xpaths):
            self.xpaths=self._xpaths;
        if set_root_xpath:
            self.RootXPath= get_common_xpath(self._xpaths)
            for path in self.xpaths:
                mpath = path.XPath.split('/');
                path.XPath = '/'.join(mpath[len(self.RootXPath.split('/')):len(mpath)]);
        if self.__RootXPath is not None:
            self.RootXPath= self.__RootXPath;
        return self;
    def get(self):
        s=self.__stage;
        if s==0:
            print(self)
        elif s==1:
            if extends.is_ipynb:
                from IPython.core.display import HTML,display
                display(HTML(self.__html));
            else:
                print(self.__html);
        elif s==2:
            print(self.print_xpaths(True))
        else :
            return extends.get(self._datas);

    def _get_datas(self, root, xpaths, root_path=None):
        tree = etree.ElementTree(root);
        documents = [];
        if self.IsMultiData =='One':
            doc = {};
            for r in xpaths:
                data = get_xpath_data(tree, r.XPath, r.IsHtml);
                if data is not None:
                    doc[r.Name] = data;
                else:
                    doc[r.Name] = "";
            return doc;
        else:
            if is_none(root_path):
                root_path = get_common_xpath(xpaths);
            else:
                root_path=root_path;
            nodes = tree.xpath(root_path)
            if nodes is not None:
                for node in nodes:
                    doc = {};
                    for r in xpaths:
                        path=r.XPath;
                        if is_none(root_path):
                            paths=r.XPath.split('/');
                            path='/'.join(paths[len(root_path.split('/')):len(paths)]);
                        else:
                            path=  tree.getpath(node)+ path;
                        data = get_xpath_data(node, path, r.IsHtml);
                        if data is not None:
                            doc[r.Name] = data;
                    if len(doc) == 0:
                        continue;
                    documents.append(doc);
                return documents;

def para_to_dict(para, split1, split2):
    r = {};
    for s in para.split(split1):
        rs = s.split(split2);
        if len(rs) < 2:
            continue;
        key = rs[0];
        value = s[len(key) + 1:];
        r[rs[0]] = value;
    return r;


def get_web_file(url, code=None):
    url = url.strip();
    if not url.startswith('http'):
        url = 'http://' + url;
        print("auto transform %s" % (url));
    socket.setdefaulttimeout(30)
    i_headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
                    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                 "Accept-Encoding": "gzip, deflate, sdch",
                 "Connection":"keep-alive",
                 "Accept-Language": "zh-CN,zh;q=0.8,en;q = 0.6"
    }
    req = urllib.request.Request(url=url, headers=i_headers)
    page = urllib.request.urlopen(req)
    html = page.read()
    return html;




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




