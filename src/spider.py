# coding=utf-8
import extends
import sys;
import re
if extends.PY2:
    import urllib2
    from urlparse import urlparse
    from urlparse import urlunparse
    import cookielib
    from urllib import quote
else:
    import http.cookiejar
    from urllib.request import quote
    from urllib.parse import urlparse, urlunparse
    import urllib.request

import socket
from xspider import *
import random;
box_regex = re.compile(r"\[\d{1,3}\]");

agent_list = []
# with open('agent.list.d') as f:
#     for line_data in f:
#         agent_list.append(line_data.strip())


class XPath(extends.EObject):
    def __init__(self, name=None, xpath=None, is_html =False, sample=None, must=False):
        self.path = xpath;
        self.sample = sample;
        self.name = name;
        self.must = must;
        self.is_html = is_html;
        self.children = [];

    def __str__(self):
        return "%s %s %s" % (self.name, self.path, self.sample);

def xpath_rm_last_num(paths):
    v = paths[-1];
    m = box_regex.search(v);
    if m is not  None:
        s = m.group(0);
        paths[-1] = v.replace(s, "");
    return '/'.join(paths);

def get_common_xpath(xpaths):
    paths = [r.path.split('/') for r in xpaths];
    minlen = min(len(r) for r in paths);
    c = None;
    for i in range(minlen):
        for index in range(len(paths)):
            path = paths[index];
            if index == 0:
                c = path[i];
            elif c != path[i]:
                first = path[0:i + 1];
                return  xpath_rm_last_num(first);

def xpath_take_off(path,root_path):
    r= path.replace(root_path,'');
    if r.startswith('['):
        r= '/'.join(r.split('/')[1:])
    return r

def xpath_iter_sub(path):
    xps= path.split('/');
    for i in range(2,len(xps)):
        xp  =xpath_rm_last_num(xps[:i])
        yield xp;
attrsplit=re.compile('@|\[');


def get_xpath_data(node, path, is_html=False,only_one=True):
    p = node.xpath(path);
    if p is None:
        return None;
    if len(p) == 0:
        return None;
    paths = path.split('/');
    last = paths[-1];
    attr=False;
    if last.find('@')>=0 : #and last.find('[1]')>=0:
        attr=True;
    results=[];
    def get(x):
        if attr:
            return extends.to_str(x)
        elif is_html:
            return etree.tostring(x).decode('utf-8')
        else:
            return get_node_text(x);
    for n in p:
        result=get(n)
        if only_one:
            return result;
        results.append(result);
    return results;

extract = re.compile('\[(\w+)\]');
charset = re.compile('<meta[^>]*?charset="?(\\w+)[\\W]*?>');
charset = re.compile('charset="?([A-Za-z0-9-]+)"?>');


def parse_url(r_url, url):
    u = extends.para_to_dict(urlparse(r_url).query, '&', '=');
    for r in extract.findall(url):
        url = url.replace('[' + r + ']', u[r])
    return url;



def _build_opener():
    if extends.PY2:
        cookie_support = urllib2.HTTPCookieProcessor(cookielib.CookieJar())
        opener = urllib2.build_opener(cookie_support, urllib2.HTTPHandler)
        urllib2.install_opener(opener)
    else:

        cj = http.cookiejar.CookieJar()
        pro = urllib.request.HTTPCookieProcessor(cj)
        opener = urllib.request.build_opener(pro)
    return opener;


default_encodings=['utf-8','gbk'];
class Requests(extends.EObject):
    '''
    save request parameters and can query_xpath html from certain url
    '''
    def __init__(self):
        self.url = ''
        self.cookie = '';
        self.headers = {};
        self.timeout = 30;
        self.opener = "";
        self.post_data= ''
        self.best_encoding= 'utf-8'
        self.method='GET'

    def set_headers(self, headers):
        dic = extends.para_to_dict(headers, '\n', ':')
        extends.merge(self.headers, dic);
        return self;

    def add_proxy(self, address, proxy_type='all',
                  user=None, password=None):
        if proxy_type == 'all':
            self.proxies = {'http': address, 'https': address, 'ftp': address}
        else:
            self.proxies[proxy_type] = address
        proxy_handler = urllib2.ProxyHandler(self.proxies)
        self._build_opener()
        self.opener.add_handler(proxy_handler)

        if user and password:
            pwd_manager = urllib2.HTTPPasswordMgrWithDefaultRealm()
            pwd_manager.add_password(None, address, user, password)
            proxy_auth_handler = urllib2.ProxyBasicAuthHandler(pwd_manager)
            self.opener.add_handler(proxy_auth_handler)
        urllib2.install_opener(self.opener)


    def remove_proxy(self):
        self._build_opener()
        urllib2.install_opener(self.opener)



    def get_page(self, url=None,post_data=''):
        if url is None:
            url = self.url;
        if post_data is None:
            post_data=self.post_data;
        return _get_page(url,self.headers,post_data,self.timeout);



    def get_html(self, url=None,post_data=''):
        page = self.get_page(url,post_data)
        return _get_page_html(page,self.best_encoding)

def _get_page(url=None, headers=None, post_data='', timeout=30):
    def irl_to_url(iri):
        parts = urlparse(iri)
        pp = [(i, part) for i, part in enumerate(parts)]
        res = [];
        for p in pp:
            res.append(p[1] if p[0] != 4 else quote(p[1]))
        return urlunparse(res);

    opener= _build_opener();
    if headers is None:
        headers={};
    headers['User-Agent'] = random.choice(agent_list) if len(agent_list)>0 else 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0'
    socket.setdefaulttimeout(timeout);
    t = [(r.strip(), headers[r]) for r in headers];
    opener.addheaders = t;
    import urllib

    try:
        url.encode('ascii')
    except Exception as e:
        url = irl_to_url(url)

    try:
        if post_data == '':
            page = opener.open(url);
        else:
            import requests
            request = requests.post(
                 url,
                data=post_data)

            print(request.content)
            #page=opener.open(request)

            #binary_data = urllib.urlencode(post_data)  # post_data #.encode('utf-8')
            #page = opener.open(url, binary_data)
        return page;
    except Exception as e:
        sys.stderr.write(str(e));
        return None;


def _decoding( html, except_encoding):
    try:
        result = html.decode(except_encoding)
        return result;
    except UnicodeDecodeError as e:
        pass;

    for en in default_encodings:
        if en == except_encoding:
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

def _get_page_html(page,best_encoding):
    if page is None:
        return "";
    html = page.read();
    if page.info().get('Content-Encoding') == 'gzip':
        if extends.PY2:
            import StringIO
            import gzip
            compressed_stream = StringIO.StringIO(html)
            gzipper = gzip.GzipFile(fileobj=compressed_stream)
            html = gzipper.read()  # data就是解压后的数据
        else:
            import gzip
            html = gzip.decompress(html)
    encoding = charset.search(str(html))
    if encoding is not None:
        encoding = encoding.group(1);
    if encoding is None:
        encoding = best_encoding
    return _decoding(html, encoding);


def get_html(url):
    page=_get_page(url);
    html=_get_page_html(page,'utf-8')
    return html;

def is_none(data):
    return  data is  None or data=='';

def __get_node_text(node, array):
    if  hasattr(node,'tag')  and  isinstance(node.tag,str) and node.tag.lower() not in ['script','style','comment']:
        t = node.text;
        if t is not None:
            t=t.strip()
            if t != '':
                array.append(t)
        t = node.tail;
        if t is not None:
            t = t.strip()
            if t != '':
                array.append(t)
        for sub in node.iterchildren():
            __get_node_text(sub, array)

def get_node_text(node):
    if node is None:
        return ""
    array=[];
    __get_node_text(node, array);
    return ' '.join(array);


def _get_etree( html):
    root = None
    if html != '':
        try:
            root = etree.HTML(html);
        except Exception as e:
            sys.stderr.write('html script error'+str(e))
    return root;


def _get_datas( root, xpaths, multi=True, root_path=None):
    tree = etree.ElementTree(root);
    docs = [];
    if not multi:
        doc = {};
        for r in xpaths:
            data = get_xpath_data(tree, r.path, r.is_html);
            if data is not None:
                doc[r.name] = data;
            else:
                doc[r.name] = "";
        return doc;
    else:
        if is_none(root_path):
            root_path2 = get_common_xpath(xpaths);
        else:
            root_path2 = root_path;
        nodes = tree.xpath(root_path2)
        if nodes is not None:
            for node in nodes:
                doc = {};
                for r in xpaths:
                    path = r.path;
                    if is_none(root_path):
                        paths = r.path.split('/');
                        path = '/'.join(paths[len(root_path2.split('/')):len(paths)]);
                    else:
                        path = tree.getpath(node) + path;
                    data = get_xpath_data(node, path, r.is_html);
                    if data is not None:
                        doc[r.name] = data;
                if len(doc) == 0:
                    continue;
                docs.append(doc);
            return docs;



class SmartCrawler(extends.EObject):
    '''
    A _crawler with httpitem and parse html to structured data by search & script
    '''
    def __init__(self):
        self.multi = True;
        self.requests = Requests()
        self.name = None;
        self.login = '';
        self._has_login = False;
        self.clear();
        self.url='http://wwww.cnblogs.com';
    def auto_login(self, login):
        if login.postdata is None:
            return;
        import http.cookiejar
        cj = http.cookiejar.CookieJar()
        pro = urllib.request.HTTPCookieProcessor(cj)
        opener = urllib.request.build_opener(pro)
        t = [(r, login.headers[r]) for r in login.headers];
        opener.addheaders = t;
        binary_data = login.postdata.encode('utf-8')
        op = opener.open(login.Url, binary_data)
        data = op.read().decode('utf-8')
        print(data)
        self.requests.url = op.url;
        return opener;

    def great_hand(self,attr=False):
        tree=self._tree;
        self._stage=2;
        if not self.multi :
            print('great hand can only be used in list')
            return self;
        root_path,xpaths= search_properties(tree,self.xpaths,attr);
        if root_path is None:
            print ('great hand failed')
            return self;
        datas= _get_datas(tree,xpaths,self.multi, None)
        self._datas= datas;
        self._xpaths= xpaths;
        return self;

    def set_paras(self, is_list=True,post_data='', root=None):
        self.multi=is_list;
        if root is not None:
            self.root=root;
        self.requests.post_data=post_data;
        return self;

    def rename(self,column):
        if column.find(u':') >= 0:
            column = extends.para_to_dict(column, ',', ':')
            for path in self.xpaths:
                if path.name in column:
                    path.name=column[path.name]
        elif column.find(' ') > 0:
            column = [r.strip() for r in column.split(' ')]
            for i in min(len(column),len(self.xpaths)):
                self.xpaths[i].name=column[i]

        return self;
    def test(self):
        paths= self.xpaths if self._stage > 3 else self._xpaths;
        root= self.root if self._stage >3  else self._root;
        self._stage = 3;
        self._datas=self.__get_data_from_html(self._html, paths, root)
        if isinstance(self._datas,dict):
            self._datas=[self._datas]
        return self;

    def crawl(self,url,post_data=''):
        if   self.login != "" and  self._has_login == False:
            self.requests.opener = self.auto_login(self.login);
            self._has_login = True;
        html='';
        try:
            html = self.requests.get_html(url,post_data);
        except Exception as e:
            sys.stderr.write('url %s get error, %s'%(url,str(e)));
        return self.__get_data_from_html(html, self.xpaths, self.root);

    def __get_data_from_html(self, html, xpaths, root):
        if isinstance(xpaths, list) and len(xpaths) == 0:
            return {'Content': html};
        tree = _get_etree(html);
        if tree is None:
            return {} if self.multi == False else [];
        return _get_datas(tree, xpaths, self.multi, root);

    def add_xpath(self, name, xpath, is_html=False):
        for r in self.xpaths:
            if r.name==name:
                return ;
        xpath = XPath(name, xpath, is_html);
        self.xpaths.append(xpath);''
        return self;

    def query_xpath(self, xpath, is_html=False):
        if self._stage < 1:
            return 'please visit one url first';
        datas= get_xpath_data(self._tree,xpath,is_html,False);
        return datas;

    def py_query(self):
        from pyquery import PyQuery as pyq
        root = pyq(self._html);
        return root;

    def search_xpath(self, keyword, name=None, attr=False, is_html=False, mode='str'):
        if self._stage<1:
            return 'please visit one url first';
        result= search_xpath(self._tree, keyword, mode, attr);
        key=name if name is not  None else 'unknown';
        print('%s  : %s'%(key,result))
        if result is not None and name is not None:
            if self.root is not None:
                result=xpath_take_off(result,self.root)
            self.add_xpath(name,result,is_html)
        return self;


    def visit(self, url=None,post_data=''):
        if url is not None:
           self.url=url;
        else:
            url= self.url;
        self._stage=1;
        html = self.requests.get_html(url,post_data);
        self._html= html;
        self._tree= _get_etree(html);
        return self;

    def clear(self):
        self._stage=0;
        self.xpaths=[];
        self._xpaths=[];
        self.tree=None;
        self._datas=None;
        self._tree=None;
        self.root=None;
        self._root=None;
        return self;
    def print_xpaths(self,is_test=True):
        if is_test:
            paths=self._xpaths;
        else:
            paths= self.xpaths;
        if paths is None:
            print( 'xpaths is None');
        if len(paths)==0:
            print('script  is empty')
        buf=[];
        if self.root is not None:
            rpath=self.root if not is_test else self._root;
            buf.append('root:'+ rpath);
        for r in paths:
            buf.append('%s\t%s\tis_html: %s' % (r.name, r.path, r.is_html))
        result= '\n'.join(buf);
        return result;

    def __str__(self):
        return self.print_xpaths(False);

    def accept(self,set_root_xpath=False):
        self._stage=4;
        if len(self._xpaths)>0:
            self.xpaths=self._xpaths;
        if set_root_xpath:
            self.root= get_common_xpath(self._xpaths)
            for path in self.xpaths:
                m_path = path.path.split('/');
                path.XPath = '/'.join(m_path[len(self.root.split('/')):len(m_path)]);
        if self._tree is not None:
            self.tree= self._tree;
        return self;
    def get(self,format='print'):
        s=self._stage;
        if s==0:
            print(self)
        elif s==1:
            if extends.is_ipynb:
                from IPython.core.display import HTML,display
                display(HTML(self._html));
            else:
                print(self._html);
        elif s==2:
            print(self.print_xpaths(True))
        else :
            return extends.get(self._datas, format);






def get_web_file(url, code=None):
    url = url.strip();
    if not url.startswith('http'):
        url = 'http://' + url;
        print("auto transform %s" % (url));
    socket.setdefaulttimeout(30)
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
                    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                 "Accept-Encoding": "gzip, deflate, sdch",
                 "Connection":"keep-alive",
                 "Accept-Language": "zh-CN,zh;q=0.8,en;q = 0.6"
    }
    try:
        opener = _build_opener();
        t = [(r.strip(), headers[r]) for r in headers];
        opener.addheaders = t;
        page = opener.open(url)

        html = page.read()
    except Exception as e:
        sys.stderr.write(str(e))
        return None
    return html;




def get_img_format(name):
    if name is None:
        return None, None;
    p = name.split('.');
    if len(p) != 2:
        return name, 'jpg';

    back = p[-1];
    if back == "jpg" or back == "png" or back == "gif":  # back=="png"  ignore because png is so big!
        return p[-2], back;
    return None, None;




