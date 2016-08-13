# coding=utf-8

import extends;
if extends.PY2:
    import urllib2
    from urlparse import urlparse
    from urlparse import urlunparse
    import cookielib
    from urllib import quote,unquote
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
    def __init__(self, name=None, xpath=None, is_html =False, sample=None, must=False):
        self.path = xpath;
        self.sample = sample;
        self.name = name;
        self.must = must;
        self.is_html = is_html;
        self.children = [];

    def __str__(self):
        return "%s %s %s" % (self.name, self.path, self.sample);



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
                return remove_last_xpath_num(first);


attrsplit=re.compile('@|\[');

def get_xpath_data(node, path, is_html=False):
    p = node.xpath(path);
    if p is None:
        return None;
    if len(p) == 0:
        return None;
    paths = path.split('/');
    last = paths[-1];
    if last.find('@')>=0 : #and last.find('[1]')>=0:
        return extends.to_str(p[0]);
    if is_html:
        return etree.tostring(p[0]).decode('utf-8');
    return  get_node_text(p[0]);


extract = re.compile('\[(\w+)\]');
charset = re.compile('<meta[^>]*?charset="?(\\w+)[\\W]*?>');
charset = re.compile('charset="?([A-Za-z0-9-]+)"?>');


def parse_url(r_url, url):
    u = para_to_dict(urlparse(r_url).query, '&', '=');
    for r in extract.findall(url):
        url = url.replace('[' + r + ']', u[r])
    return url;


default_encodings=['utf-8','gbk'];
class Requests(extends.EObject):
    '''
    save request parameters and can query html from certain url
    '''
    def __init__(self):
        self.url = ''
        self.cookie = '';
        self.headers = {};
        self.timeout = 30;
        self.opener = "";
        self.post_data= ''
        self.best_encoding= 'utf-8'

    def set_headers(self, headers):
        dic = para_to_dict(headers, '\n', ':')
        extends.merge(self.headers, dic);

        return self;

    def get_page(self, url=None):
        def irl_to_url(iri):
            import string
            #iri= quote(iri, safe=string.printable)
            parts = urlparse(iri)
            pp = [(i, part) for i, part in enumerate(parts)]
            res = [];
            for p in pp:
                res.append(p[1] if p[0] != 4 else quote(p[1]))
            return urlunparse(res);
        if url is None:
            url = self.url;
        url = parse_url(self.url, url);
        #self.headers['User-Agent'] = random.choice(agent_list)
        socket.setdefaulttimeout(self.timeout);
        if extends.PY2:
            cookie_support = urllib2.HTTPCookieProcessor(cookielib.CookieJar())
            opener = urllib2.build_opener(cookie_support, urllib2.HTTPHandler)
            urllib2.install_opener(opener)
        else:

            cj = http.cookiejar.CookieJar()
            pro = urllib.request.HTTPCookieProcessor(cj)
            opener = urllib.request.build_opener(pro)

        t = [(r.strip(), self.headers[r]) for r in self.headers];
        opener.addheaders = t;
        binary_data = self.post_data.encode('utf-8')
        try:
            url.encode('ascii')
        except Exception as e:
            url =  irl_to_url(url)

        try:
            if self.post_data== '':
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
        page = self.get_page(url);
        if page is None:
            return "";
        html=page.read();
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
            encoding = self.best_encoding
        return self._decoding(html,encoding);


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
    A _crawler with httpitem and parse html to structured data by search & xpath
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

    def great_hand(self,has_attr=False):
        tree=self._tree;
        self._stage=2;
        if not self.multi :
            print('great hand can only be used in list')
            return self;
        root_path,xpaths=search_properties(tree,self.xpaths,has_attr);
        datas= _get_datas(tree,xpaths,self.multi, None)
        self._datas= datas;
        self._xpaths= xpaths;
        return self;

    def set_paras(self, is_list=True, root=None):
        self.multi=is_list;
        if root is not None:
            self.root=root;
        return self;


    def test(self):
        paths= self.xpaths if self._stage > 3 else self._xpaths;
        root= self.root if self._stage >3  else self._root;
        self._stage = 3;
        self._datas=self.__get_data_from_html(self._html, paths, root)
        if isinstance(self._datas,dict):
            self._datas=[self._datas]
        return self;

    def crawl(self,url):
        if   self.login != "" and  self._has_login == False:
            self.requests.opener = self.auto_login(self.login);
            self._has_login = True;
        html='';
        try:
            html = self.requests.get_html(url);
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

    def xpath(self,  keyword, name=None, has_attr=False,is_html=False):
        if self._stage<1:
            return 'please visit one url first';
        result= search_xpath(self._tree , keyword, has_attr);
        key=name if name is not  None else 'unknown';

        print('%s  : %s'%(key,result))
        if result is not None and name is not None:
            self.add_xpath(name,result,is_html)
        return self;


    def visit(self, url=None):
        if url is not None:
           self.url=url;
        else:
            url= self.url;
        self._stage=1;
        html = self.requests.get_html(url);
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
            print('xpath  is empty')
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
        if any(self._xpaths):
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
            return extends.get(self._datas,format);



def para_to_dict(para, split1, split2):
    r = {};
    for s in para.split(split1):
        s=s.strip();
        rs = s.split(split2);
        if len(rs) < 2:
            continue;
        key = rs[0].strip();
        value = s[len(key) + 1:].strip();
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




