# coding=utf-8
from extends import  *
import sys;
import re
import requests


from xspider import *
import random;
box_regex = re.compile(r"\[\d{1,3}\]");

agent_list = []

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

default_encodings=['utf-8','gbk'];


def get_encoding(html):
    encoding = charset.search(html)
    if encoding is not None:
        encoding = encoding.group(1);
    if encoding is None:
        encoding = 'utf-8'
    except_encoding=encoding
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

def get_html(url):
    r = requests.get(url)
    return r.text;

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
                    if path=='':
                        path='/'
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
        self.name = None;
        self.headers = {};
        self.timeout = 30;
        self.best_encoding = 'utf-8'
        self.clear();


    def great_hand(self,attr=False,index=0):
        tree=self._tree;
        self._stage=2;
        if not self.multi :
            print('great hand can only be used in list')
            return self;
        result=  first_or_default( get_mount(search_properties(tree,self.xpaths,attr),take=1,skip=index))
        if result is None:
            print ('great hand failed')
            return self;
        root,xpaths= result
        datas= _get_datas(tree,xpaths,self.multi, None)
        self._datas= datas;
        self._xpaths= xpaths;
        return self;

    def set_paras(self, is_list=True,post_data='', root=None):
        self.multi=is_list;
        if root is not None:
            self.root=root;
        self.post_data=post_data;
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
        return self


    def set_headers(self, headers):
        dic = extends.para_to_dict(headers, '\n', ':')
        extends.merge(self.headers, dic);
        return self


    def test(self):
        paths= self.xpaths if self._stage > 3 else self._xpaths;
        root= self.root if self._stage >3  else self._root;
        self._stage = 3;
        self._datas=self._get_data_from_html(self._html, paths, root)
        if isinstance(self._datas,dict):
            self._datas=[self._datas]
        return self

    def crawl(self,url,post_data='',get_data=True):
        req_data=(url,post_data)
        for r in  self.crawls([req_data],get_data=True):
            if not get_data:
                r=r['Content']
            return r


    def crawls(self,req_datas,exception_handler=None,async=False,get_data=False,default_key='Content'):

        if async:
            import grequests
            def get_requests(req):
                url,post=req
                if post == '':
                    r = grequests.get(url, headers=self.headers)
                else:
                    r = grequests.post(url, headers=self.headers, data=post)
                return r

            res = grequests.map((get_requests(re) for re in req_datas), exception_handler=exception_handler);
        else:


            res=[]
            for m_req in req_datas:
                url,post=m_req
                if post=='':
                    r=requests.get(url,headers=self.headers)
                else:
                    r=requests.post(url,headers=self.headers,data=post)
            res.append(r)
        for response in res:
            if response is not None:
                data=get_encoding(response.content)
                if get_data:
                    data=self._get_data_from_html(data, self.xpaths, self.root)
                    if extends.is_str(data):
                        data = {default_key: data}
            else:
                data={default_key:''}
            yield data;


    def _get_data_from_html(self, html, xpaths, root):
        if isinstance(xpaths, list) and len(xpaths) == 0:
            return html
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
        self._stage=1;
        html = self.crawl(url,post_data,False);
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




