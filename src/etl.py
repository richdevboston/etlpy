# coding=utf-8
import csv
import json;
import os;
import time;
import traceback
import urllib
import xml.etree.ElementTree as ET

import spider
import extends
from extends import *

if PY2:
    pass;
else:
    import html

MERGE_APPEND= '+';
MERGE_CROSS= '*'
MERGE_MERGE= '|'
MERGE_MIX= 'mix'


CONV_ENCODE='e'
CONV_DECODE='d'

GET_HTML='html'
GET_NODE='node'
GET_TEXT='text'
GET_COUNT='count'
GET_ARRAY='list'


def __get_match_counts(mat):
    return mat.lastindex if mat.lastindex is not None else 1;

class ETLTool(EObject):
    def __init__(self):
        super(ETLTool, self).__init__()
        self.enabled=True;
        self.debug = False
        self.p=''

    def process(self, data,column):
        return data
    def init(self):
        pass;

    def get_p(self,data):
        return query(data,self.p)

    def _is_mode(self,mode):
        if not hasattr(self,'mode'):
            return False
        return mode in self.mode.split('|')

    def _eval_script(self,p, global_para=None, local_para=None):
        if p == '':
            return True
        if not is_str(p):
            return p(self)
        result=None;
        from datetime import datetime

        from time import mktime,strptime,strftime
        def get_time(mtime):
            from pytz import utc
            if mtime == '':
                mtime = datetime.now()
            ts = mktime(utc.localize(mtime).utctimetuple())
            return int(ts);
        try:
            if global_para is not None:
                result = eval(p,global_para,locals())

            else:
                result=eval(p);
        except Exception as e:
            traceback.print_exc()
        return result

    def __str__(self):
        return get_type_name(self)+'\t'+to_str(self.p)


class Transformer(ETLTool):
    def __init__(self):
        super(Transformer, self).__init__()
        self.one_input = False;
        self._m_process=False
    def transform(self,data):
        pass;

    def m_process(self,data,column):
        for r in data:
            yield r

    def _process(self,data,column,transform_func):
        def edit_data(col, n_col=None):
            n_col = n_col if n_col != '' and n_col is not None  else col;
            if col != '' and not has(data,col)  and (not isinstance(self, (SetTF, MapTF))):
                return data
            if self.one_input:
                res = transform_func(get_value(data,col))
                return set_value(data,n_col,res)
            else:
                n_col = n_col if n_col != '' and n_col is not None else col;
                try:
                    transform_func(data, col, n_col);
                    return data
                except Exception as e:
                    if extends.debug_level==0:
                        print e
                    else:
                        traceback.print_exc()
        if is_str(column):
            if column.find(u':') >= 0:
                column = para_to_dict(column, ' ', ':')
            elif column.find(' ') > 0:
                column = [r.strip() for r in column.split(' ')]
        if isinstance(column, dict):
            for k, v in column.items():
                edit_data(k, v);
        elif isinstance(column, (list, set)):
            for k in column:
                edit_data(k)
        else:
            return edit_data(column, None)
        return data
    def process(self,data,column,_m_process=True):
        if self._m_process==True and _m_process:
            for r  in self.m_process(data,column):
                yield r;
            return
        if data is None:
            return
        for d in data:
            if d is None:
                continue
            res= self._process(d,column,self.transform);
            yield res;

class Executor(ETLTool):
    def __init__(self):
        super(Executor, self).__init__()

    def execute(self,data,column):
        pass;
    def process(self,data,column):
        for r in data:
            yield self.execute(r,column);
    def init(self):
        pass


class Filter(ETLTool):
    def __init__(self):
        super(Filter, self).__init__()
        self.reverse=False;
        self.stop_while=False;
        self.one_input=True;
    def filter(self,data):
        return True;
    def process(self, data,column):
        error=0;
        for r in data:
            item = None
            result=False
            if self.one_input:
                if column in r:
                    item = r[column]
                    result = self.filter(item)
                if item is None and self.__class__ != NullFT:
                    continue;
            else:
                item=r;
                result = self.filter(item, column)

            if result == True and self.reverse == False:
                yield r;
            elif result == False and self.reverse == True:
                yield r;
            else:
                error+=1
                if self.stop_while==False:
                    continue
                elif  self.stop_while==True:
                    sys.stdout.write('stop iter \n')
                    break;
                elif isinstance(self.stop_while, int) and error >= self.stop_while:
                    sys.stdout.write('stop iter \n')
                    break



class Ascend(ETLTool):
    def __init__(self):
        super(Ascend, self).__init__()
        self._reverse= False
        self.p='value'
        self.one_input=True;

    def process(self, data,column):
        all_data= [r for r in data]
        p='value' if self.p=='' else  self.p
        def sort_map(data):
            import inspect
            if column=='':
                c=data
            else:
                c=data.get(column,None)
            if is_str(p):
                dic = merge({'value': c, 'data': data}, data)
                result = self._eval_script(p, dic)
            elif inspect.isfunction(p):
                result = p(c)
            return result

        all_data.sort(key=lambda x:sort_map(x),reverse=self._reverse)
        for r in all_data:
            yield r

class Descend(Ascend):
    def __init__(self):
        super(Descend, self).__init__()
        self._reverse =True



class Generator(ETLTool):
    def __init__(self):
        super(Generator, self).__init__()
        self.mode= MERGE_APPEND
        self.pos= 0;
    def generate(self,generator,column):
        pass;

    def process(self, generator,column):
        if generator is None:
            return  self.generate(None,column);
        else:
            if self.p== MERGE_APPEND:
                return append(generator, self.process(None,column));
            elif self.p==MERGE_MERGE:
                return merge(generator, self.process(None,column));
            elif self.p==MERGE_CROSS:
                return cross(generator, self.generate,column)
            else:
                return mix(generator, self.process(None,column))


EXECUTE_INSERT='insert'
EXECUTE_SAVE='save'
EXECUTE_UPDATE='update'


class MongoDBConnector(EObject):
    def __init__(self):
        super(MongoDBConnector, self).__init__()
        self.connect_str='';
        self.db=''

    def init(self):
        import pymongo
        client = pymongo.MongoClient(self.connect_str);
        self._db = client[self.db];






class DBBase(ETLTool):
    def __init__(self):
        super(DBBase, self).__init__();
        self.table = '';
        self.mode= EXECUTE_INSERT
    def get_table(self,data):
        c= query(data,self.p);
        t= query(data,self.table)
        connector = self._proj.env[c];
        connector.init()
        table = connector._db[t];
        return table


class DbEX(Executor,DBBase):
    def __init__(self):
        super(DbEX, self).__init__();

    def init(self):
        DBBase.init(self)
    def process(self,datas,column):
        for data in datas:
            table =self.get_table(data)
            work={EXECUTE_INSERT: lambda d: table.save(d), EXECUTE_UPDATE: lambda d: table.save(d)};
            new_data= copy(data)
            etype=query(data,self.mode)
            work[etype](new_data);
            yield data;


class DbGE(Generator,DBBase):
    def generate(self,data,column):
        table = self.get_table(self.table)
        for data in table.find():
            yield data;


class JoinDBTF(Transformer,DBBase):
    def __init__(self):
        super(JoinDBTF, self).__init__();
        self.index = ''
        self.mapper=''
    def transform(self, data, col,ncol):
        table = self.get_table(data)
        if table is None:
            buf=[]
        else:
            value = data[self.index]
            mapper= query(data,self.mapper)
            mapper=para_to_dict(mapper,' ',':');

            def db_filter(d):
                if '_id' in d:
                    del d['_id']
            keys= { r :1 for r  in mapper.keys()}
            result = table.find({self.index: value},keys)
            buf=[]
            for r in result:
                db_filter(r)
                r=conv_dict(r,mapper)
                buf.append(r)
        data[ncol]=buf



def _set_value(data, value, col, ncol=None):
    if ncol!='' and ncol is not None:
        set_value(data,ncol,value)
    else:
        set_value(data,col,value)
class MatchFT(Filter):
    def __init__(self):
        super(MatchFT, self).__init__();
        self.mode='str'
    def init(self):
        if self.mode=='re':
            self.regex = re.compile(self.p);
        self.count=1;

    def filter(self,data):
        p= self.get_p(data)
        if self.mode=='str':
            return data.find(p)>=0
        else:
            v = self.regex.findall(data);
            if v is None:
                return False;

        return self.count <= len(v)

class InnerTF(Filter):
    def __init__(self):
        super(InnerTF, self).__init__()
        self.one_input= False
    def filter(self,data,column):
        item=data[column]
        p=self.get_p(data)
        if p is None:
            return False
        sp=to_str(p).split(':')
        if len(sp)==1:
            p=query(data,p)
            return float(p)==item
        else:
            min=float(query(data,sp[0]))
            max=float(query(data,sp[1]))
            return min <= item <= max

class RepeatFT(Filter):
    def __init__(self):
        super(RepeatFT, self).__init__()
    def init(self):
        self.set=set();
    def filter(self,data):
        if data in self.set:
            return False;
        else:
            self.set.add(data);
            return True;

class NullFT(Filter):
    def filter(self,data):
        if data is None:
            return False;
        if is_str(data):
            return data.strip() != '';
        return True;




class SetTF(Transformer):
    def __init__(self):
        super(SetTF, self).__init__()

    def transform(self, data, col,ncol):
        p=self.get_p(data)
        data[col] = p


class LetTF(Transformer):
    def __init__(self):
        super(LetTF, self).__init__()
        self.value=None

    def transform(self, data, col, ncol):
        if self.value is not None:
            data[col] = self.value;


class IncrTF(Transformer):
    def init(self):
        super(IncrTF, self).__init__()
        self._index = 0;

    def transform(self, data):
        self._index += 1;
        return self._index;

class TagTF(Transformer):
    pass

class CopyTF(Transformer):
    def __init__(self):
        super(CopyTF, self).__init__()
    def transform(self, data, col, ncol):
        v=get_value(data, col)
        set_value(data, ncol, v)

class Copy2TF(CopyTF):
    pass

class MoveTF(Transformer):
    def transform(self, data, col, ncol):
        if col !=ncol:
            set_value(data, ncol, get_value(data, col))
            del_value(data,col)

class RemoveTF(Transformer):
    def __init__(self):
        super(RemoveTF, self).__init__()

    def transform(self, data,col,ncol):
        del_value(data,col)

class KeepTF(Transformer):
    def __init__(self):
        super(KeepTF, self).__init__()
        self._m_process=True

    def m_process(self,datas,col):

        if col.find(':') > 0:
            col = para_to_dict(col, ' ', ':');
            for data in datas:
                doc={}
                for k, v in data.items():
                    if k in col:
                        doc[col[k]] = v
                yield doc
        else:
            col = col.split(' ');
            for data in datas:
                doc={}
                for k, v in data.items():
                    if k in col:
                        doc[k] = v
                yield doc


class EscapeTF(Transformer):
    def __init__(self):
        super(EscapeTF, self).__init__()
        self.one_input = True;
        self.p=CONV_DECODE
    def transform(self, data):
        p=self.get_p(data)
        if  p== CONV_DECODE:
            if PY2:
                return data.encode('utf-8').decode('string_escape').decode('utf-8').replace('\'','\"')
            else:
                return data.decode('unicode_escape')
        return data;

class CleanTF(Transformer):
    def __init__(self):
        super(CleanTF, self).__init__()
        self.one_input=True;
        self.p=CONV_DECODE
    def transform(self, data):
        if PY2:
            if self.p!=CONV_ENCODE:
                import HTMLParser
                html_parser = HTMLParser.HTMLParser()
                return html_parser.unescape(data)
            else:
                import cgi
                return  cgi.escape(data)
        else:
            return html.escape(data) if self.p == CONV_ENCODE else html.unescape(data);


class UrlTF(Transformer):
    def __init__(self):
        super(UrlTF, self).__init__()
        self.one_input = True;
        self.p = CONV_DECODE
    def transform(self, data):
        p=self.get_p(data)
        if p == CONV_ENCODE:
            url = data.encode('utf-8');
            return urllib.parse.quote(url);
        else:
            return urllib.parse.unquote(data);


class MergeTF(Transformer):
    def __init__(self):
        super(MergeTF, self).__init__()
        self.p= '{0}'
        self._re= re.compile('\{([\w_]+)\}')
    def transform(self, data, col, ncol=None):
        def get_value(data, k):
            r=None
            if k== '0':
                r= data[col]
            if k in data:
                r= data[k]
            if r is None:
                return ''
            else:
                return to_str(r)
        input= self.p
        result = self._re.finditer(self.p);
        if result is None:
            return

        for r in result:
            all,key= r.regs
            all= input[all[0]:all[1]]
            key= input[key[0]:key[1]]
            input=input.replace(all,get_value(data,key))
        data[ncol]=input

class HtmlTF(Transformer):
    def __init__(self):
        super(HtmlTF, self).__init__()
        self.one_input = True;

    def transform(self, data):
        res = spider.get_node_html(data)
        return res


class TextTF(Transformer):
    def __init__(self):
        super(TextTF, self).__init__()
        self.one_input = True;

    def transform(self, data):
        res = spider.get_node_text(data)
        return res

class RegexTF(Transformer):
    def __init__(self):
        super(RegexTF, self).__init__()
        self.one_input = True;

    def transform(self, data):
        regex = re.compile(self.p);
        items = re.findall(regex, to_str(data));
        return [r for r in items]

class LastTF(Transformer):
    def __init__(self):
        super(LastTF,self).__init__()
        self.count=1
        self._m_process=True

    def m_process(self, data,column):
        r0 = None
        while True:
            try:
                # 获得下一个值:
                x = next(data)
                r0=x
            except StopIteration:
                # 遇到StopIteration就退出循环
                break
        yield r0



class AggTF(Transformer):
    def __init__(self):
        super(AggTF, self).__init__()
        self._m_process = True
    def m_process(self,data,column):
        p=self.get_p(data)
        r0=None
        import inspect
        for m in data:

            if column!='':
                r=m[column]
            if r0==None:
                r0=r
                yield m
                continue
            if is_str(p):
                r2=self._eval_script(p,global_para={'a': r0, 'b': r})
            elif inspect.isfunction(p):
                r2 = p(r0,r)
            if r2 != None:
                r =r2
            if column!='':
                m[column]=r;
            else:
                m=r
            yield m
            r0=r



class ReplaceTF(RegexTF):
    def __init__(self):
        super(ReplaceTF, self).__init__()
        self.value = '';
        self.mode='str';
        self.one_input=False

    def transform(self, data,col,ncol):
        ndata=data[col]
        if ndata is None:
            return
        new_value= query(data,self.value)
        p=self.get_p(data)
        if self.mode=='re':
            result= re.sub(p, new_value, ndata);
        else:
            if ndata is None:
                result= None
            else:
                result=ndata.replace(p,new_value);
        data[ncol]=result
class NumTF(Transformer):
    def __init__(self):
        super(NumTF, self).__init__()
        self.one_input=True;
        self.index=0
    def init(self):
        self.regex=  re.compile('\d+');
        self.index=int(self.index)
    def transform(self, data):
        item = re.findall(self.regex, to_str(data));
        if self.index < 0:
            return '';
        if len(item) <= self.index:
            return '';
        else:
            r = item[self.index];
            return r if is_str(r) else r[0];

class SplitTF(Transformer):
    def __init__(self):
        super(SplitTF, self).__init__()
        self.one_input = True;

    def init(self):
        self.splits = self.p.split(' ');
        if '' in self.splits:
            self.splits.remove('')
    def transform(self, data):
        if len(self.splits)==0:
            return data;
        for i in self.splits:
            data = data.replace(i, '\001');
        r=data.split('\001');
        return r
class TrimTF(Transformer):
    def __init__(self):
        super(TrimTF, self).__init__()
        self.one_input = True;

    def transform(self, data):
        return data.strip();

class ExtractTF(Transformer):
    def __init__(self):
        super(ExtractTF, self).__init__()
        self.has_margin=False;
        self.one_input=True;
        self.end= ''

    def transform(self, data):
        start = data.find(self.p);
        if start == -1:
            return
        end = data.find(self.end, start);
        if end == -1:
            return;
        if self.has_margin:
            end += len(self.end);
        if not self.has_margin:
            start += len(self.p);
        return data[start:end];

class MapTF(Transformer):
    def __init__(self):
        super(MapTF, self).__init__()
        self.p='script'


    def _get_data(self, data, col):
        p=self.get_p(data)
        if col=='':
            if is_str(p):
                dic = merge({'data': data}, data)
                self._eval_script(p,dic);
            else:
                p(data);
            return None
        else:
            value = data[col]
            if is_str(p ):
                dic = merge({'value': value, 'data': data}, data)
                result = self._eval_script(p,dic);
            else:
                result =p(value);
        return result;

    def transform(self, data,col,ncol):
        js = self._get_data( data,col)
        if ncol!='':
            data[ncol]=js

class Create(Generator):
    def __init__(self):
        super(Create, self).__init__()
        self.p='xrange(1,20,1)'
    def can_dump(self):
        return  is_str(self.p);
    def generate(self,generator,column):
        p=self.get_p(generator)
        import inspect;
        import copy
        if is_str(p):
            result = self._eval_script(p);
        elif inspect.isfunction(p):
            result= p()
        else:
            result= p;
        for r in result:
            if column!= '':
                yield {column:r};
            else:
                yield copy.copy( r)

class Where(Filter):
    def __init__(self):
        super(Where, self).__init__()
        self.p='True';
        self.one_input=False;
    def can_dump(self):
        return  is_str(self.p);
    def filter(self, data,column):
        p=self.get_p(data)
        import inspect
        data=copy(data)
        if column=='':
            value=data
        else:
            value = data[column] if column in data else '';
        if is_str(p):
            dic=merge({'value': value, 'data': data},data)
            result = self._eval_script(p,dic);
        elif inspect.isfunction(p):
            result = p(value)
        if result==None:
            return False
        return result;

class DetectTF(Transformer):
    def __init__(self):
        super(DetectTF, self).__init__()
        self.index=0;
        self.attr=False


    def transform(self, data, col,ncol):
        p=self.get_p(data)
        from spider import search_properties,get_datas
        root = data[col]
        if root is None:
            return

        if is_str(root):
            from lxml import etree
            root = spider._get_etree(root)
        tree = etree.ElementTree(root);
        if p!='':
            xpaths= spider.get_diff_nodes(tree,root,self.p,self.attr)
            root_path=p
        else:
            result = first_or_default(get_mount(search_properties(root, None, self.attr), start=1, end=self.index))
            if result is None:
                print ('great hand failed')
                yield data
                return
            root_path, xpaths = result
        for r in xpaths:
            r.path= spider.get_sub_xpath(root_path,r.path);

        code0=  '\n.'.join((u"cp(':{col}').xpath('/{path}')\\" .format(col=r.name,path=r.path,sample=r.sample) for r in xpaths))
        code= u".xpath('%s').list().html().tree()\\\n.%s" %(root_path,code0 );
        print code
        code2= '\n'.join(u"#{key} : #{value}".format(key=r.name,value=r.sample.strip()) for r in xpaths);
        print code2
        datas = get_datas(root, xpaths, True, root_path=root_path)
        data[ncol]=datas
class CacheTF(Transformer):
    def __init__(self):
        super(CacheTF, self).__init__()
        self._m_process = True


    def m_process(self,datas,column):
        i=0
        cache=self.p
        if cache is None:
            for data in datas:
                yield data
            return
        while i<len(cache):
            yield cache[i]
            i+=1
        del cache[:]
        for r in datas:
            cache.append(r)
            yield r;


class CrawlerTF(Transformer):
    def __init__(self):
        super(CrawlerTF, self).__init__()
        self.encoding = 'utf-8'
        self._mode='get'
        self.header=''


    def transform(self, data, col,ncol):
        import requests
        from spider import get_encoding
        url=data[col]
        paras = {}
        paras['url']=url
        if self.header in self._proj.env:
            headers = self._proj.env[self.header]
            paras['headers']=headers
        if self.p != '':
            para_data = self.get_p(data)
            key = 'data' if self._mode == 'post' else 'params';
            paras[key] = para_data
        if self._mode == 'get':
            r = requests.get(**paras)
        else:
            r = requests.post(**paras)
        if r is not None:
            result = get_encoding(r.content)
        data[ncol]=result

class GetTF(CrawlerTF):
    def __init__(self):
        super(GetTF, self).__init__()
        self._mode = 'get';

class PostTF(CrawlerTF):
    def __init__(self):
        super(PostTF, self).__init__()
        self._mode = 'post';


class TreeTF(Transformer):
    def __init__(self):
        super(TreeTF, self).__init__()
        self.one_input = True;

    def transform(self, data):
        root = spider._get_etree(data);
        return root


class SearchTF(Transformer):
    def __init__(self):
        super(SearchTF, self).__init__()
        self._m_yield=False;
        self.one_input=True;
        self.mode='str'
    def transform(self, data):
        from spider import search_xpath
        if data is None:
            return None;
        if is_str(data):
            from lxml import etree
            tree = spider._get_etree(data);
        result = search_xpath(tree, self.p,self.mode, True);
        print result
        return result

class ListTF(Transformer):
    def __init__(self):
        super(ListTF, self).__init__()
        self._m_process=True
    def m_process(self, datas, col):
        for data in datas:
            root = data[col]
            p=self.get_p(data)
            for r in root:
                r={col:r}
                my = merge_query(r, data, p)
                yield my


class XPathTF(Transformer):
    def _trans(self,data):
        from lxml import etree
        root=None
        if isinstance(data, (str, unicode)):
            root = spider._get_etree(data);
            tree = etree.ElementTree(root)
        else:
            tree = data
        return tree,root


    def transform(self,data,col,ncol):
        target=data[col]
        tree,root= self._trans(target)
        if tree is None:
            return
        node_path = query(data, self.p);
        if node_path is None or node_path=='':
            nodes=[target]
        else:
            nodes = tree.xpath(self.p);
            if nodes is None:
                nodes=[target]
        data[ncol]= nodes;




class PyQTF(XPathTF):
    def __init__(self):
        super(PyQTF, self).__init__()

    def transform(self, data, col,ncol):
        from pyquery import PyQuery as pyq
        root = pyq(data[col]);
        if root is None:
            return;
        node_path = self.get_p(data)
        if node_path == '' or node_path is None:
            return
        nodes = root(node_path);
        data[ncol]= nodes if nodes is not None else []


class AtTF(Transformer):
    def __init__(self):
        super(AtTF, self).__init__()
        self.one_input = True;

    def transform(self, data):

        if isinstance(data,(list,tuple)):
            p=get_num(self.get_p(data),0)
            if len(data)<=p:
                return None
            return data[p]
        elif data is None:
            return None
        else:
            p=self.get_p(data)
            return data.get(p,None)

class TnTF(Transformer):

    def __init__(self):
        super(TnTF,self).__init__()
        self.rule=None;
        self.one_input=True;

    def init(self):
        import codecs
        from tn_py.tnpy import tn
        import tn_py.tnnlp as tnnlp
        import tn_py.custom as custom
        TnTF.log=False
        if not hasattr(TnTF,'core'):
            TnTF.core = tn()
            core=TnTF.core

            core.init_tn_rule('src/tn_py/cnext', True)
            core.init_py_rule(tnnlp)
            core.init_py_rule(custom)
            if TnTF.log:
                core.log_file= codecs.open("../test/log.txt", 'w', encoding='utf-8')
            core.rebuild()
    def transform(self,data):
        core=TnTF.core;
        data=to_str(data)
        result=core.extract(data, entities=[self.rule]);
        if any(result):
            result= result[0]['#rewrite'];
            return result
        if TnTF.log:
            core.log_file.flush()
        return '';


class ParallelTF(Transformer):
    def __init__(self):
        super(ParallelTF, self).__init__()
        self.p=1


class DumpTF(Transformer):
    def __init__(self):
        super(DumpTF, self).__init__()
        self.one_input=True
        self.p='json'
    def transform(self, data):
        if self.p=='json':
            return json.dumps(data)

        elif self.p=='yaml':
            import yaml
            return yaml.dumps(data)



class LoadTF(Transformer):
    def __init__(self):
        super(LoadTF, self).__init__()
        self.one_input = True
        self.p='json'

    def transform(self, data):
        p=self.p
        if p=='json':
            return json.loads(data)
        elif p=='yaml':
            import yaml
            return yaml.loads(data)

class Range(Generator):
    def __init__(self):
        super(Range, self).__init__()
        self.p = ':'
    def generate(self,generator,column):
        p=self.get_p(generator)
        start,end,interval= get_range(p)
        if start==end:
            yield {column:start}

            return
        values=range(start,end,interval)
        for i in values:

            item= {column:i}
            yield item;



class SubBase(ETLTool):
    def __init__(self):
        super(SubBase, self).__init__()
        self.range = '0:100'

    def _get_task(self, data):
        p = self.get_p(data)
        if isinstance(p,ETLTask):
            return p
        if p not in self._proj.env:
            sys.stderr.write('sub task %s  not in current project' % p);
        sub_etl = self._proj.env[p];
        return sub_etl;


    def _get_tools(self,data,column):
        sub_etl = self._get_task(data)
        start,end,interval= get_range(self.range)
        tools=  tools_column(tools_filter(sub_etl.tools[start:end:interval],excluded=self))
        return tools

    def _generate(self, data,column,execute=False,init=False):
        doc=copy(data)
        generator=[doc] if doc is not None else None
        for r in ex_generate(self._get_tools(doc,column),generator,execute=execute,init=init):
            yield r;



class SubGE(Generator, SubBase):
    def __init__(self):
        super(SubGE, self).__init__()


    def generate(self, data,column):
        return self._generate(data)

class SubEx(Executor, SubBase):
    def __init__(self):
        super(SubEx, self).__init__()


    def process(self,data,column):
        for d in data:
            if self.debug == True:
                for r in self._generate(d, False):
                    yield r;
            else:
                yield self.execute(d)

    def execute(self,data,column):

        count=0
        try:
            for r in self._generate( data,self.enabled):
                count+=1;
        except Exception as e:
            sys.stderr.write('subtask fail')
        print('subtask:'+to_str(count))
        return data;

class SubTF(Transformer, SubBase):

    def __init__(self):
        super(SubTF, self).__init__()
    # def m_transform(self,data,col):
    #     if self.cycle:
    #         result = first_or_default(self._generate(data))
    #         if result is None:
    #             return
    #         yield copy(result)
    #     else:
    #         for r in self._generate(data):
    #             if r is not None:
    #                 yield r


    def transform(self,data,col,ncol):
        data[ncol]= (r for r in self._generate(data,col))


class StrTF(Transformer):
    def __init__(self):
        super(StrTF, self).__init__()
        self.one_input = True
    def transform(self,node):
        if hasattr(node, 'text'):
            res = spider.get_node_text(node);
        else:
            res = to_str(node)
        return res


class RotateTF(Transformer):
    def __init__(self):
        super(RotateTF, self).__init__();
        self._m_process = True

    def m_process(self, datas,column):
        result={}
        for data in  datas:
            p = self.get_p(data)
            key= data.get(column,None)
            if key is None:
                continue
            value = query(data,p);
            result[key]=value
        yield result


class ToDictTF(Transformer):
    def __init__(self):
        super(ToDictTF, self).__init__();
        self._m_process=True

    def m_process(self,datas,col):
        for data in datas:
            doc={}
            merge_query(doc,data,self.p)
            data[col]=doc
            yield data

class DrillTF(Transformer):
    def __init__(self):
        super(DrillTF, self).__init__();
        self._m_process = True

    def m_process(self, datas, col):
        for data in datas:
            doc = copy(data)
            col_data = data[col]
            if self.p != '':
                merge_query(doc, col_data, self.p)
            else:
                merge(doc, col_data)
            yield doc


class TakeTF(Transformer):
    def __init__(self):
        super(TakeTF, self).__init__();


    def process(self,data,column):
        p= get_num(self.get_p(data));
        for r in get_mount(data,p):
            yield r;

class SkipTF(Transformer):
    def __init__(self):
        super(SkipTF, self).__init__();

    def process(self,data,column):
        p = get_num(self.get_p(data));
        for r in get_mount(data,None, p):
            yield r;


class DelayTF(Transformer):
    def __init__(self):
        super(DelayTF, self).__init__();
        self.p=100;

    def m_process(self,datas,col):
        import time
        for data in datas:
            p=get_num(self.get_p(data),default=0)
            time.sleep(float(p)/1000.0)
            yield data;

class Read(Generator):
    pass

class DownloadEX(Executor):
    def __init__(self):
        super(DownloadEX, self).__init__()
        self.path= '';


    def execute(self,data,column):
        import requests
        p=self.get_p(data)
        (folder,file)=os.path.split(p);
        if not os.path.exists(folder):
            os.makedirs(folder);
        url= data[column];
        target= open(p,'wb');
        req = requests.get(url)
        if req is None:
            return
        target.write(req.content);
        target.close()
        return data



class Project(EObject):
    def __init__(self):
        self.env={};
        self.__cache=[]
        self.desc="edit project description here";

    def clear(self):
        self.env.clear()
        return self;

    def dumps_json(self):
        dic = convert_dict(self )
        return json.dumps(dic, ensure_ascii=False, indent=2)

    def dumps_yaml(self):
        import yaml
        dic = convert_dict(self)
        return yaml.dump(dic)

    def dump_yaml(self, path):
        with extends.open(path, 'w', encoding='utf-8') as f:
            f.write(self.dumps_yaml());
    def load_yaml(self, path):
        import yaml
        with open(path, 'r', encoding='utf-8') as f:
            d = yaml.load(f);
            return self.load_dict(d)


    def loads_json(self, js):
        d = json.loads(js);
        return self.load_dict(d)

    def dump_json(self,path):
        with extends.open(path,'w',encoding='utf-8') as f:
            f.write(self.dumps_json());

    def load_json(self,path):
        with extends.open(path,'r',encoding='utf-8') as f:
            js= f.read();
            return self.loads_json(js)


    def load_dict(self,dic):
        items =dic.get('env',{});
        for key, item in items.items():
            if 'Type' in item:
                obj_type = item['Type']
                task = eval('%s()' % obj_type);
                if obj_type == 'ETLTask':
                    for r in item['tools']:
                        etl = eval('%s()' % r['Type']);
                        for attr, value in r.items():
                            if attr in ['Type']:
                                continue;
                            setattr(etl, attr, value);
                        etl._proj = self;
                        task.tools.append(etl)
                else:
                    dict_copy_poco(task,item);
            else:
                task=item
            self.env[key]=task
        return self;




def convert_dict(obj):
    if not isinstance(obj, ( int, float, list, dict, tuple, EObject)) and not is_str(obj):
        return None
    if isinstance(obj, EObject):
        d={}
        obj_type= type(obj);
        typename= get_type_name(obj)
        default= obj_type().__dict__;
        for key, value in obj.__dict__.items():
            if value== default.get(key,None):
                    continue;
            if key.startswith('_'):
                continue;
            p =convert_dict(value)
            if p is not None:
                d[key]=p
        d['Type']= typename;
        return d;

    elif isinstance(obj, list):
       return [convert_dict(r) for r in obj];
    elif isinstance(obj,dict):
        return {key: convert_dict(value) for key,value in obj.items()}
    return obj;


def ex_generate(tools, generator=None, init=True, execute=False, enabled=True):

    mapper, reducer, tolist = parallel_map(tools_filter(tools,init,execute,enabled))
    for r in generate(mapper, generator, init=False, execute=execute):
        if reducer is None:
            yield r
        else:
            if isinstance(r, dict):
                r = [r]
            for p in ex_generate(reducer, r, init=False, execute=execute):
                yield p


def tools_filter(tools, init=True, executed=False, enabled=True,excluded=None):

    buf=[]
    for tool in tools:
        if excluded==tool:
            continue
        if tool.enabled == False and enabled == True:
            continue
        if isinstance(tool, Executor):
            if executed == False and tool.debug == False:
                continue
        if init:
            tool.init()
        buf.append(tool)
    return buf

def tools_column(tools,start_column=''):
    buf = []
    column=start_column
    for tool in tools:
        if isinstance(tool, LetTF):
            column = tool.p
            continue
        elif isinstance(tool, (RemoveTF, KeepTF)):
            column = tool.p
        if isinstance(tool, (CopyTF, MoveTF,Copy2TF)):
            column=p=tool.p
            if is_str(p):
                if p.find(u':') >= 0:
                    p2 = para_to_dict(p, ' ', ':')
                elif p.find(' ') > 0:
                    p2 = [r.strip() for r in p.split(' ')]
            if isinstance(p2, dict):
                if not isinstance(tool, Copy2TF):
                    p2 = p2.values()
                else:
                    p2= p2.keys()
            next_column = ' '.join(p2)
        else:
            next_column = ''
        buf.append((tool,column))
        if next_column != '':
            column = next_column
    return buf


def generate(tools, generator=None, init=True, execute=False, enabled=True,start_column=''):
    if tools is not  None:
        for tool, column in tools_column(tools_filter(tools, init, execute, enabled),start_column):
            generator = tool.process(generator, column)
    if generator is None:
        return []
    return generator;


def count(generator):
    if isinstance(generator,list):
        return len(generator);
    if isinstance(generator,Generator):
        if hasattr(generator,'_mount'):
            return generator._mount;
    return 100;



def parallel_map(tools):
    index= get_index(tools, lambda x:isinstance(x, ParallelTF))
    if index==-1:
        return tools,None,None;
    mapper = tools[:index]
    reducer=tools[index+1:]
    parameter= tools[index];
    return mapper,reducer,parameter;




class ETLTask(EObject):
    def __init__(self):
        self.tools = [];
        self.name=''
        self._master=None;
        self._last_column=''
    def clear(self):
        self.tools=[]
        return self;

    def __getitem__(self, item):
        tool = AtTF()
        tool._proj = self._proj
        tool.p = item
        self.tools.append(tool);
        return self

    def to_json(self):
        dic = convert_dict(self)
        return json.dumps(dic, ensure_ascii=False, indent=2)

    def to_yaml(self):
        import yaml
        dic = convert_dict(self)
        return yaml.dump(dic)

    def eval(self,script=''):
        pass
    def to_graph(self,path='etl.jpg'):
        import pygraphviz as pgv
        A = pgv.AGraph(directed=True, strict=True);
        last = "root"
        A.add_node(last)
        i=0
        for tool,column in tools_column(self.tools):
            i+=1
            m_type=get_type_name(tool)
            node=m_type+'_'+str(i)
            A.add_node(node)
            print column
            A.add_edge(last,node,column)
            last= node

        A.graph_attr['epsilon'] = '0.001'
        A.layout('dot')  # layout with dot
        A.draw(path)  # write to file

    def check(self,count=10):
        tools=  force_generate(tools_filter(self.tools))
        for i in range(1,len(tools)):
            attr=EObject()
            tool=tools[i];
            title= get_type_name(tool).replace('etl.','')+' '+tool.column;
            list_datas =  to_list(progress_indicator(get_keys(get_mount(ex_generate(tools[:i],init=False), start=count), attr), count=count, title=title));
            keys= ','.join(attr.__dict__.keys())
            print('%s, %s, %s'%(str(i),title,keys))

    def distribute(self ,take=90999999,skip=0,port= None,monitor_connector_name=None,table_name=None):
        import distributed
        self._master= distributed.Master(
        self._master.start_project(self._proj, self.name,take,skip,port,monitor_connector_name,table_name))

    def _pl_generator(self,take=-1,skip=0):
        mapper, reducer, parallel = parallel_map(self.tools)
        if parallel is None:
            print 'this script do not support pl...'
            return

        count_per_group = parallel.p if parallel is not None else 1;
        task_generator = extends.group_by_mount(ex_generate(mapper), count_per_group, take, skip);
        task_generator = extends.progress_indicator(task_generator, 'Task Dispatcher', count(mapper))
        for r in task_generator:
            yield r

    def _get_related_tasks(self,tasks):
        for r in self.tools:
            if isinstance(r, SubBase) and r not in tasks:
                tasks.add(r.name)
                r._get_related_tasks(tasks)
    def get_related_tasks(self):
        tasks=set()
        self._get_related_tasks(tasks)
        return tasks

    def rpc(self,method='finished',server='127.0.0.1',port=60007,take=-1,skip=0):
        import requests

        if method in ['finished','dispatched','clean']:
            url="http://%s:%s/task/query/%s"%(server,port,method);
            data= json.loads(requests.get(url).content)
            print 'remain: %s'%(data['remain'])
            return fetch(data[method],count=1000000)
        elif method=='insert':
            url="http://%s:%s/task/%s"%(server,port,method);
            tasks=self.get_related_tasks()
            n_proj=convert_dict(self._proj);
            for k,v in n_proj.items():
                if  isinstance(v,dict) and v.get('Type',None)=='ETLTask' and v['name'] not in tasks:
                    del n_proj[k]
            id=0;
            for task in progress_indicator( self._pl_generator(take,skip)):
                job={'proj':n_proj,'name':self.name,'tasks':task,'id':id}
                id+=1
                res=requests.post(url,json=job)
            print 'total push tasks: %s'%(id);



    def stop_server(self):
        if self._master is None:
            return 'server is not exist';
        self._master.manager.shutdown();
    def __str__(self):
        def conv_value(value):
            if is_str(value):
                value= value.replace('\n',' ').replace('\r','');
                sp="'"
                if value.find("'")>=0:
                    if value.find('"')>=0:
                        sp="'''"
                    else:
                        sp='"'
                return "%s%s%s"%(sp,value,sp);
            return value;
        array = [];
        array.append('##task name:%s'%self.name);
        array.append('.clear()')
        for t in self.tools:
            typename = get_type_name(t);
            s = ".%s(%s" % (typename, conv_value(t.column));
            attrs = [];
            defaultdict = type(t)().__dict__;
            for att in t.__dict__:
                value = t.__dict__[att];
                if att in ['one_input','OneOutput', 'column', '_m_yield']:
                    continue
                if not isinstance(value, ( int, bool, float)) and not is_str(value):
                    continue;
                if value is None or att not in defaultdict or defaultdict[att] == value:
                    continue;
                attrs.append(',%s=%s' % (att.lower(), conv_value(value)))
            if any(attrs):
                s += ''.join(attrs)
            s+=')\\'
            array.append(s)
        return '\n'.join(array);

    def query(self, etl=100, execute=False):
        tools= self.tools[:etl];
        for r in ex_generate(tools,execute=execute):
            yield r;

    def run(self, etl=100, execute=True):
        count= force_generate(progress_indicator(self.query(etl, execute)))
        print('task finished,total count is %s'%(count))



    def fetch(self,take=10, format='print', etl=100, skip=0,execute=False,paras=None):
        datas= get_mount(self.query(etl,execute=execute), take, skip)
        if etl<len(self.tools):
            print 'current position: '+ str(self.tools[etl])
        return fetch(datas,format,count=take,paras=paras);

    def run_m(self, thread_count=10, execute=True, take=999999, skip=0):
        import threadpool
        pool = threadpool.ThreadPool(thread_count)
        mapper,reducer,parallel= parallel_map(self.tools);
        count_per_group = parallel.p if parallel is not None else 1;
        mapper_generator= generate(mapper,execute=execute);
        def function(item):
            def get_task_name():
                if parallel is None:
                    return 'sub task';
                return query(item,parallel.column);
            generator= generate(reducer, item, execute);
            count = force_generate(progress_indicator(generator,title=get_task_name()))

        requests = threadpool.makeRequests(function, to_list(group_by_mount(mapper_generator, count_per_group, skip=skip, take=take)));
        [pool.putRequest(req) for req in requests]
        pool.wait()
    def run_async(self, max_size=10, execute=True):
        from gevent import monkey
        monkey.patch_socket()
        import gevent
        from gevent.queue import Queue, Empty
        mapper, reducer, parallel = parallel_map(self.tools);
        tasks = Queue(max_size)
        def worker(n):
            try:
                while True:
                    task = tasks.get(timeout=5)
                    generator = generate(reducer, task, execute);
                    count = force_generate(progress_indicator(generator, title="sub task"))
                    gevent.sleep(0)
                    print count
            except Empty:
                pass
        def boss():
            mapper_generator = generate(mapper, execute=execute);
            for task in mapper_generator:
                tasks.put([task])
        joins= [gevent.spawn(boss)]
        for i in range(0,max_size):
            joins.append(gevent.spawn(worker, 'job' + str(i)))
        gevent.joinall(joins)






