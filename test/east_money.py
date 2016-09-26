
# coding=utf-8
import sys,os
import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
sys.path.insert(0,parentdir)
from spider import *
from repl import  *
from extends import *
from pyquery import PyQuery as pyq
s=spider('s')

def conv(x):
    x=x['html']
    #x=pyq(x['html'])
    x=x.attrib['href']
    return x;
url='http://data.eastmoney.com/cjsj/consumerpriceindex.aspx?p=2';

c=task('total')
c.textge('url','http://data.eastmoney.com/cjsj/gyzjz.html')
c.crawler('url',selector='s')
c.pyquery('Content',script='.uls li div a')
c.py('html',script=lambda x:conv(x))
c.split('html:type',script='.')
c.rename('html:url')
c.etlex('url', selector='cpi',range='1:100',new_col='type')


mongo= get_default_connector()
mongo.db='ant'

def get_content(c):

    root = pyq(c['Content']);
    if root is None:
        return [{}]
    def get_texts(p):
        buf=[]
        for r in p:
            buf.append(r.text)
        return buf;

    first = root('.firstTr th strong')
    cols = get_texts(first)[1:]
    second = root('.secondTr td')
    sec_cols = get_texts(second)
    l=0 if len(sec_cols)==0 else 1
    if l!=0:
        sec_cols= sec_cols[:len(sec_cols)/len(cols)]
        cols = to_list(cross_array(cols, sec_cols, lambda a, b: a + '_' + b));
    cols.insert(0,u'日期')
    buf=[]
    i=0;
    for r in to_list(root('tr')):
        i+=1
        if i<l+2:
            continue
        r=pyq(r)
        buf1=[]
        for p in pyq(r)('td'):
            p= get_node_text(p).strip()
            buf1.append(p)
        buf1= dict(zip(cols,buf1))
        buf.append(buf1)
    return buf;

#s.test().get()




t=task('cpi')
t.clear()
t.textge('url','cpi.html')
t.merge('url',script='http://data.eastmoney.com/cjsj/{0}')
t.crawler('url',selector='s')
t.strextract('Content:rurl',start='<form id="form2" action="',end='<input')
t.split('rurl',script='"')
t.strextract('Content:p',start="pageit('",end="');")
t.delete('Content')
t.rangege('p',max=7,min=1)
t.py('p',script='str(int(value))')
t.merge('p:url',script='http://data.eastmoney.com/cjsj/{1}?p={0}',merge_with='rurl')
t.crawler('url',selector='s')
t.py('Content',script=get_content,mode='docs',new_col='type')
t.nullft(u'日期')
t.dbex(selector='mongo',table='[type]')
t.get(etl_count=3)

c.execute()