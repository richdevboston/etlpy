
# coding=utf-8
import sys,os
import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
sys.path.insert(0,parentdir)
from spider import *
from repl import  *

from py2neo import Graph, Node,Subgraph,Relationship,NodeSelector
graph = Graph("http://10.244.0.112:7474/db/data/",user='neo4j',password='kensho')
relation=graph.data("MATCH (n:stock) RETURN n.stock_code LIMIT 4000")
s=spider('s')
mongo=get_default_connector()
mongo.db='ant'

t=task()
t.pyge(script=relation)
t.pl(count_per_thread=10)
t.rename('n.stock_code:code')
t.addnew('url',value='http://www.cninfo.com.cn/cninfo-new/disclosure/szse/fulltext')
t.merge('code:post',script='stock={0}&searchkey=&category=&pageNum=1&pageSize=1&column=sse&tabName=latest&sortName=&sortType=&limit=&seDate=')
t.crawler('url',selector='s',post_data='[post]')
t.delete('post')
t.json('Content')
t.py('Content:l',script=lambda x: x['Content']['totalRecordNum']/15+2)
t.rangege('p',max='[l]',mode='cross')
t.py('p',script='str(int(value))')
t.merge('code:post',merge_with='p',script='stock={0}&searchkey=&category=&pageNum={1}&pageSize=15&column=sse&tabName=latest&sortName=&sortType=&limit=&seDate=')
t.delete('Content l')
t.crawler('url',selector='s',post_data='[post]')
t.delete('post')
t.json('Content')
t.py('Content',script="value['classifiedAnnouncements'][0]",mode='docs',new_col='code')
t.merge('adjunctUrl',script='http://www.cninfo.com.cn/{0}')
t.split('adjunctUrl:file',script='/',index=-1)
t.merge('code:save',script='../juchao/{0}/{1}',merge_with='file')
t.savefileex('adjunctUrl',savepath='[save]')
t.delete('save secCode')
t.dbex(selector='mongo',table='juchao')
t.get()

t.distribute()