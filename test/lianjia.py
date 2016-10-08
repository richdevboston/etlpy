
# coding=utf-8
import sys,os
import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
sys.path.insert(0,parentdir)
from spider import *
from repl import  *



# In[1]:




# In[2]:

s=spider('s')


# In[11]:

mongo=get_default_connector()
mongo.db='ant_temp'


# In[3]:

xq=spider('xq')
xq.visit('http://bj.lianjia.com/xiaoqu/chaoyang/').great_hand(attr=True).test().accept()
#xq.add_xpath('id','/html/body/div[4]/div[1]/ul/li[1]/div[1]/div[2]/a[2]/@href[1')
xq.get()


# In[4]:



# In[5]:

t=task('lj')
t.textge('url','http://bj.lianjia.com/ershoufang/rs/')
t.crawler('url:con',sl='s')
t.xpath('con',sc='/html/body/div[3]/div[2]/dl[2]/dd/div[1]/div/a',m_yield=True)
t.split('html',sc='"',index=1)
t.split('html',sc='/',index=2)
t.merge('html:url',sc='http://bj.lianjia.com/xiaoqu/{0}/')
t.crawler('url',sl='xq')
t.keep(u'col10_title:name,col12:rent_num,col14_href:href,col17_title:region,col4:name,col7:kind,col9:monthdeal,col21:seal,col20:price,bizcircle:desc')
t.number('kind,price,seal,rent_num,desc:age')
t.number('monthdeal',index=1)
t.replace('name',sc=u'网签')
t.replace('region',sc=u'小区') 
t.regex('href:id',sc='c\d+')
t.pl()
t.etlex('id', sl='xqh',new_col='age name region')
t.dbex('id',sl='mongo',table='bj_district')


# In[6]:

xqhs=spider('xqhs')
xqhs.visit('http://bj.lianjia.com/ershoufang/c1111027380785/').great_hand(True,2).test().accept().get()


# In[7]:

purl='http://bj.lianjia.com/ershoufang/pg{1}{0}/'
xqh=task('xqh')
xqh.textge('id','c1111027380785')
xqh.merge('id:url',sc=purl ,merge_with='1')
xqh.crawler('url',sl='s')
xqh.xpath('url',sc='/html/body/div[4]/div[1]/div[2]/h2/span')
xqh.number('url')
xqh.py('url',sc='int(value)/30+2')
xqh.rangege('p',max='[url]',mode='cross')
xqh.delete('url')
xqh.merge('id',sc=purl,merge_with='p')
xqh.crawler('id',sl='xqhs')
xqh.keep(u'col17_data-hid:hid,taxfree,col4:介绍,col13:总价,col14:单价,haskey,subway:地铁,title:标题,unique,col5_href:url')
#xqh.merge('hid:url',sc='http://bj.lianjia.com/ershoufang/{0}.html')
xqh.number(u'单价 总价')
xqh.pl()
xqh.etl(sl='h',mode='doc',range='1:100')
xqh.dbex(sl='mongo',table='lj_20161008')
#xqh.get(etl_count=100)


# In[8]:




# In[9]:

h= task('h')
h.textge('url','http://bj.lianjia.com/ershoufang/101100362062.html')
h.crawler('url',sl='s')
h.xpath('url',sc='//*[@class="content"]//li', m_yield=True)
h.trim('text')
h.matchft('html',sc='label',mode='re')
h.delete('html')
h.py('text:key',sc='value[:4]')
h.py('text:value',sc='value[4:]')
h.delete('text')
h.rotate('key',sc='[value]')

#h.get(2000)


# In[10]:

t.distribute()


# In[ ]:




