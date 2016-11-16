
# coding=utf-8
import sys,os
import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
sys.path.insert(0,parentdir)
from spider import *
from repl import  *

mongo=get_default_connector()

mongo.db='ant135'
import hashlib

def md5(src):
    m2 = hashlib.md5()
    m2.update(src.encode('utf-8'))
    return m2.hexdigest()

mongo=get_default_connector()
mongo.db='ant135'

url='http://www.yidianzixun.com/api/q/?path=channel|\
news-list-for-channel&channel_id={0}&fields=docid&\
fields=category&fields=date&fields=image&fields=image_urls\
&fields=like&fields=source&fields=title&fields=url&fields=comment_count\
&fields=summary&fields=up&cstart={1}&cend={2}&version=999999&infinite=true'

y= task(u'一点').textge('url',sc='http://www.yidianzixun.com/home?page=channellist').crawler().pyq(sc='.cate-box',mode='list|html').list()
y.xpath(':class',sc='//h3',mode='text').pyq(sc='ul>li',mode='list|html').list('url',sc='class').xpath(':stype',sc='//a/@href').text('url:sclass').pl()
y.split('stype',sc='&').at(sc='-1').split(sc='=').at(sc='1')

y.rangege('p',max=100000,min=0,interval=100,mode='cross').py('p:p2',sc='int(value)+100')
y.merge('stype',sc=url,merge_with='p p2').delete('url').crawler('stype').json(mode='decode').py('stype',sc='value["result"]')
y.pyft('stype',sc='len(value)>0',stop_while=True).list('stype',sc='class sclass').dict().keep('class sclass ctype title date docid url summary source comment_count like up is_gov')
y.nullft('ctype').matchft(sc='news',mode='re').crawler('url:html').xpath('html',sc='',mode='text').nullft().nullft('class')
y.dbex(sl='mongo',table='yidian')
y.get()
#y.rpc('insert',server='10.101.167.107')


exit()

url='http://www.yidianzixun.com/api/q/?path=channel|news-list-for-keyword&display={0}&word_type=token&fields=docid&fields=category&fields=date&fields=image&fields=image_urls&fields=like&fields=source&fields=title&fields=url&fields=comment_count&fields=summary&fields=up&cstart={1}&cend={2}&version=999999&infinite=true'


y= task(u'一点').textge('url',sc='http://www.yidianzixun.com/').crawler().pyq(sc='.top-bar-nav-items>ul>li',mode='list|html').list()
y.html().text('url:text').split('url',sc='"').at(sc='1').matchft(sc='keyword',mode='re').split(sc='=').at(sc='-1').rangege('p',mode='cross',max=1000,min=0,interval=10)
y.py('p:p1',sc='int(value)+10').merge('url',sc=url,merge_with='p p1').crawler().json(mode='decode').py('url',sc='value["result"]').list(sc='text').dict()
y.keep('title url source summary category content_type text docid').rename('text:class').nullft('title').matchft('content_type',sc='news').crawler('url:html').xpath('html',sc='',mode='text')
y.get().ix[3]['url']


exit()
#print t.get(etl=70).ix[0]['text']
#t.get(etl_count=60)
#t.push()


c=task('fromdb')
c.dbge(sl='mongo',table='vogue').addnew('label',sc=u'时尚')
c.py('title:id',sc=lambda x:md5(x))
c.addnew('url',sc='https://recproxy-pre-seachportal.alipay.com/recommend.json')
c.addnew('_sid_',sc='6338')
c.addnew('delimiter',sc=',')
c.addnew('needTag',sc='false')
c.dict('get',sc='_sid_ delimiter needTag text')
c.crawler('url:result',mode='get',sc='[get]')
c.get()


exit()


m=task('douguo').rangege('p',max=890,min=0,interval=10).pl().addnew('url',sc='http://www.douguo.com/article/')\
.merge(sc='{0}{1}',merge_with='p').crawler().pyq(mode='html',sc='.oarticle',m_yield=True).rename(':html')
m.xpath('html',sc='//a/@href').crawler().pyq(sc='.wzifo').text(':text').rename('html:content').split('text:title',sc=u'阅读',index=0)
m.get()


#t.get(format='xx')
#proj.load_json('vogue.json')

#t=proj.env['vogue']
#t.get()
#t.get(etl_count=60)