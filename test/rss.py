
# coding=utf-8
import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
sys.path.insert(0,parentdir)
from spider import *
from repl import  *
import extends
import time
import feedparser
extends.enable_progress=False
import requests
import re
# In[2]:

extends.debug_level=100
execute= False

if len(sys.argv)>1 and sys.argv[1]=="true":
    execute = True




def get_xpath(s):
    if s=='guokr':
        return '//div[1]/div'
    else:
        return None


def get_content(s):
    content=''
    buf=[]
    if 'content' in s:
        buf.append(s['content'][0]["value"])
    if 'desc' in s:
        buf.append(s['desc'])
    if 'description' in s:
        buf.append(s['description'])
    if 'summary' in s:
        buf.append(s['summary'])

    if 'summary_detail' in s:
        buf.append(s['summary_detail']['value'])
    for b in buf:
        b=to_str(b)
        if len(content)<len(b):
            content=to_str(b)
    s['content']=content
    return

def get_hash(s):
    link= s.get('link','')
    t=hash(s['title']+link)
    s['hash']=t

def get_cover(s):
    if 'cover' in s:
        return
    elif 'cover2' in s:
        s['cover']=s['cover2']
        del s['cover2']



def filterhtml(data):
    source = data['link']
    html = data['content']
    html= html.replace(u'来自新浪新闻','').replace(u'[微博]','')
    if source.find('hupu')>0:
        hh = html.split(u'[来源')
        if len(hh) == 2:
            hh = hh[0] + '</body>'
            html = hh
    elif source.find('mafengwo')>0:
        html = html.replace(u'99%的人在看的旅游攻略，关注蚂蜂窝微信：mafengwo2006', '')
    elif source.find('haibao')>0:
        html = html.replace(u'图片延伸阅读：', '')
    data['content'] = html
    return data

# In[42]:

mongo=get_default_connector()
mongo.db="ant_temp"
table_name='life_rss_online'
table_name_all='life_rss_all'
count_per_id= 5
#remote='http://recproxy.cz00b.alipay.net/recommend.json?_sid_=44040'
remote='http://recproxy-pre.alipay.com/recommend.json?_sid_=9457'
#remote='https://recproxy.alipay.com/recommend.json?appid=4250'
rlist=requests.get(remote+'&invoke_method=get_rss_list')
rlist=rlist.json()
rlist= rlist.get('rss_list','')

if rlist=='':
    print 'get rss list empty ,quit'
    exit()

rlist2=[];
for item in rlist.split('\n'):
    kv= item.split('\t')
    if len(kv)<2:
        continue
    k=kv[0].strip()
    v=kv[1].strip()
    rlist2.append({'app_id':k,'rss':v})

#print '\n'.join([r['rss'] for r in rlist2])
#exit()

ins= task('insert').let('error').nullft(reverse=True).let('msg_id').dbex('mongo',table=table_name)


if execute:
    current=time.localtime(time.time());
    logfile='log_'+ time.strftime('%Y-%m-%d',current)
    log_file= extends.open(logfile,'a',encoding='utf-8')
    log_file.write('##'+time.strftime('%H-%M',current)+'\n')



def write_log(data):
    keys='app_id title url'.split(' ')
    values='\t'.join([data[key] for key in keys]);
    log_file.write(values+'\n')



error_str=u"([\w\s\u4e00-\u9fa5]+)"
error_re= re.compile(error_str)

def error_code_ft(cont):
    res=error_re.findall(cont)
    right_len= sum([len(r) for r in res])
    total=float(len(cont))
    r=right_len/total;
    return r>0.3


keep1='author hash content description:desc published:publish_time link title source xpath app_id'
dict1='title desc liked comment content cover url app_id invoke_method source_from'
rss_button='<img src="http://ocpk3ohd2.qnssl.com/rss_bottom.jpg" />'


b= task('back')
b.create([{'public_id':'2016092601973157', 'msg_id':'2016092601973157e2d600be-1f56-4736-a0a7-ee617fc0bdab'}]).let('r_url').set(remote).let('invoke_method').set('recall').let('post').todict('invoke_method public_id msg_id')
b.cp('r_url:resp').post('[post]').dump('json').drill('recall_result error').rm('resp post')
#b.get(1,etl=100)


def get_rss(address):
    if address.find('yaolan')>0:
        data = requests.get('http://open.api.yaolan.com/api/v1/rss/knowledge')
        c= feedparser.parse(data.content)
    else:
        c= feedparser.parse(address)
    return c['entries'][:count_per_id]


rss = task('rss')
#rss.create([r for r in rlist2]).let('rss').matchft('mafengwo')
rss.create().let('rss').set('http://open.api.yaolan.com/api/v1/rss/knowledge').let('appid').set('2016111702925166')
rss.cp('rss:source').split('.')[0].cp('source:xpath').map(get_xpath).let('rss').map(get_rss)
rss.list('source xpath app_id cover').drill().let('').map(get_hash).cp('hash:hash2').nullft().joindb('mongo',index='hash',mapper='title:title2', table=table_name).at(0).let('title2')  #
# . nullft(reverse=True)
rss.let('').map(get_content).keep(keep1)
rss.let('content').tree().xpath('[xpath]')[0].html().clean().where('len(value)>100').where(error_code_ft)
rss.replace('https://',value='http://').replace('href=\".*?\"',mode='re').replace(rss_button,mode='str').cp('content:cover2')
rss.xpath('//img[1]/@src').at(0).let('').map(get_cover).let('cover').nullft().where('len(value)<300').matchft('data:',reverse=True).matchft('http:')
rss.let('r_url').set(remote).let('invoke_method').set('send').let('comment').set('true').let('liked').set('true').rm('xpath').let('title').repeatft().mv('link:url')
rss.let('source_from').set('RSS')
#rss.let('app_id').set('2016092601973157') #.let('content title').set("artical"+str(id)*100)
if execute: #True:  # execute:
    rss.let('post').todict(dict1).cp('r_url:resp').post('[post]').dump('json').drill('error app_id msg_id success store_id')
    #rss.py(write_log)
rss.rm('r_url resp post')
rss.dbex('mongo',table=table_name_all).subex('insert')
rss.let('error').nullft(reverse=True).take(10)
r=rss.collect(etl=50, format='sd')
print r[0] #['summary_detail']['value']
exit()



send_result=rss.collect(500, etl=100, execute=execute, format='df')
#rss.check()
#rss.get(etl=100)
print send_result[['title','url','hash']]

if execute:
    log_file.close()







