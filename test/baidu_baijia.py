# coding=utf-8
import sys,os
import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
sys.path.insert(0,parentdir)
from spider import *
from repl import  *
from tn_py.custom import get_time
import datetime
from dateutil.relativedelta import relativedelta


# In[2]:

end=get_time(datetime.datetime.now())
minute_count=200
java_url='http://10.244.0.8:60006/antnlp/postnews';
output_format='key'
start= get_time(datetime.datetime.now()-relativedelta(minutes=minute_count))
time_script='%s<script<%s'%(start,end)


# In[3]:

def conv_time(data):
    t=data['m_create_time']
    now= datetime.datetime.now()
    if t.find(':')>0:
        t=t.split(':')
        hour=int(t[0])
        minute=int(t[1])
        time= datetime.datetime(now.year,now.month,now.day,hour=hour,minute=minute);
    elif t.find('-')>0:
        t = t.split('-')
        month = int(t[0])
        day = int(t[1])
        time= datetime.datetime(now.year, month, day);
    return get_time(time)


# In[4]:

s=spider('s')


# In[5]:

artical= spider(u'百度百家文章')
artical.set_paras(is_list=False)
artical.add_xpath('script','//*[@id=\"page\"]/div[2]',True)


# In[6]:

url='http://baijia.baidu.com/ajax/labellatestarticle?page={1}&pagesize={2}&labelid={0}&prevarticalid=533025'
t=task(u'百度百家')
t.textge('class',sc='1 2 3 4 5 6 7 8 9 100 101 102 103 104 105 106 107 108')
t.pl()
t.merge('class:url',script=url,merge_with='1 1')
t.crawler('url:Content',selector='s')
t.json('Content')
t.py('Content:total',sc="script['data']['total']")
t.pyft('total',sc="'0'!=str(script)")
t.py('total:p',script="int(script)/20+1")
t.delete('url Content')
t.rangege('pp',max='[p]',mode='cross')
t.merge('class:url',script=url,merge_with='pp 20')
t.crawler('url:Content',selector='s',pl_count=5)
t.json('Content')
t.py('Content',script="script['data']['list']",mode='docs',new_col='class')
t.py('m_create_time:timestamp',script=conv_time)
t.pyft('timestamp',script=time_script,stop_while=True)
t.rename('m_display_url:url')
t.crawler('url',selector=u'百度百家文章')
t.keep('m_title:title,m_summary:desc,timestamp:publish_time,desc,m_image_url:cover,m_display_url:url,script')
t.get(format=output_format,etl_count=2)

exit()
# In[7]:

url0='http://36kr.com/api/info-flow/main_site/posts?column_id=&per_page=1&_=1474948445522'
url1='http://36kr.com/api/info-flow/main_site/posts?column_id=&b_id={0}&per_page=100&_=1474948898213'


# In[8]:

kr_artical=spider('kr_artical')
kr_artical.is_list=False
kr_artical.visit('http://36kr.com/p/5053488.html')
kr_artical.add_xpath(u'artical','/html/body/script[5]')


# In[9]:

kr=task('36kr')
kr.textge('url',url0)
kr.crawler('url:Content',selector='s')
kr.json('Content')
kr.py('Content',script="script['data']['items'][0]['id']")
kr.rangege('p',max='[Content]',min=1,interval='-100')
kr.delete('Content url')
kr.merge('p',script=url1)
kr.crawler('p:Content',selector='s')
kr.json('Content')
kr.py('Content',script="script['data']['items']",mode='docs')
kr.rename('summary:description')
kr.py('updated_at:date',script="datetime.strptime(script,'%Y-%m-%d %H:%M:%S')")
kr.py('date:timestamp',script='get_time(script)')
kr.pyft('timestamp',script=time_script,stop_while=10)
kr.merge('id:url',script='http://36kr.com/p/{0}.html')
kr.crawler('url:Content',selector='s')
kr.xpath('Content',script='/html/body/script[5]',mode='html')
kr.strextract('Content:html',start='"script":',end=',"cover"')
kr.escape('html')
kr.delete('Content')
kr.html('html:script')
kr.rename('description:desc,timestamp:publish_time')
kr.keep('publish_time title cover desc script url')
#kr.get(format=output_format)


# In[10]:

sc=spider('street_spider')
sc.visit('http://wallstreetcn.com/news?status=published&type=news&order=-created_at&limit=30&page=1')
sc.great_hand(attr=True).test().accept()


# In[11]:

st=task('street')
st.rangege('p',max=3000,min=1)
st.py('p',sc='str(int(script))')
st.merge('p',sc='http://wallstreetcn.com/news?status=published&type=news&order=-created_at&limit=30&page={0}')
st.crawler('p',sl='street_spider')
st.nullft('title')
st.rename('col1_href:url,col0_data-original:cover,col4:author,meta_time_visible-lg-inline-block:date,summary_hidden-xxs:desc')
st.delete('col3_href col5_href col8_class')
st.crawler('url:Content',sl='s')
st.xpath('Content:text',sc='//*[@id="main"]/article/div[2]')
st.nullft('text')
st.xpath('Content:html',sc='//*[@id="main"]/article/div[2]',mode='html')
st.delete('Content')
st.replace('date',sc=u'年|月',new_value='-',re=True)
st.replace('date',sc=u'日',new_value='')
st.py('date',sc=u"datetime.strptime(script,'%Y-%m-%d %H:%M:%S')")
st.py('date:timestamp',sc='get_time(script)')
st.pyft('timestamp',sc=time_script,stop_while=True)
st.keep('timestamp:publish_time,html:script,cover,desc,url') #date:timestamp
#st.get(take=3,etl_count=100, format=output_format)

#exit()

# In[12]:

url='http://feed.mix.sina.com.cn/api/roll/get?pageid={0}&lid={1}&num={2}&versionNumber=1.2.8&page=4&encode=utf-8'
si= task('sina_fun')
si.textge('id','37')
si.merge('id:url',sc=url,merge_with='531 1')
si.crawler('url:Content',sl='s')
si.json('Content')
si.py('Content:total',sc="script['result']['total']")
si.py('total',script='script/30+1')
si.delete('Content')
si.rangege('p',max='[total]',min=1,interval=1,mode='cross')
si.py('p',sc='str(int(script))')
si.merge('id:url',sc=url,merge_with='531 p')
si.crawler('url:Content',sl='s')
si.json('Content')
si.py('Content',sc="script['result']['data']",mode='docs')
si.matchft('url',sc='slide',revert=True)
si.py('img:cover',sc='script["u"]')
si.rename('ctime:date')
si.crawler('url:Content',sl='s')
si.xpath('Content',sc='//*[@id="artibody"]',mode='html')
si.keep('intime:publish_time,intro:desc,title,cover,Content:script,url') #keywords
st.pyft('publish_time',sc=time_script,stop_while=True)
si.replace('desc,script',sc=u'新浪娱乐讯',new_value='')
#si.get(etl_count=100)


mongo=get_default_connector()
mongo.db='ant135'


# In[14]:

main=task('main')
main.etlge(sl=u'百度百家')
main.etlge(sl='36kr',mode='mix')
main.etlge(sl='street',mode='mix')
#main.etlge(sl='sina_fun',mode='mix')
main.nullft('script')
main.pyft('script', sc='len(script)>500')
main.html('script', mode='decode')
main.replace('cover',sc='https',new_value='http')
main.regex('cover:cover_ext',sc='(\.jpg)|(\.png)')
main.nullft('cover_ext')
main.nullft('title')
main.py('title:hash',sc='hash(script)')
main.joindb('hash',sl='mongo', table='news', sc='title:title2',mode='doc')
main.nullft('title2',revert=True)
main.dict('request',sc='title desc script cover publish_time cover_ext')
main.json('request',conv_mode='encode')
main.addnew('url',value=java_url)
main.crawler('url:Content',sl='s',post_data='[request]',debug=True)
main.json('Content',mode='doc')
main.delete('Content request url title2')
main.rename('alipay_open_public_life_msg_send_response:alipay_result')
main.dbex(sl='mongo',table='news')
main.take(take=10)

main.get(100,etl_count=100)

