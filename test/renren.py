
# coding: utf-8

# In[1]:
import sys
sys.path.append("..")
from src.repl import *


# In[2]:

headers='''
Host: browse.renren.com
Connection: keep-alive
Cache-Control: max-age=0
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Referer: http://browse.renren.com/search.do?ref_search=searchResult_ReSearch
Accept-Encoding: gzip, deflate, sdch
Accept-Language: zh-CN,zh;q=0.8,en;q=0.6
Cookie: anonymid=iqxs3arh-hus2ah; _r01_=1; wp=0; l4pager=0; XNESSESSIONID=66f26cc3c627; depovince=GW; ick_login=22fd41ee-4be1-46cf-a41c-f8af30155a20; _de=B4F0273EEBE5D47A32E0B9ADC37B4602; p=b7d146e8b3602bfed1f774e5307a26072; first_login_flag=1; t=e49887e147b5239311b6aa238930f2d72; societyguester=e49887e147b5239311b6aa238930f2d72; id=230246512; xnsid=4ff79798; ln_uact=zym_by@126.com; ln_hurl=http://hdn.xnimg.cn/photos/hdn421/20120822/2245/h_main_1Hbx_528700010b791376.jpg; jebecookies=6ab80578-c489-4085-86f5-65fc095788a8|||||; ver=7.0; loginfrom=null; jebe_key=b3f7048e-7d42-4f18-a23e-03d274b01a39%7Cd1c91d21c23af205fbde41b372732601%7C1471845601197%7C1%7C1471845633949; WebOnLineNotice_230246512=1; JSESSIONID=98F0E8F5E9341AD15393E42BF8A7E142; wp_fold=0
'''


# In[3]:

t=spider('list')


# In[4]:

t.requests.set_headers(headers)


# In[5]:

ct=spider('ct')
ct.requests.set_headers(headers)


# In[6]:

url='''http://browse.renren.com/sAjax.do?ajax=1&q=%20&p=%5B%7B%22t%22%3A%22birt%22%2C%22astr%22%3A%22%E6%91%A9%E7%BE%AF%22%7D%5D&s=0&u=230246512&act=search&offset=90&sort=0'''


# In[7]:

t.visit(url).great_hand(True).accept()


# In[8]:



# In[9]:

format = 'http://browse.renren.com/sAjax.do?ajax=1&q=&p={0}&s=0&u=230246512&act=search&offset={1}&sort=0'


# In[10]:

l=task('renrenlist')


# In[11]:

import urllib


# In[12]:

mongo =get_default_connector()


# In[13]:

province='北京 上海 天津 重庆 黑龙江 吉林 辽宁 山东 山西 陕西 河北 河南 湖北 湖南 海南 江苏 江西 广东 广西 云南 贵州 四川 内蒙古 宁夏 甘肃 青海 安徽 浙江 福建 台湾 香港 澳门'


# In[14]:

tp= province.split(' ')
query_format='[{"t":"birt","month":"{1}","year":"{0}","day":"{2}"},{"prov":"{3}","gend":"{4}","t":"base"}]'


# In[15]:

b=task('birth')


# In[16]:

b.clear();
b.pyge('pro',script=province.split(' '))
b.rangege('year',max=2005,min=1980,mode='cross')
b.rangege('month',max=13,min=1,mode='cross')
b.rangege('day',max=32,min=1,mode='cross')
b.pyge('gend',script=['男生','女生'],mode='cross')
b.merge('year:query',script=query_format,merge_with='month day pro gend')
#b.get()


# In[ ]:

l.clear()
l.etlge(selector='birth')
l.py('query:js', script=lambda x:quote(x['query']))
l.merge('js:url',script=format,merge_with='0')
l.pl(count_per_thread=20)
l.crawler('url',selector='ct')
l.xpath('Content:page',mode='html', script='//*[@id="resultNum"]')
l.number('page')
l.delete('Content')
l.py('page',script=lambda x:min(500,int(x['page'])))
l.rangege('p',max='[page]',mode='cross',min=1, interval=10)
l.merge('js:url', script=format, merge_with='p')
l.crawler('url',selector='list',new_col='month day pro gend year')
l.json('col5_popval',mode='doc')
l.number('col7:common_friends')
l.number('col1_href:id',index=1)
l.delete('col1_href col3_href col5_popval col7 col9_data-id col10_data.name col8_data-common')
l.number('user_lively')
l.rename('col6:expr col0_data-src:head col2:name')
l.replace('expr',script='经历 : ')
l.dbex('id',connector='mongo',table='renren')


# In[ ]:


l.check()

# In[ ]:

l.execute()


# In[ ]:

l.stop_server()


# In[ ]:




# In[ ]:



