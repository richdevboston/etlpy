
# coding: utf-8
import  sys
sys.path.append('../')
from repl import *



headers='''
Host: browse.renren.com
Connection: keep-alive
Cache-Control: max-age=0
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Referer: http://friend.renren.com/managefriends
Accept-Encoding: gzip, deflate, sdch
Accept-Language: zh-CN,zh;q=0.8,en;q=0.6
Cookie: anonymid=iqxs3arh-hus2ah; _r01_=1; wp=0; l4pager=0; depovince=GW; XNESSESSIONID=66f26cc3c627; jebecookies=6cccd682-db67-4bac-b91e-28cec04e1d4c|||||; ick_login=22fd41ee-4be1-46cf-a41c-f8af30155a20; _de=B4F0273EEBE5D47A32E0B9ADC37B4602; p=e79b0c3d0276515691fd1485da2f36da2; first_login_flag=1; ln_uact=zym_by@126.com; ln_hurl=http://hdn.xnimg.cn/photos/hdn421/20120822/2245/h_main_1Hbx_528700010b791376.jpg; t=defda9613d7567c1d47639166dd820992; societyguester=defda9613d7567c1d47639166dd820992; id=230246512; xnsid=5dd9356e; ver=7.0; loginfrom=null; jebe_key=b3f7048e-7d42-4f18-a23e-03d274b01a39%7Cd1c91d21c23af205fbde41b372732601%7C1469193870662%7C1%7C1471599277795; WebOnLineNotice_230246512=1; JSESSIONID=4EEE784B69E867D320981D1A5DDDEF24; wp_fold=0f8dbb-c2ca-42a2-b20e-f2b27e06683d; jebe_key=b3f7048e-7d42-4f18-a23e-03d274b01a39%7Cd1c91d21c23af205fbde41b372732601%7C1469193870662%7C1%7C1470913691096; _de=B4F0273EEBE5D47A32E0B9ADC37B4602; p=106e8e0309b73b46a189dac5990846e02; first_login_flag=1; ln_uact=zym_by@126.com; ln_hurl=http://hdn.xnimg.cn/photos/hdn421/20120822/2245/h_main_1Hbx_528700010b791376.jpg; t=3b336ee1a515e4b6c3863e19f56b3bfb2; societyguester=3b336ee1a515e4b6c3863e19f56b3bfb2; id=230246512; xnsid=86b63f6a; ver=7.0; loginfrom=null; JSESSIONID=1D8D98426C02643F07F39F2B8D9B46B3; wp_fold=0
'''


t=spider('list')



t.requests.set_headers(headers)



ct=spider('ct')
ct.requests.set_headers(headers)


# In[8]:

url='''http://browse.renren.com/sAjax.do?ajax=1&q=%20&p=%5B%7B%22t%22%3A%22birt%22%2C%22astr%22%3A%22%E6%91%A9%E7%BE%AF%22%7D%5D&s=0&u=230246512&act=search&offset=90&sort=0'''


# In[9]:

d=t.visit(url).great_hand(True).accept()


# In[12]:

format = 'http://browse.renren.com/sAjax.do?ajax=1&q=&p={0}&s=0&u=230246512&act=search&offset={1}&sort=0'


# In[15]:

l=task('renrenlist')


# In[16]:

import urllib


# In[21]:

mongo =get_default_connector()


# In[101]:

province=u'北京 上海 天津 重庆 黑龙江 吉林 辽宁 山东 山西 陕西 河北 河南 湖北 湖南 海南 江苏 江西 广东 广西 云南 贵州 四川 内蒙古 宁夏 甘肃 青海 安徽 浙江 福建 台湾 香港 澳门'


# In[102]:

tp= province.split(' ')
query_format='[{"t":"birt","month":"{1}","year":"{0}","day":"{2}"},{"prov":"{3}","gend":"{4}","t":"base"}]'


# In[103]:

b=task('birth')


# In[104]:

b.clear();
b.pyge('pro',script=province.split(' '))
b.rangege('year',max=2005,min=1980,mode='cross')
b.rangege('month',max=13,min=1,mode='cross')
b.rangege('day',max=32,min=1,mode='cross')
b.pyge('gend',script=[u'男生',u'女生'],mode='cross')
b.merge('year:query',script=query_format,merge_with='month day pro gend')
b.delete('day pro year month gend')
#b.get().ix[0]['query']


# In[111]:

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


# In[112]:

l.get(etl_count=12)


# In[29]:

l.distribute()


# In[ ]:

l.stop_server()


# In[ ]:




# In[ ]:



