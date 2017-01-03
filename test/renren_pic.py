# coding: utf-8

# In[1]:

import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
sys.path.insert(0,parentdir)
from spider import *
from repl import  *
from extends import *

# In[2]:

mongo =get_default_connector()


headers='''
Accept-Encoding:gzip, deflate, sdch
Accept-Language:zh-CN,zh;q=0.8,en;q=0.6
Cache-Control:max-age=0
Connection:keep-alive
Cookie:anonymid=ivx5m485aviu4v; _r01_=1; _de=B4F0273EEBE5D47A32E0B9ADC37B4602; jebe_key=b3f7048e-7d42-4f18-a23e-03d274b01a39%7Cd1c91d21c23af205fbde41b372732601%7C1480040039624%7C1%7C1481346239483; depovince=GW; jebecookies=703d96eb-eaa3-4e64-a443-9cae04025e92|||||; JSESSIONID=abcmuQGori7TdfGvhUJLv; p=17b42e45ef9a5ab7414531e5e5a0d64a2; ap=230246512; first_login_flag=1; t=3733bd111f05a175d9f60fa90bb5ee892; societyguester=3733bd111f05a175d9f60fa90bb5ee892; id=230246512; xnsid=9c4789c9; ln_uact=zym_by@126.com; ln_hurl=http://hdn.xnimg.cn/photos/hdn421/20120822/2245/h_main_1Hbx_528700010b791376.jpg; ver=7.0; loginfrom=null; jebe_key=b3f7048e-7d42-4f18-a23e-03d274b01a39%7Cd1c91d21c23af205fbde41b372732601%7C1483437113305%7C1%7C1483437108393; wp_fold=0
Host:www.renren.com
'''
proj.env['header']=para_to_dict(headers,'\n',':')

# In[3]:

b=task('pic')
b.dbge('mongo',table='renren').let('friendNum').nullft().num().where('int(value)>5').keep('id').pl(10000)
#b.textge('id',sc='230246512')
b.cp('id:url').merge('http://photo.renren.com/photo/{0}/albumlist/v7#').get(header='header')
b.extract('\nnx.data.photo =',end=';\nnx.data.hasHiddenAlbum').escape().replace(' ').load('json').map('value["albumList"]["albumList"]').list().cp('url:date').regex('\d{8}').at(0).nullft()



b.fetch(2,etl=15)
#b.fetch(20,etl=10).ix[0]['url']