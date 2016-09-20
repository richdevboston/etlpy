# coding=utf-8
import sys,os
import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
sys.path.insert(0,parentdir)



from repl import *



# In[2]:

mongo =get_default_connector()


# ### 获取淘女郎列表的爬虫

# In[3]:

sl= spider('list')


# 先获取淘女郎的列表，这个列表采用自动嗅探，后来发现id没有加进来，再手工搜索一下xpath即可

# In[4]:

s1=sl.visit('https://mm.taobao.com/json/request_top_list.htm?page=1').great_hand().test().accept()


# In[5]:

s1.search_xpath('687471686','id',attr=True)


# 我的命名比较恶心，都是一个字母，main是核心流程，负责调用其他子流程

# ### 核心流程

# In[6]:

b=task('main')


# In[7]:

b.clear()
b.rangege('id',max=4300)
b.merge('id:url',script='https://mm.taobao.com/json/request_top_list.htm?page={0}')
b.crawler('url',selector='list')
b.rename('col1:age,col2:city,col3:job')
b.number('col9:deal,col7:like,col8:count,col6:good_rate,col4:fensi')
b.number('age id')
b.delete('col9 col8 col5 col6 col7 col4 mm-photolike-count_radius-3')
b.pl(count_per_thread=20)
b.dbex(connector='mongo',table='taobao_gril')
b.etlex(selector='albumlist',range='1:100')


# album通过手气不错，来获取一个网红的所有迷之相册,同样id也没有自动获取。
# 此处多说一句，如果调用`great_hand(attr=True)`的话，确实也能获取相册的id，但这种做法会引入大量无用的信息。
# 我宁愿先嗅探出核心内容，再手工添加我觉得需要的东西

# In[8]:

detail=spider('album')


# In[9]:

eurl='https://mm.taobao.com/self/album/open_album_list.htm?_charset=utf-8&user_id%20=141234233'


# In[10]:

detail.visit(eurl).great_hand().accept().get()


# In[11]:

detail.search_xpath('10000920122','aid',attr=True)


# In[12]:

l= task('albumlist')


# In[13]:

l.textge('id','141234233')
l.merge('id:url',script='https://mm.taobao.com/self/album/open_album_list.htm?_charset=utf-8&user_id%20={0}')
l.crawler('url',selector='album')
l.delete('col0')
l.replace('mm-photo-date',script=u'创建时间:',new_value='')
l.number('mm-pic-number')
l.number('aid:uid',index=0)
l.number('aid',index=1)
l.rename('col1:name,id:aid')
l.etlex(selector='albumdetail',range='2:100')
l.dbex(connector='mongo',table='taobao_girl_album')


# In[14]:

d= spider('default')
d.set_paras(is_list=False)


# In[15]:

al=task('albumdetail')


# In[16]:

al.clear()
al.textge('uid','141234233')
al.addnew('aid',value='10000920122')
al.merge('uid:url',script='https://mm.taobao.com/album/json/get_album_photo_list.htm?user_id={0}&album_id={1}',         merge_with='aid')
al.crawler('url',selector='default')
al.json('Content')
al.py('Content:size',script="data['Content']['pageSize']")
al.delete('Content')
al.rangege('p', max='[size]',mode='cross')
al.py('p',script=lambda x:str(int(x['p'])))
al.merge('url',script='{0}&page={1}',merge_with='p')
al.crawler('url',selector='default')
al.json('Content')
al.py('Content:size',script="data['Content']['picList']",mode='docs')
al.merge('picUrl',script='https:{0}')
al.merge('userId:save',script='../taobao_images/{0}/{1}.jpg',merge_with='picId')
al.savefileex('picUrl',savepath='[save]')
al.delete('albumUserId url des status save')
al.dbex(connector='mongo',table='taobao_girl_pic')


# In[17]:

#b.get()


# In[18]:

b.get()

b.distribute()

