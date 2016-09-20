# coding: utf-8

# In[1]:

import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
from repl import *


# In[2]:

mongo =get_default_connector()


# In[3]:

b=task('pic')

b.clear()
b.dbge(connector='mongo',table='renren')
b.regexft('col0_data-src',script='gif',revert=True)
b.regex('id:folder',script='\d{2}')
b.merge('id:img',script='../renren_images/{1}/{0}.jpg',merge_with='folder')
b.pl(count_per_thread=1000)
b.savefileex('col0_data-src',savepath='[img]')
b.distribute()