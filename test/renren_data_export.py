# coding:utf8
import codecs
import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
sys.path.insert(0,parentdir)
from repl import  *
from extends import *
import shutil

mongo =get_default_connector()

data_file= codecs.open('renren_export.txt','w',encoding='utf-8')

keys='name gend viewCount user_lively friendRank id pro passive friendNum birth active popValue'.split(' ')
data_file.write('\t'.join(keys)+'\n')
def write(x):
    values= '\t'.join([to_str(x[k]) for k in keys])+'\n'
    data_file.write(values)



b=task('pic')
b.dbge('mongo',table='renren').rm('_id').let('year').format('{0}-{month}-{day}').rm('month day col0_data-src').mv('year:birth col2:name').let('name').nullft().count(10000).let('').map(write)
b.fetch(20000,format='count')

data_file.close()