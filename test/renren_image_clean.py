
# coding:utf8

import sys,os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/src'
sys.path.insert(0,parentdir)
from repl import  *
from extends import *
import shutil

mongo =get_default_connector()


def get_header(d):
    album= d['albumList']['albumList']
    for r in album:
        if r['albumName']==u'头像相册':
            return r['albumId']
    return None

def copy(data):
    folder = data['fo']
    album = data['url']
    id = data['id']
    if not os.path.exists(folder):
        return
    files = os.listdir(folder)
    nfolder = '/gruntdata/desert.zym/dev/renren_album/%s/%s' % (id[:2], id)
    if os.path.exists(nfolder):
        return
    os.makedirs(nfolder)
    for f in files:
        if f.find(album) > 0:
            shutil.copy(folder + f, nfolder + '/' + f)



b=task('pic')
b.dbge('mongo',table='renren_album').rm('_id').let('url').map(get_header).nullft().cp('id:fo').\
    map('value[:2]').format('/gruntdata/desert.zym/dev/renren_large/{0}/{id}/').let('').map(copy).count(1000).fetch(10000000000,format='count')
