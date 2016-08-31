# coding=utf-8

import time
import httplib, urllib
import random
import json
import codecs
addr= '127.0.0.1'
port=  60006
import pprint

def get_html_data(url='',html='',has_attr=False,mode='list'):
    request={'url':url, 'html':html,'attr':has_attr};
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    params = urllib.urlencode({"reqparams": json.dumps(request)});
    httpClient = httplib.HTTPConnection(addr, port, timeout=30)
    httpClient.request("POST", "/extract/%s"%(mode), params,headers )
    response = httpClient.getresponse()
    data = response.read()
    return json.loads(data);


news_url='http://renjian.163.com/16/0817/20/BUMRU2KL00015C18.html'
data = get_html_data(url='http://www.cnblogs.com')
content=get_html_data(url=news_url,mode='content');
print( json.dumps(data,encoding='utf-8',ensure_ascii=False,indent=2))
print(json.dumps(content,encoding='utf-8',ensure_ascii=False,indent=2))
news_html= urllib.urlopen(news_url).read().decode('gbk');
content=get_html_data(html=news_html, mode='content',has_attr=True)