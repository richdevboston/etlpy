# coding=utf-8
import etl
import extends;
from distributed import *
from etl import cols
proj=etl.Project();
import urllib
def html(text):
    from IPython.core.display import HTML, display
    display(HTML(text));

def new_spider(name='_crawler'):
    import spider;
    sp=spider.SmartCrawler();
    proj.modules[name]=sp;
    sp._proj=proj
    setattr(proj,name,sp);
    return sp;

def get_default_connector():
    mongo = etl.MongoDBConnector();
    mongo.connect_str = 'mongodb://10.101.167.107'
    mongo.db = 'ant_temp'
    con = new_connector('mongo', mongo)
    return con

def new_connector(name,connector):
    proj.connectors[name]=connector;
    return connector

def new_task(name='etl'):
    import etl;
    import inspect
    import extends;
    base_type= [etl.Filter,etl.Generator,etl.Executor,etl.Transformer];
    ignore_paras=['one_input','multi','column'];
    task=etl.ETLTask();
    task._proj=proj;
    task.name=name;
    proj.modules[name]=task;
    setattr(proj,name,task);

    def set_attr(val, dic):
        for k in val.__dict__:
            if k.lower() in dic:
                setattr(val, k, dic[k.lower()])
    def _rename(module):
        repl={'TF':'','Python':'py'}
        for k,v in repl.items():
            module= module.replace(k,v)
        return module.lower();


    dynaimc_method = '''def __%s(column='',%s):
        import etl
        new_tool=etl.%s();
        new_tool._proj=proj
        set_attr(new_tool,locals())
        task.tools.append(new_tool);
        return task;
    '''

    def merge_func(k, v):
        if extends.is_str(v ):
            v = "'%s'" % (v);
        return '%s=%s' % (k.lower(), v);
    for name,tool_type in etl.__dict__.items():
        if not inspect.isclass(tool_type):
            continue;
        if  not issubclass(tool_type,etl.ETLTool) :
            continue
        if tool_type in base_type:
            continue;
        tool=tool_type();
        paras=[merge_func(k,v) for k,v in tool.__dict__.items() if not k.startswith('_')  and k not in ignore_paras]
        paras.sort()
        paras=','.join(paras);
        new_name=_rename(name)
        method_str= dynaimc_method%(new_name,paras,name);
        locals()['proj']=proj
        locals()['cols']=cols;
        exec(method_str,locals());
        func= locals()['__'+ new_name];
        setattr(task,new_name,func);
    return task;


if __name__ == '__main__':
    from etl import *
    import xspider

    city = 'guangzhou'
    url = 'http://apply.gzjt.gov.cn/apply/norm/personQuery.html'
    post_format = 'pageNo={1}&issueNumber={0}&applyCode='
    post_example = 'pageNo=8&issueNumber=201607&applyCode='
    issue = new_spider('issue')
    _issues = issue.visit(url, post_data=post_example).py_query()('#issueNumber')
    issues = [r.text for r in _issues.children()]

    s = new_spider('list')

    s.visit(url, post_data=post_example) \
        .search_xpath('\d{13}', 'id', mode='re') \
        .search_xpath('^杨[\u4e00-\u9fa5]{2}$', 'name', mode='re').accept().test().get()



    t=new_task('main')

    t.clear()
    t.pyge('issue', script=issues)
    t.merge('issue:post', script=post_format, merge_with='2')
    t.addnew('url', value=url)
    t.crawler('url', selector='issue', post_data='[post]')
    t.xpath('Content:page', script='/html/head/script[2]', mode='html')
    t.html('page', mode='decode')
    t.regex('page', script="'\d+'", index=2)
    t.number('page')
    t.delete('Content post')
    t.rangege('p', max='[page]', mode='cross')
    t.get(etl_count=10)
    exit()

    s=new_spider('list')
    headers='''
    Host: browse.renren.com
Connection: keep-alive
Cache-Control: max-age=0
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Referer: http://www.renren.com/296695625/profile
Accept-Encoding: gzip, deflate, sdch
Accept-Language: zh-CN,zh;q=0.8,en;q=0.6
Cookie: anonymid=iqxs3arh-hus2ah; _r01_=1; XNESSESSIONID=ee0362aff077; l4pager=0; depovince=GW; jebecookies=c4bc4880-d117-40a7-94cc-3dd2d21391b4|||||; ick_login=c8df8dbb-c2ca-42a2-b20e-f2b27e06683d; jebe_key=b3f7048e-7d42-4f18-a23e-03d274b01a39%7Cd1c91d21c23af205fbde41b372732601%7C1469193870662%7C1%7C1470913691096; _de=B4F0273EEBE5D47A32E0B9ADC37B4602; p=106e8e0309b73b46a189dac5990846e02; first_login_flag=1; ln_uact=zym_by@126.com; ln_hurl=http://hdn.xnimg.cn/photos/hdn421/20120822/2245/h_main_1Hbx_528700010b791376.jpg; t=3b336ee1a515e4b6c3863e19f56b3bfb2; societyguester=3b336ee1a515e4b6c3863e19f56b3bfb2; id=230246512; xnsid=86b63f6a; ver=7.0; loginfrom=null; JSESSIONID=1D8D98426C02643F07F39F2B8D9B46B3; wp_fold=0
    '''
    s.requests.set_headers(headers)
    url = '''http://browse.renren.com/sAjax.do?ajax=1&q=%20&p=%5B%7B%22t%22%3A%22birt%22%2C%22astr%22%3A%22%E6%91%A9%E7%BE%AF%22%7D%5D&s=0&u=230246512&act=search&offset=90&sort=0'''


    ct=new_spider('ct')
    ct.requests.set_headers(headers)

    #datas=s.visit(url).great_hand(True).test().accept()
    pro_cities={'北京':['昌平','顺义']}
    def pro_cities_generator():
        for p, c in pro_cities.items():
            for s in c:
                yield {'city': s, 'pro': p, 'gend': '男生'}
                yield {'city': s, 'pro': p, 'gend': '女生'}


    query_format = '[{"t":"birt","month":"{1}","year":"{0}","day":"{2}"},{"prov":"{3}","gend":"{5}","city":"{4}","t":"base"}]'

    b = new_task('birth')

    b.clear();
    b.pyge(script=pro_cities_generator)
    b.rangege('year', max=2005, min=1980, mode='cross')
    b.rangege('month', max=13, min=1, mode='cross')
    b.rangege('day', max=32, min=1, mode='cross')
    b.merge('year:query_xpath', script=query_format, merge_with='month day pro city gend')
    b.delete('city day pro year month gend')
    b.get()
    exit()





    l.clear()
    l.pyge('star', script=star)
    l.merge({'star': 'js'}, script='[{"t":"birt","astr":"{0}"}]')
    l.py('js', script=lambda x: urllib.request.quote(str(x['js'])))
    l.merge({'js': 'url'}, script=format, merge_with='20')
    l.crawler('url', selector='ct')
    l.search_xpath('Content', mode=etl.GET_HTML, xpath='//*[@id="resultNum"]')
    l.number('Content')
    l.rangege('p', max='[Content]', mode=etl.MERGE_TYPE_CROSS)
    l.tolist(count_per_thread=50)
    l.merge({'js': 'url'}, script=format, merge_with='p')
    l.crawler('url', selector='list', new_col='star')
    l.json('col5_popval', mode=etl.GENERATE_DOC)
    l.number({'col7': 'common_friends'})
    l.number({'col1_href': 'id'}, index=1)
    l.delete(['col1_href', 'col3_href', 'col5_popval', 'col7', 'col9_data-id', 'col10_data.name', 'col8_data-common'])
    l.number('user_lively')
    l.rename({'col6': 'expr', 'col0_data-src': 'head', 'col2': 'name'})
    # l.merge({'id':'url'},script='http://www.renren.com/{0}/profile')
    # l.crawler('url',selector='people')
    # l.replace('home',script='来自')
    # l.replace('now_live',script='现居')
    l.replace('expr', script='经历 : ')
    l.dbex('id', connector='mongo', table='renren')
    l.get()
    # l.delete('url')
    l.distribute()
    exit()

    #or 如果多次调用这种效率更高,第二行代码就不用执行搜索了
    data,xpaths= xspider.get_list(html);
    data= xspider.get_list(html,xpaths)[0];

    datas = open('/Users/zhaoyiming/Documents/stock.news').read().split('\001')
    datas = [json.loads(r) for r in datas if len(r) > 10]
    for r in datas:
        r['url'] = r['text'].split('"')[1]
        del r['text']
    c = new_connector('cc', MongoDBConnector())
    c.connect_str='mongodb://10.101.167.107'
    c.db='ant_temp';
    s=new_spider('sp')
    t = new_task('xx')
    t.clear()
    t.pyge(script=datas)
    t.tolist(count_per_thread=5)
    t.crawler('url', selector='sp')
    t.xpath({'Content': 'content'}, gettext=True)
    t.delete('Content')
    t.dbex(connector='cc', table='news')

    t.get();
    exit()

    t.distribute()
    exit()

    t2=new_task()
    t2.clear() \
        .textge('text', content='1 2 3 4 5 6 7 8 9 100 101 102 103 104 105 106 107 108') \
        .tolist('text') \
        .merge({'text':'url'},format='http://baijia.baidu.com/ajax/labellatestarticle?page=1&pagesize=20&labelid={0}&prevarticalid=533025' ) \
        .crawler('url', selector='spider') \
        .json('Content', scriptworkmode=etl.GENERATE_NONE) \
        .py({'Content':'total'}, script="value['data']['total']" ) \
        .delete('Content') \
        .merge({'text':'classurl'}, format='http://baijia.baidu.com/?tn=listarticle&labelid={0}') \
        .crawler('classurl', selector='spider') \
        .search_xpath({'Content': 'class'}, xpath='//*[@id="page_title"]/h1') \
        .delete('Content') \
        .py({'total':'p'}, script='int(value)/100+1') \
        .rangege('p', mergetype='Cross', maxvalue='[p]') \
        .tolist('p') \
        .merge('p',format='http://baijia.baidu.com/ajax/labellatestarticle?page={0}&pagesize=100&labelid={1}&prevarticalid=533025',mergewith='text') \
        .crawler('p', selector='spider') \
        .json('Content', scriptworkmode=etl.GENERATE_NONE) \
        .py('Content',ncol='class', script="value['data']['list']", scriptworkmode=etl.GENERATE_DOCS ) \
        .tn({'hotcount':'like'},rule= 'integer_int') \
        .rename({'m_summary':'description','m_title':'title','m_create_time':'date'}) \
        .tn({'date':'timestamp'},rule='daterule')\
        .pyft('timestamp',script='get_time(datetime(2016,7,26,15,15))>timestamp>get_time(datetime(2016,7,20,12,15))',stopwhile=True)\
        .rename({'m_writer_name': 'source','m_display_url':'url'}) \
        .crawler('url', selector=u'baidubaijia') \
        .tn({'date':'timestamp'}, rule='daterule' )
        #.get(script='key',etl_count=50,take=2)
    print(proj.dumps_yaml())
    print(t2)
    #print(buf);