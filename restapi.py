from flask import request
import json;
import sys;
from flask import Flask, jsonify
import copy;
app = Flask(__name__);

proj=None;
import etl;
@app.route('/demo')
def root():
    return app.send_static_file('extracttext.html')

@app.after_request
def after_request(response):
    print('after_request')
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response


@app.route('/etl/', methods=['GET'])
def extract():
    js= request.args;
    source= js['source'];
    keyword=js['keyword'];
    start=js['start'];
    end=js['end'];
    etl=100000;
    if 'count' in js:
        count = int(js['count']);
    task=proj.modules[source];
    dic= {'百度新闻搜索':Baidu};
    func=None;
    if source in dic:
        func=dic[source]
    func(task,keyword,start,end);
    buff = [];

    for r in task.query():
        if len(buff)<count:
            buff.append(r)
        else:
            break
    return buff[0]['html']
    js = json.dumps(buff, indent=2, ensure_ascii=False);
    return js;

def Baidu(task,keyword,start,end):
    tools=task.AllETLTools;
    tools[0].Content=keyword;
    tools[1].MinValue=start;
    tools[2].MaxValue=end;


if __name__ == '__main__':
    argv = sys.argv;
    projfile = '../Hawk-Projects/新闻抓取/百度新闻.xml';
    name = '百度新闻搜索'
    port=5555;
    if len(argv) > 1:
        projfile = argv[1];
    if len(argv) > 2:
        name = argv[2];
    if len(argv) > 3:
        port = argv[3];
    try:
        proj = etl.proj_load_xml(projfile);
    except Exception as e:
        print('load project failed:' + str(e));
        exit();
    app.run(host='0.0.0.0', port=int(port), debug=False)