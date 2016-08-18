# coding=utf-8
from flask import request
import json;
import sys;
from flask import Flask, jsonify
import copy;
app = Flask(__name__);
from spider import  *
from xspider import  *
proj=None;
import etl;
@app.route('/demo')
def root():
    return app.send_static_file('extracttext.html')

@app.after_request
def after_request(response):
    print('after_request')
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response


@app.route('/echo/<data>')
def echo(data):
    return data

def _get_html(request):
    data = request.values.get('reqparams', None)
    data=json.loads(data);
    url = data.get('url', None)
    params = data.get('attr', False)
    html = data.get('html', None);
    if html in [None,'']:
        if url is None:
            return None,jsonify({'error': 'url is None'})
        html = get_html(url);
    return html,params

@app.route('/extract/list', methods=['POST'])
def extract_list():
    html,params= _get_html(request)
    if html is None:
        return params;
    data,xpaths= get_list(html,has_attr=params)
    if data is None:
        return jsonify({'error':'extract list empty'})
    else:
        result= jsonify(**{'result':data});
        return result;

@app.route('/extract/content', methods=['POST'])
def extract_content():
    html,params= _get_html(request)
    if html is None:
        return params;
    content= get_main(html,params);
    return jsonify({'result':content})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=60006, debug=False)