# coding=utf-8

from etlpy.extends import PY2

if PY2:
    import StringIO
    from BaseHTTPServer import BaseHTTPRequestHandler
else:
    from http.server import BaseHTTPRequestHandler, HTTPServer
import gzip
import requests
import time


USER_AGENTS = [
	"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
	"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
	"Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
	"Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
	"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
	"Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
	"Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
	"Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
	"Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
	"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
	"Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
	"Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
	"Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
	"Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
]








class LoggingProxyHTTPHandler(BaseHTTPRequestHandler):

    # send_response pretty similar to BaseHTTPServer send_response
    # but without Server or Date headers
    def send_response(self, code, message=None):
        self.log_request(code)
        if message is None:
            if code in self.responses:
                message = self.responses[code][0]
            else:
                message = ''
        if self.request_version != 'HTTP/0.9':
            response = "{0} {1} {2}\r\n".format(self.protocol_version,
                                                code, message)
            self.wfile.write(response)

    def respond(self, response):
        if response.status_code < 400:
            self.send_response(response.status_code)
        else:
            self.send_error(response.status_code)
        for k, v in response.headers.items():
            self.send_header(k, v)
        self.end_headers()

        output = response.content
        # handle gzip
        # http://stackoverflow.com/questions/8506897/how-do-i-gzip-compress-a-string-in-python
        if 'content-encoding' in response.headers and \
                response.headers['content-encoding'].lower() == 'gzip':
            buffer = StringIO.StringIO()
            with gzip.GzipFile(fileobj=buffer, mode="w") as f:
                f.write(output)
            output = buffer.getvalue()

        # handle chunking
        # (thanks to https://gist.github.com/josiahcarlson/3250376)
        # although we only pretend to chunk and send it all at once!
        if 'transfer-encoding' in response.headers and \
                response.headers['transfer-encoding'].lower() == 'chunked':
            self.wfile.write('%X\r\n%s\r\n' %
                             (len(output), output))
            # send the chunked trailer
            self.wfile.write('0\r\n\r\n')
        else:
            self.wfile.write(output)
        self.log_response(response)

    def do_GET(self):
        response = requests.get(self.path)
        self.respond(response)

    def do_POST(self):
        self.data = self.rfile.read(int(self.headers['Content-Length']))
        response = requests.post(self.path, headers=headers, data=self.data)
        self.respond(response)

    def do_PUT(self):
        self.data = self.rfile.read(int(self.headers['Content-Length']))
        response = requests.put(self.path, headers=headers, data=self.data)
        self.respond(response)

    def log_error(format, *args):
        pass

    def log_request(self, *args):
        print ("*** REQUEST ***")
        print(self.command + ' ' + self.path)
        for (k, v) in rewrite_headers(self.headers).items():
            print ("{0} = {1}".format(k, v))
        print
        if self.command in ['POST', 'PUT']:
            print(self.data)
        print ("*** END REQUEST ***")

    def log_response(self, response):
        print("*** RESPONSE ***")
        if response.status_code in self.responses:
            shortmessage, longmessage = self.responses[response.status_code]
        else:
            shortmessage = longmessage = "Not a code known by requests module!"
        print ("{0} {1}".format(response.status_code, shortmessage))
        for (k, v) in rewrite_headers(response.headers).items():
            print ("{0} = {1}".format(k, v))
        print(response.content)
        print ("*** END RESPONSE ***")