import unittest
import pickle

from etl import *
from repl_test import  *
class Test(unittest.TestCase):

    def test_zhihu(self):
        s = spider('zhihu')
        url = 'https://www.zhihu.com/topic/19551137/top-answers'
        s.visit(url).great_hand().accept()
        t = task('list')
        t.pyge('url', script=(url + '?page=%s' % (i) for i in range(1, 50)))
        t.crawler('url', selector='zhihu')
        t.get(etl_count=1)
        #self.assertEqual(get('http://www.cnblogs.com'), 20, 'test list fail')