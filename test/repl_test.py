import unittest

from src.repl import  *


class Test(unittest.TestCase):


    def setUp(self):
        s = spider('zhihu')
        url = 'https://www.zhihu.com/topic/19551137/top-answers'
        s.visit(url).great_hand().accept().test().get(format='df')
        self.t = task('list')
        t=self.t
        t.pyge('url', script=(url + '?page=%s' % (i) for i in range(1, 50)))
        t.crawler('url', selector='zhihu')

    def test_get(self):
        self.t.get(take=100,format='df')

    def test_distribute(self):
        self.t.distribute()

    def test_exec(self):
        self.t.execute()

    def test_m_exec(self):
        self.t.m_execute()



if __name__ =='__main__':
    t=Test()
    t.setUp();
   # t.test_get()
#    t.test_exec()
    t.test_m_exec()
  #  t.test_distribute()