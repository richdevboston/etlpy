import unittest

from etl_test import *

from src.xspider import *
class Test(unittest.TestCase):

    def test_list_data(self):
        def get(url):
            r= len(get_list(get_html(url))[0]);
            return r;
        self.assertEqual(get('http://www.cnblogs.com'),20, 'test list fail')

    def test_main(self):
        def get(url,para=True):
            content= get_main(get_html(url),is_html=para);
            return len(content)>100
        self.assertEqual(get('http://renjian.163.com/16/0817/20/BUMRU2KL00015C18.html'), True, 'test sub fail')
        self.assertEqual(get('http://renjian.163.com/16/0817/20/BUMRU2KL00015C18.html',False), True, 'test sub fail')




if __name__ =='__main__':
  unittest.main()