# etlpy

##designed by desert

a smart stream-like crawler &amp; etl python library

##1.简介


etlpy是基于流和函数式范式的数据采集和清洗工具。能大大减少数据抓取所需的资源，能在尽量短的代码内实现以下功能：

###基础目标：
 - 自动提取新闻正文和表格内容
 - 能够快速发现并模拟翻页逻辑
 - 能尽量高效地实现增量更新
 - 自动识别文中的关键信息
 - 提供BFS的访问方式
 - 分布式抓取

###高级目标：
 - 实现网站信息**动态查询**,即可输入类似SQL的代码，实时查询网站数据
 - 多数据源快速集成，如能实现多个新闻网站的统一聚合器，实时获取其增量数据内容
 - 对网站改版有充分的鲁棒性，能自动调整搜索算法



##2.基本原理

模块分为 生成，过滤，排序，转换，执行四种。  

利用Python的生成器，可以将不同模块组织起来，定义一个流水线，数据（python的字典）会在流水线上被加工和消费。  

爬虫，计算，清洗，任何符合一定计算范式的数据，都可以使用它来完成。


 
##3. 核心技术

**对DOM树的搜索和分析**

能够自动评估最有价值的内容。目前采用评分机制，考虑使用分类器和神经网络的方法，来进一步提升准确性。
 
**自动分布式**

使用函数式设计，所有模块构成链条，方便保存为模块配置文件并进行分发。系统能自动寻找可分布式的入口切分该链条。

**支持良好的语法**
    能够在尽可能短的语法结构内描述抓取流程：

    下面描述了从8万个新闻url中获取新闻正文，并保存到数据库的**全部代码**：
```
    datas = open('/Users/zhaoyiming/Documents/stock.news').read().split('\001')
    datas = [json.loads(r) for r in datas if len(r) > 10]
    for r in datas:
        r['url'] = r['text'].split('"')[1]
    c = new_connector('cc', MongoDBConnector())
    c.connect_str='mongodb://10.101.167.107'
    c.db='ant_temp';
    s=new_spider('sp')
    t.dbex(connector='cc', table='news')
```


 
 **内置丰富方便的多种数据抓取函数**
 
 支持XPath,pyquery，还能通过关键词和tn规则搜索关键信息。例如

`s.search(rule='datetime')`即可搜索到网页中表达时间的节点xpath。

**可视化支持**

`s.search(rule='datetime')`即可搜索到网页中表达时间的节点xpath。

**可视化支持**

借助于IPython Notebook，能提供可视化配置的ETL流和搜索逻辑，从而进一步降低工作量。





