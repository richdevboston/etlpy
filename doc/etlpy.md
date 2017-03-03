
# etlpy: Python编写的流式爬虫系统

## 简介

etlpy是纯Python开发的函数库，实现流式DSL(领域特定语言)，能一行内完成爬虫，文件处理和数据清洗等。能和pandas等类库充分集成。

它和linux的bash pipeline,C#的Linq以及作者本人开发的Hawk有高度的相似性。

下面一行代码实现了获取博客园第1到10页的所有html:
```
from etlpy import *
t= task().p.create(range(1,10)).cp('p:html').format('www.cnblogs.com/p{}').get()
#t.to_df()  生成DataFrame
for data in t:
    print data

```
在p列生成从1到10的数，拷贝p列到html列，将html列合并为url,并发送web请求，最后的html正文保存在html列。

etlpy的特性有：

- 同时支持python2和python3
- 内置方便的代理，http get/post请求，写法与requests库非常相似
- 内置正则解析，html转义，json转换等数据清洗功能，直接输出
- 能方便地将任务按照协程，线程，进程，和多机分布式的方式进行任务并行



## 核心概念

使用etlpy，先介绍以下核心概念：

## 数据流(generator)

etlpy特别适合批量处理数据，我们称为数据流，典型的数据流如：

- 数组(数组元素可能是类的实例化对象，但最常见的是字典)，
- 一行行的文本
- Excel，pandas的DataFrame表格
- 1000个web请求

它们共同的特征是每个元素都很相似，能通过迭代器访问。为了简化讨论，我们处理的就是字典的迭代器。想象一个工作流，它能不断地生成和加工字典，最后将字典输出。

### 属性(property)

一个字典如 `{'a':22,'b':33}`，那么a,b就是字典的键，如果有多个字典，都有a,b两个属性，就能形成表格，a,b就是列了，因此属性和列，可认为是一个概念。

真实的数据处理，一般都会对某个特定属性做连续操作。当let指定了某些列后，之后所有的操作都可针对这些列，从而简化代码，直到重新设定了列，被操作的列称为目标列。

### 算子(tool)

算子可以对字典做修改，所有的算子分为四种类型：

- 生成器（GE）:如生成100个字典，键为p，值为‘1’到‘100’
- 转换器（TF）:如将'地址'列中的数字提取到'电话'列中
- 过滤器（FT）:如过滤所有某一列的值为空的的字典
- 执行器（GE）:如将所有的字典存储到MongoDB中。
- 排序器 (ST):如将数据流按a列进行降序排列


算子可以类比于加法和乘法等基本操作。etlpy提供了简单方便的文件读写，web访问等算子，你也可以方便地扩展其他工具来增强功能(TODO:参考自定义算子)。

绝大多数算子都包含一个最常用参数，它作用在目标列上，个别算子包含一些可选参数。例如:

```
task().create(datas).a.split(',')   #split的参数就是分割字符，作用的列是a列
```
参数也可以从其他列读取:
```
task().create(datas).p.set(',').a.split('[p]')   #等价于上面的值，方括号表达式指代从其他列读取
```

算子会对属性做处理，按照其作用域，算子能够分为三种：

- col:作用于特定的属性: 如清除特定属性的空白符 
- data:作用于整个字典： 例如清除所有以a开头的属性
- stream:作用于数据流： 只获取数据流的前三个数据，或跳过2个数据。


下面的表，整理了算子的使用方法：

| 名称 |  参数 | 功能  | 执行后结果 | 例子  | 类型 | 
| -- |  --  |   --   |  --  |---| --  | 
|   |    |     | 设定目标列   |  |   | 
| let | 'new' | 设置目标列为new |  new | let('new') |TF-data |
| mv | 'old:new' | 从原始列移动到新列 |  new | mv('a:b') |TF-data |
| cp | 'old:new' | 从原始列复制到新列 |  new | cp('a:b') |TF-data |
| cp2 | 'old:new' | 从原始列复制到新列 | old | cp('a:b') |TF-data |
| rm | 'a' | 删除a列 | 不变 | rm('a b') | TF-data |
| keep | 'a'  | 保留a列 | a | keep('a') |TF-data| 
|   |    |     | 转换器   |  |   | 
| incr | 无 | 增加自增主键 | 不变 | incr() |TF-col |
| split | 分隔符 | 对目标列分割 | [] | split(',') |TF-col |
| into | 'a b' | 将数组分配到不同列 |  无 | into('a b')  | TF-data|
| at |  索引 | 提取数组/字典的项 | 无 |at('a') |  TF-data|
| escape | html/url | 进行html/url转义 | str | escape('html') | TF-col|
| clean | html/url | 进行html/url反转义 | str | clean('url') |TF-col|
| format | 合并表达式 | 合并多个列 | str | format('{0}-{col}') |  TF-col|
| regex | 正则表达式 |  获取节点的text或str | regex('\d+') | [str] |   TF-col|
| replace | 正则表达式 | 替换匹配的值 | replace('\d+',value='new') | str|   TF-col|
| num | 无 | 提取数字 | num() | [float] |   TF-col|
| strip | char | 清除首尾符号 | strip() | str |   TF-col|
| extract| 起始字符串 | 首尾夹逼提取 | extract('start',end='end') | str |   TF-col|
| map| func/lambda | 对每个元素操作 | map(print)  | ? |   TF-col/data|
| dump| 'json/yaml/xml'  | 将目标列导出 | dump('json')  | str |   TF-col|
| load| 'json/yaml/xml'  | 从文本导入 | load('json')  | obj |   TF-col|
| todict| 'a b'  | 将a,b列合并到字典 | todict('a b')  | obj |   TF-data|
| drill| 'a:b'  | 将目标列字典提到外部 | drill('a:b')  | 无 |   TF-data|
|   |    |     | 爬虫工具   |  |   | 
| html |  无 | 获取节点的html | html() |str|  TF-col|
| text |  无 | 获取节点的text或str | text() |str|  TF-col|
| xpath | xpath-str | 通过xpath提取html | [node] | xpath('//div') | TF-col|
| pyq | jquery-str | 通过jquery提取html | [node] | pyq('.a')| TF-col|
| detect | int | 自动嗅探网页列表 | 表格+xpath | detect(0)| TF-data|
| cache | 空数组 | 缓存之前结果 | 无 | cache([])| TF-data|
| get | 发送的数据 | http-get请求 | 无 |get('[p]')| TF-col|
| post | 发送的数据 | http-post请求 | 无 |post('[p]')| TF-col|
| tree | html | 对html建立etree | 无 | tree() |  TF-col|
| search | 关键字 | 对html搜索xpath | 打印xpath | search('aa') |  TF-col|
| dl | 目标路径 | 将目标列链接下载到某路径 | 无 | dl('abc.zip') |  TF-col|
|   |    |     | 流处理   |  |   | 
| take | 数量p | 获取前p个元素 | 无 | take(5) |TF-stream |
| list | 要提取的列 | 将某列数组数据上钻 | 新列表 | list('a b c') |TF-stream |
| skip | 数量p | 跳过前p个元素 | 无 | skip(5) |TF-stream |
| last | 无参数 | 获取最后的元素 | 无 | last() |TF-stream |
| count | 数量p | 每p个元素打印index | 不变 | count(5) |TF-stream |
| tag | 注释 | 仅需注释 | 不变 | tag('注释') |TF-stream |
| agg | 函数/lambda | 对相邻数据聚合 | 不变 | agg(lambda a,b: a+b) |TF-stream |
| delay | 数量p | 延迟p毫秒 | 不变 | delay(100) |TF-stream |
| pl | 并行模式 | 插入并行算子 | 无 | pl(5) |TF-stream |
|   |    |     | 生成器   |  |   | 
| create | 生成器/数组 | 生成数据 | 不变 | create([{}]) |GE-data/col|
| range | 区间表达式字符串 | 生成固定区间数 | 不变 | range('1:100:2') |GE-col |
| read | 文件路径 | 按文件行读取 | 不变 | read('a.txt') |GE-col |
|   |    |     | 过滤器   |  |   | 
| where | 函数/lambda | 对数据做过滤 | 不变 | where(lambda x:x!=None) | FT-col|
| null | 无参数 | 过滤空对象 | 不变 | null() | FT-col|
| repeat | 无参数 | 过滤重复数据 | 不变 | repeat() | FT-col|
| match | 正则 | 过滤非匹配数据 | 不变 | match('\d') | FT-col|
|   |    |     | 执行器   |  |   | 
| dbex | 连接器名称 | 将数据写入连接器 | 不变 | dbex('connector') |EX-data |
| write | 文件路径 | 将数据按行写入文件 | 不变 | write('abc.txt') |EX-col |
|   |    |     | 排序器   |  |   | 
| ascend | 函数/lambda | 升序排列 | 不变 | ascend(lambda x:x[0]) |ST-data |
| descend | 函数/lambda | 降序排列 | 不变 | 同上 |ST-col |
|   |    |     | 子任务   |  |   | 
| subge | 子任务名称或实例 | 以生成器方式调用| 不变 | subge(task().create()..) |GE-stream |
| subex | 子任务名称或实例 | 以执行器方式调用| 不变 | subex('task_name') | EX-stream |
| sub | 子任务名称或实例 | 以转换器方式调用| 不变 | sub('task_name') | TF-stream |


#### 算子和列

> 算子的设计，尽量追求功能正交化，只实现一个功能，col算子作用于目标列，参数为默认参数。能通过let算子，指定要处理的列,例如:

`task().create(datas).let('a').strip().format('{0}{0}')`

让a属性的值，去除空白符，并将其值重复两次。对多个列可批量处理，中间用空格分割。如`a b c`

- let('*') 表示对所有的列做处理
- 参数可以写正则表达式，能成功匹配的列都能做处理。

如果只有一个列，且列名符合python命名规则，则let函数可以省略，上面的代码能简化为:

`task().create(datas).a.strip().format('{0}{0}').b....`

> 注意cp和cp2的区别，cp拷贝后做滑动，cp2不做滑动。cp2等价于cp('a:b').let('a'), mv,cp,rm的命名都参考了linux的风格


#### 生成器

生成器用于生成数据(废话)，因此需要和其他数据流做融合。控制融合的方式是默认的mode参数：

 - cross(*) :  笛卡尔积
 - append(+):  纵向拼接 (默认)
 - merge(|) :  横向拼接
 - mix (mix):  类似ABABAB，依次交错

> create: 若不指定目标列，参数为字典数组或字典生成器，例如:
```
task().create(('a':i for i in range(10)))
{'a':1}
{'a':2}
{'a':3}
...
```
若指定目标列，参数为一般元素的数组或生成器，上面的代码等价于:
`task().a.create(range(10))`

> range用于创建区间数，其参数为字符串，如'1:10', '10',  '1:10:1'， 上面的代码也等价于: `task().a.range('1:10')`

既然有create为何还要有range函数呢？ 因为最大值或间隔可能来自其他列：

`task().p.create([20]).a.range('1:[p]',mode='*')`

上面的代码为：先创建列p，值为20，再在a列上创建区间数，范围从1到20(p列的值)， 组合模式为* (cross).

#### 执行器

执行器是特殊的转换器，区别在于只有处于执行模式时才会生效。etlpy明确区分调试模式和运行模式，因为执行器通常是具有副作用的操作(如访问web)，只在执行时才会启动。

#### 转换器

> at: 对目标列数据取索引值，和python的字典和数组的索引使用方法一致。但若使用slice，则需要使用字符串。

`create(datas).p.at(0)`

等价于：

`create(datas).p[0]['hello']`

#### 自定义算子

> 算子在源代码中，体现为一个个的类。为了便于辨识，类命名都为名字+简写，例如CreateGE,就是一个生成器。但在实际编写任务时，所有的算子都体现为函数，全部名称小写，省略GE,TF等标识。如果操作不够高频，那么只需要传递函数或lambda表达式，即可自定义功能。map,where,sort算子都支持函数传递。

> 对于data,stream两类算子，目标列对它们不起作用。但map,where,sort和format是特殊情况，它们可接受函数作为参数，能同时兼容A,B两类情况，举例如下:

```
task().create(datas).let('a').map(lambda x:x+2)  #map只针对a列，即a+=2

def func(data):
    data['a']+=1
    print data 
task().create(datas).let('').map(func)  #map会针对整个字典对象。 let('')用于指定整个字典
```


### 任务(task)

所有的任务都以task()来定义，通过组合不同的算子，就能定义完整的task.

通常来说，task都以生成器开头:
```
generator= task('task_name').let('p').create(range(1,20))

for r in t:
    print r

# 等价于:
def generator():
    for i in range(1,20):
        yield {'p':i}
for r in generator():
    print r
 

```
task()函数的参数即该任务的名称,每个任务都需要独一无二的名称，默认为'task'

任务可以理解为函数，可以定义多个任务，父任务可调用子任务。任务也可以调用自身。任务能够被切分，或者保存为json等格式，在网络上传输。

### 工程(project)

一个工程维护了多个任务，这些任务互相可能有依赖。工程也提供了一个环境(env)。例如，一些算子需要读取字典(比如请求头)作为参数，反复地创建字典并传递是不方便的，可将该字典放在工程中。
```
p=project()
p.env['header']={'header': 'abc'}
t=task().let('t').create(['url']).get(header='header')
#这样就访问了工程里的header参数。
```

使用`from etlpy import *` ，就会创建默认的工程。 



## 原理

etlpy的源代码非常简洁，只有三个py文件，核心etl.py只有1500行。

### 核心代码

etlpy的解释器会分析链式语法，并构建相应的计算图。 但只有真实迭代时，才会进行求值。

generate函数，会将多个tool拼接，组合成高阶函数，其实现如下：
```
def generate(tools, generator=None, env=None):
    '''
    evaluate a tool stream
    :param tools: [ETLTool]
    :param generator: seed generator for generator
    :param init: bool, if initiate every tool
    :param execute: bool, if enable executors
    :param env:
    :return: a generator
    '''
    if env is None:
        env = {'column': ''}
    if tools is not None:
        for tool in tools:
            env= get_column(tool,env)
            if isinstance(tool,LetTF):
                continue
            column= env['column']
            generator = tool.process(generator, column) #核心部分
            env['column']=env['next']
    if generator is None:
        return []
    return generator
```

对每个tool进行遍历，在核心部分被组合成新的迭代器。环境会在构建函数时被传递和修改（目前环境仅维护了列名）



### 生成器(GE)

> html 可以合并到dump里？


#### 

#### 



    



### 过滤器（FT)

### 执行器（EX）



## 子任务

你可以设计多个任务，并将其组合起来，子任务也可以分别设计为转换，过滤，执行和生成器。





## 日志和调试

etlpy的编程风格导致其很难使用单步调试，因此合理的日志很重要。你可以设置不同的日志等级:

- >4: 对异常打印完整的信息堆栈
- >5: 对每条数据的处理过程给出完整信息
- >2：

除此之外，你还可以将日志信息重定向到文件，使用方法参考  TODO


## 数据库连接

数据库操作包含生成器DbGE, 和执行器DbEX. etlpy内置了mongodb连接器。之所以这样设计，是为了能方便地将各种参数在json上传递。

一般情况下，只要实现字典的生成器函数，就能通过create函数创建。执行亦是同理，因此大多数情况不需要自定义连接器。


## 多线程操作




