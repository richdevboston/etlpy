

##
生成器，range(1,1,1),返回默认值

增加mix方法


def cross(a, gene_func):
    for r1 in a:
        r1=dict.copy(r1);
        for r2 in gene_func(r1):
            for key in r2:
                r1[key] = r2[key]
                yield dict.copy(r1);

因为改了一下缩进，让yield往前退了一步，调试超过半个小时。。。

## 2016年10月08日

增加RotateTF

新建链家爬虫

### 需要解决

并行执行器在子流层面的问题？

### 曹操曹操!!

判断是否是字符串,这个东西花了多长的时间才暴露出来!!!  merge_query!!!

## 生成器

如果生成器返回的是字典数组，后期的每个操作都会改变这个字典的值，导致无法重复执行，显然要copy一下

一旦引擎发生改变，要修改的代码就很多。。。所以一定要稳定

