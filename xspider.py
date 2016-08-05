import sys
from itertools import groupby

import extends
import etl
import re
import spider
from lxml import etree
import importlib
if extends.PY2:
    pass;
else:
    import html.parser as h
    html_parser = h.HTMLParser()



PM25 = 2.4;

ignoretag = re.compile('script|style');
boxRegex = re.compile(r"\[\d{1,3}\]");


def is_contain_kw(text, keyword):
    if text is None: return 0;
    keyword = keyword.strip();
    items = [r.strip() for r in re.split('\t|\r|\n', text)];
    for r in items:
        if r.find(keyword) >= 0:
            return 1;
    return 0;


def get_node_leaf_count(node):
    count = 0;
    if node is None: return;
    nodes = [r for r in node.iterchildren()];
    c = len(nodes);
    if c == 0: count += 1;
    for node in nodes:
        count += get_node_leaf_count(node);
    return count;


def _is_same_path(p1, p2, root_path):
    path1 = p1.replace(root_path,'');
    path2 = p2.replace(root_path,'');
    return str(path1) == str(path2);



def search_text_root(tree, node):
    class ParaClass(object):
        def __init__(self):
            self.tlen = 0;
            self.path = ''
    para=ParaClass();
    __search_text_root(tree, node, para);
    if para.path=='':
        return None;
    path=para.path.split('/');
    if len(path)<2:
        return para.path;
    path='/'.join(path[:-1])
    return path;


def __search_text_root(tree, node, para):
    if  hasattr(node,'tag')  and  isinstance(node.tag,str) and node.tag.lower() not in ['script','style','comment']:
        childnodes=[n for n in node.iterchildren()];
        if len(childnodes)>0:
            for child in childnodes:
                __search_text_root(tree, child, para);
        else:
            text = node.text;
            if text ==None:
                return ;
            tlen = len(text)
            if tlen > para.tlen:
                para.tlen = tlen;
                para.path = tree.getpath(node);


def remove_last_xpath_num(paths):
    v = paths[-1];
    m = boxRegex.search(v);
    if m is None:
        return paths;
    s = m.group(0);
    paths[-1] = v.replace(s, "");
    return '/'.join(paths);





def __get_sub_xpath(path,slice):
    r=path.split('/');
    paths= slice(r)
    return '/'.join(paths)
def __search_table_root(tree, nodes, path_dict, has_child):
    if nodes is None:
        return;
    if len(nodes) == 0:
        return None;
    node = nodes[0];
    if has_child:
        for node in nodes:
            all_childs=[child for child in node.iterchildren() if str(child.__class__).find('Element')>0]
            childs= groupby(all_childs,key=lambda node:node.tag);
            for key,node_group in childs:
                node_group = list(node_group);
                __search_table_root(tree, node_group, path_dict, has_child);
    target_node = list(node for node in nodes);  # filter(lambda x:not x.tag.startswith("#"),nodes);
    childCount = float(len(target_node));
    if 5 > childCount:
        return;
    sameNameCount = len([x for x in target_node if x.tag == target_node[1].tag]);
    if sameNameCount < childCount * 0.7: return;
    childCounts = [];
    for n in target_node:
        childCounts.append(len(list(r for r in n.iterchildren())));
    v = extends.variance(childCounts);
    if v > 2: return;
    leafCount = get_node_leaf_count(target_node[0]);
    if leafCount<2:
        return ;
    value = childCount * PM25 + leafCount;
    xpath = remove_last_xpath_num(tree.getpath(node).split('/'));
    path_dict[xpath] = value;


def search_table_root(root, has_child=True):
    d = {};
    tree = etree.ElementTree(root);
    __search_table_root(tree, root, d, has_child);
    return d;


def search_xpath(node, keyword, has_attr=False):
    tree = etree.ElementTree(node);
    return __search_xpath(tree,node,keyword,has_attr);
def __search_xpath(tree, node, keyword, has_attr=False):
    if node is None or keyword is None: return;
    nodes = node.iterchildren();
    for node in nodes:
        if str(node.__class__).find("Element") > 0:
            path = __search_xpath(tree, node, keyword, has_attr);
            if path is not None:
                return path;
            if node.text is not None and is_contain_kw(node.text, keyword):
                xpath = tree.getpath(node)
                return xpath;
            if has_attr:
                for r in node.attrib:
                    if is_contain_kw(node.attrib[r], keyword):
                        xpath = tree.getpath(node);
                        return xpath;
    return None;

def __get_nearest_node(targets, node):
    dic={};
    for target in targets:
        if type(target)!=type(node):
            continue;
        if target.tag!=node.tag:
            continue;
        dis= len(target.attrib.keys()) - len(node.attrib.keys())
        dis=abs(dis);
        dis+= abs(get_node_leaf_count(target) - get_node_leaf_count(node))
        dic[target]=dis;
    minv=99999;
    selectnode=None;
    for k,v in dic.items():
        if v<minv:
            selectnode=k;
    return selectnode;

def __get_diff_nodes(tree, nodes, xpaths, has_attr):
    ischildcontailInfo = False;
    node1 = nodes[0]
    tree1 = etree.ElementTree(node1);
    node1path = tree.getpath(node1);
    for childnode1 in node1.iterchildren():
        path = '/'.join(tree1.getpath(childnode1).split('/')[2:])
        nodechild2 = [];
        for r in nodes:
            targets = r.xpath(path);
            if len(targets) == 0:
                break;
            nodechild2.append(targets[0]);
        if len(nodechild2) <= 1:
            continue;
        ischildcontailInfo |= __get_diff_nodes(tree, nodechild2, xpaths, has_attr);
    if ischildcontailInfo == False:
        for r in nodes:
            if not  __is_same_string(r.text,node1.text):
                propname = __search_node_name(r, xpaths);
                xpath = spider.XPath(propname)
                xpath.Sample = node1.text;
                xpath.XPath = node1path if len(xpaths) % 2 == 0 else tree.getpath(r);
                xpaths.append(xpath);
                ischildcontailInfo = True;
                break;
    if not has_attr:
        return ischildcontailInfo;
    for r in node1.attrib:
        v = node1.attrib[r];
        for node in nodes:
            value = node.attrib.get(r, None);
            if value is None:
                break;
            if node.attrib[r] != v:
                xpath = spider.XPath(__search_node_name(r, xpaths) + "_" + r);
                xpath.XPath = node1path if len(xpaths) % 2 == 0 else tree.getpath(node);
                xpath.XPath += '/@' + r;
                xpath.Sample = v;
                xpaths.append(xpath);
                break;
    return ischildcontailInfo;


def get_diff_nodes(tree, root, rootpath, has_attr, exists=None):
    xpaths = [];
    nodes = [r for r in root.xpath(rootpath)]
    count = len(nodes);
    if count > 1:
        __get_diff_nodes(tree, nodes, xpaths, has_attr);
    if exists is not None:
        for r in exists:
            for p in xpaths:
                shortpath = spider.XPath(p.XPath).takeoff(rootpath);
                if r.XPath == str(shortpath):
                    p.Name = r.Name;
                    break;
    return xpaths;

def __is_same_string(t1,t2):
    if t1 is None and t2 is None:
        return True;
    elif t1 is not None and t2 is not None:
        return t1.strip()==t2.strip();
    return False


def __search_node_name(node, xpaths):
    if not  hasattr(node,'attrib'):
        return 'col%s' % (len(xpaths));
    attrkey = ["class","id"];
    for key in attrkey:
        name = node.attrib.get(key, None);
        if name is not None:
            break;
    if name is None:
        return  'col%s'%(len(xpaths));
    for c in xpaths:
        if c.Name == name:
            name2 = node.getparent().attrib.get(name, None);
            if name2 is None:
                return 'col%s' % (len(xpaths));
            else:
                name = name2 + '_' + name;
    return name.replace(' ', '_');



def search_properties(root, exist_xpaths=None, is_attr=False):
    if exist_xpaths == None: exist_xpaths = [];
    tree = etree.ElementTree(root);
    existLen = len(exist_xpaths);
    if existLen > 1:
        rootxpath = spider.get_common_xpath(exist_xpaths);
        return get_diff_nodes(tree, root, rootxpath, is_attr, exist_xpaths);

    elif existLen == 1:
        realPath =spider.XPath(exist_xpaths[0].XPath);
        path_dict = {};
        for r in realPath.itersub():
            __search_table_root(root.xpath(str(r)), path_dict, False);
        maxp = 0;
        path = None;
        for r in path_dict:
            if path_dict[r] > maxp:
                path = r;
            maxp = path_dict[r];
        if path is not None:
            return get_diff_nodes(tree, root, path, is_attr, exist_xpaths);
    else:
        path_dict = {};
        __search_table_root(tree, [root], path_dict, True);
        path_dict = sorted(path_dict, key=lambda d: path_dict[d], reverse=True);
        for rootpath in path_dict:
            items = get_diff_nodes(tree, root, rootpath, is_attr, exist_xpaths);
            if len(items) > 1:
                return rootpath,items;







