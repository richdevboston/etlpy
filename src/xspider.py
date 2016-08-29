import re
from itertools import groupby

from lxml import etree

from src import extends, spider

if extends.PY2:
    pass
else:
    import html.parser as h
    html_parser = h.HTMLParser()




PM25 = 2.4;

ignoretag = re.compile('script|style');
boxRegex = re.compile(r"\[\d{1,3}\]");


def str_match(text, keyword, match_func):
    if text is None: return 0;
    keyword = keyword.strip();
    items = [r.strip() for r in re.split('\t|\r|\n', text)];
    for r in items:
        if match_func(r,keyword):
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
        child_nodes=[n for n in node.iterchildren()];
        if len(child_nodes)>0:
            for child in child_nodes:
                __search_text_root(tree, child, para);
        else:
            text = node.text;
            if text ==None:
                return ;
            tlen = len(text)
            if tlen > para.tlen:
                para.tlen = tlen;
                para.path = tree.getpath(node);








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
    xpath = spider.remove_last_xpath_num(tree.getpath(node).split('/'));
    path_dict[xpath] = value;


def search_table_root(root, has_child=True):
    d = {};
    tree = etree.ElementTree(root);
    __search_table_root(tree, root, d, has_child);
    return d;

def _str_find(string,word):
    return string.find(word)>=0;

def _regex_find(string,regex):
     res=re.match(regex,string);
     if res:
         return res;
     return res;

def _tn_find(string,rule):
    from src.tn import core
    return core.match(string,rule) is not None;


def search_xpath(node, keyword, match_func='str',has_attr=False):
    tree = etree.ElementTree(node);
    dics={'str':_str_find,'tn':_tn_find,'re':_regex_find};
    return __search_xpath(tree,node,keyword,dics[match_func] ,has_attr);


def __search_xpath(tree, node, keyword,match_func, has_attr=False):
    if node is None or keyword is None: return;
    nodes = node.iterchildren();
    for node in nodes:
        if str(node.__class__).find("Element") > 0:
            path = __search_xpath(tree, node, keyword, match_func, has_attr);
            if path is not None:
                return path;
            if node.text is not None and str_match(node.text, keyword,match_func):
                xpath = tree.getpath(node)
                return xpath;
            if has_attr:
                for r in node.attrib:
                    if str_match(node.attrib[r], keyword,match_func):
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
    is_child_contain_info = False;
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
        is_child_contain_info |= __get_diff_nodes(tree, nodechild2, xpaths, has_attr);
    if is_child_contain_info == False:
        for r in nodes:
            if not  __is_same_string(r.text,node1.text):
                prop_name = __search_node_name(r, xpaths);
                xpath = spider.XPath(prop_name)
                xpath.sample = node1.text;
                xpath.path = node1path if len(xpaths) % 2 == 0 else tree.getpath(r);
                xpaths.append(xpath);
                is_child_contain_info = True;
                break;
    if not has_attr:
        return is_child_contain_info;
    for r in node1.attrib:
        v = node1.attrib[r];
        for node in nodes:
            value = node.attrib.get(r, None);
            if value is None:
                break;
            if node.attrib[r] != v:
                xpath = spider.XPath(__search_node_name(r, xpaths) + "_" + r);
                xpath.path = node1path if len(xpaths) % 2 == 0 else tree.getpath(node);
                xpath.path += '/@' + r;
                xpath.sample = v;
                xpaths.append(xpath);
                break;
    return is_child_contain_info;


def get_diff_nodes(tree, root, root_path, has_attr, exists=None):
    xpaths = [];
    nodes = [r for r in root.xpath(root_path)]
    count = len(nodes);
    if count > 1:
        __get_diff_nodes(tree, nodes, xpaths, has_attr);
    if exists is not None:
        for r in exists:
            for p in xpaths:
                short_path = spider. XPath(p.path).takeoff(root_path);
                if r.path == extends.to_str(short_path):
                    p.name = r.name;
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
    attr_key = ["class","id"];
    for key in attr_key:
        name = node.attrib.get(key, None);
        if name is not None:
            break;
    if name is None:
        return  'col%s'%(len(xpaths));
    for c in xpaths:
        if c.name == name:
            name2 = node.getparent().attrib.get(name, None);
            if name2 is None:
                return 'col%s' % (len(xpaths));
            else:
                name = name2 + '_' + name;
    return name.replace(' ', '_');



def search_properties(root, exist_xpaths=None, is_attr=False):
    if exist_xpaths == None: exist_xpaths = [];
    tree = etree.ElementTree(root);
    exist_len = len(exist_xpaths);
    if exist_len > 1:
        root = get_common_xpath(exist_xpaths);
        return get_diff_nodes(tree, root, root, is_attr, exist_xpaths);

    elif exist_len == 1:
        real_path = spider.XPath(exist_xpaths[0].path);
        path_dict = {};
        for r in real_path.itersub():
            __search_table_root(root.xpath(str(r)), path_dict, False);
        max_p = 0;
        path = None;
        for r in path_dict:
            if path_dict[r] > max_p:
                path = r;
            max_p = path_dict[r];
        if path is not None:
            return get_diff_nodes(tree, root, path, is_attr, exist_xpaths);
    else:
        path_dict = {};
        __search_table_root(tree, [root], path_dict, True);
        path_dict = sorted(path_dict, key=lambda d: path_dict[d], reverse=True);
        for root_path in path_dict:
            items = get_diff_nodes(tree, root, root_path, is_attr, exist_xpaths);
            if len(items) > 1:
                return root_path,items;


def get_list(html,xpaths=None, has_attr=False):
    root= spider._get_etree(html);
    if xpaths is None:
        root_path, xpaths = search_properties(root, None,has_attr );
    datas = spider._get_datas(root, xpaths, True, None)
    return datas,xpaths;

def get_main(html,is_html=False):
    root= spider._get_etree(html);
    tree = etree.ElementTree(root);
    node_path = search_text_root(tree, root);
    nodes = tree.xpath(node_path);
    if len(nodes)==0:
        return ''
    node=nodes[0]
    if is_html:
        res = etree.tostring(node).decode('utf-8');
    else:
        if hasattr(node, 'text'):
            res = spider.get_node_text(node);
        else:
            res = extends.to_str(node)
    return res;