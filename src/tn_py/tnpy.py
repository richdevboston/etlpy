# coding=utf-8
import re
import itertools;
import sys

PY2 = sys.version_info[0] == 2
int_max = 9999999;

NULL_MATCH = 'NULL_MATCH'

EXTRACT_DEFAULT=0
EXTRACT_REWRITE=1
EXTRACT_DICT=2

def is_str(s):
    if PY2:
        if isinstance(s, (str, unicode)):
            return True
    else:
        if isinstance(s, (str)):
            return True;
    return False;

def _attr(item,key,default=None):
    if hasattr(item,key):
        return getattr(item,key)
    return default;

def to_str(s):
    try:
        return str(s);
    except Exception as e:
        if PY2:
            return unicode(s);
        return 'to_str error:' + str(e);


def merge(d1, d2):
    if d2 is None:
        return
    for r in d2:
        d1[r] = d2[r];
    return d1;


def find_any(iteral, func):
    for r in iteral:
        if func(r):
            return r;
    return None;


def get_index(iteral, func):
    for r in range(len(iteral)):
        if func(iteral[r]):
            return r;
    return -1;


def __get_public_route(m):
    from collections import deque
    d = deque()
    route = []
    route.append(m.entity.order)
    d.append(m)
    while True:
        if len(d) == 0:
            break
        m = d.popleft()
        route.append(m.m_index)
        m = m.children
        while m is not None:
            d.append(m)
            m = m.next_match
    return route




class MatchResult(object):
    def __init__(self, entity, match, start, children=None, rewrite=None, name=''):
        super(MatchResult, self).__init__()
        self.order = 0
        self.m_index = 0
        self.name = name
        self.children = children
        self.entity = entity
        self.match = match
        self.rewrite = rewrite;
        self.pos = start
        self.should_rewrite = None;
        self.can_split = False;


    def get_end(self):
        return self.pos + len(self.match);

    def __getitem__(self, item):
        if isinstance(item, int):
            if self.children is None:
                return self;
            return self.children[item]

    def get_should_rewrite(self):
        if self.should_rewrite != None:
            return self.should_rewrite;
        if self.children is None:
            if self.entity is None:
                return False;
            if isinstance(self.entity, ScriptEntity) == False:
                return self.entity.rewrite is not None

            else:
                return True;
        else:
            r = False;
            order = 0;
            ms = self.children;
            for m in ms:
                if order != m.order:  # order diff must be rewrite
                    r = True;
                    break;
                order += 1;
                r |= m.get_should_rewrite();
            self.should_rewrite = r;
            return r;

    def rewrite_item(self):
        if self.rewrite is not None:
            return self.rewrite;
        if self.should_rewrite==False:
            self.rewrite = self.match;

        if self.children is None:
            self.rewrite = self.entity.rewrite_item(self.match)
        elif isinstance(self.entity, ScriptEntity):
            r=self.entity.rewrite_item(self);
            if r is not None:
                self.rewrite=r
        else:
            match = self.children[:];
            match = sorted(match, key=lambda m: m.order);
            rewrite = '';
            if len(match)==1:
                self.rewrite=match[0].rewrite_item();
            else:
                for m in match:
                    rewrite += to_str(m.rewrite_item());
                self.rewrite=rewrite;

        return self.rewrite

    def __str__(self):
        return self.match;

    def extract(self, doc, mode=0):
        def dict_copy(source,target):
            for r in source:
                target[r]=source[r];
        extract_mode=  self.entity.extract_mode;
        if extract_mode==EXTRACT_REWRITE:
            result=self.rewrite_item()
            return extract_mode,result
        is_repeat = isinstance(self.entity, RepeatEntity);
        child_doc ={}
        if _attr(self.entity,'extract_kind',False):
            doc['#'+self.entity.name] = self.entity.tables[self.m_index].name;
        if self.children is not None:
            if is_repeat:
                child_doc=[]
            for m in self.children:
                sub_mode,result= m.extract(child_doc,1 if is_repeat else 0);
                if sub_mode== EXTRACT_DICT:
                    extract_mode= EXTRACT_DICT
                elif sub_mode==EXTRACT_REWRITE and result is not None:
                    child_doc= result

        if mode == 0:
            if len(self.name) != 0:
                if self.name == '$value':
                    doc[doc['$key']] = self.rewrite_item();
                    del doc['$key']
                else:
                    if extract_mode==EXTRACT_DICT and len(child_doc) != 0:
                        if _attr(self.entity,'extract_doc_up',False):
                            dict_copy(child_doc, doc)
                        else:
                            doc[self.name]=child_doc
                    else:
                        value = self.rewrite_item();
                        name=self.name;
                        if name not in doc:
                            doc[name] = value;
                        else:
                            if not isinstance( doc[name],list):
                                doc[name]=[doc[name]];
                            doc[name].append(value);

            else:
                if extract_mode!=EXTRACT_REWRITE:
                    dict_copy(child_doc, doc);
        else:
            doc.append(child_doc);
        return extract_mode,None


class EntityBase(object):
    def __init__(self):
        self.order = 0
        self.name = ""
        self.start = False
        self.type = ""
        self._core = None
        self._can_cache = True;
        self.rewrite = None;
        self.extract_mode=EXTRACT_DEFAULT
    def _get_entity(self, value):
        if isinstance(value, EntityBase):
            name = value.name;
            if name == '':
                value._core = self._core
                return value;
            result = value;
        elif is_str(value):
            result = self._core.entities.names.get(value, None);
        else:
            result = value();
        if result is None:
            return value;
        result._core = self._core;
        return result;

    def _travel(self, child):
        if is_str(child):
            self.log('entity %s can not be found' % child);
        self._can_cache = self._can_cache and child._can_cache;

    def clone(self, macro):
        import copy;
        new = copy.copy(self);
        new._macro = macro;
        return new;

    def rewrite_item(self, input):
        m = self.match_item(input, 0, None, True);
        return m.rewrite_item();

    def rebuild(self):
        pass;

    def set_values(self, values):
        if isinstance(values, dict):
            value = values.get("Order", None);
            if value is not None:
                self.order = int(value);
            value = values.get("type", None);
            if value is not None:
                self.type = value;
            value = values.get("Parameter", None);
            if value is None:
                return;
            if value.find('|') >= 0:
                return;
            va = value.split(',');
            for v in va:
                vs = v.split('=');
                if len(vs) != 2:
                    continue;
                key, value = vs[0].strip(), vs[1].strip();
                value = eval(value);
                setattr(self, vs[0].strip(), value);

    def eval_script(self, m, input=None, script=None):
        if script is None:
            script = self.script;
        if self.script == '':
            return True
        if not is_str(self.script):
            return self.script(self, m)
        if input is None:
            input = m[0].match;
        core = self._core

        def check(condition, result, else_work=None):
            if eval(condition):
                r = eval(result);
                return r;
            elif else_work is not None:
                r = eval(else_work);
                return r;

        def invoke(func, para):
            return eval(func)(para);

        def e(entity_name):
            entity = self._get_entity[entity_name]
            header = None
            header = entity.match_item(input, 0, True, header)
            if not is_fail(header):
                header = MatchResult(entity, None, -100)
                return header
            return None

        def dist(name, i=0):
            header = e(name)
            if header is None:
                return int_max;
            return abs(header.pos - m[i].pos)

        result = eval(self.script)
        return result

    def log(self, data):
        if self._core is None or self._core.log_file is None:
            return
        self._core.log_file.write(to_str(data) + '\n');

    def log_in(self, input, start, end=None):
        if self._core is None or self._core.log_file is None:
            return

        if self._core.log_file.name.find('htm') < 0:
            if end is not None:
                end = start + 200;
            input = input[start: end].replace('\n', '<\\n>').replace('\r', '<\\r>');

            self._core.log_file.write(' ' * self._core.match_level * 2)
            if self._core.log_file.encoding != 'utf-8':
                input = input.encode('utf-8')
            self._core.log_file.write('%s,Raw  =%s\n' % (to_str(self), to_str(input)))
        else:
            if self._core.log_file.encoding != 'utf-8':
                input = input.encode('utf-8')
            self._core.log_file.write('<p>' + '&nbsp;' * self._core.match_level * 4)
            self._core.log_file.write('%s,Raw= <font color="#FF0000">%s</font></p>\r' % (to_str(self), to_str(input)))
        self._core.match_level += 1

    def log_out(self, match, buffered=False):
        if self._core is None or self._core.log_file is None:
            return
        if self._core.log_file.encoding != 'utf-8' and match is not None:
            match = match.encode('utf-8')
        self._core.match_level -= 1
        if self._core.log_file.name.find('htm') < 0:
            self._core.log_file.write(' ' * self._core.match_level * 2)
            if match is not None:
                match = match[:200].replace('\n', '<\\n>').replace('\r', '<\\r>');

                self._core.log_file.write(
                    '%s,%s=%s\n' % (to_str(self), ('Buff ' if buffered else 'match'), to_str(match)))
            else:
                self._core.log_file.write('%s,NG\n' % to_str(self))
        else:

            self._core.log_file.write('<p>' + '&nbsp;' * self._core.match_level * 4)
            if match != None:
                self._core.log_file.write(
                    '%s,<b>OK</b>,Raw= <font color="#FF0000">%s</font></p>\r' % (to_str(self), to_str(match)))
            else:
                self._core.log_file.write('%s,<font color="#FF0000"><b>NG</b></font></p>\r' % to_str(self))

    def match_item(self, input, start, end, must_start, mode=None):
        return None;

    def _get_name(self):
        name = self.name if self.name != "" else "unknown"
        return "%s,%s" % (name, self.__class__.__name__.replace('Entity', ''))

    def __str__(self):
        return self._get_name()


class StringEntity(EntityBase):
    def __init__(self, match="", rewrite=None, condition=''):
        super(StringEntity, self).__init__()
        self.match = match
        self.rewrite = rewrite
        self.condition = condition

    def rewrite_item(self, input):
        if None == self.rewrite:
            return input
        return input.replace(self.match, self.rewrite);

    def set_values(self, values):
        super(StringEntity, self).set_values(values);
        if isinstance(values, dict):
            return;
        self.match = values[0]
        if len(values) > 1:
            self.rewrite = values[1]

    def match_item(self, input, start, end, must_start, mode=None):
        self.log_in(input, start)
        if end is None:
            end = int_max;
        pos = input.find(self.match, start, end)
        if pos < 0 or (must_start == True and pos != start):
            self.log_out(None, False)
            return int_max if pos < 0 else pos;

        self.log_out(self.match)
        m = MatchResult(self, self.match, pos)
        m.rewrite = self.match if self.rewrite is None else self.rewrite;
        return m;


class RefEntity(EntityBase):
    def __init__(self, name):
        super(RefEntity, self).__init__()
        self.rule = name;
        self._can_cache = False;

    def match_item(self, input, start, end, must_start, mode=None):
        import inspect
        stack = inspect.stack(6)
        entity = None;
        i = 5;
        while entity is None and i > 0:
            macro = stack[i][0].f_locals;
            macro = macro['self']
            if not isinstance(macro, EntityBase):
                i -= 1;
                continue;
            if not hasattr(macro, '_macro'):
                i -= 1;
                continue
            macro = macro._macro;
            if self.rule in macro:
                entity = macro[self.rule];
                if isinstance(entity, str):
                    entity = self._core.entities.names[entity]
                break;
            else:
                i -= 1;
        return entity.match_item(input, start, end, must_start, mode);


class RepeatEntity(EntityBase):
    def __init__(self, entity=None, least=1, most=1, equal=False):
        super(RepeatEntity, self).__init__()
        self.least = least
        self.most = most
        self.entity = entity
        self.equal = equal;
        self.property=''
    __splitre = re.compile('[,{}]');

    def rebuild(self):
        self.entity = self._get_entity(self.entity);
        self._travel(self.entity);

    def set_values(self, values):
        super(RepeatEntity, self).set_values(values);
        if isinstance(values, dict):
            return
        cal = values[0];
        if cal == '*':
            self.least = 0
            self.most = -1
        elif cal == '+':
            self.least = 1
            self.most = -1
        elif cal == '?':
            self.least = 0
            self.most = 1
        elif cal.startswith('{'):
            sp = self.__splitre.split(cal);
            self.least = int(sp[1])
            self.most = int(sp[2])
        if self.most == -1:
            self.most = 99999;

    def match_item(self, input, start, end, must_start, mode=None):
        self.log_in(input, start)
        right = 0
        start = start
        lresult = None
        isStop = False;
        isReset = False;
        best_results = [];

        omax = -1;
        while right < self.most:
            result = self.entity.match_item(input, start, end, must_start, None)
            if not is_fail(result):
                if right == 0:
                    start = result.pos
                    best_results.append(result);
                else:
                    if self.equal:
                        if result.pos != start or lresult.match != result.match:
                            if not isinstance(self.entity, RepeatEntity):
                                isStop = True;
                            else:
                                if omax == -1:
                                    omax = self.entity.most;
                                    self.entity.most = self.entity.least;
                                else:
                                    self.entity.most += 1;
                                if self.entity.most >= omax:
                                    isStop = True;
                                right = 0;
                                start = 0;
                                isReset = True;
                        else:
                            best_results.append(result)

                    elif result.pos != start:
                        isStop = True;
                    else:
                        best_results.append(result)
                if isStop:
                    break
                if not isReset:
                    lresult = result;
                    start = result.get_end()
                    lresult.order = right;
                    right += 1
                isReset = False;

            else:
                break;
        if right < self.least:
            self.log_out(None, False)
            return start;
        if any(best_results) == 0:  # this is ? or * ,can be null
            p = MatchResult(None, '', 0);
            p.rewrite = '';
        else:
            start = best_results[0].pos;
            end = best_results[-1].get_end()
            match_str = input[start:end]
            p = MatchResult(self, match_str, start, best_results)
        p.name=self.property;
        self.log_out(match_str)
        return p;


class DiffEntity(EntityBase):
    def __init__(self, universe=None, complements=None):
        super(DiffEntity, self).__init__()
        self.universe = universe
        self.complements = complements if complements is not None else [];

    def rebuild(self):
        self.universe = self._get_entity(self.universe);
        self._travel(self.universe);
        for r in range(len(self.complements)):
            self.complements[r] = self._get_entity(self.complements[r]);
            self._travel(self.complements[r]);

    def match_item(self, input, start, end, must_start, mode=None):
        self.log_in(input, start)
        un_result = self.universe.match_item(input, start, end, must_start, None)
        if is_fail(un_result):
            self.log_out(None)
            return un_result;
        matchResult = None
        if len(self.complements) != 0:
            for en in self.complements:
                matchResult = en.match_item(un_result.match, 0, None, True, matchResult)
                if is_fail(matchResult):
                    self.log_out(None)
                    return un_result.pos;
        p = MatchResult(self, un_result.match, un_result.pos, [un_result])
        self.log_out(un_result.match)
        return p;


class RegexEntity(StringEntity):
    def __init__(self, match="", rewrite=None):
        super(RegexEntity, self).__init__(match, rewrite)
        self.regex = None
        self.merge = False;
        self.is_match_max = True;
        if self.match != "":
            self.rebuild();

    def rewrite_item(self, input):
        if self.rewrite is None:
            return input
        m = self.regex.search(input);
        return self.__replace(m, self.rewrite)

    def rebuild(self):
        if self.regex is None:
            try:
                self.regex = re.compile(self.match)
            except:
                print("regex Format error %s" % (self.match));

    def set_values(self, values):
        super(RegexEntity, self).set_values(values);
        if isinstance(values, dict):
            return;
        self.match = values[0]
        if len(values) > 1:
            if is_str(values[1]):
                self.rewrite = values[1]
            else:
                self.merge = True;
                self.maps = values[1];

        try:
            self.regex = re.compile(self.match)
        except:
            print("regex Format error %s" % (self.match));

    def __replace(self, m, string):
        if (m.lastindex != None):
            c = m.lastindex + 1;
        else:
            c = 1;
        for index in range(c):
            string = string.replace('$' + str(index), m.group(index)).replace('\\n', '\n');
        return string;

    def match_item(self, input, start, end, must_start, mode=None):
        self.log_in(input, start)
        if end is None:
            if must_start:
                m = self.regex.match(input, start);
            else:
                m = self.regex.search(input, start);
        else:
            if must_start:
                m = self.regex.match(input, start, end);
            else:
                m = self.regex.search(input, start, end)
        if m is None or (must_start == True and m.start() != start):
            self.log_out(None)
            return int_max if m is None else m.start();

        p = MatchResult(self, m.group(), m.start())
        if self.merge:

            p.rewrite = self.maps[p.match].rewrite_item(p.match);
        elif self.rewrite is None:
            p.rewrite = p.match;
        else:
            p.rewrite = self.__replace(m, self.rewrite);
        self.log_out(m.group())
        return p;


class ScriptEntity(EntityBase):
    def __init__(self, script=""):
        super(ScriptEntity, self).__init__()
        self.script = script

    def set_values(self, values):
        super(ScriptEntity, self).set_values(values);
        if isinstance(values, list):
            self.script = values[0]

    def rewrite_item(self, match):
        res = self.eval_script(match);
        self.rewrite=res
        return res;

    def match_item(self, input, start, end, must_start, mode=None):
        core = self._core;
        return eval(self.script);

    def match_item2(self, raw_input, r_target, is_rewrite=False):
        input = r_target.match;
        self.log_in(input, r_target.pos)
        if is_rewrite:
            r = input;
        else:
            r = self.eval_script(None, raw_input, input);
            if r is None:
                return None;
            pos = input.find(r);
            if pos < 0:
                return None;
        p = MatchResult(self, r, r_target.pos, [r_target])
        self.log_out(r)
        return p;


def is_fail(x):
    if isinstance(x, int):
        return True;
    return False;


def _bsearch_index(arr, v):
    l = len(arr);
    if l == 0:
        return 0;
    left = 0;
    right = l - 1;
    while left <= right:
        mid = (left + right) >> 1;
        if arr[mid] > v:
            right = mid - 1;
        elif arr[mid] < v:
            left = mid + 1;
        else:
            break;
    if v < arr[mid]:
        return mid;
    elif v == arr[mid]:
        if mid % 2 == 0:
            return mid + 1;
        else:
            return mid;
    return mid + 1;


def in_area(area, pos):
    i = _bsearch_index(area, pos);
    if i % 2 == 0:
        return pos;
    return area[i];



class TableEntity(EntityBase):
    def __init__(self, tables=None, groups=False,property=''):
        super(TableEntity, self).__init__()
        if tables is not None and not isinstance(tables, list):
            tables = [tables]
        self.tables = tables if tables is not None else [];
        self.property=property
        if groups:
            self.group = range(0, len(tables));
        elif isinstance(groups, list):
            self.group = groups
        else:
            self.group = [];
        self.match_max = True;


    def rebuild(self):
        if not isinstance(self.tables, list):
            raise Exception('TableEntity must be array')
        for i in range(len(self.tables)):
            self.tables[i] = self._get_entity(self.tables[i]);
            self._travel(self.tables[i]);
            if isinstance(self.tables[i], SequenceEntity):
                self.tables[i].rebuild();

        if not tn.auto_merge:
            return;

        seqs = [m for m in self.tables if isinstance(m, StringEntity) and not isinstance(m, RegexEntity)];
        if len(seqs) < 2:
            return;
        ms = {};
        rex = RegexEntity();
        rex.name = self.name + "_merge";
        rex._core = self._core;
        match = "";

        def repl_escape(s):
            for c in list('+-*().'):
                s = s.replace(str(c), '\\' + str(c));
            return s;

        for i in seqs:
            m = repl_escape(i.match);
            ms[i.match] = i;
            match += m + '|';
        match = match[:-1];

        rex.set_values([match, ms]);
        for i in seqs:
            self.tables.remove(i);
        self.tables.append(rex);
        for t in self.tables:
            t._core = self._core;
        return

    def set_values(self, values):
        super(TableEntity, self).set_values(values);
        if isinstance(values, list):
            return;
        value = values.get("Property", None);
        if value is not None:
            items = [x.strip() for x in value.split('|')]
            for i in range(min(len(items), len(self.tables))):
                self.tables[i].set_values({'Property':items[i]});

    def match_item(self, input, start, end, must_start, mode=None):
        self.log_in(input, start)
        best_len = -1
        best_seq_id = -1
        best_start = int_max;
        r_pos = best_start;
        sub_mode = None;
        buff = self._core.entities.buff
        if buff.input!=input:
            buff=BuffHelper(input)
        best_match = None
        total = len(self.tables)
        for seq_id in range(total):
            if seq_id in self.group and best_seq_id != -1 and best_start == start:
                break;
            entity = self.tables[seq_id]
            if mode is not None and mode.m_index != -1:
                if seq_id != mode.m_index:
                    continue
                sub_mode = mode.children;
            seq_value = buff.get_match(entity, input, start, end);
            if seq_value == -1:
                continue
            if seq_value is not None:
                t_header = seq_value;
            else:
                t_header = entity.match_item(input, start, end, must_start, sub_mode)
                if is_fail(t_header):
                    r_pos = min(t_header, r_pos);
                    if not must_start:
                        buff.add_area(entity, start, start);
                    if sub_mode is not None and mode.m_index == -1:
                        mode.m_index = -1;
                        sub_mode = None;
                else:
                    buff.add_area(entity, start, t_header.pos);
                    buff.add_entity(entity, t_header)
            if is_fail(t_header):
                continue
            spos, slen = t_header.pos, len(t_header.match);
            if (spos < best_start or (
                            spos == best_start and (slen > best_len if self.match_max == True else slen < best_len))):
                best_len = slen
                best_start = spos
                best_seq_id = seq_id
                best_match = t_header;

        if best_match is not None:
            match = MatchResult(self, best_match.match, best_match.pos, [best_match]);
            match.m_index = best_seq_id;
            match.name=self.property;
            self.log_out(match.match)
            return match;
        self.log_out(None)
        return r_pos;


def add_area(sb, start, end):
    l = len(sb);
    insp = 0;
    if l == 0:
        sb.append(start);
        sb.append(end);
    else:
        left = 0;
        right = l - 2;
        while left <= right:
            mid = ((left + right) >> 2) << 1;
            if sb[mid] < start:
                left = mid + 2
                if right < left:
                    if l <= left:
                        sb.append(start)
                        sb.append(end)
                        insp = l
                    else:
                        sb.insert(left, start)
                        sb.insert(left + 1, end)
                        insp = left;
                        break
            elif sb[mid] > start:
                right = mid - 2
                if right < left:
                    sb.insert(left, start)
                    sb.insert(left + 1, end)
                    insp = left;
                    break
            else:
                if sb[mid + 1] < end:
                    sb[mid + 1] = end
                break

    l = len(sb);
    if insp > 2:
        i = insp - 2;
    else:
        i = 0;
    while i < l:
        pi = i - 2
        if pi < 0 or sb[pi + 1] < sb[i] - 1:
            pi += 2;

        else:
            if sb[pi + 1] <= sb[i + 1]:
                sb[pi + 1] = sb[i + 1];
            del sb[pi + 2:pi + 4];
            l -= 2;
            i -= 2;
        i += 2;
    return sb;


class BuffHelper(object):
    def __init__(self, input_str=None,s_len=None):
        self.scan_buf = {};
        self.entity_buf = {};
        if input_str is not None:
            self.input= input_str
            self.s_len = len(input_str)
        else:
            self.s_len =s_len
        self.known_area = [];

    def clear(self):
        self.scan_buf = {};
        self.entity_buf = {};
        self.known_area = [];

    def add_entity(self, entity, matchResult):
        entityid = id(entity);
        sb = self.entity_buf.get(entityid, None);
        if sb is None:
            sb = [];
            self.entity_buf[entityid] = sb;
        lo = 0
        hi = len(sb)
        while lo < hi:
            mid = (lo + hi) // 2
            if matchResult.pos < sb[mid].pos:
                hi = mid
            else:
                lo = mid + 1
        sb.insert(lo, matchResult)

    def add_area(self, entity, start, end=None):

        if entity != 0:
            entityid = id(entity);
            sb = self.scan_buf.get(entityid, None);
        else:
            sb = self.known_area;
        if sb is None:
            sb = [];
            self.scan_buf[entityid] = sb;
        if end is None:
            end = self.s_len;
        add_area(sb, start, end);

    def get_match(self, entity, input, start, end):
        if not entity._can_cache:
            return None;
        entity_id = id(entity);
        sb = self.scan_buf.get(entity_id, None);
        if sb is None:
            return None;
        i = _bsearch_index(sb, start);
        if i % 2 == 0:
            return None;
        start = start;
        end = sb[i]
        eb = self.entity_buf.get(entity_id, None);
        if eb is None:
            entity.log_in(input, start, end)
            entity.log_out(None, True);
            return end;
        hi = len(eb)
        lo = 0;
        while lo < hi:
            mid = (lo + hi) // 2
            if start < eb[mid].pos:
                hi = mid
            elif start == eb[mid].pos:
                lo = mid;
                break;
            else:
                lo = mid + 1
        if lo >= len(eb):
            entity.log_in(input, start, end)
            entity.log_out(None);
            return None;
        if eb[lo].pos <= end:
            entity.log_in(input, start, end)
            entity.log_out(eb[lo].match, True)
            return eb[lo];
        return None;


class TreeNode(object):
    def __init__(self):
        self.left = None;
        self.right = None;
        self.root = None;
        self.match = None;
        self.rewrite = None;
        self.index = 0;
        self.order = 0

    def get_left(self):
        tree = self;
        while tree.left is not None:
            tree = tree.left;
        return tree;

    def get_right(self):
        tree = self;
        while tree.right is not None:
            tree = tree.right;
        return tree;

    def in_order_travel(self, node, func):
        if node is None:
            return;
        self.in_order_travel(node.left, func);
        func(node);
        self.in_order_travel(node.right, func)


def _is_same(arr, l, r):
    if r < l + 2:
        return False;
    for i in range(l + 1, r):
        if arr[i] != arr[l]:
            return False;
    return True;


def _get_max_index(arr, l, r):
    max_value = -100;
    max_index = -1;
    for i in range(l, r):
        if arr[i] > max_value:
            max_index = i;
            max_value = arr[i];
    return max_index;


class SequenceEntity(EntityBase):
    def __init__(self, m_entities=None, r_entities=None,properties=None,  m_orders=None, r_orders=None, accept_error=False, script=None):
        super(SequenceEntity, self).__init__()
        self.direct_replace = "direct_replace"
        if isinstance(m_entities, EntityBase) and isinstance(r_entities, EntityBase):
            self.m_entities = [m_entities]
            self.r_entities = [r_entities]
        else:

            self.m_entities = m_entities if m_entities is not None else [];
            self.r_entities = r_entities if r_entities is not None else [];
        self.r_orders = r_orders if r_orders is not None else []
        if m_orders is not None:
            if accept_error:
                print('can not set accept_error be true when orders is enabled')
                self.accept_error = False
            self.m_orders = m_orders;
        else:
            self.m_orders = None;
        self.properties=properties if properties is not None else [];
        self.script = script;
        self._root = None;
        self.accept_error = accept_error;

    def set_values(self, values):
        super(SequenceEntity, self).set_values(values);
        if isinstance(values, list):
            return;
        value = values.get("Property", None);
        if value is not None:
            self.properties = [x.strip() for x in value.split(',')]

    def _build_tree(self, l, r):
        if l > r or l >= len(self.m_orders):
            return None;
        if r == l:
            tree = TreeNode();
            tree.match = self.m_entities[l];
            return tree;
        if _is_same(self.m_orders, l, r):
            tb = TableEntity();
            tb._core = self._core;
            for item in itertools.combinations(self.m_entities[l:r], r - l + 1):
                se = SequenceEntity(item);
                se._core = tb.core;
                tb.tables.append(se);
            tree = TreeNode();
            tree.match = tb;
            return tree;
        max_index = _get_max_index(self.m_orders, l, r)
        tree = TreeNode();
        tree.order = self.m_orders[max_index];
        tree.index = max_index;
        tree.match = self.m_entities[max_index];
        if max_index < len(self.r_entities):
            tree.rewrite = self.r_entities[self.r_orders[max_index]]
        tree.left = self._build_tree(l, max_index - 1);
        if tree.left is not None:
            tree.left.Root = tree;
        tree.right = self._build_tree(max_index + 1, r);
        if tree.right is not None:
            tree.right.Root = tree;
        return tree;

    def rebuild(self):
        if len(self.r_orders) == 0:
            self.r_orders = [i for i in range(0, len(self.m_entities))];
        for i in range(len(self.m_entities)):
            self.m_entities[i] = self._get_entity(self.m_entities[i])
            self._travel(self.m_entities[i]);
        for i in range(len(self.r_entities)):
            self.r_entities[i] = self._get_entity(self.r_entities[i])
            self._travel(self.r_entities[i]);
        if self.m_orders is None:
            self.m_orders = [i for i in range(len(self.m_entities), 0, -1)];
        self._tree = self._build_tree(0, len(self.m_orders));

    def _tree_match(self, tree_node, input, start, end, final_script, must_start=False):
        buff = self._core.entities.buff
        if input!=buff.input:
            buff=BuffHelper(input);
        match_entity = tree_node.match;
        m_result = buff.get_match(match_entity, input, start, end);
        fail = False;
        if m_result is None:
            m_result = match_entity.match_item(input, start, end, tree_node.left is None and must_start)
            if not is_fail(m_result):
                buff.add_area(match_entity, start, m_result.pos);
                buff.add_entity(match_entity, m_result);
        if not is_fail(m_result):
            if tree_node.right is None and end is not None and m_result.get_end() != end:
                fail = True;
            r_entity = None;
            if not final_script:
                r_entity = tree_node.rewrite;
            if r_entity is not None and r_entity.name != self.direct_replace:
                if isinstance(r_entity, ScriptEntity):
                    m_result = r_entity.match_item2(input, m_result, True)
                    if m_result is None:
                        fail = True;
                else:
                    m_result.rewrite = r_entity.rewrite_item(m_result.match)

        if not fail and not is_fail(m_result):
            m_left = m_result.pos;
            m_len = len(m_result.match);
            m_right = m_left + m_len;
            runtime_tree = TreeNode();
            runtime_tree.match = m_result;
            if tree_node.left is not None:
                left = self._tree_match(tree_node.left, input, start, m_left, final_script);
                if is_fail(left):
                    fail = True;
                elif must_start == True and left.get_left().match.pos != start:
                    fail = True;
                else:
                    rm = left.get_right().match;
                    if rm.get_end() != m_left:
                        fail = True;
                    else:
                        left.root = runtime_tree;
                runtime_tree.left = left;
            if not fail and tree_node.right is not None:
                right = self._tree_match(tree_node.right, input, m_right, end,
                                         final_script, tree_node.right.left is None);

                if is_fail(right):
                    fail = True;
                else:
                    if m_len == 0:
                        m_result.pos = right.get_left().match.pos;
                    elif right.get_left().match.pos != m_right:
                        fail = True;
                    elif end is not None:
                        r_right = right.get_right().match;
                        r_pos = r_right.get_end()
                        if r_pos != end:
                            fail = True;
                        else:
                            right.root = runtime_tree;
                runtime_tree.right = right;
        if is_fail(m_result):
            return m_result;
        elif fail:
            return m_result.get_end()
        return runtime_tree;

    def match_item(self, input, start, end, must_start, mode=None):
        self.log_in(input, start)
        final_script = False;
        if len(self.m_entities) > 1 and len(self.r_entities) == 1 and isinstance(self.r_entities[0],
                                                                                 ScriptEntity):
            final_script = True;
        tree_result = self._tree_match(self._tree, input, start, end, final_script, must_start);
        if is_fail(tree_result):
            self.log_out(None)
            return tree_result;
        m_results = [];
        tree_result.in_order_travel(tree_result, lambda m: m_results.append(m.match));
        for i in range(len(m_results)):
            m=m_results[i];
            if i < len(self.properties):
                m.name = self.properties[i]
            if i < len(self.r_orders):
                m.order = self.r_orders[i]
            else:
                m.order = i
        if self.script is not None and self.eval_script(m_results, input) == False:
            self.log_out(None)
            return start;
        start = m_results[0].pos;
        sum = 0;
        for i in range(0, len(m_results)):
            sum += len(m_results[i].match);
        match_str = input[start:start + sum];
        if final_script:
            script = self.r_entities[0];
            p = MatchResult(script, match_str, start, m_results);
            return p;
        p = MatchResult(self, match_str, start, m_results)
        self.log_out(match_str, False)
        return p;


class NoSequenceEntity(EntityBase):
    def __init__(self, m_entities=None, r_entities=None, r_orders=None, min_match=None, max_match=None, check_script=None,
                 move_right=False, accept_error=False, accept_blank=True,accept_back=False,accept_overlap=False,
                 properties=None):
        super(NoSequenceEntity, self).__init__()
        self.direct_replace = "direct_replace"
        self.m_entities = m_entities if m_entities is not None else [];
        self.r_entities = r_entities if r_entities is not None else [];
        self.min_match = min_match if min_match is not None else [1 for i in range(len(m_entities))];
        self.max_match = max_match if max_match is not None else [9999 for i in range(len(m_entities))];
        self.properties = properties if properties is not None else [];
        self.r_orders = r_orders if r_orders is not None else [];
        self.script = check_script;
        self.move_right = move_right;
        self.accept_error = accept_error;  #internal can be error and skip it
        self.accept_back= accept_back      #find error and back to start,only when move_right is true
        self.accept_blank = accept_blank;  #only available when move_right is true
        self.accept_overlap=accept_overlap  #when match, other part can not be match


    def rebuild(self):
        for i in range(len(self.m_entities)):
            self.m_entities[i] = self._get_entity(self.m_entities[i]);
            self._travel(self.m_entities[i]);
        for i in range(len(self.r_entities)):
            self.r_entities[i] = self._get_entity(self.r_entities[i]);
            self._travel(self.r_entities[i]);

    def match_item(self, input, start, end, must_start, mode=None):
        self.log_in(input, start)
        end = len(input) if end is None else end;
        final_script = False;
        dict_buf = self._core.entities.buff
        overlap_buf= BuffHelper(input[:start])
        if len(self.m_entities) > 1 and len(self.r_entities) == 1 and isinstance(self.r_entities[0],
                                                                                 ScriptEntity):
            final_script = True;
        start0 = start;
        stop = False;
        sort_buf={}
        for mi in range(0, len(self.m_entities)):
            if stop:
                break
            fail = False;
            match_count = 0;
            if fail:
                break;
            m_entity = self.m_entities[mi];
            if not self.move_right:
                start0 = start;
            end0 = end;
            has_back=False
            if self.accept_back and start0>=end:
                start0=start
            while start0 < end:
                m_result = dict_buf.get_match(m_entity, input, start0, None);
                _must_start =must_start or ( mi>0 and self.accept_blank == False);
                if m_result is None:
                    m_result = m_entity.match_item(input, start0, None, _must_start)
                if is_fail(m_result):
                    if self.accept_error == False and match_count < self.min_match[mi]:
                        fail = True;
                        break
                    if self.accept_error:
                        if len(sort_buf) > 0:
                            if match_count < self.min_match[mi]:
                                fail = True
                        break;
                    else:
                        if self.accept_back and has_back==False:
                            start0=start
                            sort_buf.clear()
                            overlap_buf.clear()
                            has_back=True
                        else:
                            start0 = m_result if m_result != start0 else m_result + tn.interval;
                    continue;
                start0 = m_result.get_end()
                if mi in sort_buf:
                    sort_buf[mi].append(m_result);
                else:
                    sort_buf[mi]=[m_result]
                    overlap_buf.add_area(0,m_result.pos,m_result.get_end())
                dict_buf.add_area(m_entity, start0, m_result.pos)
                dict_buf.add_entity(m_entity, m_result)
                if m_result.pos >= end0:
                    continue;
                r_entity = None;
                if not final_script and mi < len(self.r_entities):
                    r_entity = self.r_entities[mi];
                if r_entity is not None and r_entity.name != self.direct_replace:
                    if isinstance(r_entity, ScriptEntity):
                        m_result = r_entity.match_item2(input, m_result, True)
                        if m_result is None:
                            start0 += 1;
                            continue;
                        else:
                            m_result.rewrite = r_entity.rewrite_item(m_result.match)
                match_count += 1;
                if match_count >= self.max_match[mi]:
                    break;
            if match_count < self.min_match[mi]:
                fail = True;
                break;
        if fail == True:
            self.log_out(None)
            return end if end is not None else len(input);
        prop_len = len(self.properties)
        order_len = len(self.r_orders);
        m_results = []
        for mi, buf in sort_buf.items():
            for m in buf:
                if mi < prop_len:
                    m.name = self.properties[mi]
                if mi < order_len:
                    m.order = self.r_orders[mi];
                else:
                    m.order = mi;
                m_results.append(m)
        m_results = sorted(m_results, key=lambda m: m.pos);
        if self.script is not None and self.eval_script(m_results, input) == False:
            self.log_out(None)
            return start;

        start = m_results[0].pos;
        end = m_results[-1].get_end()
        match = input[start:end]
        if final_script:
            script = self.r_entities[0];
            p = MatchResult(script, match, start, m_results);
            return p;
        p = MatchResult(self, match, start, m_results)
        p.pos = start;
        self.log_out(match, False)
        return p;


class EntityManager(object):
    def __init__(self):
        super(EntityManager, self).__init__()
        self.all_entities = []
        self.valid_entities = []
        self.names = {}
        self.ids = {}

    def append_id(self, entity):
        if -1 != entity.order:
            self.ids[entity.order] = entity
        self.valid_entities.append(entity)

    def append(self, entity):
        if entity.name is not None:
            self.names[entity.name] = entity
        self.all_entities.append(entity)

    def __getitem__(self, item):
        if item in self.names:
            return self.names[item]
        else:
            print("entity name %s can not be found!" % (item));
        for entity in self.all_entities:
            if (entity.name == item):
                return entity
        return None


class tn(object):
    auto_study = True
    log_file = None
    match_level = 0
    auto_merge = True
    match_all = False;
    interval=4
    def __init__(self, rule=None):
        super(tn, self).__init__()
        self.entities = None
        self.__entity_name = re.compile(r"^(\w+)\s*=\s*")
        self.__entity_reexp = re.compile(r"^\(/((?:.(?!/\)))*?)/\s*:\s*/((?:(?!\(/).)*?)/\)\s*")
        self.__entity_string = re.compile("^\(\"((?:(?!\"\)).)*?)\"\s*:\s*\"(.*?)\"(?:\s*:\s*\"(.*?)\")?\s*\)\s*")
        self.__numbeRegex = re.compile(r"[0-9]+")
        self.__r_bar = re.compile(r"^\s*\|\s*")
        self.__r_bar2 = re.compile(r"^\s*/\s*")
        self.__r_colon = re.compile(r"^\s*:\s*")
        self.__r_conds = re.compile(r'^\s*\"([^"]*)\"')
        self.__r_entity = re.compile("^\$\((\w+)\)\s*")
        self.__r_minus = re.compile(r"^\s*-\s*")
        self.__r_order = re.compile(r"^\$0*(\d+)\s*")
        self.__r_reexp = re.compile(r"^\(/((?:.(?!/\s*:\s*/))*?)/\)\s*")
        self.__r_repeat = re.compile(r"^\s*([*?+]|{(\d+),(-1|\d+)})\s*")
        self.__r_semicolon = re.compile(r"^\s*;")
        self.__r_string = re.compile(r"^\(\s?\"(.*?)\"\s?\)\s*")
        self.tn_file_name = None
        self.entities = EntityManager()
        self.entities.all_entities = []
        self.entities.valid_entities = []
        self.entities.buff = BuffHelper(s_len=200);
        if rule is not None:
            self.init_tn_rule(rule);

    def rebuild(self):
        for entity in self.entities.all_entities:
            entity.rebuild();
        self.entities.valid_entities = sorted(self.entities.valid_entities, key=lambda x: x.order)

    def init_py_rule(self, pyrule):
        for r in pyrule.__dict__:
            s = getattr(pyrule, r);
            if isinstance(s, EntityBase):
                s._core = self;
                s.name = r;
                self.entities.append(s);
                if s.order != 0:
                    self.entities.append_id(s);

    def to_html(self, new_file):
        def header(file):
            file.write(
                '''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.1//EN" "http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd"><html xmlns="http://www.w3.org/1999/xhtml"><head><meta http-equiv="Content-type" script="text/html; charset=utf-8" /><title>{title}}</title></head><body>''');

        def end(file):
            file.write('''</body></html>''');

        file_object = open(self.tn_file_name, 'r', 'utf-8')
        new_file = open(new_file, 'w', 'utf-8')
        text = file_object.read()
        texts = text.split('\n')
        r_entity = re.compile(r"\$\((\w+)\)\s*")
        header(new_file)
        for t in texts:
            m = self.__entity_name.match(t)
            if m != None:
                mt = m.group(1)
                t = t.replace(mt, '<a name="%s"><b>%s</b></a>' % (mt, mt), 1)
            m = r_entity.findall(t)
            if len(m) > 0:
                for mt in m:
                    t = t.replace(mt, '<a href="#%s">%s</a>' % (mt, mt))
            if t.startswith("#"):
                new_file.write('<p><font color="#909090">%s</font></p>\n' % t)
            else:
                new_file.write('<p>%s</p>\n' % t)
        end(new_file)
        new_file.close()
        file_object.close()

    def init_text_rule(self, text, add_order=True):
        class RegexToken:
            def __init__(self, regex, token, count, type=None):
                self.regex = regex
                self.token_type = token
                self.count = count
                self.entity_type = type

        class TokenItem:
            def __init__(self, token):
                self.rule = None
                self.token = token
                self.entity = None
                self.values = []

        class Token:
            (NAME, ENTITY, MINUS, COLON, END, BAR, REPEAT, Script) = range(8)

        property_regex = re.compile("#%(\w+)%\s(.+)")

        def __get_no_table_entity(tokens, only_one):
            repeat = get_index(tokens, lambda r: r.token == Token.REPEAT)
            if repeat < 0:
                pass
            elif repeat != 1:
                raise Exception("repeat format error");
            else:
                entity = RepeatEntity()
                entity._core = self
                entity.entity = tokens[0].entity
                entity.set_values(tokens[1].values)
                return entity
            minus = get_index(tokens, lambda r: r.token == Token.MINUS)
            if minus < 0:
                pass
            elif minus != 1:
                raise Exception("diff format error");
            else:
                entity = DiffEntity()
                entity._core = self
                entity.universe = tokens[0].entity
                id = 1
                while id < len(tokens):
                    tokenItem = tokens[id]
                    if tokenItem.token == Token.END:
                        return entity
                    if tokenItem.token == Token.MINUS:
                        entity.complements.append(tokens[id + 1].entity)
                    id += 1
                return entity
            if len(tokens) == 1:
                if not only_one:
                    return tokens[0].entity
                if isinstance(tokens[0].entity, EntityBase) and tokens[0].entity.name == "":
                    return tokens[0].entity
            entity = SequenceEntity()
            entity._core = self;
            state = 0
            for id in range(len(tokens)):
                tokenItem = tokens[id]
                if tokenItem.token == Token.END:
                    return entity
                if tokenItem.token == Token.COLON:
                    state += 1
                    continue
                if state == 0:
                    entity.m_entities.append(tokenItem.entity)
                elif state == 1:
                    if tokenItem.entity is None:
                        entity.r_orders.append(int(tokenItem.rule.replace("$", "")) - 1)
                    else:
                        entity.r_entities.append(tokenItem.entity)
                        entity.r_orders.append(len(entity.r_orders))
                else:
                    entity.condition = tokenItem.entity
            return entity

        token_regex = [RegexToken(self.__entity_name, Token.NAME, 2),
                       RegexToken(self.__entity_reexp, Token.ENTITY, 3, RegexEntity),
                       RegexToken(self.__entity_string, Token.ENTITY, 3, StringEntity),
                       RegexToken(self.__r_reexp, Token.ENTITY, 1, RegexEntity),
                       RegexToken(self.__r_order, Token.ENTITY, 1),
                       RegexToken(self.__r_entity, Token.ENTITY, 2),
                       RegexToken(self.__r_string, Token.ENTITY, 2, StringEntity),
                       RegexToken(self.__r_minus, Token.MINUS, 2),
                       RegexToken(self.__r_colon, Token.COLON, 1),
                       RegexToken(self.__r_repeat, Token.REPEAT, 2),
                       RegexToken(self.__r_bar, Token.BAR, 1),
                       RegexToken(self.__r_bar2, Token.BAR, 1),
                       RegexToken(self.__r_semicolon, Token.END, 1),
                       RegexToken(self.__r_conds, Token.ENTITY, 1, ScriptEntity),
                       ]
        sb = ""
        real_rules = []
        rules = [x.strip() for x in text.split('\n')]  # PreProcessing
        for rule in rules:
            if property_regex.match(rule):
                real_rules.append(rule.strip())
                continue
            if rule.startswith("#"):
                sb = ""
                continue
            if rule.endswith(';'):
                sb += rule
                real_rules.append(sb.strip())
                sb = ""
                continue
            else:
                sb += rule
        properties = {};
        for rule in real_rules:
            m = property_regex.match(rule);
            if m is not None:
                if m.lastindex is not None and m.lastindex == 2:
                    name, value = m.group(1), m.group(2);
                    if name == "script":
                        item = __import__(value)
                        setattr(self, value, item)
                    elif name == "Include":
                        value = value.split(' ');
                        isadd = False;
                        if len(value) > 1:
                            isadd = value[1] == "True";
                        self.init_tn_rule(value[0], isadd);
                    else:
                        properties[m.group(1)] = m.group(2);
                continue

            token_items = []  # Lexical Analyse
            while True:
                if len(rule) == 0:  break
                can_match = False;
                for token in token_regex:
                    mat = token.regex.match(rule)
                    if mat is None: continue

                    mcount = mat.lastindex if mat.lastindex is not None else 1;
                    if mcount < token.count - 1:
                        continue
                    can_match = True;
                    tokenItem = TokenItem(token.token_type)
                    for r in range(mcount):
                        tokenItem.values.append(mat.string if mat.lastindex is None else mat.group(r + 1))
                    tokenItem.rule = mat.group(0)

                    e = None
                    if token.entity_type is not None:
                        e = token.entity_type()
                        e._core = self
                        e.set_values(tokenItem.values)
                    elif token.regex == self.__r_entity:
                        e = tokenItem.values[0]
                    if e is not None:
                        tokenItem.entity = e
                    rule = rule[len(tokenItem.rule):]
                    token_items.append(tokenItem)
                    break
                if not can_match:
                    print("rule format error%s" % (rule));
                    return;

            if Token.NAME != token_items[0].token:  # Grammer Analyse
                print("name must be the first")
            if token_items[-1].token != Token.END:
                print("rule must be ended by ;")

            if find_any(token_items, lambda r: r.token == Token.BAR):
                entity = TableEntity()
                entity._core = self;

                lastid = 0
                for id in range(1, len(token_items)):
                    if token_items[id].token == Token.BAR or token_items[id].token == Token.END:
                        tentity = __get_no_table_entity(token_items[lastid + 1:id], only_one=False);
                        if isinstance(tentity, EntityBase) and tentity.name == "":
                            tentity.name = "%s_%d" % (token_items[0].values[0], len(entity.tables));
                        entity.tables.append(tentity);
                        lastid = id
                        if token_items[id].rule.find("/") == 0:
                            entity.group.append(len(entity.tables))
            else:
                entity = __get_no_table_entity(token_items[1:-1], only_one=True)
            entity.name = token_items[0].values[0]
            entity.set_values(properties);
            if entity.order != 0 and add_order:
                self.entities.append_id(entity);
            properties = {};
            self.entities.append(entity)

    def init_tn_rule(self, file_name, add_order=True):
        self.tn_file_name = file_name
        if PY2:
            import codecs
            file_object = codecs.open(file_name, 'r', 'utf-8');
        else:
            file_object = open(file_name, 'r', encoding='utf-8')
        texts = file_object.read()
        print("success load tn rules:%s" % (file_name))
        self.init_text_rule(texts, add_order)
        file_object.close()

    def match_entity(self, entity, input, mode=None):
        start_pos = 0
        m_results = [];
        input_len = len(input)
        while (1):
            if start_pos >= input_len:
                break
            m_result = entity.match_item(input, start_pos, None, entity.start, mode)
            if is_fail(m_result):
                if mode is not None and start_pos == 0 and tn.auto_study:
                    m_result = entity.match_item(input, start_pos, entity.start)
                    if m_result is not None:
                        self.__get_public_tree(mode, m_result)
                    else:
                        start_pos = m_result;
                break

            start_pos = m_result.get_end()
            m_results.append(m_result);
        return m_results;

    def rewrite_entity(self, entity, input, mode=None):
        m_results = self.match_entity(entity, input, mode);
        if len(m_results) == 0:
            return input, False;
        else:
            pos = 0;
            rewrite = "";
            for m in m_results:
                m.get_should_rewrite();
                m.rewrite_item()
                rewrite += input[pos:m.pos] + to_str(m.rewrite);
                pos = m.get_end()
            rewrite += input[pos:];
            return rewrite, True;

    def __get_public_tree(self, item1, item2):
        if item1 is None:
            return item2
        stack1 = []
        stack2 = []
        stack1.append(item1)
        stack2.append(item2)
        while len(stack1) > 0:
            m1 = stack1.pop()
            m2 = stack2.pop()
            if m1.m_index != m2.m_index:
                m1.m_index = -1
                continue
            if isinstance(m1.children, EntityBase):
                continue
            m1 = m1.children
            m2 = m2.children
            while m1 != None:
                stack1.append(m1)
                stack2.append(m2)
                m1 = m1.next_match
                m2 = m2.next_match
        return item1

    def compile_str(self, input, modes):
        start_pos = 0
        while (1):
            if start_pos >= len(input):
                break
            mode_index = -1;
            m_result = None;
            is_success = False;
            if modes is not None:
                for index in range(0, len(modes)):
                    mode = modes[index];
                    m_result = mode.entity.match_item(input, start_pos, entity.start, mode);
                    if m_result is not None:
                        mode_index = index;
                        is_success = True;
                        break;
            if not is_success:
                for entity in self.entities.valid_entities:
                    m_result = entity.match_item(input, start_pos, None, entity.start)
                    if m_result is not None:
                        if modes is None:
                            modes = [];
                        modes.append(m_result);
                        break;
            if m_result is None:
                return modes;
            if modes is not None and mode_index != -1:
                modes[mode_index] = self.__get_public_tree(m_result, modes[mode_index]);
            start_pos += len(m_result.match);
        return modes

    def compile_strs(self, texts):
        modes = None;
        for text in texts:
            modes = self.compile_str(text, modes);
        return modes;

    def rewrite(self, raw_input, mode=None, entities=None):
        raw_input = str(raw_input);
        if entities is None:
            entities = self.entities.valid_entities;
        if mode is not None:
            self.entities.buff = BuffHelper(raw_input);
            return self.rewrite_entity(mode.entity, raw_input, mode)
        else:
            self.entities.buff = BuffHelper(raw_input);
            for entity in entities:
                if is_str(entity):
                    entity = self.entities[entity];
                rewrite, succ = self.rewrite_entity(entity, raw_input, None)
                if tn.match_all == False and succ == True:
                    return rewrite;
                if rewrite != raw_input:
                    raw_input = rewrite;
                    self.entities.buff = BuffHelper(raw_input);
            return rewrite

    def _get_entities(self, entities):
        if entities is None:
            for e in self.entities.valid_entities:
                yield e;
            return;
        if isinstance(entities, (list, tuple)):
            for e in entities:
                if is_str(e):
                    e = self.entities[e]
                    if e is not None:
                        yield e;
                else:
                    yield e;
        elif is_str(entities):
            e = self.entities[entities]
            if e is not None:
                yield e;
        else:
            yield entities;

    def match(self, raw_input, entity=None, mode=None):
        if mode is not None:
            self.entities.buff = BuffHelper(raw_input);
            return self.match_entity(mode.entity, raw_input, mode)
        else:
            self.entities.buff = BuffHelper(raw_input);
            for entity in self._get_entities(entity):
                match = self.match_entity(entity, raw_input, None)
                if not tn.match_all:
                    return match;
            return None;

    def __match_to_doc__(self, m_result):
        docu = {};
        m_result.rewrite_item();
        m_result.extract(docu, 0);
        docu['#type'] = m_result.entity.name;
        docu['#pos'] = m_result.pos;
        docu['#match'] = m_result.match;
        docu['#rewrite'] = m_result.rewrite;
        return docu

    def extract_entity(self, entity, input, mode=None):
        start = 0
        docs = [];
        buff = self.entities.buff;
        inputlen = len(input)
        while (1):
            if start >= inputlen:
                break;
            start = in_area(buff.known_area, start);
            m_result = buff.get_match(entity, input, start, None)
            if m_result is None:
                m_result = entity.match_item(input, start, None, entity.start, mode)
            if is_fail(m_result):
                if m_result == start:
                    start = m_result + 1;
                else:
                    start = m_result;
                continue;

            p = in_area(buff.known_area, m_result.pos);
            buff.add_entity(entity, m_result)
            start = m_result.get_end()
            if len(m_result.match) == 0:
                start += 1;
            if p == m_result.pos:
                docu = self.__match_to_doc__(m_result);
                docs.append(docu);
                buff.add_area(0, m_result.pos, start);
        return docs;

    def extract(self, input, modes=None, entities=None):
        if entities is None:
            entities = self.entities.valid_entities;
        self.entities.buff = BuffHelper(input);
        docs = [];
        succ = False;
        if modes is not None:
            for mode in modes:
                entity = mode.entity;
                mdocs = self.extract_entity(entity, input, mode)
                for doc in mdocs:
                    docs.append(doc);
                succ = True;
                break;
        if not succ:
            for entity in self._get_entities(entities):
                mdocs = self.extract_entity(entity, input)
                for doc in mdocs:
                    docs.append(doc);
        return docs;


