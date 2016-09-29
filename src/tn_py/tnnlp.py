# encoding: UTF-8

from tnpy import StringEntity as SE,ScriptEntity as SCE, RegexEntity as RE, NoSequenceEntity as NSE, TableEntity as TE, SequenceEntity as SQE, RepeatEntity as RPE, \
    EntityBase,int_max,MatchResult
import sys
import re


PY2 = sys.version_info[0] == 2



###########datetime########
x_year= TE([SQE(['date_YYYY','date_nian']),\
            SQE(['year_control'])\
            ]);

x_month = TE([SQE(['int_01_12', 'date_yue3']),\
             SQE(['month_control']) \
             ]);

x_day = TE([SQE(['int_01_99', 'date_ri']), \
              SQE(['day_control']) \
              ]);


x_hour0 = TE([SQE(['int_0_23', 'time_cnv_dian0']),
            ]);

def hour_translate(e,ms):
    control= ms[0].match;
    hour= get_int( ms[1].rewrite);
    if hour<12  and control==u'下午':
        return 12+hour;
    return hour
x_hour = TE([x_hour0,SQE(['hour_control',x_hour0],[SCE(hour_translate)])])

x_minute = TE([SQE(['int_00_10' ,'time_fen3']),\
             SQE(['int_10_59','time_fen3']),
               SQE(['time_special']),
              ])

x_second = SQE(['int_0_60', 'time_miao']);

def date_check(e,ms):
    l=len(ms)
    if l==0:
        return False;
    if l==1:
        if ms[0].entity.name not in ['x_year','x_month','x_day']:
            return False;
    return True


x_time_properties=['year', 'month', 'day', 'hour', 'minute', 'second'];

x_datetime= NSE([x_year,x_month,x_day,x_hour,x_minute,x_second], move_right=True, accept_error=True, accept_blank=False, check_script= date_check, properties= x_time_properties, min_match=[0, 0, 0, 0, 0, 0], max_match=[1, 1, 1, 1, 1, 1])
_num_re = re.compile('\d+')


def str_to_str_entity(str, revert=False):
    seqs=[];
    for l in re.split('\n|;', str):
        if l=='':
            continue
        wo=l.split(':');
        sl= len(wo)
        wo0= wo[0].split(' ');
        for w in wo0:
            w=w.strip()
            if sl==1:
                seqs.append(SE(w));
            elif sl==2:
                if not revert:
                    seqs.append(SE(w,wo[1]))
                else:
                    seqs.append(SE(wo[1],w))
    return seqs


def p_dict(dic,columns):
    res={}
    for c in columns:
        if c in dic:
            res[c]=dic[c]
    return res

def conv_dict(dic,para_dic):
    import copy
    dic=copy.copy(dic)
    for k,v in para_dic.items():
        if k==v:
            continue
        if k in dic:
            dic[v]=dic[k]
            del dic[k]
    return dic

def para_to_dict(para, split1, split2):
    r = {};
    for s in para.split(split1):
        s=s.strip();
        rs = s.split(split2);
        if len(rs) < 2:
            continue;
        key = rs[0].strip();
        value = s[len(key) + 1:].strip();
        r[rs[0]] = value;
    return r;


def get_int(m):
    if m is None or m == '':
        return 0
    if isinstance(m,int):
        return m;
    else:
        num = _num_re.search(m)
        if num is None:
            return None
        return int(num.group())


def date_translate(m, curr_time=None):
    import time
    import datetime
    doc={};
    t=m.name;
    m.name=''
    m.extract(doc);

    m.name=t;
    max_index=6;
    ms=m[0].children;
    if ms is None:
        ms= m
    for e in ms:
        name=e[0].entity.name;
        if name.find('control')>=0:
            name= name.split('_')[0];
            max_index= min(max_index, x_time_properties.index(name))
            doc['relative_'+name]=doc[name]
            del doc[name]
    if curr_time is not None:
        ctime=curr_time;
    else:
        ctime= time.localtime()
    time_now= [ctime[i] for i in range(0,6)]
    time_part= [ctime[0],1,1,0,0,0]
    year_dic={u'前年':-2,u'去年':-1,u'今年':0,u'明年':1,u'后年':2};
    month_dic={u'上月':-1,u'本月':0,u'下月':1};
    day_dic={u'大前天':-3,u'前天':-2,u'昨天':-1,u'今天':0,u'明天':1,u'后天':2,u'大后天':3};
    dic_array= [year_dic,month_dic,day_dic];
    pos=0;
    index=0;
    for time in x_time_properties:
        _time_key= 'relative_'+time;
        if time in doc:
            time_now[index]=get_int(doc[time]);
            pos=index;
        elif _time_key in doc:
            time_now[index]+=dic_array[index][doc[_time_key]]
            pos=index;
        else:
            time_now[index]=time_part[index] if index>max_index else time_now[index]
        index+=1;
    time_dic= {x_time_properties[i]:time_now[i] for i in range(6) }
    try:
        ttime= datetime.datetime(**time_dic);
    except:
        ttime ='';
    return ttime,pos;





if not PY2:
    class WordEntity(EntityBase):
        def __init__(self, word_list):

            super(WordEntity, self).__init__()
            from noaho import NoAho;
            self.trie = NoAho()
            for r in sorted(word_list):
                self.trie.add(r);
            self._last=None;
            self._last_result=None;

        def rewrite_item(self, input):
            return input

        def match_item(self, input, start, end, must_start, mode=None):
            if  self._last !=input:
                self._last=input;
                self._last_result= [r for r in self.trie.findall_long(input) if start <= r[0]];
            res=self._last_result;
            if len(res)==0:
                return int_max;
            if must_start and res[0][0]!=start:
                return int_max;
            for r in res:
                if r[0]>=start:
                    sword=input[r[0]:r[1]];
                    return  MatchResult(self, sword, r[0]);
            return int_max;

else:


    KIND = 16

    # BASE = ord('a')

    class Node():
        static = 0

        def __init__(self):
            self.fail = None
            self.next = [None] * KIND
            self.end = False
            self.word = None
            Node.static += 1


    class AcAutomation():
        def __init__(self):
            self.root = Node()
            self.queue = []

        def getIndex(self, char):
            return ord(char)  # - BASE

        def insert(self, string):
            p = self.root
            for char in string:
                index = self.getIndex(char)
                if p.next[index] == None:
                    p.next[index] = Node()
                p = p.next[index]
            p.end = True
            p.word = string

        def build_automation(self):
            self.root.fail = None
            self.queue.append(self.root)
            while len(self.queue) != 0:
                parent = self.queue[0]
                self.queue.pop(0)
                for i, child in enumerate(parent.next):
                    if child == None: continue
                    if parent == self.root:
                        child.fail = self.root
                    else:
                        failp = parent.fail
                        while failp != None:
                            if failp.next[i] != None:
                                child.fail = failp.next[i]
                                break
                            failp = failp.fail
                        if failp == None: child.fail = self.root
                    self.queue.append(child)

        def matchOne(self, string,start):
            p = self.root
            pos=0;
            spos=0;
            for char in string:
                if char=='#':
                    pos+=1;
                    spos+=1;
                    continue;
                if pos<start:
                    continue;
                index = self.getIndex(char)

                while p.next[index] == None and p != self.root:
                    p = p.fail
                    if p == None:
                        return -1, None;
                if p.next[index] == None:
                    p = self.root
                    spos=0;
                else:
                    p = p.next[index]
                if p.end:
                    return pos-spos+1, p.word

            return -1, None


    class UnicodeAcAutomation():
        def __init__(self, encoding='utf-8'):
            self.ac = AcAutomation()
            self.encoding = encoding

        def getAcString(self, string,addsplit=False):
            buff=[];
            for char in string:
                arr = bytearray(char.encode(self.encoding))
                for b in arr:
                    buff.append( chr(b % 16))
                    buff.append(chr(b // 16))
                if addsplit:
                    buff.append('#');
            return ''.join(buff);


        def insert(self, string):
            if type(string) != unicode:
                raise Exception('UnicodeAcAutomation:: insert type not unicode')
            ac_string = self.getAcString(string)
            self.ac.insert(ac_string)

        def build_automation(self):
            self.ac.build_automation()

        def match_one(self, string, start=0):
            if type(string) != unicode:
                raise Exception('UnicodeAcAutomation:: insert type not unicode')
            ac_string = self.getAcString(string,True)
            retcode, ret = self.ac.matchOne(ac_string,start)
            if ret != None:
                s = ''
                for i in range(len(ret) // 2):
                    s += chr(ord(ret[2 * i]) + ord(ret[2 * i + 1]) * 16)
                ret = s.decode('utf-8')
            return retcode, ret


    class WordEntity(EntityBase):
        def __init__(self, word_list):

            super(WordEntity, self).__init__()
            self.trie= UnicodeAcAutomation();
            for r in word_list:
                self.trie.insert(r.strip());
            self.trie.build_automation()

        def rewrite_item(self, input):
            return input

        def match_item(self, input, start, end, must_start, mode=None):
            self.log_in(input, start)
            pos,word=self.trie.match_one(input, start);
            if pos == -1:
                self.log_out(None)
                return int_max;

            if must_start and pos != start:
                self.log_out(None)
                return int_max;
            self.log_out(word)
            return MatchResult(self,word,pos);





