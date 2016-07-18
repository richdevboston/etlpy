import sys
from pytz import utc, timezone
from datetime import datetime
from time import mktime
sys.path.append('../tnpy/src');

from tnpy import RegexCore, BuffHelper, StringEntity as SE, RegexEntity as RE, TableEntity as TE, SequenceEntity as SQE, \
    NoSequenceEntity as NSE, ScriptEntity as SCE;

import time;
import datetime;

def date_translate(entity,ms):
    import time
    doc={};
    m=ms[0]
    m.ExtractDocument(doc);
    ctime= time.localtime()
    curtime= [ctime[i] for i in range(0,6)]
    timekey=['Year','Month','Day', 'Hour','Minute','Second'];
    max_accu =0;
    for i in range(len(timekey)):
        if timekey[i] in doc:
            max_accu=i+1;
    if max_accu==0:
        max_accu=1;
    def getvalue(index):
        key=timekey[index]
        if key in doc:
            return int(doc[key])
        else:
            return curtime[index];
    year_key= 'Relative_Year';
    year_dic={u'前年':-2,u'去年':-1,u'今年':0,u'明年':1,u'后年':2};
    month_key='Relative_Month'
    month_dic={u'上月':-1,u'本月':0,u'下月':1};
    if year_key in doc:
        value= doc[year_key];
        curtime[0]+=year_dic[value]
    if month_key in doc:
        value=doc[month_key];
        curtime[1]+=month_dic[value];
    ttime= datetime.datetime(year=getvalue(0),month=getvalue(1),day=getvalue(2),hour=getvalue(3),minute=getvalue(4),second=getvalue(5));
    ts=mktime(utc.localize(ttime).utctimetuple())
    m.rewrite=ts;

    return m.rewrite;

daterule = SQE(['datetime'], [SCE(date_translate)]);

core= RegexCore();
core.InitTNRule('../tnpy/rules/cnext');