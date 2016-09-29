# encoding: UTF-8

from tnpy import StringEntity as SE,ScriptEntity as SCE, RegexEntity as RE, NoSequenceEntity as NSE, TableEntity as TE, SequenceEntity as SQE, RepeatEntity as RPE, \
    EntityBase,int_max,MatchResult
import sys
from tnnlp import  date_translate
import re


def get_time(mtime):
    from pytz import utc
    from time import mktime
    import datetime
    if mtime=='':
        mtime=datetime.datetime.now()
    ts = mktime(utc.localize(mtime).utctimetuple())
    return int(ts);

PY2 = sys.version_info[0] == 2

def date_trans0(e,ms):
    res=date_translate(ms[0]);
    return get_time(res[0])

def date_trans1(e,ms):
    res=date_translate(ms[0]);
    return str(res[0])


my_datetime=SQE(['x_datetime'],[SCE(date_trans1)])

timestamp= SQE(['x_datetime'],[SCE(date_trans0)])