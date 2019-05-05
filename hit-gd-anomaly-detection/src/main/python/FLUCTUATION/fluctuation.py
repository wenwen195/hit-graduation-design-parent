# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 17:27:29 2019

@author: ryw
"""

import pandas as pd
import numpy as np
from scipy import  stats
import time
import statsmodels.tsa.stattools as ts
from datetime import datetime
import matplotlib
from matplotlib.font_manager import FontProperties
from matplotlib.font_manager import _rebuild
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
from pylab import mpl

myfont = matplotlib.font_manager.FontProperties(fname='/Users/weihuang/Downloads/simheittf/simhei.ttf') 
mpl.rcParams['font.sans-serif'] = ['SimHei'] # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题

#%% 转化函数
dateTimePattern="%Y-%m-%d %H:%M:%S"
minPattern="%Y-%m-%d %H:%M"
zscore = lambda x: (x-x.mean())/x.std()
timestamp=lambda x :int(time.mktime(time.strptime(x,dateTimePattern)))
hour=lambda x :int((int(time.mktime(time.strptime(x,dateTimePattern))))/3600)
minute=lambda x :int((int(time.mktime(time.strptime(x,dateTimePattern))))/60)
threeSecond=lambda x :int(
        (int(time.mktime(time.strptime(x,dateTimePattern))))+
        (3-int(time.mktime(time.strptime(x,dateTimePattern))))%3)
#    int(time.mktime(datetime.strptime(x[:-3],"%Y-%m-%d %H:%M")) 
#        if ((int(time.mktime(time.strptime(x,dateTimePattern))))%86400-33900)<600
##        time.strftime('%H:%M',datetime.strptime(x[11:-3],minPattern))
#        else 
date23Second=lambda x :time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x))
date2Min=lambda x :datetime.strptime(x[:-3],"%Y-%m-%d %H:%M")
date2Hour=lambda x :datetime.strptime(x[:-6],"%Y-%m-%d %H")
date2Date=lambda x :datetime.strptime(x[:-9],"%Y-%m-%d").strftime("%Y-%m-%d")

#%% 读入股票tick日期-最新价
#dfStk=pd.read_csv('merge-30.csv')
#dfStk=pd.read_csv('D:/18-19/graduationDesign/data/Stk_Tick/Stk_Tick_SZ01/mergeSZ000001_Tick_201505.csv')
dfStk=pd.read_csv('D:/18-19/graduationDesign/data/Stk_Tick/Stk_Tick_SZ01/SZ000001_20150504.csv')
#head="市场代码,证券代码,时间,最新价,成交笔数,成交额,成交量,方向,买一价,买二价,买三价,买四价,买五价,卖一价,卖二价,卖三价,卖四价,卖五价,买一量,买二量,买三量,买四量,买五量,卖一量,卖二量,卖三量,卖四量,卖五量"
#columnHeadTuple=head.split(",")
dfStk['日期']=dfStk['时间'].transform(date2Date)
dfStk['交易时间按3s']=dfStk['时间'].transform(threeSecond)
dfStk['3s']=dfStk['交易时间按3s'].transform(date23Second)
dfStkU=dfStk.loc[:,['时间','交易时间按3s','日期','最新价','3s']]
#print(dfStkU.columns)
#print(dfStkU[0:5])

#%% 读入股票日线 昨收价
dfSDay=pd.read_csv('D:/18-19/graduationDesign/data/stk_day_20190307/SZ000001_day.csv')
#dayHead='代码,时间,开盘价,最高价,最低价,收盘价,成交量(股),成交额(元)'
#dayHeadTuple=dayHead.split(',')
dfSDay.rename(columns={'时间':'当前交易日'}, inplace = True)
lastDay=list(dfSDay['当前交易日'].drop(0))
lastDay.append(lastDay[-1])
dfSDay['下一交易日']=lastDay
dfSDayU=dfSDay.loc[:,['当前交易日','下一交易日','收盘价']]
#print(dfSDayU.columns)
#print(dfSDayU[0:5])


#%% 股票tick -join昨收
dfStkDay=pd.merge(dfStkU, dfSDayU, how='left', on=None, left_on='日期', right_on='下一交易日',
      left_index=False, right_index=False, sort=False,
      suffixes=('_x', '_y'), copy=True, indicator=False)
#print(dfStkDay[0:5])

#%% 计算股票涨跌幅
dfSPrice=dfStkDay.loc[:,['交易时间按3s','最新价','收盘价']]
dfSPrice.rename(columns={'收盘价':'昨收价'}, inplace = True)
dfSFluctuation=dfSPrice.groupby(dfStk['交易时间按3s']).last()
dfSUniqueTime=dfStk.drop_duplicates(['3s'])['3s']
dfSFluctuation.index = pd.Index(dfSUniqueTime)
dfSFluctuation.dropna(inplace=True)
dfSFluctuation['股票涨跌幅']=(dfSFluctuation['最新价']-dfSFluctuation['昨收价'])/dfSFluctuation['昨收价']
#print(dfSFluctuation[0:5])

#%% 读入股指tick日期-最新价
#dfItk=pd.read_csv('merge-30.csv')
#dfItk=pd.read_csv('D:/18-19/graduationDesign/data/Stk_Tick/Stk_Tick_SZ399107/mergeSZ399107_Tick_201505.csv')
dfItk=pd.read_csv('D:/18-19/graduationDesign/data/Stk_Tick/Stk_Tick_SZ399107/sz399107_20150504.csv')
#head="市场代码,证券代码,时间,最新价,成交笔数,成交额,成交量,方向,买一价,买二价,买三价,买四价,买五价,卖一价,卖二价,卖三价,卖四价,卖五价,买一量,买二量,买三量,买四量,买五量,卖一量,卖二量,卖三量,卖四量,卖五量"
#columnHeadTuple=head.split(",")
dfItk['日期']=dfItk['时间'].transform(date2Date)
dfItk['交易时间按3s']=dfItk['时间'].transform(threeSecond)
dfItk['3s']=dfItk['交易时间按3s'].transform(date23Second)
dfItkU=dfItk.loc[:,['时间','交易时间按3s','日期','最新价','3s']]
#print(dfItkU.columns)
#print(dfItkU[0:5])

#%% 读入股指日线 昨收价
dfIDay=pd.read_csv('D:/18-19/graduationDesign/data/Idx_DAY_20190223/SZ399107_day.csv')
#dayHead='代码,时间,开盘价,最高价,最低价,收盘价,成交量(股),成交额(元)'
#dayHeadTuple=dayHead.split(',')
dfIDay.rename(columns={'时间':'当前交易日'}, inplace = True)
lastDay=list(dfIDay['当前交易日'].drop(0))
lastDay.append(lastDay[-1])
dfIDay['下一交易日']=lastDay
dfIDayU=dfIDay.loc[:,['当前交易日','下一交易日','收盘价']]
#print(dfIDayU.columns)
#print(dfIDayU[0:5])


#%% 股指tick -join昨收
dfItkDay=pd.merge(dfItkU, dfIDayU, how='left', on=None, left_on='日期', right_on='下一交易日',
      left_index=False, right_index=False, sort=False,
      suffixes=('_x', '_y'), copy=True, indicator=False)
#print(dfItkDay[0:5])

#%% 计算股指涨跌幅
dfIPrice=dfItkDay.loc[:,['交易时间按3s','最新价','收盘价']]
dfIPrice.rename(columns={'收盘价':'昨收价'}, inplace = True)
dfIFluctuation=dfIPrice.groupby(dfItk['交易时间按3s']).last()
dfIUniqueTime=dfItk.drop_duplicates(['3s'])['3s']
dfIFluctuation.index = pd.Index(dfIUniqueTime)
dfIFluctuation.dropna(inplace=True)
dfIFluctuation['股指涨跌幅']=(dfIFluctuation['最新价']-dfIFluctuation['昨收价'])/dfIFluctuation['昨收价']
#print(dfIFluctuation[0:5])

#%% 计算涨跌幅偏离
dfFluctuationDeviation=pd.merge(dfSFluctuation, dfIFluctuation, how='left', 
      left_index=True, right_index=True, sort=False,
      suffixes=('_x', '_y'), copy=True, indicator=False)
dfFluctuationDeviation.fillna(method = 'backfill', axis = 0)
dfFluctuationDeviation['偏离值']=abs(dfFluctuationDeviation['股票涨跌幅']-dfFluctuationDeviation['股指涨跌幅'])
#print(dfFluctuationDeviation[0:5])
dfOutliers=dfFluctuationDeviation[dfFluctuationDeviation['偏离值']>0.02]
#print(dfOutliers[0:5])
dfOutliers.to_csv('dfFluctuationDeviation.csv')

dfOutliersDraw=dfFluctuationDeviation.loc[:,['最新价_x','昨收价_x','股票涨跌幅','股指涨跌幅','偏离值']]#
#dfOutliersDraw.plot(figsize=(12,8))
dfOutliersDraw.dropna(inplace=True)
dfOutliersDraw['离群点']=dfOutliersDraw['最新价_x']
dfOutliersDraw['离群点'][dfOutliersDraw['偏离值']<0.02]=None
#print(dfOutliersDraw[0:5])


fig = plt.figure(figsize=(12,8))
plt.subplots_adjust(hspace=0.8)
ax1 = fig.add_subplot(211)
A1,=plt.plot(dfOutliersDraw.index.values,dfOutliersDraw['最新价_x'].values,label='最新价_x')
B1,=plt.plot(dfOutliersDraw.index.values,dfOutliersDraw['昨收价_x'].values,label='昨收价_x')
C1,=plt.plot(dfOutliersDraw.index.values,dfOutliersDraw['离群点'].values,label='离群点',color='r',linewidth = '1')
ax1.legend(handles=[A1,B1,C1],loc='lower right') 
#ax1.plot(dfOutliersDraw.index.values,dfOutliersDraw['最新价_x'].values,dfOutliersDraw['昨收价_x'].values)
#设置坐标轴数量和倾斜角度
for ind, line in enumerate(ax1.xaxis.get_ticklines()):
    if ind % (dfOutliersDraw.index.values.size//20) == 0:  # every 10th label is kept
        line.set_visible(True)
    else:
        line.set_visible(False)
for ind, label in enumerate(ax1.xaxis.get_ticklabels()):
    if ind % (dfOutliersDraw.index.values.size//20) == 0:  # every 10th label is kept
        label.set_visible(True)
        label.set_rotation(70)
    else:
        label.set_visible(False)
ax1.set_xlabel("时间",fontsize=17)
ax1.set_ylabel("最新价",fontsize=17)


ax2 = fig.add_subplot(212)
plt.subplots_adjust(hspace=0.8)
A2,=plt.plot(dfOutliersDraw.index.values,dfOutliersDraw['股票涨跌幅'].values,label='股票涨跌幅')
B2,=plt.plot(dfOutliersDraw.index.values,dfOutliersDraw['股指涨跌幅'].values,label='股指涨跌幅')
C2,=plt.plot(dfOutliersDraw.index.values,dfOutliersDraw['偏离值'].values,color='r',label='偏离值')
ax2.legend(handles=[A2,B2,C2],loc='lower right') 
#ax2.plot(dfOutliersDraw.index.values,dfOutliersDraw['最新价_x'].values,dfOutliersDraw['昨收价_x'].values)
#设置坐标轴数量和倾斜角度
for ind, line in enumerate(ax2.xaxis.get_ticklines()):
    if ind % 120 == 0:  # every 10th label is kept
        line.set_visible(True)
    else:
        line.set_visible(False)
for ind, label in enumerate(ax2.xaxis.get_ticklabels()):
    if ind % (dfOutliersDraw.index.values.size//20) == 0:  # every 10th label is kept
        label.set_visible(True)
        label.set_rotation(70)
    else:
        label.set_visible(False)
ax2.set_xlabel("时间",fontsize=17)
ax2.set_ylabel("涨跌幅",fontsize=17)
plt.savefig('最新价+涨跌幅_20150504.png',bbox_inches = 'tight')
#plt.show()


