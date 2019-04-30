#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 16:53:10 2018

@author: weihuang
"""

from __future__ import print_function
import pandas as pd
import numpy as np
from scipy import  stats
import matplotlib.pyplot as plt
import statsmodels.api as sm
from statsmodels.graphics.api import qqplot
import time
import statsmodels.tsa.stattools as ts
from datetime import datetime
import matplotlib
from matplotlib.font_manager import FontProperties
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import matplotlib  
from pylab import mpl
from matplotlib.font_manager import _rebuild
_rebuild()

myfont = matplotlib.font_manager.FontProperties(fname='/Users/weihuang/Downloads/simheittf/simhei.ttf') 
mpl.rcParams['font.sans-serif'] = ['SimHei'] # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题
#
# 移动平均图
font = {'family': 'simhei', 'weight': 'normal', 'size': 20}
def draw_trend(timeSeries, size):
    f = plt.figure(facecolor='white',figsize=(10,8))
    # 对size个数据进行移动平均
    rol_mean = timeSeries.rolling(window=size).mean()


    timeSeries.plot(color='blue', label='Original')
    rol_mean.plot(color='red', label='Rolling Mean')
    #rol_weighted_mean.plot(color='black', label='Weighted Rolling Mean')
    #print(ts.adfuller(rol_weighted_mean))
    print (rol_mean)
    plt.legend(loc='best')
    plt.title('Rolling Mean')
    plt.show()
    return rol_mean

def draw_ts(timeSeries):
    f = plt.figure(facecolor='white')
    timeSeries.plot(color='blue')
    plt.show()
def testStationarity(ts):
    dftest = ts.adfuller(ts)
    # 对上述函数求得的值进行语义描述
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    for key,value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    return dfoutput

# 自相关和偏相关图，默认阶数为31阶
def draw_acf_pacf(ts, lags=31):
    f = plt.figure(facecolor='white')
    ax1 = f.add_subplot(211)
    plot_acf(ts, lags=40, ax=ax1)
    ax2 = f.add_subplot(212)
    plot_pacf(ts, lags=40, ax=ax2)
    plt.show()
def adf_test(inputSeries):
    print(type(inputSeries))
    adftest = ts.adfuller(inputSeries, autolag='AIC')
    adf_res = pd.Series(adftest[0:4], index=['Test Statistic','p-value','Lags Used','Number of Observations Used'])
    for key, value in adftest[4].items():
        adf_res['Critical Value (%s)' % key] = value
    return adf_res
#% 日期-最新价
#df=pd.read_csv('merge-30.csv')
df=pd.read_csv('D:/18-19/graduationDesign/data/Stk_Tick/Stk_Tick_SZ01/mergeSZ000001_Tick_201505.csv')
#df=pd.read_csv('D:/18-19/graduationDesign/data/Stk_Tick/Stk_Tick_SZ01/mergeSZ000001_Tick_201812.csv')
dateTimePattern="%Y-%m-%d %H:%M:%S"
minPattern="%Y-%m-%d %H:%M"
head="市场代码,证券代码,时间,最新价,成交笔数,成交额,成交量,方向,买一价,买二价,买三价,买四价,买五价,卖一价,卖二价,卖三价,卖四价,卖五价,买一量,买二量,买三量,买四量,买五量,卖一量,卖二量,卖三量,卖四量,卖五量"
columnHeadTuple=head.split(",")
#print (columnHeadTuple)
numericalCols=columnHeadTuple[3:]
numericalCols.remove('方向')
discreteCols=columnHeadTuple[:3]
discreteCols.append('方向')
#print(numericalCols,discreteCols)
zscore = lambda x: (x-x.mean())/x.std()
timestamp=lambda x :int(time.mktime(time.strptime(x,dateTimePattern)))
hour=lambda x :int((int(time.mktime(time.strptime(x,dateTimePattern))))/3600)
minute=lambda x :int((int(time.mktime(time.strptime(x,dateTimePattern))))/60)
date2Min=lambda x :datetime.strptime(x[:-3],"%Y-%m-%d %H:%M")
date2Hour=lambda x :datetime.strptime(x[:-6],"%Y-%m-%d %H")

#for item in numericalCols:
#    df[item]=df[item].transform(zscore)
df['交易时间戳']=df['时间'].transform(timestamp)
df['交易时间按小时']=df['时间'].transform(hour)
df['交易时间按分钟']=df['时间'].transform(minute)
df['分钟']=df['时间'].transform(date2Min)
df['小时']=df['时间'].transform(date2Hour)
del df['证券代码']
del df['市场代码']

#price=pd.Series(df['最新价'].groupby(df['交易时间按小时']).mean())
#price.index = pd.Index(pd.date_range('20171115','20171116',freq='1h')[9:15]) 
#price.plot(figsize=(12,8))
#plt.show()

#price1=pd.Series(df['最新价'].groupby(df['交易时间按分钟']).mean())
#uniqueTime=df.drop_duplicates(['分钟'])['分钟']
#price1.index = pd.Index(uniqueTime) 
#price1.plot(figsize=(12,8))
#plt.show()

#df=df.head(10)
price2=pd.Series(df['最新价']).groupby(df['交易时间按分钟']).mean()
uniqueTime=df.drop_duplicates(['分钟'])['分钟']
price2.index = pd.Index(uniqueTime)
price2.dropna(inplace=True)
price2.plot(figsize=(12,8),markersize=25)
plt.xlabel("日期",fontsize=17)
plt.ylabel("最新价",fontsize=17)
#plt.legend(loc='best',prop=font)
plt.show()
print (ts.adfuller(price2))
#%差分
def draw1(timeSeries):
    f = plt.figure(facecolor='white',figsize=(10,8))
    # 对size个数据进行移动平均
    origin = timeSeries
#    一阶差分
    diff1 = origin.diff(1)
    print (diff1)
#    二阶差分
    diff2 = origin.diff(2)
    print (diff2)
    origin.plot(color='blue', label='Original')
    diff1.plot(color='red', label='Diff 1')
    diff2.plot(color='green', label='Diff 2')
    #rol_weighted_mean.plot(color='black', label='Weighted Rolling Mean')
    #print(ts.adfuller(rol_weighted_mean))

    plt.legend(loc='best')
    plt.title('Steady origin')
    plt.show()



#draw_acf_pacf(newHourPrice)
#
#adf_res=adf_test(price2)
#print(int(adf_res['Lags Used']))





#    一阶差分
fig = plt.figure(figsize=(12,8))
ax1 = fig.add_subplot(211)
diff1 = price2.diff(1)
diff1.plot(ax=ax1)
plt.xlabel("日期",fontsize=17)
plt.ylabel("一阶差分最新价",fontsize=17)
plt.show()

#    二阶差分
fig = plt.figure(figsize=(12,8))
ax1 = fig.add_subplot(212)
diff2 = price2.diff(2)
diff2.plot(ax=ax1)
plt.xlabel("日期",fontsize=17)
plt.ylabel("二阶差分最新价",fontsize=17)
plt.show()

diff1.dropna(inplace=True)
print (ts.adfuller(diff1))
diff2.dropna(inplace=True)
print (ts.adfuller(diff2))

#
#fig = plt.figure(figsize=(12,8))
#ax1= fig.add_subplot(111)
#diff2 = price2.diff(3)
#diff2.plot(ax=ax1)
#plt.show()
#%平稳时间序列的自相关图和偏自相关图
#diff1= price1.diff(1)
#fig = plt.figure(figsize=(12,8))
#ax1=fig.add_subplot(211)
#fig = sm.graphics.tsa.plot_acf(price1,lags=40,ax=ax1)
#ax2 = fig.add_subplot(212)
#fig = sm.graphics.tsa.plot_pacf(price1,lags=40,ax=ax2)


#diff1= price1.diff(3)
logDiffer=diff1
fig = plt.figure(figsize=(12,8))
ax1=fig.add_subplot(211)
fig = sm.graphics.tsa.plot_acf(logDiffer,lags=30,ax=ax1)
ax2 = fig.add_subplot(212)
fig = sm.graphics.tsa.plot_pacf(logDiffer,lags=30,ax=ax2)



#%% ARMA(,)的aic，bic，hqic aic，bic，hqic均最小，因此是最佳模型
arma_mod_1 = sm.tsa.ARMA(logDiffer,(3,2)).fit()
print(arma_mod_1.aic,arma_mod_1.bic,arma_mod_1.hqic)

arma_mod_2 = sm.tsa.ARMA(logDiffer,(3,3)).fit()
print(arma_mod_2.aic,arma_mod_2.bic,arma_mod_2.hqic)

arma_mod_3 = sm.tsa.ARMA(logDiffer,(3,4)).fit()
print(arma_mod_3.aic,arma_mod_3.bic,arma_mod_3.hqic)

#arma_mod_4 = sm.tsa.ARMA(price2,(3,1)).fit()

# 残差是否（自）相关 DW检验
resid = arma_mod_3.resid 
fig = plt.figure(figsize=(12,8))
ax1 = fig.add_subplot(211)
fig = sm.graphics.tsa.plot_acf(resid.values.squeeze(), lags=30, ax=ax1)
ax2 = fig.add_subplot(212)
fig = sm.graphics.tsa.plot_pacf(resid, lags=30, ax=ax2)
plt.show()
print(sm.stats.durbin_watson(arma_mod_1.resid.values))
print(sm.stats.durbin_watson(arma_mod_2.resid.values))
print(sm.stats.durbin_watson(arma_mod_3.resid.values))
#print(sm.stats.durbin_watson(arma_mod_4.resid.values))

#
#print(stats.normaltest(resid))
#fig = plt.figure(figsize=(12,8))
#ax = fig.add_subplot(111)
#fig = qqplot(resid, line='q', ax=ax, fit=True)


#%% 
model = sm.tsa.ARIMA(logDiffer,(3,1,4))
results_AR = model.fit()  
plt.figure(figsize=(12,10))
l1,=plt.plot(logDiffer,color='green')
l2,=plt.plot(results_AR.fittedvalues, color='red',linestyle='--')
#sum1=(results_AR.fittedvalues-price2.diff(1))**2
#print(sum1)
#plt.title('SSE: %.4f'%0.1=1249,fontsize=18)
plt.legend(handles=[l1, l2], labels=['最新价', '预测价'],loc = 0,prop={'size':15})
plt.xlabel("日期",fontsize=17)
plt.ylabel("一阶差分预测-最新价",fontsize=17)
plt.show()
