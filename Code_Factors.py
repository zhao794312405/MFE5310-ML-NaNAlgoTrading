#To get factors
import baostock as bs
import pandas as pd
import numpy as np
import math
import datetime
import time
import talib as ta
from pandas.core.frame import DataFrame
import copy
import time
import pickle

# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

#自定义功能函数区###############

#确定当日是否是交易日或寻找下一个交易日
def trading_ornext(date):
    rs = bs.query_trade_dates(start_date=date, end_date=date)
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    if result.iloc[0]['is_trading_day']=='1':
        return date
    else:
        year=int(date[:4])
        month=int(date[5:7])
        day=int(date[8:])
        dt=datetime.date(year,month,day)
        dt=dt+datetime.timedelta(days=1)
        dt=dt.strftime('%Y-%m-%d')
        return trading_ornext(dt)

#确定当日是否是交易日或寻找上一个交易日
def trading_orlast(date):
    rs = bs.query_trade_dates(start_date=date, end_date=date)
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    if result.iloc[0]['is_trading_day']=='1':
        return date
    else:
        year=int(date[:4])
        month=int(date[5:7])
        day=int(date[8:])
        dt=datetime.date(year,month,day)
        dt=dt-datetime.timedelta(days=1)
        dt=dt.strftime('%Y-%m-%d')
        return trading_orlast(dt)


def check_float(string):
    str1 = str(string)
    if str1.count('.')>1:#判断小数点是不是大于1
        return False
    elif str1.isdigit():
        return True#判断是不是整数
    else:
        new_str  = str1.split('.')#按小数点分割字符
        frist_num = new_str[0]#取分割完之后这个list的第一个元素
        if frist_num.count('-')>1:#判断负号的格数，如果大于1就是非法的
            return False
        else:
            frist_num = frist_num.replace('-','')#把负号替换成空
        if frist_num.isdigit() and new_str[1].isdigit():
        #如果小数点两边都是整数的话，那么就是一个小数
            return True
        else:
            return False


#季度计算函数
def qua(date):
    month=float(date[5:7])
    if month <4:
        quarter=1
    elif month>=4 and month <7:
        quarter=2
    elif month>=7 and month <10:
        quarter=3
    else:
        quarter=4
    return quarter

#测试是否为float
def isfloat(str):
    try:
        float(str)
        return True
    except:
        return False
    
#测试是否存在不合规数据（空值，空框）
def test(dataframe,column):
    if isfloat(dataframe[column]) and not(dataframe.empty):
        return True
    else:
        return False
    
    

###########################################

startdate="2010-01-01"
enddate="2020-03-31"

# 获取所有交易日: all_trading_date
rs = bs.query_trade_dates(start_date=startdate, end_date=enddate)
data_list = []
while (rs.error_code == '0') & rs.next():
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=rs.fields)
all_trading_date=result[result['is_trading_day']=='1']['calendar_date']

# 循环所有的交易年，获取交易日当年的第一个交易日的沪深300股票成分,并获得当年的所有交易日
dict_stock={}
for year in range(2010,2020):
    date_ori1='{}-01-01'.format(year)
    date1=trading_ornext(date_ori1)
    date_ori2='{}-12-31'.format(year)
    date2=trading_orlast(date_ori2)
    rs = bs.query_hs300_stocks(date=date1)
    hs300_stocks = []
    while (rs.error_code == '0') & rs.next():
        hs300_stocks.append(rs.get_row_data())
    result = pd.DataFrame(hs300_stocks, columns=rs.fields)
    
    rs2 = bs.query_trade_dates(start_date=date1, end_date=date2)
    data_list = []
    while (rs2.error_code == '0') & rs2.next():
        data_list.append(rs2.get_row_data())
    result2 = pd.DataFrame(data_list, columns=rs2.fields)
    
    trading_date=result2[result2['is_trading_day']=='1']['calendar_date']
    
    dict1={}
    dict2={}
    count=1
    for j in range(0,len(result)):
        df=trading_date.to_frame().reset_index(drop=True)        
        dict_temp={}
        code=result.iloc[j]['code']
        dict_temp['stock{}:{}'.format(count,code)]=df  #个股信息此处插入
        dict2.update(dict_temp)
        count+=1
    dict1[year]=dict2
    dict_stock.update(dict1)
#插入2020年第一季度的数据####################################################
startdate="2020-01-01"
enddate="2020-03-31"

# 循环所有的交易年，获取交易日当年的第一个交易日的沪深300股票成分,并获得当年的所有交易日
lg=bs.login()
date_ori1='2020-01-01'
date1=trading_ornext(date_ori1)
date_ori2='2020-03-31'
date2=trading_orlast(date_ori2)
rs = bs.query_hs300_stocks(date=date1)
hs300_stocks = []
while (rs.error_code == '0') & rs.next():
    hs300_stocks.append(rs.get_row_data())
result = pd.DataFrame(hs300_stocks, columns=rs.fields)

rs2 = bs.query_trade_dates(start_date=date1, end_date=date2)
data_list = []
while (rs2.error_code == '0') & rs2.next():
    data_list.append(rs2.get_row_data())
result2 = pd.DataFrame(data_list, columns=rs2.fields)

trading_date=result2[result2['is_trading_day']=='1']['calendar_date']

dict2020={}
dict2={}
count=1
for j in range(0,len(result)):
    df=trading_date.to_frame().reset_index(drop=True)        
    dict_temp={}
    code=result.iloc[j]['code']
    dict_temp['stock{}:{}'.format(count,code)]=df  #个股信息此处插入
    dict2.update(dict_temp)
    count+=1
dict2020[2020]=dict2

# 计算各季度有多少交易日   
dict_quarter=[]
for year in dict_stock:
    print(year,'--quarter_calculation')
    for stock in dict_stock[year]:    
        date1='{}-01-01'.format(year)
        date2='{}-03-31'.format(year)
        rs = bs.query_trade_dates(start_date=date1, end_date=date2)
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        count=0
        for i in range(0,len(data_list)):
            if data_list[i][1]=='1':
                count+=1
        dict_quarter.append(count)  
        date1='{}-04-01'.format(year)
        date2='{}-06-30'.format(year)
        rs = bs.query_trade_dates(start_date=date1, end_date=date2)
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        count=0
        for i in range(0,len(data_list)):
            if data_list[i][1]=='1':
                count+=1
        dict_quarter.append(count)  
        date1='{}-07-01'.format(year)
        date2='{}-09-30'.format(year)
        rs = bs.query_trade_dates(start_date=date1, end_date=date2)
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        count=0
        for i in range(0,len(data_list)):
            if data_list[i][1]=='1':
                count+=1
        dict_quarter.append(count)  
        date1='{}-10-01'.format(year)
        date2='{}-12-31'.format(year)
        rs = bs.query_trade_dates(start_date=date1, end_date=date2)
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        count=0
        for i in range(0,len(data_list)):
            if data_list[i][1]=='1':
                count+=1
        dict_quarter.append(count)  
        break

###开始获取因子###############
# 成长能力  
for year in dict_stock:
    print(year,'--YOYPNI（GROWTH）')
    for stock,df in dict_stock[year].items():
        code=stock[-9:]
        
        growth_list1 = []
        growth_list2 = []
        growth_list3 = []
        growth_list4 = []
        
        rs_growth1 = bs.query_growth_data(code=code, year=year-1, quarter=3)
        while (rs_growth1.error_code == '0') & rs_growth1.next():
            growth_list1.append(rs_growth1.get_row_data())
        result_growth1 = pd.DataFrame(growth_list1, columns=rs_growth1.fields)
        rs_growth2 = bs.query_growth_data(code=code, year=year-1, quarter=4)
        while (rs_growth2.error_code == '0') & rs_growth2.next():
            growth_list2.append(rs_growth2.get_row_data())
        result_growth2 = pd.DataFrame(growth_list2, columns=rs_growth2.fields)
        rs_growth3 = bs.query_growth_data(code=code, year=year, quarter=1)
        while (rs_growth3.error_code == '0') & rs_growth3.next():
            growth_list3.append(rs_growth3.get_row_data())
        result_growth3 = pd.DataFrame(growth_list3, columns=rs_growth3.fields)
        rs_growth4 = bs.query_growth_data(code=code, year=year, quarter=2)
        while (rs_growth4.error_code == '0') & rs_growth4.next():
            growth_list4.append(rs_growth4.get_row_data())
        result_growth4 = pd.DataFrame(growth_list4, columns=rs_growth4.fields)
        
        a1=float(result_growth1['YOYPNI']) if test(result_growth1,'YOYPNI') else 0
        a2=float(result_growth2['YOYPNI']) if test(result_growth2,'YOYPNI') else 0
        a3=float(result_growth3['YOYPNI']) if test(result_growth3,'YOYPNI') else 0
        a4=float(result_growth4['YOYPNI']) if test(result_growth4,'YOYPNI') else 0
        YOYPNI_list = []
        for i in range(0,dict_quarter[(year-2010)*4+0]):
            YOYPNI_list.append(a1)
        for i in range(0,dict_quarter[(year-2010)*4+1]):
            YOYPNI_list.append(a2)
        for i in range(0,dict_quarter[(year-2010)*4+2]):
            YOYPNI_list.append(a3)
        for i in range(0,dict_quarter[(year-2010)*4+3]):
            YOYPNI_list.append(a4)
            
        result_YOYPNI = pd.DataFrame({"Profit_G_q": YOYPNI_list})
        dict_stock[year][stock]=pd.concat([df,result_YOYPNI],axis=1)

# MBRevenue
for year in dict_stock:
    print(year,'--MBRevenue（Profit）')
    for stock,df in dict_stock[year].items():
        code=stock[-9:]
        profit_list4 = []
        rs_profit4 = bs.query_profit_data(code=code, year=year-1, quarter=4)
        while (rs_profit4.error_code == '0') & rs_profit4.next():
            profit_list4.append(rs_profit4.get_row_data())
        result_profit4 = pd.DataFrame(profit_list4, columns=rs_profit4.fields)
        a4=float(result_profit4['MBRevenue']) if test(result_profit4,'MBRevenue') else 0
        MBRevenue_list = []
        for i in range(0,len(df)):
            MBRevenue_list.append(a4)          
        result_MBRevenue = pd.DataFrame({"MBRevenue": MBRevenue_list})
        dict_stock[year][stock]=pd.concat([df,result_MBRevenue],axis=1)

"""净利润（TTM）同比增长率/PE_TTM"""
for year in dict_stock:
    print(year,'--G/PE')
    for stock,df in dict_stock[year].items():
      
        GPE1=[]
        for i in range(0, len(dict_stock[year][stock])):
            try:
                GPE_temp = float(dict_stock[year][stock]['Profit_G_q'][i])/float(dict_stock[year][stock]['peTTM'][i])         
            except:
                GPE_temp = float(dict_stock[year][stock]['Profit_G_q'][i])
            GPE1.append(GPE_temp)
        
        result = pd.DataFrame({"G/PE":GPE1})
        dict_stock[year][stock]=pd.concat([df,result],axis=1)
  
#Sales_G_q
      
for year in dict_stock:
    print(year,'--Salesgq')
    for stock,df in dict_stock[year].items():
        # print(stock)
        # try的内容因为删掉一年应该都是失败的，但是直接except没影响结果
        try:
            dict_stock[year-1][stock]
            if float(dict_stock[year][stock]['MBRevenue'][0])==0:
                sales_temp = 0
            else:
                sales_temp = (float(dict_stock[year][stock]['MBRevenue'][0])/float(dict_stock[year-1][stock]['MBRevenue'][0]))-1
            count+=1
            sales_list = []
            for i in range(0,len(df)):
                sales_list.append(sales_temp)
            result_Sales = pd.DataFrame({"Sales_G_q": sales_list})
            dict_stock[year][stock]=pd.concat([df,result_Sales],axis=1)
        except:        
            code=stock[-9:]
            profit_list4 = []
            rs_profit4 = bs.query_profit_data(code=code, year=year-2, quarter=4)
            while (rs_profit4.error_code == '0') & rs_profit4.next():
                profit_list4.append(rs_profit4.get_row_data())
            result_profit4 = pd.DataFrame(profit_list4, columns=rs_profit4.fields)
            a4=float(result_profit4['MBRevenue']) if test(result_profit4,'MBRevenue') else 1
            sales_temp = ((float(dict_stock[year][stock]['MBRevenue'][0])-a4)/a4)
            count+=1
            sales_list = []
            for i in range(0,len(df)):
                sales_list.append(sales_temp)
            result_Sales = pd.DataFrame({"Sales_G_q": sales_list})
            dict_stock[year][stock]=pd.concat([df,result_Sales],axis=1)      
        
        
# BIAS(n) =（当日股市收盘价－n日移动平均值）÷ n日移动平均值
for year in dict_stock:
    print(year,'--BIAS')
    for stock,df in dict_stock[year].items():
        code=stock[-9:]
        date1='{}-10-01'.format(year-1)
        date2='{}-12-31'.format(year-1)
        rs = bs.query_history_k_data_plus(code, "close", start_date=date1, 
        end_date=date2, frequency="d", adjustflag="3")
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())     
        result_last = pd.DataFrame(data_list, columns=rs.fields)
        result_last19 = result_last['close'][-19:]
        close_list = list(result_last19)+list(dict_stock[year][stock]['close'])       
        close_float = map(float, close_list)
        close_float = list(close_float)
        ma20 = ta.MA(np.array(close_float), timeperiod=20)      
        bias20 = []
        for j in range(19,len(ma20)):    
            bias_temp = (float(dict_stock[year][stock]['close'][j-19])-ma20[j])/ma20[j]
            bias20.append(bias_temp)         
        result = pd.DataFrame({"BIAS":bias20})
        dict_stock[year][stock]=pd.concat([df,result],axis=1)       
        
# MACD,DIF
for year in dict_stock:
    print(year,'--MACD,DIF')
    for stock,df in dict_stock[year].items():      
        code=stock[-9:]
        date1='{}-10-01'.format(year-1)
        date2='{}-12-31'.format(year-1)
        rs = bs.query_history_k_data_plus(code, "close", start_date=date1, 
        end_date=date2, frequency="d", adjustflag="3")
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        result_last = pd.DataFrame(data_list, columns=rs.fields)
        result_last43 = result_last['close'][-43:]
        close_list = list(result_last43)+list(dict_stock[year][stock]['close'])       
        close_float = map(float, close_list)
        close_float = list(close_float)
        dif1, dea, hist1 = ta.MACD(np.array(close_float), fastperiod=10, slowperiod=30, signalperiod=15)      
        dif = dif1[43:]
        hist = hist1[43:]
        
        result_macd = pd.DataFrame({"DIF":dif,"MACD":hist})
        dict_stock[year][stock]=pd.concat([df,result_macd],axis=1)

#DP 
for date in dict_stock: 
    print(date)
    for stock,df in dict_stock[date].items(): 
        print(stock)
        code=stock
        date_ori1='{}-01-01'.format(date)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(date)
            date2=trading_orlast(date_ori2)
        else:
            date2='2020-03-31'

        profit_list = []
        rs_profit = bs.query_profit_data(code,date-1,3) #统计季度，可为空，默认当前季度
        while (rs_profit.error_code == '0') & rs_profit.next():
            profit_list.append(rs_profit.get_row_data())

        rs_profit2 = bs.query_profit_data(code,date-1,4) 
        while (rs_profit2.error_code == '0') & rs_profit2.next():
            profit_list.append(rs_profit2.get_row_data())

        rs_profit3 = bs.query_profit_data(code,date,1)
        while (rs_profit3.error_code == '0') & rs_profit3.next():
            profit_list.append(rs_profit3.get_row_data())

        rs_profit4 = bs.query_profit_data(code,date,2) 
        while (rs_profit4.error_code == '0') & rs_profit4.next():
            profit_list.append(rs_profit4.get_row_data())

        result_profit = pd.DataFrame(profit_list, columns=rs_profit.fields)


        epsTTM=result_profit['epsTTM'].astype('float') 

        rs_list=[]
        rs_dividend = bs.query_dividend_data(code,date,yearType="report")
        while (rs_dividend.error_code == '0') & rs_dividend.next():
            rs_list.append(rs_dividend.get_row_data())
        result_dividend = pd.DataFrame(rs_list, columns=rs_dividend.fields)

        div = result_dividend['dividCashPsBeforeTax'].tolist()#每股股利税前  dividCashPsAfterTax 
        if len(div)==0 or div[0]=='':
            dividCashPs =pd.Series(0) 
        else:
            dividCashPs = pd.Series(div[0]).astype('float')

        dict_stock[date][stock]['epsTTM']=None                
        for i in range(len(df)):
            if qua(df.iloc[i]['calendar_date'])==1:
                dict_stock[date][stock]['epsTTM'][i]=epsTTM[0] if len(epsTTM)>0 else 0
            elif qua(df.iloc[i]['calendar_date'])==2:
                dict_stock[date][stock]['epsTTM'][i]=epsTTM[1] if len(epsTTM)>1 else 0
            elif qua(df.iloc[i]['calendar_date'])==3:
                dict_stock[date][stock]['epsTTM'][i]=epsTTM[2] if len(epsTTM)>2 else 0
            elif qua(df.iloc[i]['calendar_date'])==4:
                dict_stock[date][stock]['epsTTM'][i]=epsTTM[3] if len(epsTTM)>3 else 0


        dict_stock[date][stock]['dividCashPs'] = dividCashPs
        dict_stock[date][stock]['dividCashPs'] = dict_stock[date][stock]['dividCashPs'].fillna(method='pad',axis=0)
        
        dict_stock[date][stock]['DP'] = 0
        
        for j in range(len(df)):
            dict_stock[date][stock]['DP'][i] = dict_stock[date][stock]['dividCashPs'][i] / dict_stock[date][stock]['epsTTM'][i] if dict_stock[date][stock]['epsTTM'][i]!=0 else 0


#换手率'turn_3m',beta(60)
# 对每只股票回测时都遍历一遍参数，选择使超额收益率最大的参数作为该股票的最优参数
# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)
for date in dict_stock: 
    print(date)
    for stock in dict_stock[date]:  
#         date = 2017
#         stock = 'sh.600000'
        code=stock
        date_ori1='{}-09-28'.format(date-1)
        date1=trading_ornext(date_ori1)

        if year!=2020:
            date_ori2='{}-12-31'.format(date)
            date2=trading_orlast(date_ori2)
        else:
            date2='{}-03-31'.format(date+1)

        result_turn = []
        rs_turn = bs.query_history_k_data_plus(stock,
        "date,high,low,turn",
        start_date=date1, end_date=date2,
        frequency="d", adjustflag="3")
        while (rs_turn.error_code == '0') & rs_turn.next():
            result_turn.append(rs_turn.get_row_data())

        result_all = pd.DataFrame(result_turn, columns=rs_turn.fields)
        result_all = result_all.fillna(method='ffill')

        # fill not num
        flt = []
        for i in range(0,len(result_all['turn'])):
            if check_float(result_all['turn'][i]) == False:
                result_all['turn'][i] = 0
        print(result_all['turn'])
        turn=result_all['turn'].astype('float')
        turn_3m = turn.rolling(60).mean()

        high = result_all['high'].astype('float')
        low = result_all['low'].astype('float')
        beta = ta.BETA(high, low, timeperiod=60)


        begin=0
        for i in range(len(result_all)):
            if result_all['date'][i][:4]!=str(date):
                begin+=1

        turn = turn[begin:].reset_index(drop=True)
        turn_3m = turn_3m[begin:].reset_index(drop=True)
        beta = beta[begin:].reset_index(drop=True)

        dict_stock[date][stock]['turn']= turn
        dict_stock[date][stock]['turn_3m'] = turn_3m
        dict_stock[date][stock]['beta'] = beta
        print('success')

# RSI
for date in dict_stock:
    print(date)
    for stock in dict_stock[date]:  
#         date = 2016
#         stock ='sh.600000'  #stock code
        code=stock
        date_ori1='{}-10-31'.format(year-1)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(year)
            date2=trading_orlast(date_ori2)
        else:
            date2='2020-03-31'

        result_turn = []
        rs_turn = bs.query_history_k_data_plus(stock,
        "date,close",
        start_date=date1, end_date=date2,
        frequency="d", adjustflag="3")
        while (rs_turn.error_code == '0') & rs_turn.next():
            result_turn.append(rs_turn.get_row_data())

        result_all = pd.DataFrame(result_turn, columns=rs_turn.fields)

        begin=0
        for i in range(len(result_all)):
            if result_all['date'][i][:4]!=str(date):
                begin+=1

        close = result_all['close'].astype('float')
        RSI =ta.RSI(close, timeperiod=20)

        RSI = RSI[begin:].reset_index(drop=True)

        dict_stock[date][stock]['RSI'] = RSI

        print(dict_stock[date][stock]['RSI'])

# # debtequityratio(DE) 债务股本比 = 负债总额/股东权益
# for stock in dict_stock[2019][sh.600000]  'sh.600008','sh.600009','sh.600018':  #stock code

for date in dict_stock: 
    print(date)
    for stock,df in dict_stock[date].items():  
        code=stock
        date_ori1='{}-01-01'.format(date)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(date)
            date2=trading_orlast(date_ori2)
        else:
            date2='2020-03-31'

        balance_list = []
        rs_balance1 = bs.query_balance_data(code, year=date-1, quarter=3)
        rs_balance2 = bs.query_balance_data(code, year=date-1, quarter=4)
        rs_balance3 = bs.query_balance_data(code, year=date, quarter=1)
        rs_balance4 = bs.query_balance_data(code, year=date, quarter=2)
        while (rs_balance1.error_code == '0') & rs_balance1.next():
            balance_list.append(rs_balance1.get_row_data())
        while (rs_balance2.error_code == '0') & rs_balance2.next():
            balance_list.append(rs_balance2.get_row_data())
        while (rs_balance3.error_code == '0') & rs_balance3.next():
            balance_list.append(rs_balance3.get_row_data())
        while (rs_balance4.error_code == '0') & rs_balance4.next():
            balance_list.append(rs_balance4.get_row_data())

        result_balance = pd.DataFrame(balance_list, columns=rs_balance1.fields)
        for i in range(len(result_balance)):
            result_balance['liabilityToAsset'][i] = float(result_balance['liabilityToAsset'][i]) if isfloat(result_balance['liabilityToAsset'][i]) else 0
            result_balance['assetToEquity'][i] = float(result_balance['assetToEquity'][i]) if isfloat(result_balance['assetToEquity'][i]) else 0

        #     liabilityToAsset = .astype('float')
        #     assetToEquity = result_balance.astype('float')
        DE = result_balance['liabilityToAsset']*result_balance['assetToEquity']
        print(DE)
        dict_stock[date][stock]['DE']=None    
        for i in range(len(df)):
            if qua(df.iloc[i]['calendar_date'])==1:
                dict_stock[date][stock]['DE'][i]=DE[0] if len(DE)>0 else 0
            elif qua(df.iloc[i]['calendar_date'])==2:
                dict_stock[date][stock]['DE'][i]=DE[1] if len(DE)>1 else 0
            elif qua(df.iloc[i]['calendar_date'])==3:
                dict_stock[date][stock]['DE'][i]=DE[2] if len(DE)>2 else 0
            elif qua(df.iloc[i]['calendar_date'])==4:
                dict_stock[date][stock]['DE'][i]=DE[3] if len(DE)>3 else 0


        print(dict_stock[date][stock]['DE'])

# PSY 一段交易日内股价上涨天数所占的比例 取20个交易日
# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

for date in dict_stock: #date year 前闭后开
    print(date)
    for stock in dict_stock[date]: 
#         date = 2017
#         stock ='sh.600000'  #stock code
        code=stock
        date_ori1='{}-11-25'.format(date-1)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(date)
            date2=trading_orlast(date_ori2)
        else:
            date2='2020-03-31'

        result_turn = []
        rs_turn = bs.query_history_k_data_plus(stock,
        "date,close",
        start_date=date1, end_date=date2,
        frequency="d", adjustflag="3")
        while (rs_turn.error_code == '0') & rs_turn.next():
            result_turn.append(rs_turn.get_row_data())

        result_all = pd.DataFrame(result_turn, columns=rs_turn.fields)

        begin=0
        for i in range(len(result_all)):
            if result_all['date'][i][:4]!=str(date):
                begin+=1

        close = result_all['close'].astype('float')
        lable = np.where(close>close.shift(1),1,0) 
        lable = pd.Series(lable)
        up_days =lable.rolling(20).sum()
        PSY = up_days / 20
        PSY = PSY[begin:].reset_index(drop = True)

        dict_stock[date][stock]['PSY'] = PSY
        print(dict_stock[date][stock]['PSY'])

#return_3m
# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)
time0=time.time()
for year in range(2020,2021):
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date_ori1='{}-10-01'.format(year-1)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(year)
            date2=trading_orlast(date_ori2)
        else:
            date2='{}-03-31'.format(year)
        rs = bs.query_history_k_data_plus(code,
            "date,close",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        roc=ta.ROC(result['close'],timeperiod=60)*0.01
        roc=roc[begin:].reset_index(drop=True)
        dict_stock[year][stock]=pd.concat([df,roc],axis=1)
        dict_stock[year][stock].rename(columns={0:'return_3m'},inplace=True)        
                    
time1=time.time()
print(time1-time0)

#return_6m
# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)
time0=time.time()
for year in dict_stock:
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date_ori1='{}-07-01'.format(year-1)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(year)
            date2=trading_orlast(date_ori2)
        else:
            date2='{}-03-31'.format(year+1)

        rs = bs.query_history_k_data_plus(code,
            "date,close",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        
        # fill NAN
        result['close'] = result['close'].fillna(method='ffill')
            
        roc=ta.ROC(result['close'],timeperiod=120)*0.01
        roc=roc[begin:].reset_index(drop=True)
        dict_stock[year][stock]=pd.concat([df,roc],axis=1)
        dict_stock[year][stock].rename(columns={0:'return_6m'},inplace=True)        
                    
time1=time.time()
print(time1-time0)

#wgt_return_1m
time0=time.time()
for year in range(2020,2021):
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date_ori1='{}-12-01'.format(year-1)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(year)
            date2=trading_orlast(date_ori2)
        else:
            date2='{}-03-31'.format(year)

        rs = bs.query_history_k_data_plus(code,
            "date,turn,pctChg",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        avelist=[]
        for i in range(begin,len(result)):
            list1=[]
            for j in range(i-21,i+1):
                a=float(result.iloc[j]['pctChg']) 
                b=float(result.iloc[j]['turn']) if isfloat(result.iloc[j]['turn']) else 0
                c=a*b
                list1.append(c)
            ave=np.mean(list1)
            avelist.append(ave)
        avedf=DataFrame(avelist,columns=['wgt_return_1m'])
        dict_stock[year][stock]=pd.concat([df,avedf],axis=1)
time1=time.time()
print(time1-time0)

#wgt_return_3m
time0=time.time()
for year in dict_stock:
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date_ori1='{}-10-01'.format(year-1)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(year)
            date2=trading_orlast(date_ori2)
        else:
            date2='{}-03-31'.format(year+1)

        rs = bs.query_history_k_data_plus(code,
            "date,turn,pctChg",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        avelist=[]
        for i in range(begin,len(result)):
            list1=[]
            for j in range(i-119,i+1):
                a=float(result.iloc[j]['pctChg']) 
                b=float(result.iloc[j]['turn']) if isfloat(result.iloc[j]['turn']) else 0
                c=a*b
                list1.append(c)
            ave=np.mean(list1)
            avelist.append(ave)
        avedf=DataFrame(avelist,columns=['wgt_return_3m'])
        dict_stock[year][stock]=pd.concat([df,avedf],axis=1)
time1=time.time()
print(time1-time0)

#wgt_return_3m
time0=time.time()
print(year,'-----')
for year in range(2020,2021):
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date_ori1='{}-10-01'.format(year-1)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(year)
            date2=trading_orlast(date_ori2)
        else:
            date2='{}-03-31'.format(year+1)

        rs = bs.query_history_k_data_plus(code,
            "date,turn,pctChg",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        avelist=[]
        for i in range(begin,len(result)):
            list1=[]
            for j in range(i-119,i+1):
                a=float(result.iloc[j]['pctChg']) 
                b=float(result.iloc[j]['turn']) if isfloat(result.iloc[j]['turn']) else 0
                c=a*b
                list1.append(c)
            ave=np.mean(list1)
            avelist.append(ave)
        avedf=DataFrame(avelist,columns=['wgt_return_3m'])
        dict_stock[year][stock]=pd.concat([df,avedf],axis=1)
time1=time.time()
print(time1-time0)

#wgt_return_6m
time0=time.time()
for year in range(2020,2021):
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date_ori1='{}-07-01'.format(year-1)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(year)
            date2=trading_orlast(date_ori2)
        else:
            date2='{}-03-31'.format(year)

        rs = bs.query_history_k_data_plus(code,
            "date,turn,pctChg",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        avelist=[]
        for i in range(begin,len(result)):
            list1=[]
            for j in range(i-119,i+1):
                a=float(result.iloc[j]['pctChg']) 
                b=float(result.iloc[j]['turn']) if isfloat(result.iloc[j]['turn']) else 0
                c=a*b
                list1.append(c)
            ave=np.mean(list1)
            avelist.append(ave)
        avedf=DataFrame(avelist,columns=['wgt_return_6m'])
        dict_stock[year][stock]=pd.concat([df,avedf],axis=1)
time1=time.time()
print(time1-time0)

#wgt_return_12m
# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)
time0=time.time()
for year in dict_stock:
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date_ori1='{}-12-31'.format(year-2)
        date1=trading_ornext(date_ori1)
        if year!=2020:
            date_ori2='{}-12-31'.format(year)
            date2=trading_orlast(date_ori2)
        else:
            date2='{}-03-31'.format(year)

        rs = bs.query_history_k_data_plus(code,
            "date,turn,pctChg",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        avelist=[]
        for i in range(begin,len(result)):
            list1=[]
            for j in range(i-238,i+1):
                a=float(result.iloc[j]['pctChg']) 
                b=float(result.iloc[j]['turn']) if isfloat(result.iloc[j]['turn']) else 0
                c=a*b
                list1.append(c)
            ave=np.mean(list1)
            avelist.append(ave)
        avedf=DataFrame(avelist,columns=['wgt_return_12m'])
        dict_stock[year]['wgt_return_12m']=pd.concat([df,avedf],axis=1)
time1=time.time()
print(time1-time0)

#ROE_G_q（已修改）
time0=time.time()
for year in dict_stock:
    for stock,df in dict_stock[year].items():
        print(year,'-----')
        print(stock)
        code=stock[-9:]
        growth_list1 = []
        growth_list2 = []
        growth_list3 = []
        growth_list4 = []
        rs_growth1 = bs.query_growth_data(code=code, year=year, quarter=1)
        while (rs_growth1.error_code == '0') & rs_growth1.next():
            growth_list1.append(rs_growth1.get_row_data())
        result_growth1 = pd.DataFrame(growth_list1, columns=rs_growth1.fields)
        rs_growth2 = bs.query_growth_data(code=code, year=year, quarter=2)
        while (rs_growth2.error_code == '0') & rs_growth2.next():
            growth_list2.append(rs_growth2.get_row_data())
        result_growth2 = pd.DataFrame(growth_list2, columns=rs_growth2.fields)
        rs_growth3 = bs.query_growth_data(code=code, year=year-1, quarter=3)
        while (rs_growth3.error_code == '0') & rs_growth3.next():
            growth_list3.append(rs_growth3.get_row_data())
        result_growth3 = pd.DataFrame(growth_list3, columns=rs_growth3.fields)
        rs_growth4 = bs.query_growth_data(code=code, year=year-1, quarter=4)
        while (rs_growth4.error_code == '0') & rs_growth4.next():
            growth_list4.append(rs_growth4.get_row_data())
        result_growth4 = pd.DataFrame(growth_list4, columns=rs_growth4.fields)
        b1=float(result_growth1['YOYEquity']) if test(result_growth1,'YOYEquity') else 0
        b2=float(result_growth2['YOYEquity']) if test(result_growth2,'YOYEquity') else 0
        b3=float(result_growth3['YOYEquity']) if test(result_growth3,'YOYEquity') else 0
        b4=float(result_growth4['YOYEquity']) if test(result_growth4,'YOYEquity') else 0
        a1=float(result_growth1['YOYPNI']) if test(result_growth1,'YOYPNI') else 0
        a2=float(result_growth2['YOYPNI']) if test(result_growth2,'YOYPNI') else 0
        a3=float(result_growth3['YOYPNI']) if test(result_growth3,'YOYPNI') else 0
        a4=float(result_growth4['YOYPNI']) if test(result_growth4,'YOYPNI') else 0
        rate1=(1+a1)/(1+b1)-1
        rate2=(1+a2)/(1+b2)-1
        rate3=(1+a3)/(1+b3)-1
        rate4=(1+a4)/(1+b4)-1
        df['ROE_G_q']=None                
        for i in range(len(df)):
            if qua(df.iloc[i]['calendar_date'])==1:
                dict_stock[year][stock]['ROE_G_q'][i]=rate3
            elif qua(df.iloc[i]['calendar_date'])==2:
                dict_stock[year][stock]['ROE_G_q'][i]=rate4
            elif qua(df.iloc[i]['calendar_date'])==3:
                dict_stock[year][stock]['ROE_G_q'][i]=rate1
            elif qua(df.iloc[i]['calendar_date'])==4:
                dict_stock[year][stock]['ROE_G_q'][i]=rate2
time1=time.time()
print(time1-time0)
###########
time0=time.time()
year=2020
for stock,df in dict2020[2020].items():
    print(stock)
    code=stock[-9:]
    growth_list1 = []
    growth_list2 = []
    growth_list3 = []
    growth_list4 = []
    rs_growth3 = bs.query_growth_data(code=code, year=year-1, quarter=3)
    while (rs_growth3.error_code == '0') & rs_growth3.next():
        growth_list3.append(rs_growth3.get_row_data())
    result_growth3 = pd.DataFrame(growth_list3, columns=rs_growth3.fields)
    b3=float(result_growth3['YOYEquity']) if test(result_growth3,'YOYEquity') else 0
    a3=float(result_growth3['YOYPNI']) if test(result_growth3,'YOYPNI') else 0
    rate3=(1+a3)/(1+b3)-1
    df['ROE_G_q']=None                
    for i in range(len(df)):
        dict2020[2020][stock]['ROE_G_q'][i]=rate3
time1=time.time()
print(time1-time0)

#OCF_G_q（已修改）
time0=time.time()
for year in dict_stock:
    for stock,df in dict_stock[year].items():
        print(year,'-----')
        print(stock)
        code=stock[-9:]
        cash_flow_list1 = []
        cash_flow_list2 = []
        cash_flow_list3 = []
        cash_flow_list4 = []
        rs_cash_flow1 = bs.query_cash_flow_data(code=code, year=year, quarter=1)
        while (rs_cash_flow1.error_code == '0') & rs_cash_flow1.next():
            cash_flow_list1.append(rs_cash_flow1.get_row_data())
        result_cash_flow1 = pd.DataFrame(cash_flow_list1, columns=rs_cash_flow1.fields)        
        rs_cash_flow2 = bs.query_cash_flow_data(code=code, year=year, quarter=2)
        while (rs_cash_flow2.error_code == '0') & rs_cash_flow2.next():
            cash_flow_list2.append(rs_cash_flow2.get_row_data())
        result_cash_flow2 = pd.DataFrame(cash_flow_list2, columns=rs_cash_flow2.fields)        
        rs_cash_flow3 = bs.query_cash_flow_data(code=code, year=year-1, quarter=3)
        while (rs_cash_flow3.error_code == '0') & rs_cash_flow3.next():
            cash_flow_list3.append(rs_cash_flow3.get_row_data())
        result_cash_flow3 = pd.DataFrame(cash_flow_list3, columns=rs_cash_flow3.fields)        
        rs_cash_flow4 = bs.query_cash_flow_data(code=code, year=year-1, quarter=4)
        while (rs_cash_flow4.error_code == '0') & rs_cash_flow4.next():
            cash_flow_list4.append(rs_cash_flow4.get_row_data())
        result_cash_flow4 = pd.DataFrame(cash_flow_list4, columns=rs_cash_flow4.fields)        
        CFOToNP1=float(result_cash_flow1['CFOToNP']) if test(result_cash_flow1,'CFOToNP') else 0
        CFOToNP2=float(result_cash_flow2['CFOToNP']) if test(result_cash_flow2,'CFOToNP') else 0
        CFOToNP3=float(result_cash_flow3['CFOToNP']) if test(result_cash_flow3,'CFOToNP') else 0
        CFOToNP4=float(result_cash_flow4['CFOToNP']) if test(result_cash_flow4,'CFOToNP') else 0
        profit_list1 = []
        profit_list2 = []
        profit_list3 = []
        profit_list4 = []
        rs_profit1 = bs.query_profit_data(code=code, year=year, quarter=1)
        while (rs_profit1.error_code == '0') & rs_profit1.next():
            profit_list1.append(rs_profit1.get_row_data())
        result_profit1 = pd.DataFrame(profit_list1, columns=rs_profit1.fields)        
        rs_profit2 = bs.query_profit_data(code=code, year=year, quarter=2)
        while (rs_profit2.error_code == '0') & rs_profit2.next():
            profit_list2.append(rs_profit2.get_row_data())
        result_profit2 = pd.DataFrame(profit_list2, columns=rs_profit2.fields)        
        rs_profit3 = bs.query_profit_data(code=code, year=year-1, quarter=3)
        while (rs_profit3.error_code == '0') & rs_profit3.next():
            profit_list3.append(rs_profit3.get_row_data())
        result_profit3 = pd.DataFrame(profit_list3, columns=rs_profit3.fields)        
        rs_profit4 = bs.query_profit_data(code=code, year=year-1, quarter=4)
        while (rs_profit4.error_code == '0') & rs_profit4.next():
            profit_list4.append(rs_profit4.get_row_data())
        result_profit4 = pd.DataFrame(profit_list4, columns=rs_profit4.fields)
        netProfit1=float(result_profit1['netProfit']) if test(result_profit1,'netProfit') else 0        
        netProfit2=float(result_profit2['netProfit']) if test(result_profit2,'netProfit') else 0        
        netProfit3=float(result_profit3['netProfit']) if test(result_profit3,'netProfit') else 0        
        netProfit4=float(result_profit4['netProfit']) if test(result_profit4,'netProfit') else 0        
        OCF1=CFOToNP1*netProfit1
        OCF2=CFOToNP2*netProfit2
        OCF3=CFOToNP3*netProfit3
        OCF4=CFOToNP4*netProfit4

        cash_flow_list1 = []
        cash_flow_list2 = []
        cash_flow_list3 = []
        cash_flow_list4 = []
        rs_cash_flow1 = bs.query_cash_flow_data(code=code, year=year-1, quarter=1)
        while (rs_cash_flow1.error_code == '0') & rs_cash_flow1.next():
            cash_flow_list1.append(rs_cash_flow1.get_row_data())
        result_cash_flow1 = pd.DataFrame(cash_flow_list1, columns=rs_cash_flow1.fields)        
        rs_cash_flow2 = bs.query_cash_flow_data(code=code, year=year-1, quarter=2)
        while (rs_cash_flow2.error_code == '0') & rs_cash_flow2.next():
            cash_flow_list2.append(rs_cash_flow2.get_row_data())
        result_cash_flow2 = pd.DataFrame(cash_flow_list2, columns=rs_cash_flow2.fields)        
        rs_cash_flow3 = bs.query_cash_flow_data(code=code, year=year-2, quarter=3)
        while (rs_cash_flow3.error_code == '0') & rs_cash_flow3.next():
            cash_flow_list3.append(rs_cash_flow3.get_row_data())
        result_cash_flow3 = pd.DataFrame(cash_flow_list3, columns=rs_cash_flow3.fields)        
        rs_cash_flow4 = bs.query_cash_flow_data(code=code, year=year-2, quarter=4)
        while (rs_cash_flow4.error_code == '0') & rs_cash_flow4.next():
            cash_flow_list4.append(rs_cash_flow4.get_row_data())
        result_cash_flow4 = pd.DataFrame(cash_flow_list4, columns=rs_cash_flow4.fields)        
        CFOToNP1=float(result_cash_flow1['CFOToNP']) if test(result_cash_flow1,'CFOToNP') else 0
        CFOToNP2=float(result_cash_flow2['CFOToNP']) if test(result_cash_flow2,'CFOToNP') else 0
        CFOToNP3=float(result_cash_flow3['CFOToNP']) if test(result_cash_flow3,'CFOToNP') else 0
        CFOToNP4=float(result_cash_flow4['CFOToNP']) if test(result_cash_flow4,'CFOToNP') else 0
        profit_list1 = []
        profit_list2 = []
        profit_list3 = []
        profit_list4 = []
        rs_profit1 = bs.query_profit_data(code=code, year=year-1, quarter=1)
        while (rs_profit1.error_code == '0') & rs_profit1.next():
            profit_list1.append(rs_profit1.get_row_data())
        result_profit1 = pd.DataFrame(profit_list1, columns=rs_profit1.fields)        
        rs_profit2 = bs.query_profit_data(code=code, year=year-1, quarter=2)
        while (rs_profit2.error_code == '0') & rs_profit2.next():
            profit_list2.append(rs_profit2.get_row_data())
        result_profit2 = pd.DataFrame(profit_list2, columns=rs_profit2.fields)        
        rs_profit3 = bs.query_profit_data(code=code, year=year-2, quarter=3)
        while (rs_profit3.error_code == '0') & rs_profit3.next():
            profit_list3.append(rs_profit3.get_row_data())
        result_profit3 = pd.DataFrame(profit_list3, columns=rs_profit3.fields)        
        rs_profit4 = bs.query_profit_data(code=code, year=year-2, quarter=4)
        while (rs_profit4.error_code == '0') & rs_profit4.next():
            profit_list4.append(rs_profit4.get_row_data())
        result_profit4 = pd.DataFrame(profit_list4, columns=rs_profit4.fields)
        netProfit1=float(result_profit1['netProfit']) if test(result_profit1,'netProfit') else 0        
        netProfit2=float(result_profit2['netProfit']) if test(result_profit2,'netProfit') else 0        
        netProfit3=float(result_profit3['netProfit']) if test(result_profit3,'netProfit') else 0        
        netProfit4=float(result_profit4['netProfit']) if test(result_profit4,'netProfit') else 0        
        OCF1_=CFOToNP1*netProfit1
        OCF2_=CFOToNP2*netProfit2
        OCF3_=CFOToNP3*netProfit3
        OCF4_=CFOToNP4*netProfit4
        rate1=OCF1/OCF1_-1 if OCF1_!=0 else 0
        rate2=OCF2/OCF2_-1 if OCF2_!=0 else 0
        rate3=OCF3/OCF3_-1 if OCF3_!=0 else 0
        rate4=OCF4/OCF4_-1 if OCF4_!=0 else 0
        df['OCF_G_q']=None                
        for i in range(len(df)):
            if qua(df.iloc[i]['calendar_date'])==1:
                dict_stock[year][stock]['OCF_G_q'][i]=rate3
            elif qua(df.iloc[i]['calendar_date'])==2:
                dict_stock[year][stock]['OCF_G_q'][i]=rate4
            elif qua(df.iloc[i]['calendar_date'])==3:
                dict_stock[year][stock]['OCF_G_q'][i]=rate1
            elif qua(df.iloc[i]['calendar_date'])==4:
                dict_stock[year][stock]['OCF_G_q'][i]=rate1
time1=time.time()
print(time1-time0)
#####################
time0=time.time()
year=2020
for stock,df in dict2020[2020].items():
    print(stock)
    code=stock[-9:]
    cash_flow_list3 = []
    rs_cash_flow3 = bs.query_cash_flow_data(code=code, year=year-1, quarter=3)
    while (rs_cash_flow3.error_code == '0') & rs_cash_flow3.next():
        cash_flow_list3.append(rs_cash_flow3.get_row_data())
    result_cash_flow3 = pd.DataFrame(cash_flow_list3, columns=rs_cash_flow3.fields)        
    CFOToNP3=float(result_cash_flow3['CFOToNP']) if test(result_cash_flow3,'CFOToNP') else 0
    profit_list3 = []
    rs_profit3 = bs.query_profit_data(code=code, year=year-1, quarter=3)
    while (rs_profit3.error_code == '0') & rs_profit3.next():
        profit_list3.append(rs_profit3.get_row_data())
    result_profit3 = pd.DataFrame(profit_list3, columns=rs_profit3.fields)        
    netProfit3=float(result_profit3['netProfit']) if test(result_profit3,'netProfit') else 0        
    OCF3=CFOToNP3*netProfit3

    cash_flow_list3 = []
    rs_cash_flow3 = bs.query_cash_flow_data(code=code, year=year-2, quarter=3)
    while (rs_cash_flow3.error_code == '0') & rs_cash_flow3.next():
        cash_flow_list3.append(rs_cash_flow3.get_row_data())
    result_cash_flow3 = pd.DataFrame(cash_flow_list3, columns=rs_cash_flow3.fields)        
    CFOToNP3=float(result_cash_flow3['CFOToNP']) if test(result_cash_flow3,'CFOToNP') else 0
    profit_list3 = []
    rs_profit3 = bs.query_profit_data(code=code, year=year-2, quarter=3)
    while (rs_profit3.error_code == '0') & rs_profit3.next():
        profit_list3.append(rs_profit3.get_row_data())
    result_profit3 = pd.DataFrame(profit_list3, columns=rs_profit3.fields)        
    netProfit3=float(result_profit3['netProfit']) if test(result_profit3,'netProfit') else 0        
    OCF3_=CFOToNP3*netProfit3
    rate3=OCF3/OCF3_-1 if OCF3_!=0 else 0
    df['OCF_G_q']=None                
    for i in range(len(df)):
        dict2020[2020][stock]['OCF_G_q'][i]=rate3
time1=time.time()
print(time1-time0)


#DEA
time0=time.time()
for year in dict_stock:
    for stock,df in dict_stock[year].items():
        df=dict_stock[year][stock]
        print(year,'-----')
        print(stock)
        code=stock[-9:]
        date1='{}-10-25'.format(year-1)
        print(date1)
        date2='{}-12-31'.format(year)
        rs = bs.query_history_k_data_plus(code,"date,close",
        start_date=date1, end_date=date2,frequency="d", adjustflag="3")
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)   
        begin=0
        for i in range(len(result)):
            if result['date'][i][:4]!=str(year):
                begin+=1
        close=result['close']
        dif, dea, hist = ta.MACD(close,fastperiod=10,slowperiod=30,signalperiod=15) 
        dea=dea[begin:].reset_index(drop=True)
        dict_stock[year][stock]=pd.concat([df,dea],axis=1)
        dict_stock[year][stock].rename(columns={0:'DEA'},inplace=True)        
            
time1=time.time()
print(time1-time0)
   ######################
time0=time.time()
year=2020
for stock,df in dict2020[2020].items():
    print(stock)
    code=stock[-9:]
    date1='{}-10-25'.format(year-1)
    date2='{}-03-31'.format(year)
    rs = bs.query_history_k_data_plus(code,"date,close",
    start_date=date1, end_date=date2,frequency="d", adjustflag="3")
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)   
    begin=0
    for i in range(len(result)):
        if result['date'][i][:4]!=str(year):
            begin+=1
    close=result['close']
    dif, dea, hist = ta.MACD(close,fastperiod=10,slowperiod=30,signalperiod=15) 
    dea=dea[begin:].reset_index(drop=True)
    dict2020[2020][stock]=pd.concat([df,dea],axis=1)
    dict2020[2020][stock].rename(columns={0:'DEA'},inplace=True)        
            
time1=time.time()
print(time1-time0)


#ln_price

time0=time.time()
for year in dict_stock:
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date1='{}-01-01'.format(year)
        date2='{}-12-31'.format(year)
        rs = bs.query_history_k_data_plus(code,
        "close",start_date=date1, end_date=date2,
        frequency="d", adjustflag="3")
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        result['close']=result['close'].apply(lambda x:math.log(float(x)))
        dict_stock[year][stock]=pd.concat([df,result],axis=1)
        dict_stock[year][stock].rename(columns={'close':'ln_price'},inplace=True) 

time1=time.time()
print(time1-time0)
#####################        
time0=time.time()
year=2020
for stock,df in dict2020[year].items():
    print(stock)
    code=stock[-9:]
    date1='{}-01-01'.format(year)
    date2='{}-03-31'.format(year)
    rs = bs.query_history_k_data_plus(code,
    "close",start_date=date1, end_date=date2,
    frequency="d", adjustflag="3")
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    result['close']=result['close'].apply(lambda x:math.log(float(x)))
    dict2020[year][stock]=pd.concat([df,result],axis=1)
    dict2020[year][stock].rename(columns={'close':'ln_price'},inplace=True) 

time1=time.time()
print(time1-time0)

#std_1m (20 trading days)
time0=time.time()
for year in dict_stock:
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date1='{}-12-01'.format(year-1)
        date2='{}-12-31'.format(year)
        rs = bs.query_history_k_data_plus(code,
            "date,pctChg",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        stdlist=[]
        for i in range(begin,len(result)):
            list1=[]
            for j in range(i-19,i+1):
                a=float(result.iloc[j]['pctChg'])
                list1.append(a)
            std=np.std(list1)
            stdlist.append(std)
        stddf=DataFrame(stdlist,columns=['std_1m'])
        dict_stock[year][stock]=pd.concat([df,stddf],axis=1)
time1=time.time()
print(time1-time0)
#####################
time0=time.time()
year=2020
for stock,df in dict2020[year].items():
    print(stock)
    code=stock[-9:]
    date1='{}-12-01'.format(year-1)
    date2='{}-03-31'.format(year)
    rs = bs.query_history_k_data_plus(code,
        "date,pctChg",start_date=date1, end_date=date2,
        frequency="d", adjustflag="2")        
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)     
    begin=0
    for i in range(len(result)):
        if result['date'][i][5:7]=='01':
            begin=i
            break
    stdlist=[]
    for i in range(begin,len(result)):
        list1=[]
        for j in range(i-19,i+1):
            a=float(result.iloc[j]['pctChg'])
            list1.append(a)
        std=np.std(list1)
        stdlist.append(std)
    stddf=DataFrame(stdlist,columns=['std_1m'])
    dict2020[year][stock]=pd.concat([df,stddf],axis=1)
time1=time.time()
print(time1-time0)

#return_3m
time0=time.time()
for year in dict_stock:
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date1='{}-10-01'.format(year-1)
        date2='{}-12-31'.format(year)
        rs = bs.query_history_k_data_plus(code,
            "date,close",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        roc=ta.ROC(result['close'],timeperiod=60)*0.01
        roc=roc[begin:].reset_index(drop=True)
        dict_stock[year][stock]=pd.concat([df,roc],axis=1)
        dict_stock[year][stock].rename(columns={0:'return_3m'},inplace=True)        
                    
time1=time.time()
print(time1-time0)

#return_6m
time0=time.time()
for year in dict_stock:
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date1='{}-07-01'.format(year-1)
        date2='{}-12-31'.format(year)
        rs = bs.query_history_k_data_plus(code,
            "date,close",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        roc=ta.ROC(result['close'],timeperiod=120)*0.01
        roc=roc[begin:].reset_index(drop=True)
        dict_stock[year][stock]=pd.concat([df,roc],axis=1)
        dict_stock[year][stock].rename(columns={0:'return_6m'},inplace=True)        
                    
time1=time.time()
print(time1-time0)

#wgt_return_6m
time0=time.time()
for year in dict_stock:
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date1='{}-07-01'.format(year-1)
        date2='{}-12-31'.format(year)
        rs = bs.query_history_k_data_plus(code,
            "date,turn,pctChg",start_date=date1, end_date=date2,
            frequency="d", adjustflag="2")        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)     
        begin=0
        for i in range(len(result)):
            if result['date'][i][5:7]=='01':
                begin=i
                break
        avelist=[]
        for i in range(begin,len(result)):
            list1=[]
            for j in range(i-119,i+1):
                a=float(result.iloc[j]['pctChg']) 
                b=float(result.iloc[j]['turn']) if isfloat(result.iloc[j]['turn']) else 0
                c=a*b
                list1.append(c)
            ave=np.mean(list1)
            avelist.append(ave)
        avedf=DataFrame(avelist,columns=['wgt_return_6m'])
        dict_stock[year][stock]=pd.concat([df,avedf],axis=1)
time1=time.time()
print(time1-time0)
###################
time0=time.time()
for stock,df in dict2020[year].items():
    print(stock)
    code=stock[-9:]
    date1='{}-07-01'.format(year-1)
    date2='{}-03-31'.format(year)
    rs = bs.query_history_k_data_plus(code,
        "date,turn,pctChg",start_date=date1, end_date=date2,
        frequency="d", adjustflag="2")        
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)     
    begin=0
    for i in range(len(result)):
        if result['date'][i][5:7]=='01':
            begin=i
            break
    avelist=[]
    for i in range(begin,len(result)):
        list1=[]
        for j in range(i-119,i+1):
            a=float(result.iloc[j]['pctChg']) 
            b=float(result.iloc[j]['turn']) if isfloat(result.iloc[j]['turn']) else 0
            c=a*b
            list1.append(c)
        ave=np.mean(list1)
        avelist.append(ave)
    avedf=DataFrame(avelist,columns=['wgt_return_6m'])
    dict2020[year][stock]=pd.concat([df,avedf],axis=1)
time1=time.time()
print(time1-time0)

#停牌信号
lg=bs.login()
for year in dict_stock:
    print(year,'-----')
    for stock,df in dict_sotck[year].items():
        print(stock)
        code=stock[-9:]
        date1='{}-01-01'.format(year)
        date2='{}-12-31'.format(year)
        rs = bs.query_history_k_data_plus(code,
        "tradestatus,isST",
        start_date=date1, end_date=date2,
        frequency="d", adjustflag="2")
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        dict_stock[year][stock]=pd.concat([df,result],axis=1)
for stock,df in dict_stock[2020].items():
    dict_stock[2020][stock]=df[0:58]
    
#加入收益率pctChg
lg=bs.login()
for year in dict_stock:
    print(year,'-----')
    for stock,df in dict_stock[year].items():
        print(stock)
        code=stock[-9:]
        date1='{}-01-01'.format(year)
        if year!=2020:
            date2='{}-12-31'.format(year)
        else:
            date2='2020-03-31'             
        rs = bs.query_history_k_data_plus(code,
        "pctChg",
        start_date=date1, end_date=date2,
        frequency="d", adjustflag="2")
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        dict_stock[year][stock]=pd.concat([df,result],axis=1)
        
for d in range(3,6):
    for gain in range(0,8):
        for year in dict_stock:
            print(year)
            print(d)
            print(gain)
            for stock,df in dict_stock[year].items():
                exec('d{}_{}=[]'.format(d,gain))   
                s_preclose=df['preclose']
                s_close=df['close']
                for i in range(len(s_preclose)):
                    if i < len(s_preclose)-d:
                        if float(s_close[i+d])/float(s_preclose[i+1])-1>=gain*0.01:
                            exec('d{}_{}.append(1)'.format(d,gain))
                        else:
                            exec('d{}_{}.append(0)'.format(d,gain))
                    else:
                        break
                exec('d{}_{}_=pd.Series(d{}_{})'.format(d,gain,d,gain))
                exec('dict_stock[year][stock]=pd.concat([df,d{}_{}_],axis=1)'.format(d,gain))
                dict_stock[year][stock].rename(columns={0:'d{}_{}_'.format(d,gain)},inplace=True)


f=open('dict_factors.pickle','wb')
pickle.dump(dict_stock,f)
f.close()

