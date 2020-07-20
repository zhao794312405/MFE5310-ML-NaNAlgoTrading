import pickle
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler

f1 = open('dict_factors.pickle', 'rb')
all_data = pickle.load(f1)

dict_code_all = dict()
for year in all_data:
    ticker_df = dict()
    for ticker, df in all_data[year].items():
        code = ticker[-9:]
        temp_df = dict()
        temp_df[code] = df
        ticker_df.update(temp_df)

    temp_ticker_df = dict()
    temp_ticker_df[year] = ticker_df
    dict_code_all.update(temp_ticker_df)

    #     columns = [ 'calendar_date', 'ROE_G_q', 'OCF_G_q', 'DEA', 'ln_price', 'std_1m',
    #        'return_3m', 'return_6m', 'wgt_return_6m', 'peTTM', 'Profit_G_q',
    #        'MBRevenue', 'G/PE', 'Sales_G_q', 'BIAS', 'DIF', 'MACD', 'epsTTM',
    #        'dividCashPs', 'DP', 'turn', 'turn_3m', 'beta', 'RSI', 'DE', 'PSY',
    #        'wgt_return_12m', 'open', 'high', 'low', 'close', 'tradestatus', 'isST',
    #        'preclose', 'd3_0_', 'd3_1_', 'd3_2_', 'd3_3_', 'd3_4_', 'd3_5_',
    #        'd3_6_', 'd3_7_', 'd4_0_', 'd4_1_', 'd4_2_', 'd4_3_', 'd4_4_', 'd4_5_',
    #        'd4_6_', 'd4_7_', 'd5_0_', 'd5_1_', 'd5_2_', 'd5_3_', 'd5_4_', 'd5_5_',
    #        'd5_6_', 'd5_7_', 'pctChg']

factor = ['ROE_G_q', 'OCF_G_q', 'DEA', 'ln_price', 'std_1m',
          'return_3m', 'wgt_return_6m', 'Profit_G_q',
          'MBRevenue', 'G/PE', 'Sales_G_q', 'BIAS', 'DIF', 'MACD', 'epsTTM',
          'dividCashPs', 'DP', 'turn', 'turn_3m', 'beta', 'RSI', 'DE', 'PSY',
          'wgt_return_12m']
sorted_ticker_list = dict()
period = 3
hist_day = 20
sc = MinMaxScaler(feature_range=(0, 1))
for year in dict_code_all:
    # if year != 2016:
    #     continue

    last_code = list(dict_code_all[year - 1].keys()) if year > 2010 else []
    code = list(dict_code_all[year].keys())

    temp_day = dict()
    trade_len = len(dict_code_all[year][code[1]]['calendar_date']) - period
    last_dict = dict()

    if year == 2010:
        start = hist_day
    else:
        start = 0
        for tick, frame in dict_code_all[year - 1].items():  # 上一年的数据框
            last = dict()
            last[tick] = frame[-hist_day:]  # [-9:]
            last_dict.update(last)

    for idx in range(hist_day, trade_len, period):
        print(idx)
        print(code[1])
        day = dict_code_all[year][code[1]].loc[idx + 3]['calendar_date']  # 交易日
        print(day)
        up_possible = []
        count = 0
        up = 0
        for ticker, df in dict_code_all[year].items():
            if df.iloc[idx + period]['tradestatus'] == 0 or df.iloc[idx + period]['isST'] == 1:
                up_possible.append(0)  # 涨的概率为0，相当于剔除
                print('stop or st', len(up_possible))
                continue

            if idx - hist_day >= 0:
                x_train = df.loc[idx - hist_day:idx][factor]
                y_train = df.loc[idx - hist_day:idx]['d3_5_']  # lable
                x_train = sc.fit_transform(x_train)


            else:
                if ticker in last_code:
                    x_train = pd.concat(
                        [last_dict[ticker].iloc[- hist_day - period + idx:-period][factor], df.iloc[:idx][factor]])
                    y_train = pd.concat([last_dict[ticker].iloc[- hist_day - period + idx:-period]['d3_5_'],
                                         df.iloc[:idx]['d3_5_']])  # lable

                    x_train = x_train.fillna(0)
                    x_train = sc.fit_transform(x_train)

                else:
                    up_possible.append(0)
                    print('not have last year data:', len(up_possible))
                    continue

            x_pred = df.iloc[idx + period][factor]
            y_pred = df.iloc[idx + period]['d3_5_'].reshape(1, -1)  # lable

            x_pred = sc.fit_transform(x_pred.values.reshape(-1, len(factor)))

            model = RandomForestClassifier(class_weight='balanced',
                                           criterion='gini', max_depth=60, max_features=4,
                                           min_samples_leaf=17, n_estimators=266,
                                           n_jobs=-1)
            try:
                model.fit(x_train, y_train)
            except:
                up_possible.append(0)
                print('nan or inf', len(up_possible))
                continue

            rf_scores = model.predict_proba(x_pred)[:, 1].tolist()[0] if model.predict_proba(x_pred).shape[1] > 1 else 0
            up_possible.append(rf_scores)
            print(len(up_possible))

        final_result = np.array(up_possible)
        sorted_result = -np.sort(-final_result)  # 降序

        sorted_code = [code[j] for j in np.argsort(-final_result)]  # 返回值为up可能性从大到小的排序list

        temp_df = dict()
        temp_df[day] = pd.DataFrame([sorted_code, sorted_result])
        temp_day.update(temp_df)

    temp_trade_date = dict()
    temp_trade_date[year] = temp_day
    sorted_ticker_list.update(temp_trade_date)

# pickle
output = open('stock_list.pkl', 'wb')
pickle.dump(sorted_ticker_list, output)
output.close()
