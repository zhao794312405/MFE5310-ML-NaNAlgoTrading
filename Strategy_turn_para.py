import warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.ensemble import RandomForestClassifier
from collections import Counter
from sklearn import metrics
from sklearn.model_selection import cross_val_score
from sklearn.metrics import roc_curve, auc
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import GradientBoostingClassifier

import pickle
f1 = open('dict_factors.pickle', 'rb')
all_data = pickle.load(f1)

dict_all = dict()
for year in all_data:
    ticker_df = dict()
    print(year)
    for ticker, df in all_data[year].items():
        code = ticker[-9:]
        temp_df = dict()
        temp_df[code] = df
        ticker_df.update(temp_df)

    temp_ticker_df = dict()
    temp_ticker_df[year] = ticker_df
    dict_all.update(temp_ticker_df)
print('success')

# print(dict_all)
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
       'return_3m', 'wgt_return_6m', 'peTTM', 'Profit_G_q',
       'MBRevenue', 'G/PE', 'Sales_G_q', 'BIAS', 'DIF', 'MACD', 'epsTTM',
       'dividCashPs', 'DP', 'turn', 'turn_3m', 'beta', 'RSI', 'DE', 'PSY',
       'wgt_return_12m']


def get_performance(report, matrix, model, x, y):
    # calculate roc_auc
    fpr = dict()
    tpr = dict()
    roc_auc = dict()
    rf_scores = model.predict_proba(x)[:, 1]
    fpr, tpr, thresholds = roc_curve(y, rf_scores)
    roc_auc_rf = auc(fpr, tpr)

    index = {'-1', '1', 'macro avg', 'weighted avg'}
    test_1 = {key: value for key, value in report.items() if key in index}
    report2 = pd.DataFrame(test_1)
    report3 = pd.DataFrame([report['accuracy'],  roc_auc_rf], index=['accuracy', 'AUC'])
    report4 = pd.DataFrame(matrix, index=['Predicted Positive', 'Predicted Negative'],
                           columns=['Actual Positive', 'Actual Negative'])
    print(report4)
    print('---------------------------------------------------------')
    print(report2)
    print(report3)
    #graph
    plt.figure()
    lw = 2
    plt.plot(fpr, tpr, color='darkorange',
             lw=lw, label='ROC curve (area = %0.2f)' % roc_auc_rf)
    plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.0])
    plt.legend()


def tune_parameter(model, par, cv, X_train, y_train):
    parameters = par
    gsearch = GridSearchCV(estimator=model, param_grid=parameters, scoring='roc_auc', cv=cv, verbose=1, n_jobs=-1, refit=True)
    print('next')
    gsearch.fit(X_train, y_train)

    means = gsearch.cv_results_['mean_test_score']
    params = gsearch.cv_results_['params']
    print('the best params is %r with best score is %f' % (gsearch.best_params_, gsearch.best_score_))
    # print(gsearch)
    best_model = gsearch.best_estimator_(calss_weight='balanced')
    return best_model

# #合并数据
data2020 = dict_all[2020]
datatrain = pd.DataFrame()
datatest = pd.DataFrame()

for ticker, df in data2020.items():
    print(ticker)
    datatrain = pd.concat([datatrain, df.iloc[:20, :]])
    datatest = pd.concat([datatest, df.iloc[[23], :]])
print('success')

# Feature Scaling
col = datatrain.columns[1:]
# print(col)
sc = MinMaxScaler(feature_range = (0, 1))
datatrain_scaled = sc.fit_transform(datatrain.iloc[:, 1:])
datatest_scaled = sc.fit_transform(datatest.iloc[:, 1:])

datatrain_scaled = pd.DataFrame(datatrain_scaled, columns=col)
datatest_scaled = pd.DataFrame(datatest_scaled, columns=col)



X_train, X_test, y_train, y_test = datatrain_scaled[factor], datatest_scaled[factor], datatrain_scaled['d3_3_'], datatest_scaled['d3_3_']  #datatrain['d3_1_']


#check imbalance
c = Counter(y_train)
dic = dict(c)
print(dic)
print(' the balance ratio of this dataset is :%f\n' % (dic[1] / dic[0]))

if dic[1] / dic[0] < 4 and dic[1] / dic[0] > 0.25:
    print('This dataset not have imbanlance issue')
else:
    print('This dataset has imbanlance issue')
print('#####################################')


# # TimeSeriesSplit cv
model = RandomForestClassifier(class_weight='balanced')
tscv = TimeSeriesSplit(n_splits=5)
parameters = {'n_estimators': range(200, 301, 100),
              'max_depth': range(50, 100, 20),
              'min_samples_leaf':range(12,32,10),
              'max_features': range(10, 24, 2)
              }

best_model = tune_parameter(model, parameters, tscv, X_train, y_train)
print(best_model)
rf_predicted = best_model.predict(X_test)

#show result
con_mat = metrics.confusion_matrix(y_test, rf_predicted)
mat_df = metrics.classification_report(y_test, rf_predicted, output_dict=True)
get_performance(mat_df,con_mat, best_model, X_test, y_test)


#因子重要程度排序
importances = best_model.feature_importances_
print("importance：", importances)
# x_columns = url1.columns[1:]
indices = np.argsort(importances)[::-1]  #逆序
sort_factor = []
for f in range(X_train.shape[1]):
    sort_factor.append(factor[indices[f]])
    print("%2d) %-*s %f" % (f + 1, 30, factor[indices[f]], importances[indices[f]]))

#重要性程度可视化
plt.figure(figsize=(10,6))
plt.title("importance of factors",fontsize = 18)
plt.ylabel("import level",fontsize = 15,rotation=90)
plt.rcParams['axes.unicode_minus'] = False
for i in range(len(factor)):
    plt.bar(i,importances[indices[i]],color='orange',align='center')
    plt.xticks(np.arange(len(factor)), sort_factor, rotation=90, fontsize=15)
plt.show()

