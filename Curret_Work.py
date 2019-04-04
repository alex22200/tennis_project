from bs4 import BeautifulSoup as soup
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import sqlite3
from fuzzywuzzy import fuzz, process
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import math
from scipy.stats import trim_mean, kurtosis
from scipy.stats.mstats import mode, gmean, hmean
import seaborn as sns 
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, r2_score
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import numpy as np
import statsmodels.api as sm

os.chdir("C:\\Users\\Alex\\Documents\\Courses\\Tennis_Project2")
db="tennis.db"

df_out2 = pd.read_excel('output_mod.xlsx')

df_out2['Odds1'].sum()
df_out2['Odds2'].sum()
df_out2.groupby('Player1').count()

X = df_out2.iloc[:,[1,2,10,11,13,14,16,18,24,26,27,29,30,31,32,34,35]]
Y = df_out2.iloc[:,0]

df=pd.DataFrame(columns=['Name', 'ID', 'Win', 'Odds', 'Aces', 'BP', 'DF', 'FirstPerc', 'FirstPoints', 'ReturnPoints', 'SecondServePoints', 
                         'TotalPoints', 'Surface', 'Ranking', 'Date'])

#Create rel ranking
df_out2['Rel_Rank 1'] = df_out2['Ranking 1'] / (df_out2['Ranking 1']+df_out2['Ranking 2'])
df_out2['Rel_Rank 2'] = df_out2['Ranking 2'] / (df_out2['Ranking 1']+df_out2['Ranking 2'])

#Create DF for statistics and summaries
df['Name'] = df_out2['Player1'].append(df_out2['Player2'])
df['Win'] = df_out2['Winner'].append(abs(df_out2['Winner']-1))
df['ID'] = df_out2['ID Player 1'].append(df_out2['ID Player 2'])
df['Odds'] = df_out2['Odds1'].append(df_out2['Odds2'])
df['Aces'] = df_out2['Aces 1'].append(df_out2['Aces 2'])
df['BP'] = df_out2['Break Points 1'].append(df_out2['Break Points 2'])
df['DF'] = df_out2['Double Faults 1'].append(df_out2['Double Faults 2'])
df['FirstPerc'] = df_out2['First Serve Percentage 1'].append(df_out2['First Serve Percentage 2'])
df['FirstPoints'] = df_out2['First Serve Points Won 1'].append(df_out2['First Serve Points Won 2'])
df['ReturnPoints'] = df_out2['Return Points Won 1'].append(df_out2['Return Points Won 2'])
df['SecondServePoints'] = df_out2['Second Serve Points Won 1'].append(df_out2['Second Serve Points Won 2'])
df['Surface'] = df_out2['Surface'].append(df_out2['Surface'])
df['Ranking'] = df_out2['Ranking 1'].append(df_out2['Ranking 2'])
df['TotalPoints'] = df_out2['Total Points Won 1'].append(df_out2['Total Points Won 2'])
df['Date'] = df_out2['Date'].append(df_out2['Date'])
df['Rel_Rank'] = df_out2['Rel_Rank 1'].append(df_out2['Rel_Rank 2'])
df['Rel_Aces'] = df['Rel_Rank'] * df['Aces']
df['Rel_FirstServe'] = df['Rel_Rank'] * df['FirstPerc']
df['Rel_FirstPoints'] = df['Rel_Rank'] * df['FirstPoints']
df['Rel_FirstServe'] = df['Rel_Rank'] * df['FirstPerc']
df['Rel_DoubleF'] = df['Rel_Rank'] * df['DF']
df['Rel_Return'] = df['Rel_Rank'] * df['ReturnPoints']
df['Rel_Total'] = df['Rel_Rank'] * df['TotalPoints']





for i in range(0, len(df_out2['Odds1'])):
    print(i)
    df_out2.at[i, 'Relative Serve Strength 1'] = (df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 1'] == df['ID'])]['Rel_Aces'].mean() 
    - df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 1'] == df['ID'])]['Rel_DoubleF'].mean())
    df_out2.at[i, 'Relative Serve Strength 2'] = (df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 2'] == df['ID'])]['Rel_Aces'].mean() 
    - df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 2'] == df['ID'])]['Rel_DoubleF'].mean())
    
    df_out2.at[i, 'Average Return Strength 1'] = (df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 1'] == df['ID'])]['ReturnPoints'].mean()) 
    df_out2.at[i, 'Average Return Strength 2'] = (df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 2'] == df['ID'])]['ReturnPoints'].mean())
    
    df_out2.at[i, 'Average Serve Strength 1'] = (df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 1'] == df['ID'])]['Aces'].mean()) 
    df_out2.at[i, 'Average Serve Strength 2'] = (df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 2'] == df['ID'])]['Aces'].mean())
    
    df_out2.at[i, 'Relative Play Strength 1'] = (df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 1'] == df['ID'])]['Rel_Total'].mean()) 
    df_out2.at[i, 'Relative Play Strength 2'] = (df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 2'] == df['ID'])]['Rel_Total'].mean())
    
    df_out2.at[i, 'Average Play Strength 1'] = (df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 1'] == df['ID'])]['TotalPoints'].mean()) 
    df_out2.at[i, 'Average Play Strength 2'] = (df[(df['Date'] < df_out2.at[i, 'Date']) & (df_out2.at[i, 'ID Player 2'] == df['ID'])]['TotalPoints'].mean())
    


df_out2.columns
#summary stats by ID
df.groupby('ID').describe()

df[df['Win']==1]['Ranking'].mean()
df[df['Win']==0]['Ranking'].mean()

df.groupby('ID')['Aces'].mean()
df.groupby('ID')['Aces'].mean()


sns.distplot(df['Odds'])
sns.jointplot(x='Odds', y='Ranking', data=df)

writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
df_out2.to_excel(writer, sheet_name='Sheet1', index = False)
writer.save()

df = pd.read_excel('output_mod.xlsx')

df_out3 = df_out2.dropna(subset=['Age1','Age2', 'Relative Serve Strength 1', 'Relative Serve Strength 2', 'Average Return Strength 1', 'Average Return Strength 2',
                                 'Relative Play Strength 1', 'Relative Play Strength 2'])

df_out2.columns
#assign X/y
X=df_out3.iloc[:,[1, 2, 6, 35, 36, 39, 40, 41, 42, 45, 46]].values
y=df_out3.iloc[:,0].values

#Preprocess + Scale
X_train, X_test, y_train, y_test=train_test_split(X,y)
sc_X=StandardScaler()
X_train = X_train[:,[0,1,3,4,5,6,7,8, 9, 10]]
mid = X_test[:,2]
X_test = X_test[:,[0,1,3,4,5,6,7,8,9, 10]]
X_test=sc_X.fit_transform(X_test)
X_train=sc_X.fit_transform(X_train)


#Regress Logistics
log=LogisticRegression()
log.fit(X_train,y_train)
y_pred=log.predict_proba(X_test)
y_pred_res=log.predict(X_test)
r2_score(y_test, y_pred_res)
y_test
y_pred_res

coef = log.coef_

df_out = pd.DataFrame({'Result':y_test, 'Prediction':y_pred_res ,'Prob 1':y_pred[:,0], 'Prob 2':y_pred[:,1],
                       'Age 1':X_test[:,0], 'Age 2':X_test[:,1], 'RSS1':X_test[:,2], 'RSS2':X_test[:,3],
                       'ARS1':X_test[:,4], 'ARS2':X_test[:,5], 'APS1':X_test[:,6], 'APS2':X_test[:,7], 'MatchID':mid})


print(confusion_matrix(y_test,y_pred_res))
print('\n')
print(classification_report(y_test,y_pred_res))

sm_model = sm.Logit(y_train, sm.add_constant(X_train)).fit(disp=0)
print(sm_model.pvalues)
sm_model.summary()

won = df_out3[df_out3['Winner'] == 1]
lost = df_out3[df_out3['Winner'] == 0]

plt.scatter(won.iloc[:,0], won.iloc[:,[35,36]], s=10, label = 'Won')
plt.scatter(lost.iloc[:,0], lost.iloc[:,[35,36]], s=10, label = 'Lost')
plt.legend()
plt.show()


 #K nearest (doesnt work, need probabilities)
from sklearn.neighbors import KNeighborsClassifier
scaler = StandardScaler()
X=df_out3.iloc[:,[1, 2, 35, 36, 39, 40, 41, 42, 45, 46]].values
scaler.fit(X)
scaled_features = scaler.transform(X)
df_feat = pd.DataFrame(scaled_features)
X=df_feat
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4)
knn = KNeighborsClassifier(n_neighbors=1)
knn.fit(X_train,y_train)
pred = knn.predict(X_test)
print(confusion_matrix(y_test, pred))






writer=pd.ExcelWriter('Output1.xlsx', engine='xlsxwriter')
df_out.to_excel(writer,'Sheet1',index=False)
writer.save()

















