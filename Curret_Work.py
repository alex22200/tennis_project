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

#update PlayerIDs/MatchIDs in odds_master, ranking_master and reg_master
db="tennis.db"
os.chdir("C:\\Users\\Alex\\Documents\\Courses\\Tennis_Project2")

conn = sqlite3.connect(db)
cur = conn.cursor()

t0 = datetime.now()

#1.1 Get data from DB
#get data from players list
cur.execute("SELECT * from players_master")
ID = [tup[0] for tup in cur.fetchall()]
cur.execute("SELECT * from players_master")
Name = [tup[1] for tup in cur.fetchall()]
cur.execute("SELECT * from players_master")
Birthday = [tup[3] for tup in cur.fetchall()]
cur.execute("SELECT * from players_master")
Name_Reg = [tup[4] for tup in cur.fetchall()]
df_play = pd.DataFrame({'ID':ID, 'Name':Name, 'Birthday':Birthday, 'Name_Reg':Name_Reg})

#1.2 get data for DBs where to adjust values
#odds
cur.execute("SELECT * from odds_master")
primary_odds = [tup[0] for tup in cur.fetchall()]
cur.execute("SELECT * from odds_master")
dates_odds = [tup[1] for tup in cur.fetchall()]
cur.execute("SELECT * from odds_master")
ID_odds = [tup[2] for tup in cur.fetchall()]
cur.execute("SELECT * from odds_master")
p1_odds = [tup[3] for tup in cur.fetchall()]
cur.execute("SELECT * from odds_master")
p2_odds = [tup[4] for tup in cur.fetchall()]
df_odds = pd.DataFrame({'Primary':primary_odds, 'Dates':dates_odds, 'ID':ID_odds, 'ID_check':ID_odds, 'Player1':p1_odds, 'Player2':p2_odds})
df_odds = df_odds.loc[df_odds['ID'] == 0]

#reg
cur.execute("SELECT * from reg_master")
primary_reg = [tup[0] for tup in cur.fetchall()]
cur.execute("SELECT * from reg_master")
dates_reg = [tup[1] for tup in cur.fetchall()]
cur.execute("SELECT * from reg_master")
ID_reg = [tup[2] for tup in cur.fetchall()]
cur.execute("SELECT * from reg_master")
p1_reg = [tup[3] for tup in cur.fetchall()]
cur.execute("SELECT * from reg_master")
p2_reg = [tup[4] for tup in cur.fetchall()]
df_reg = pd.DataFrame({'Primary':primary_reg, 'Dates':dates_reg, 'ID':ID_reg, 'ID_check':ID_reg, 'Player1':p1_reg, 'Player2':p2_reg})
df_reg = df_reg[df_reg['ID'].isnull()]
df_reg.reset_index(inplace = True)
df_reg.drop('index', axis=1, inplace = True)

#ranking
#2. Calculate Match IDs: P1**2 * P2**2 + timestamp
#odds_master, exception, if empty
try:
    missing_odds = len(df_odds['ID'])
    print('In the odds DB there are ', len(missing_odds),' match IDs missing')
    for i in range(0, len(df_odds['ID'])):
        df_odds.at[i, 'ID'] = int(df_play[df_play['Name'] == df_odds.at[i, 'Player1']]['ID'].iloc[0]**2 * df_play[df_play['Name'] == df_odds.at[i, 'Player2']]['ID'].iloc[0]**2 + int(time.mktime(datetime.strptime(df_odds.at[i, 'Dates'], "%d.%m.%Y").timetuple())))
except:
    print('odds_master is up-to-date')
    pass

#reg_master
#ONLY FOR INITIAL SETUP OF DB!
p1_reg = df_reg['Player1'].tolist()
p2_reg = df_reg['Player2'].tolist()
for player in p2_reg:
    p1_reg.append(player)
    
df_reg_alt = pd.DataFrame({'Player': p1_reg})
df_reg_alt.drop_duplicates('Player', inplace = True)
df_reg_alt.reset_index(inplace = True)
df_reg_alt.drop('index', axis=1, inplace = True)

for i in range(0, len(df_reg_alt['Player'])): 
    if i%100 == 0:
        print('Progress: ', i/len(df_reg_alt['Player']))
    try:
        Player = process.extractOne(df_reg_alt.at[i, 'Player'], Name)[0]
        df_reg_alt.at[i, 'Score'] = process.extractOne(df_reg_alt.at[i, 'Player'], Name)[1]
        df_reg_alt.at[i, 'Player_DB'] = Player
        df_reg_alt.at[i, 'PID'] = int(df_play[df_play['Name'] == Player]['ID'].iloc[0])
    except:
        pass
    
#only keep if match is close enough
df_reg_alt = df_reg_alt[df_reg_alt['Score']>86]
df_reg_alt.reset_index(inplace = True)
df_reg_alt.drop('index', axis=1, inplace = True)

for i in range(0, len(df_reg['ID'])):  
    if i%100 == 0:
        print('Progress: ', i/len(df_reg['Player1']))
    try:
        if len(df_reg.at[i, 'Dates']) == 8:
            df_reg.at[i, 'Dates'] = df_reg.at[i, 'Dates'][:5] + '.20' + df_reg.at[i, 'Dates'][6:8]
    except:
        pass
    try:
        df_reg.at[i, 'ID'] = int(df_reg_alt[df_reg_alt['Player'] == df_reg.at[i, 'Player1']]['PID'].iloc[0]**2 * df_reg_alt[df_reg_alt['Player'] == df_reg.at[i, 'Player2']]['PID'].iloc[0]**2 + int(time.mktime(datetime.strptime(df_reg.at[i, 'Dates'], "%d.%m.%Y").timetuple())))
    except:
        pass

#3.update values in DBs

#players_master (values for other DB)
    
#odds_master
for i in range(0, len(df_odds['ID'])):
    if df_odds.at[i, 'ID_check'] == 0:
        cur.execute("UPDATE odds_master SET MatchID = ? WHERE Link = ?", (int(df_odds.at[i, 'ID']), df_odds.at[i, 'Primary']))
        conn.commit()
print('Match IDs for odds_master updated, step 1/3')

#reg
for i in range(0, len(df_reg['ID'])):
    if i%100 == 0:
        print('Progress: ', i/len(df_reg['Player1']))    
    try:
        cur.execute("UPDATE reg_master SET MatchID = ? WHERE Link = ?", (int(df_reg.at[i, 'ID']), df_reg.at[i, 'Primary']))
        cur.execute("UPDATE reg_master SET Dates = ? WHERE Link = ?", (df_reg.at[i, 'Dates'], df_reg.at[i, 'Primary']))
        conn.commit()       
    except:
        pass
print('Match IDs for reg_master updated, step 2/3')

#players DB 
for i in range(0, len(df_reg_alt['Player'])):
    cur.execute("UPDATE players_master SET Name_Reg = ? WHERE PlayerID = ?", (df_reg_alt.at[i, 'Player'], int(df_reg_alt.at[i, 'PID'])))
    conn.commit()

#ranking

t1 = datetime.now()
print(t1)
print("It actually took ",(t1-t0)," seconds.")

conn.close()
