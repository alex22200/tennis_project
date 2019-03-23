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

StDt="19.03.2019"
Days_to_Down = 2

#update PlayerIDs/MatchIDs in odds_master, ranking_master and reg_master
db="tennis.db"
os.chdir("C:\\Users\\Alex\\Documents\\Courses\\Tennis_Project2")

t0 = datetime.now()
print('Start time: ', t0)
time_to_download = 0.2954416
StDt = datetime.strptime(str(StDt), '%d.%m.%Y').date()
Days = 2
EndDt = StDt - timedelta(days = Days)
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute("SELECT * from links_reg_match")
Dates = [tup[1] for tup in cur.fetchall()]
cur.execute("SELECT * from links_reg_match")
links = [tup[0] for tup in cur.fetchall()]        
df = pd.DataFrame({'Dates':Dates, 'Link':links})
df['Dates'] = pd.to_datetime(df['Dates'], format='%d.%m.%Y')
l = []
for i in range(0, len(df['Dates'])):
    l.append(df.at[i, 'Dates'].date())
df['Dates'] = l
df.sort_values('Dates', inplace=True, axis=0)
df.set_index('Dates', inplace = True)
df = df.loc[EndDt:StDt]
df.reset_index(inplace = True)
time_to_download = 0.30
print('Downloading from ',EndDt,' to ',StDt)
print('Matches to download: ',len(df['Link']))
print('Estimated finish time: ',(t0 + timedelta(seconds=(time_to_download*len(df['Link'])))))
for f in range(0,len(df['Link'])):  
    #Progress
    if f%1000 == 0:
        print('Progress: ', round(f/len(df['Link']),2))
    page = requests.get(df.at[f, 'Link'])
    html_soup = soup(page.text,'html.parser')
    if html_soup.find('tr', class_='tour_head unpair') is not None:
        df.at[f, 'Date'] = html_soup.find('tr', class_='tour_head unpair').find_all('td')[0].text
        df.at[f, 'Date'] = df.at[i, 'Date'][:5] + '.20' + df.at[i, 'Date'][6:8]
        df.at[f, 'Round'] = html_soup.find('tr', class_='tour_head unpair').find_all('td')[1].text
        df.at[f, 'Player1'] = html_soup.find('tr', class_='tour_head unpair').find_all('td')[2].a.get('title')
        df.at[f, 'Player2'] = html_soup.find('tr', class_='tour_head unpair').find_all('td')[3].a.get('title')
        df.at[f, 'Score'] = html_soup.find('tr', class_='tour_head unpair').find_all('td')[4].select_one("span[id=score]").text
        df.at[f, 'Location'] = html_soup.find('tr', class_='tour_head unpair').find_all('td')[5].a.get('title')
        df.at[f, 'Surface'] = html_soup.find('tr', class_='tour_head unpair').find_all('td')[6].text
        if html_soup.find('table', class_='table_stats_match') is not None:
            for r in range(1,len(html_soup.find('table', class_='table_stats_match').find_all('tr'))):
                if html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[0].text == '1st SERVE %':
                    df.at[f, '1st Serve % P1'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[1].text
                    df.at[f, '1st Serve % P2'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[2].text
                elif html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[0].text == '1st SERVE POINTS WON':
                    df.at[f, '1st Serve Points Won P1'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[1].text
                    df.at[f, '1st Serve Points Won P2'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[2].text
                elif html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[0].text == '2nd SERVE POINTS WON':
                    df.at[f, '2nd Serve Points Won P1'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[1].text
                    df.at[f, '2nd Serve Points Won P2'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[2].text
                elif html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[0].text == 'BREAK POINTS WON':
                    df.at[f, 'BREAK POINTS WON P1'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[1].text
                    df.at[f, 'BREAK POINTS WON P2'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[2].text
                elif html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[0].text == 'TOTAL RETURN POINTS WON':
                    df.at[f, 'TOTAL RETURN POINTS WON P1'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[1].text
                    df.at[f, 'TOTAL RETURN POINTS WON P2'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[2].text
                elif html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[0].text == 'TOTAL POINTS WON':
                    df.at[f, 'TOTAL POINTS WON P1'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[1].text
                    df.at[f, 'TOTAL POINTS WON P2'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[2].text
                elif html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[0].text == 'DOUBLE FAULTS':
                    df.at[f, 'DOUBLE FAULTS P1'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[1].text
                    df.at[f, 'DOUBLE FAULTS P2'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[2].text
                elif html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[0].text == 'ACES':
                    df.at[f, 'ACES P1'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[1].text
                    df.at[f, 'ACES P2'] = html_soup.find('table', class_='table_stats_match').find_all('tr')[r].find_all('td')[2].text

df = df[~df["Player1"].str.contains("/")]

for i in range(0, len(df['Link'])):
    try:
        cur.execute("INSERT INTO reg_master VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (df.at[i, 'Link'], df.at[i, 'Date'], df.at[i, 'Player1'], df.at[i, 'Player2'], df.at[i, 'Round'], df.at[i, 'Score'], df.at[i, 'Location'], df.at[i, 'Surface'], df.at[i, '1st Serve % P1'], df.at[i, '1st Serve Points Won P1'], df.at[i, '2nd Serve Points Won P1'], df.at[i, 'BREAK POINTS WON P1'], df.at[i, 'TOTAL RETURN POINTS WON P1'], df.at[i, 'TOTAL POINTS WON P1'], df.at[i, 'DOUBLE FAULTS P1'], df.at[i, 'ACES P1'], df.at[i, '1st Serve % P2'], df.at[i, '1st Serve Points Won P2'], df.at[i, '2nd Serve Points Won P2'], df.at[i, 'BREAK POINTS WON P2'], df.at[i, 'TOTAL RETURN POINTS WON P2'], df.at[i, 'TOTAL POINTS WON P2'], df.at[i, 'DOUBLE FAULTS P2'], df.at[i, 'ACES P2']))
        conn.commit()
    except:
        pass

t1 = datetime.now()
print(t1)
print("It took ",(t1-t0)," seconds.")