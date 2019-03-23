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

class Tennis_Downloads:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.cur.execute("CREATE TABLE IF NOT EXISTS links_odds (Dates DATE PRIMARY KEY, link TEXT)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS links_reg (Dates DATE PRIMARY KEY, link TEXT)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS links_odds_match (Link TEXT PRIMARY KEY, Dates date)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS links_reg_match (Link TEXT PRIMARY KEY, Dates date)")
        #self.cur.execute("DROP TABLE odds_master")
        #self.cur.execute("CREATE TABLE IF NOT EXISTS odds_master (Link TEXT PRIMARY KEY, Dates date, MatchID INTEGER, Player1 TEXT, Player2 TEXT, Info TEXT, Location TEXT, Score TEXT, Ranking1 INTEGER, Ranking2 INTEGER, Birthday1 Date, Birthday2 DATE, Height1 REAL, Height2 REAL, Weight1 REAL, Weight2 REAL, Hand1 TEXT, Hand2 TEXT, Pro1 INTEGER, Pro2 INTEGER, O_2_5_sets REAL, U_2_5_sets REAL,  O_3_sets REAL,  U_3_sets REAL,  O_3_5_sets REAL,  U_3_5_sets REAL,  O_4_sets REAL,  U_4_sets REAL,  O_4_5_sets REAL,  U_4_5_sets REAL,  O_14_5_games REAL,  U_14_5_games REAL,  O_15_games REAL,  U_15_games REAL,  O_15_5_games REAL,  U_15_5_games REAL,  O_16_games REAL,  U_16_games REAL,  O_16_5_games REAL,  U_16_5_games REAL,  O_17_games REAL,  U_17_games REAL,  O_17_5_games REAL,  U_17_5_games REAL,  O_18_games REAL,  U_18_games REAL,  O_18_5_games REAL,  U_18_5_games REAL,  O_19_games REAL,  U_19_games REAL,  O_19_5_games REAL,  U_19_5_games REAL,  O_20_games REAL,  U_20_games REAL,  O_20_5_games REAL,  U_20_5_games REAL,  O_21_games REAL,  U_21_games REAL,  O_21_5_games REAL,  U_21_5_games REAL,  O_22_games REAL,  U_22_games REAL,  O_22_5_games REAL,  U_22_5_games REAL,  O_23_games REAL,  U_23_games REAL,  O_23_5_games REAL,  U_23_5_games REAL,  O_24_games REAL,  U_24_games REAL,  O_24_5_games REAL,  U_24_5_games REAL,  O_25_games REAL,  U_25_games REAL,  O_25_5_games REAL,  U_25_5_games REAL,  O_26_games REAL,  U_26_games REAL,  O_26_5_games REAL,  U_26_5_games REAL,  O_27_games REAL,  U_27_games REAL,  O_27_5_games REAL,  U_27_5_games REAL,  O_28_games REAL,  U_28_games REAL,  O_28_5_games REAL,  U_28_5_games REAL,  O_29_games REAL,  U_29_games REAL,  O_29_5_games REAL,  U_29_5_games REAL,  O_30_games REAL,  U_30_games REAL,  O_30_5_games REAL,  U_30_5_games REAL,  O_31_games REAL,  U_31_games REAL,  O_31_5_games REAL,  U_31_5_games REAL,  O_32_games REAL,  U_32_games REAL,  O_32_5_games REAL,  U_32_5_games REAL,  O_33_games REAL,  U_33_games REAL,  O_33_5_games REAL,  U_33_5_games REAL,  O_34_games REAL,  U_34_games REAL,  O_34_5_games REAL,  U_34_5_games REAL,  O_35_games REAL,  U_35_games REAL,  O_35_5_games REAL,  U_35_5_games REAL,  O_36_games REAL,  U_36_games REAL,  O_36_5_games REAL,  U_36_5_games REAL,  O_37_games REAL,  U_37_games REAL,  AH__1_5_sets_1 REAL,  AH__1_5_sets_2 REAL,  AH__1_5_games_1 REAL,  AH__1_5_games_2 REAL,  AH_1_5_sets_1 REAL,  AH_1_5_sets_2 REAL,  AH__7_games_1 REAL,  AH__7_games_2 REAL,  AH__6_5_games_1 REAL,  AH__6_5_games_2 REAL,  AH__6_games_1 REAL,  AH__6_games_2 REAL,  AH__5_5_games_1 REAL,  AH__5_5_games_2 REAL,  AH__5_games_1 REAL,  AH__5_games_2 REAL,  AH__4_5_games_1 REAL,  AH__4_5_games_2 REAL,  AH__4_games_1 REAL,  AH__4_games_2 REAL,  AH__3_5_games_1 REAL,  AH__3_5_games_2 REAL,  AH__3_games_1 REAL,  AH__3_games_2 REAL,  AH__2_5_games_1 REAL,  AH__2_5_games_2 REAL,  AH__2_games_1 REAL,  AH__2_games_2 REAL,  AH__1_games_1 REAL,  AH__1_games_2 REAL,  AH__0_5_games_1 REAL,  AH__0_5_games_2 REAL,  AH_0_games_1 REAL,  AH_0_games_2 REAL,  AH_0_5_games_1 REAL,  AH_0_5_games_2 REAL,  AH_1_games_1 REAL,  AH_1_games_2 REAL,  AH_1_5_games_1 REAL,  AH_1_5_games_2 REAL,  AH_2_games_1 REAL,  AH_2_games_2 REAL,  AH_2_5_games_1 REAL,  AH_2_5_games_2 REAL,  AH_3_games_1 REAL,  AH_3_games_2 REAL,  AH_3_5_games_1 REAL,  AH_3_5_games_2 REAL,  AH_4_games_1 REAL,  AH_4_games_2 REAL,  AH_4_5_games_1 REAL,  AH_4_5_games_2 REAL,  AH_5_games_1 REAL,  AH_5_games_2 REAL,  AH_5_5_games_1 REAL,  AH_5_5_games_2 REAL,  AH_6_games_1 REAL,  AH_6_games_2 REAL,  AH_6_5_games_1 REAL,  AH_6_5_games_2 REAL,  AH_7_games_1 REAL,  AH_7_games_2 REAL,  AH_7_5_games_1 REAL,  AH_7_5_games_2 REAL,  Correct_Score_3_0 REAL,  Correct_Score_2_0 REAL,  Correct_Score_2_1 REAL,  Correct_Score_3_1 REAL,  Correct_Score_3_2 REAL,  Correct_Score_0_3 REAL,  Correct_Score_0_2 REAL,  Correct_Score_1_2 REAL,  Correct_Score_1_3 REAL,  Correct_Score_2_3 REAL)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS odds_master (Link TEXT PRIMARY KEY, Dates date, MatchID INTEGER, Player1 TEXT, Player2 TEXT, Info TEXT, Location TEXT, Score TEXT, Ranking1 INTEGER, Ranking2 INTEGER, Birthday1 Date, Birthday2 DATE, Height1 REAL, Height2 REAL, Weight1 REAL, Weight2 REAL, Hand1 TEXT, Hand2 TEXT, Pro1 INTEGER, Pro2 INTEGER, Home INTEGER, Away INTEGER)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS reg_master (Link TEXT PRIMARY KEY, Dates date, MatchID INTEGER, Player1 TEXT, Player2 TEXT, Round TEXT, Score TEXT, Location TEXT, Surface TEXT, First_Serve_P1 TEXT, First_Serve_Points_Won_P1 TEXT, Second_Serve_Points_Won_P1 TEXT, Break_Points_Won_P1 TEXT, Total_Return_Points_Won_P1 TEXT, Total_Points_Won_P1 TEXT, Double_Faults_P1 INTEGER, Aces_P1 INTEGER, First_Serve_P2 TEXT, First_Serve_Points_Won_P2 TEXT, Second_Serve_Points_Won_P2 TEXT, Break_Points_Won_P2 TEXT, Total_Return_Points_Won_P2 TEXT, Total_Points_Won_P2 TEXT, Double_Faults_P2 INTEGER, Aces_P2 INTEGER)")
        #This is filled with user input, need initialize with values from the most comprehensive download, then update function to get newest players in
        self.cur.execute("CREATE TABLE IF NOT EXISTS players_master (PlayerID INTEGER PRIMARY KEY, Name TEXT, Hand TEXT, Birthdate DATE)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS ranking_master (Ident TEXT PRIMARY KEY, PlayerID INTEGER, link TEXT, rank INTEGER, Dates DATE, Name TEXT, Age INTEGER, Points INTEGER, Tourn_Played INTEGER)")
        self.conn.commit()

    def Download_Links_Odds(self, StDt, EndDt, Days, db):
        t0 = datetime.now()
        print('Start time: ', t0)
        time_to_download = 1.665023462
        self.StDt = datetime.strptime(str(StDt), '%d.%m.%Y').date()
        if EndDt != 0:
            self.EndDt = datetime.strptime(str(EndDt), '%d.%m.%Y').date()
            self.Days_between = self.StDt - self.EndDt
            self.Days_to_Down = self.Days_between.days
        if Days != 0:
            self.Days_to_Down = int(Days)
        print(time_to_download)
        print('Estimated finish time: ',(t0 + timedelta(seconds=(time_to_download*self.Days_to_Down))))
        URL = []
        datesXL = []
        links = []
        control = []
        for day in range(0,self.Days_to_Down):
            self.StDt = self.StDt - timedelta(days=1)
            datesXL.append(str(self.StDt.day) + "." + str(self.StDt.month) + "." + str(self.StDt.year))
            URL.append("http://www.tennisexplorer.com/results/?year=" + str(self.StDt.year) + "&month=" + str(self.StDt.month) + "&day=" + str(self.StDt.day))
        self.df = pd.DataFrame({'Link':URL,'Dates':datesXL})
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        #write to database links_odds
        for i in range(len(self.df['Dates'])):
            try:
                self.cur.execute("INSERT INTO links_odds VALUES (?, ?)", (self.df.at[i, 'Dates'], self.df.at[i, 'Link']))
                self.conn.commit()
            except:
                pass

        for i in range(0,len(self.df['Dates'])):
            page = requests.get(URL[i])
            html_soup = soup(page.text,'html.parser')
            link_cont = html_soup.find('table', class_ = 'result').find_all('a', title='Click for match detail')
            for s in range(0,len(link_cont)):
                links.append('http://www.tennisexplorer.com' + link_cont[s].get('href'))
                control.append(self.df.at[i, 'Dates'])

        self.df2 = pd.DataFrame({'Link': links,'Dates': control})
        for i in range(0, len(self.df2['Link'])):
            try:
                self.cur.execute("INSERT INTO links_odds_match VALUES (?, ?)", (self.df2.at[i, 'Link'], self.df2.at[i, 'Dates']))
                self.conn.commit()
            except:
                pass
        t1 = datetime.now()
        print(t1)
        print("It actually took ",(t1-t0)," seconds.")

    def Download_Links_Reg(self, StDt, EndDt, Days, db):
        t0 = datetime.now()
        print('Start time: ', t0)
        time_to_download = 0.2954416
        self.StDt = datetime.strptime(str(StDt), '%d.%m.%Y').date()
        if EndDt != 0:
            self.EndDt = datetime.strptime(str(EndDt), '%d.%m.%Y').date()
            self.Days_between = self.StDt - self.EndDt
            self.Days_to_Down = self.Days_between.days
        if Days != 0:
            self.Days_to_Down = int(Days)
        print('Estimated finish time: ',(t0 + timedelta(seconds=(time_to_download*self.Days_to_Down))))
        URL = []
        datesXL = []
        links = []
        control = []
        for day in range(0,self.Days_to_Down):
            self.StDt = self.StDt - timedelta(days=1)
            datesXL.append(str(self.StDt.day) + "." + str(self.StDt.month) + "." + str(self.StDt.year))
            if self.StDt.month in range(1,10):
                months = str("0"+str(self.StDt.month))
            else:
                months = str(self.StDt.month)
            URL.append("http://www.tennisergebnisse.net/herren/" + str(self.StDt.year) + "-" + str(months) + "-" + str(self.StDt.day) + "/")
        self.df = pd.DataFrame({'Link':URL,'Dates':datesXL})
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        #write to database links_reg
        for i in range(len(self.df['Dates'])):
            try:
                self.cur.execute("INSERT INTO links_reg VALUES (?, ?)", (self.df.at[i, 'Dates'], self.df.at[i, 'Link']))
                self.conn.commit()
            except:
                pass

        for i in range(0,len(self.df['Dates'])):
            page=requests.get(URL[i])
            html_soup = soup(page.text,'html.parser')
            link_cont = html_soup.find_all('div', class_='head2head')
            for s in range(0,len(link_cont)):
                links.append(link_cont[s].a.get('href'))
                control.append(self.df.at[i, 'Dates'])

        self.df2 = pd.DataFrame({'Link': links,'Dates': control})
        for i in range(0, len(self.df2['Link'])):
            try:
                self.cur.execute("INSERT INTO links_reg_match VALUES (?, ?)", (self.df2.at[i, 'Link'], self.df2.at[i, 'Dates']))
                self.conn.commit()
            except:
                pass
        t1 = datetime.now()
        print(t1)
        print("It actually took ",(t1-t0)," seconds.")

    def Download_Reg(self, StDt, EndDt, Days, db):
        """
        This function downloads data for given links. It outputs the interesting datapoints and takes links as inputs.
        Link: string for tennis website        
        """
        t0 = datetime.now()
        print('Start time: ', t0)
        self.StDt = datetime.strptime(str(StDt), '%d.%m.%Y').date()

        if EndDt != 0:
            self.EndDt = datetime.strptime(str(EndDt), '%d.%m.%Y').date()
            self.Days_between = self.StDt - self.EndDt
            self.Days_to_Down = self.Days_between.days
        elif Days != 0:
            self.Days_to_Down = int(Days)
            self.EndDt = self.StDt - timedelta(days = Days)
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT * from links_reg_match")
        Dates = [tup[1] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from links_reg_match")
        links = [tup[0] for tup in self.cur.fetchall()]        
        df = pd.DataFrame({'Dates':Dates, 'Link':links})
        df['Dates'] = pd.to_datetime(df['Dates'], format='%d.%m.%Y')
        l = []
        for i in range(0, len(df['Dates'])):
            l.append(df.at[i, 'Dates'].date())
        df['Dates'] = l
        df.sort_values('Dates', inplace=True, axis=0)
        df.set_index('Dates', inplace = True)
        df = df.loc[self.EndDt:self.StDt]
        df.reset_index(inplace = True)
        time_to_download = 0.30
        print('Downloading from ',self.EndDt,' to ',self.StDt)
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
         
        #exclude doubles           
        df = df[~df["Player1"].str.contains("/")]
                            
        for i in range(0,len(df['Link'])):
            try:
                self.cur.execute("INSERT INTO reg_master VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (df.at[i, 'Link'], df.at[i, 'Date'], df.at[i, 'Player1'], df.at[i, 'Player2'], df.at[i, 'Round'], df.at[i, 'Score'], df.at[i, 'Location'], df.at[i, 'Surface'], df.at[i, '1st Serve % P1'], df.at[i, '1st Serve Points Won P1'], df.at[i, '2nd Serve Points Won P1'], df.at[i, 'BREAK POINTS WON P1'], df.at[i, 'TOTAL RETURN POINTS WON P1'], df.at[i, 'TOTAL POINTS WON P1'], df.at[i, 'DOUBLE FAULTS P1'], df.at[i, 'ACES P1'], df.at[i, '1st Serve % P2'], df.at[i, '1st Serve Points Won P2'], df.at[i, '2nd Serve Points Won P2'], df.at[i, 'BREAK POINTS WON P2'], df.at[i, 'TOTAL RETURN POINTS WON P2'], df.at[i, 'TOTAL POINTS WON P2'], df.at[i, 'DOUBLE FAULTS P2'], df.at[i, 'ACES P2']))
                self.conn.commit()
            except:
                pass

        t1 = datetime.now()
        print(t1)
        print("It took ",(t1-t0)," seconds.")

    def Download_Odds(self, StDt, EndDt, Days, db):
        """
        This function downloads data for given links. It outputs the odds and takes links as inputs.
        Link: string for tennis website        
        """
        t0 = datetime.now()
        print('Start time: ', t0)
        self.StDt = datetime.strptime(str(StDt), '%d.%m.%Y').date()
        if EndDt != 0:
            self.EndDt = datetime.strptime(str(EndDt), '%d.%m.%Y').date()
            self.Days_between = self.StDt - self.EndDt
            self.Days_to_Down = self.Days_between.days
        elif Days != 0:
            self.Days_to_Down = int(Days)
            self.EndDt = self.StDt - timedelta(days = Days)
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT * from links_odds_match")
        Dates = [tup[1] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from links_odds_match")
        links = [tup[0] for tup in self.cur.fetchall()]        
        df = pd.DataFrame({'Dates':Dates, 'Link':links})
        df['Dates'] = pd.to_datetime(df['Dates'], format='%d.%m.%Y')
        l = []
        for i in range(0, len(df['Dates'])):
            l.append(df.at[i, 'Dates'].date())
        df['Dates'] = l
        df.sort_values('Dates', inplace=True, axis=0)
        df.set_index('Dates', inplace = True)
        df = df.loc[self.EndDt:self.StDt]
        df.reset_index(inplace = True)
        time_to_download = 0.434
        print('Downloading from ',self.EndDt,' to ',self.StDt)
        print('Matches to download: ',len(df['Link']))
        print('Estimated finish time: ',(t0 + timedelta(seconds=(time_to_download*len(df['Link'])))))      


        for i in range(0,len(df['Link'])): 
            #Progress
            if i%1000 == 0:
                print('Progress: ', round(i/len(df['Link']),2))
            #added this part to solve the connection issue
            session = requests.Session()
            retry = Retry(connect = 3, backoff_factor = 0.5)
            adapter = HTTPAdapter(max_retries = retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            page = session.get(df.at[i, 'Link'])

            #page = requests.get(df.at[i, 'Link'])
            html_soup = soup(page.text,'html.parser')
            #Player/match details
            if not html_soup.find('h1', class_="bg") is None:
                if not html_soup.select_one("span[class=upper]") is None:
                    df.at[i, 'Date'] = html_soup.select_one("span[class=upper]").text
                else:
                    df.at[i, 'Date'] = "-"
                if not html_soup.find('div',id='center').find_all('div',class_='box boxBasic lGray')[0].find('a',class_=False,id=False) is None:
                    df.at[i, 'Location'] = html_soup.find('div',id='center').find_all('div',class_='box boxBasic lGray')[0].find('a',class_=False,id=False).text
                else:
                    df.at[i, 'Location'] = "-"
                if not html_soup.find('div',id='center').find_all('div',class_='box boxBasic lGray') is None:
                    df.at[i, 'Info'] = html_soup.find('div',id='center').find_all('div',class_='box boxBasic lGray')[0].get_text()
                    df.at[i, 'Player1'] = html_soup.find_all('th',class_='plName')[0].text
                    df.at[i, 'Player2'] = html_soup.find_all('th',class_='plName')[1].text
                    df.at[i, 'Score'] = html_soup.find('td',class_='gScore').text
                    TblTr = html_soup.find('table',class_='result gDetail noMgB').tbody.find_all('tr')
                    df.at[i, 'Ranking1'] = TblTr[0].find_all('td')[1].text.replace('.','')
                    df.at[i, 'Ranking2'] = TblTr[0].find_all('td')[2].text.replace('.','')
                    df.at[i, 'Birthdate1'] = (TblTr[1].find_all('td')[0].text).replace(' ','')
                    df.at[i, 'Birthdate2'] = TblTr[1].find_all('td')[1].text.replace(' ','')
                    df.at[i, 'Height1'] = TblTr[2].find_all('td')[0].text
                    df.at[i, 'Height2'] = TblTr[2].find_all('td')[1].text
                    df.at[i, 'Weight1'] = TblTr[3].find_all('td')[0].text
                    df.at[i, 'Weight2'] = TblTr[3].find_all('td')[1].text
                    df.at[i, 'Hand1'] = TblTr[4].find_all('td')[0].text
                    df.at[i, 'Hand2'] = TblTr[4].find_all('td')[1].text
                    df.at[i, 'Pro1'] = TblTr[5].find_all('td')[0].text
                    df.at[i, 'Pro2'] = TblTr[5].find_all('td')[1].text  
                    #Home/Away
                if not '(0)' in html_soup.find('li',id='oddsMenu-1').a.text:
                    df.at[i, 'Home'] = html_soup.find('tr', class_='average').find('td', class_='k1').text
                    df.at[i, 'Away'] = html_soup.find('tr', class_='average').find('td', class_='k2').text
                # For NOW: only download Home/Away
                #     #Over/Under
                # if not '(0)' in html_soup.find('li',id='oddsMenu-2').a.text:
                #     for s in range(0,len(html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type'))):
                #         try:
                #             if html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 2.5 sets':
                #                 df.at[i,'O 2.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 2.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 3 sets':
                #                 df.at[i,'O 3 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 3 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 3.5 sets':
                #                 df.at[i,'O 3.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 3.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 4 sets':
                #                 df.at[i,'O 4 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 4 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 4.5 sets':
                #                 df.at[i,'O 4.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 4.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 14.5 games':
                #                 df.at[i,'O 14.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 14.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 15 games':
                #                 df.at[i,'O 15 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 15 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 15.5 games':
                #                 df.at[i,'O 15.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 15.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 16 games':
                #                 df.at[i,'O 16 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 16 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 16.5 games':
                #                 df.at[i,'O 16.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 16.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 17 games':
                #                 df.at[i,'O 17 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 17 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 17.5 games':
                #                 df.at[i,'O 17.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 17.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 18 games':
                #                 df.at[i,'O 18 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 18 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 18.5 games':
                #                 df.at[i,'O 18.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 18.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 19 games':
                #                 df.at[i,'O 19 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 19 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 19.5 games':
                #                 df.at[i,'O 19.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 19.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 20 games':
                #                 df.at[i,'O 20 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 20 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 20.5 games':
                #                 df.at[i,'O 20.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 20.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 21 games':
                #                 df.at[i,'O 21 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 21 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 21.5 games':
                #                 df.at[i,'O 21.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 21.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 22 games':
                #                 df.at[i,'O 22 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 22 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 22.5 games':
                #                 df.at[i,'O 22.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 22.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 23 games':
                #                 df.at[i,'O 23 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 23 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 23.5 games':
                #                 df.at[i,'O 23.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 23.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 24 games':
                #                 df.at[i,'O 24 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 24 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 24.5 games':
                #                 df.at[i,'O 24.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 24.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 25 games':
                #                 df.at[i,'O 25 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 25 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 25.5 games':
                #                 df.at[i,'O 25.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 25.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 26 games':
                #                 df.at[i,'O 26 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 26 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 26.5 games':
                #                 df.at[i,'O 26.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 26.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 27 games':
                #                 df.at[i,'O 27 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 27 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 27.5 games':
                #                 df.at[i,'O 27.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 27.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 28 games':
                #                 df.at[i,'O 28 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 28 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 28.5 games':
                #                 df.at[i,'O 28.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 28.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 29 games':
                #                 df.at[i,'O 29 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 29 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 29.5 games':
                #                 df.at[i,'O 29.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 29.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 30 games':
                #                 df.at[i,'O 30 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 30 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 30.5 games':
                #                 df.at[i,'O 30.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 30.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 31 games':
                #                 df.at[i,'O 31 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 31 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 31.5 games':
                #                 df.at[i,'O 31.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 31.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text   
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 32 games':
                #                 df.at[i,'O 32 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 32 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text     
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 32.5 games':
                #                 df.at[i,'O 32.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 32.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text     
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 33 games':
                #                 df.at[i,'O 33 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 33 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 33.5 games':
                #                 df.at[i,'O 33.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 33.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 34 games':
                #                 df.at[i,'O 34 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 34 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 34.5 games':
                #                 df.at[i,'O 34.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 34.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 35 games':
                #                 df.at[i,'O 35 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 35 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 35.5 games':
                #                 df.at[i,'O 35.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 35.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 36 games':
                #                 df.at[i,'O 36 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 36 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 36.5 games':
                #                 df.at[i,'O 36.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 36.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                #             elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 37 games':
                #                 df.at[i,'O 37 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'U 37 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                #         except:
                #             pass
                #     #Over/Under
                # if not '(0)' in html_soup.find('li',id='oddsMenu-3').a.text:
                #     for s in range(0,len(html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type'))):
                #         try:
                #             if html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -1.5 sets':
                #                 df.at[i,'AH -1.5 sets 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -1.5 sets 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -1.5 games':
                #                 df.at[i,'AH -1.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -1.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 1.5 games':
                #                 df.at[i,'AH 1.5 sets 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 1.5 sets 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -7 games':
                #                 df.at[i,'AH -7 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -7 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -6.5 games':
                #                 df.at[i,'AH -6.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -6.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -6 games':
                #                 df.at[i,'AH -6 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -6 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -5.5 games':
                #                 df.at[i,'AH -5.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -5.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -5 games':
                #                 df.at[i,'AH -5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -4.5 games':
                #                 df.at[i,'AH -4.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -4.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -4 games':
                #                 df.at[i,'AH -4 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -4 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -3.5 games':
                #                 df.at[i,'AH -3.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -3.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -3 games':
                #                 df.at[i,'AH -3 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -3 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -2.5 games':
                #                 df.at[i,'AH -2.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -2.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -2 games':
                #                 df.at[i,'AH -2 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -2 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -1 games':
                #                 df.at[i,'AH -1 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -1 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -0.5 games':
                #                 df.at[i,'AH -0.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH -0.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 0 games':
                #                 df.at[i,'AH 0 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 0 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 0.5 games':
                #                 df.at[i,'AH 0.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 0.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 1 games':
                #                 df.at[i,'AH 1 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 1 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 1.5 games':
                #                 df.at[i,'AH 1.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 1.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 2 games':
                #                 df.at[i,'AH 2 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 2 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 2.5 games':
                #                 df.at[i,'AH 2.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 2.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 3 games':
                #                 df.at[i,'AH 3 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 3 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 3.5 games':
                #                 df.at[i,'AH 3.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 3.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 4 games':
                #                 df.at[i,'AH 4 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 4 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 4.5 games':
                #                 df.at[i,'AH 4.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 4.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 5 games':
                #                 df.at[i,'AH 5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 5.5 games':
                #                 df.at[i,'AH 5.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 5.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 6 games':
                #                 df.at[i,'AH 6 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 6 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 6.5 games':
                #                 df.at[i,'AH 6.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 6.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 7 games':
                #                 df.at[i,'AH 7 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 7 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #             elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 7.5 games':
                #                 df.at[i,'AH 7.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #                 df.at[i,'AH 7.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                #         except:
                #             pass                                
                #     #CS
                # if not '(0)' in html_soup.find('li',id='oddsMenu-4').a.text:
                #     for s in range(0,len(html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type'))):
                #         try:
                #             if html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 3:0':
                #                 df.at[i,'Correct Score 3:0'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #             elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 2:0':
                #                 df.at[i,'Correct Score 2:0'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #             elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 2:1':
                #                 df.at[i,'Correct Score 2:1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #             elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 3:1':
                #                 df.at[i,'Correct Score 3:1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #             elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 3:2':
                #                 df.at[i,'Correct Score 3:2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #             elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 0:3':
                #                 df.at[i,'Correct Score 0:3'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #             elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 0:2':
                #                 df.at[i,'Correct Score 0:2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #             elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 1:2':
                #                 df.at[i,'Correct Score 1:2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #             elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 1:3':
                #                 df.at[i,'Correct Score 1:3'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #             elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 2:3':
                #                 df.at[i,'Correct Score 2:3'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                #         except:
                #             pass



        for i in range(0,len(df['Link'])):
            try:
                #self.cur.execute("INSERT INTO odds_master VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (df.at[i, 'Link'], df.at[i, 'Date'], df.at[i, 'MatchID'], df.at[i, 'Player1'], df.at[i, 'Player2'], df.at[i, 'Info'], df.at[i, 'Location'], df.at[i, 'Score'], df.at[i, 'Ranking1'], df.at[i, 'Ranking2'], df.at[i, 'Birthday1'], df.at[i, 'Birthday2'], df.at[i, 'Height1'], df.at[i, 'Height2'], df.at[i, 'Weight1'], df.at[i, 'Weight2'], df.at[i, 'Hand1'], df.at[i, 'Hand2'], df.at[i, 'Pro1'], df.at[i, 'Pro2'], df.at[i, 'O 2.5 sets'], df.at[i, 'U 2.5 sets'], df.at[i, 'O 3 sets'], df.at[i, 'U 3 sets'], df.at[i, 'O 3.5 sets'], df.at[i, 'U 3.5 sets'], df.at[i, 'O 4 sets'], df.at[i, 'U 4 sets'], df.at[i, 'O 4.5 sets'], df.at[i, 'U 4.5 sets'], df.at[i, 'O 14.5 games'], df.at[i, 'U 14.5 games'], df.at[i, 'O 15 games'], df.at[i, 'U 15 games'], df.at[i, 'O 15.5 games'], df.at[i, 'U 15.5 games'], df.at[i, 'O 16 games'], df.at[i, 'U 16 games'], df.at[i, 'O 16.5 games'], df.at[i, 'U 16.5 games'], df.at[i, 'O 17 games'], df.at[i, 'U 17 games'], df.at[i, 'O 17.5 games'], df.at[i, 'U 17.5 games'], df.at[i, 'O 18 games'], df.at[i, 'U 18 games'], df.at[i, 'O 18.5 games'], df.at[i, 'U 18.5 games'], df.at[i, 'O 19 games'], df.at[i, 'U 19 games'], df.at[i, 'O 19.5 games'], df.at[i, 'U 19.5 games'], df.at[i, 'O 20 games'], df.at[i, 'U 20 games'], df.at[i, 'O 20.5 games'], df.at[i, 'U 20.5 games'], df.at[i, 'O 21 games'], df.at[i, 'U 21 games'], df.at[i, 'O 21.5 games'], df.at[i, 'U 21.5 games'], df.at[i, 'O 22 games'], df.at[i, 'U 22 games'], df.at[i, 'O 22.5 games'], df.at[i, 'U 22.5 games'], df.at[i, 'O 23 games'], df.at[i, 'U 23 games'], df.at[i, 'O 23.5 games'], df.at[i, 'U 23.5 games'], df.at[i, 'O 24 games'], df.at[i, 'U 24 games'], df.at[i, 'O 24.5 games'], df.at[i, 'U 24.5 games'], df.at[i, 'O 25 games'], df.at[i, 'U 25 games'], df.at[i, 'O 25.5 games'], df.at[i, 'U 25.5 games'], df.at[i, 'O 26 games'], df.at[i, 'U 26 games'], df.at[i, 'O 26.5 games'], df.at[i, 'U 26.5 games'], df.at[i, 'O 27 games'], df.at[i, 'U 27 games'], df.at[i, 'O 27.5 games'], df.at[i, 'U 27.5 games'], df.at[i, 'O 28 games'], df.at[i, 'U 28 games'], df.at[i, 'O 28.5 games'], df.at[i, 'U 28.5 games'], df.at[i, 'O 29 games'], df.at[i, 'U 29 games'], df.at[i, 'O 29.5 games'], df.at[i, 'U 29.5 games'], df.at[i, 'O 30 games'], df.at[i, 'U 30 games'], df.at[i, 'O 30.5 games'], df.at[i, 'U 30.5 games'], df.at[i, 'O 31 games'], df.at[i, 'U 31 games'], df.at[i, 'O 31.5 games'], df.at[i, 'U 31.5 games'], df.at[i, 'O 32 games'], df.at[i, 'U 32 games'], df.at[i, 'O 32.5 games'], df.at[i, 'U 32.5 games'], df.at[i, 'O 33 games'], df.at[i, 'U 33 games'], df.at[i, 'O 33.5 games'], df.at[i, 'U 33.5 games'], df.at[i, 'O 34 games'], df.at[i, 'U 34 games'], df.at[i, 'O 34.5 games'], df.at[i, 'U 34.5 games'], df.at[i, 'O 35 games'], df.at[i, 'U 35 games'], df.at[i, 'O 35.5 games'], df.at[i, 'U 35.5 games'], df.at[i, 'O 36 games'], df.at[i, 'U 36 games'], df.at[i, 'O 36.5 games'], df.at[i, 'U 36.5 games'], df.at[i, 'O 37 games'], df.at[i, 'U 37 games'], df.at[i, 'AH -1.5 sets 1'], df.at[i, 'AH -1.5 sets 2'], df.at[i, 'AH -1.5 games 1'], df.at[i, 'AH -1.5 games 2'], df.at[i, 'AH 1.5 sets 1'], df.at[i, 'AH 1.5 sets 2'], df.at[i, 'AH -7 games 1'], df.at[i, 'AH -7 games 2'], df.at[i, 'AH -6.5 games 1'], df.at[i, 'AH -6.5 games 2'], df.at[i, 'AH -6 games 1'], df.at[i, 'AH -6 games 2'], df.at[i, 'AH -5.5 games 1'], df.at[i, 'AH -5.5 games 2'], df.at[i, 'AH -5 games 1'], df.at[i, 'AH -5 games 2'], df.at[i, 'AH -4.5 games 1'], df.at[i, 'AH -4.5 games 2'], df.at[i, 'AH -4 games 1'], df.at[i, 'AH -4 games 2'], df.at[i, 'AH -3.5 games 1'], df.at[i, 'AH -3.5 games 2'], df.at[i, 'AH -3 games 1'], df.at[i, 'AH -3 games 2'], df.at[i, 'AH -2.5 games 1'], df.at[i, 'AH -2.5 games 2'], df.at[i, 'AH -2 games 1'], df.at[i, 'AH -2 games 2'], df.at[i, 'AH -1 games 1'], df.at[i, 'AH -1 games 2'], df.at[i, 'AH -0.5 games 1'], df.at[i, 'AH -0.5 games 2'], df.at[i, 'AH 0 games 1'], df.at[i, 'AH 0 games 2'], df.at[i, 'AH 0.5 games 1'], df.at[i, 'AH 0.5 games 2'], df.at[i, 'AH 1 games 1'], df.at[i, 'AH 1 games 2'], df.at[i, 'AH 1.5 games 1'], df.at[i, 'AH 1.5 games 2'], df.at[i, 'AH 2 games 1'], df.at[i, 'AH 2 games 2'], df.at[i, 'AH 2.5 games 1'], df.at[i, 'AH 2.5 games 2'], df.at[i, 'AH 3 games 1'], df.at[i, 'AH 3 games 2'], df.at[i, 'AH 3.5 games 1'], df.at[i, 'AH 3.5 games 2'], df.at[i, 'AH 4 games 1'], df.at[i, 'AH 4 games 2'], df.at[i, 'AH 4.5 games 1'], df.at[i, 'AH 4.5 games 2'], df.at[i, 'AH 5 games 1'], df.at[i, 'AH 5 games 2'], df.at[i, 'AH 5.5 games 1'], df.at[i, 'AH 5.5 games 2'], df.at[i, 'AH 6 games 1'], df.at[i, 'AH 6 games 2'], df.at[i, 'AH 6.5 games 1'], df.at[i, 'AH 6.5 games 2'], df.at[i, 'AH 7 games 1'], df.at[i, 'AH 7 games 2'], df.at[i, 'AH 7.5 games 1'], df.at[i, 'AH 7.5 games 2'], df.at[i, 'Correct Score 3:0'], df.at[i, 'Correct Score 2:0'], df.at[i, 'Correct Score 2:1'], df.at[i, 'Correct Score 3:1'], df.at[i, 'Correct Score 3:2'], df.at[i, 'Correct Score 0:3'], df.at[i, 'Correct Score 0:2'], df.at[i, 'Correct Score 1:2'], df.at[i, 'Correct Score 1:3'], df.at[i, 'Correct Score 2:3']))
                self.cur.execute("INSERT INTO odds_master VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (df.at[i, 'Link'], df.at[i, 'Date'], 0, df.at[i, 'Player1'], df.at[i, 'Player2'], df.at[i, 'Info'], df.at[i, 'Location'], df.at[i, 'Score'], df.at[i, 'Ranking1'], df.at[i, 'Ranking2'], datetime.strptime(str(df.at[i, 'Birthdate1']), '%d.%m.%Y').date(), datetime.strptime(str(df.at[i, 'Birthdate2']), '%d.%m.%Y').date(), df.at[i, 'Height1'], df.at[i, 'Height2'], df.at[i, 'Weight1'], df.at[i, 'Weight2'], df.at[i, 'Hand1'], df.at[i, 'Hand2'], df.at[i, 'Pro1'], df.at[i, 'Pro2'], df.at[i, 'Home'], df.at[i, 'Away']))
                self.conn.commit()
            except:
                pass

        t1 = datetime.now()
        print(t1)
        print("It actually took ",(t1-t0)," seconds.")

    def view(self):
        self.cur.execute("SELECT * FROM links_reg")
        rows=self.cur.fetchall()
        return rows

    def Ranking_Down(self, StDt, EndDt, Days, db):
        t0 = datetime.now()
        print('Start time: ', t0)

        self.StDt = datetime.strptime(str(StDt), '%d.%m.%Y').date()
        if EndDt != 0:
            self.EndDt = datetime.strptime(str(EndDt), '%d.%m.%Y').date()
            self.Days_between = self.StDt - self.EndDt
            self.Days_to_Down = abs(self.Days_between.days)
        if Days != 0:
            self.Days_to_Down = int(Days)
            
        URL_list = []
        for i in range(0, self.Days_to_Down):
            if datetime.weekday(self.StDt) == 0:
                URL_list.append("https://www.atptour.com/en/rankings/singles?rankDate=" + str(self.StDt.year) + "-" + str(self.StDt.month) + "-" + str(self.StDt.day) + "&rankRange=1-5000")
            self.StDt = self.StDt - timedelta(days=1)

        #test, which dates already in database
        conn = sqlite3.connect(db)
        cur = conn.cursor()
        cur.execute("SELECT * from ranking_master")
        Links = [tup[2] for tup in cur.fetchall()]  

        # for i in range(0, len(URL_list)):
        #     if URL_list[i] in str(Links):
        #         URL_list.pop(i)
        #         pass

        #if len(URL_list) == 0:
        #    print('All dates already downloaded.')
            

        Rank = []
        Name = []
        Link = []
        Points = []
        Tourn = []
        Age = []
        Ident = []
        Dates = []

        for i in range(0, len(URL_list)):
            if URL_list[i] not in str(Links):
                page=requests.get(URL_list[i])
                html_soup = soup(page.text,'html.parser')
                cont = html_soup.find('table', class_='mega-table')
                for s in range(0, len(cont.tbody.find_all('tr'))):
                    Rank.append(s+1)
                    Name.append(cont.tbody.find_all('tr')[s].find_all('td')[3].find('a').get_text().encode('utf-8').strip())
                    Points.append(str(cont.tbody.find_all('tr')[s].find_all('td')[5].get_text()[1:]).replace(",",""))
                    Tourn.append(str(cont.tbody.find_all('tr')[s].find_all('td')[6].get_text())[1:-1])
                    Link.append(URL_list[i])
                    if str(cont.tbody.find_all('tr')[s].find_all('td')[4].get_text()[2:-4])=="":
                        Age.append(0)
                    else:
                        Age.append(str(cont.tbody.find_all('tr')[s].find_all('td')[4].get_text()[2:-4]))
                    Ident.append(URL_list[i][52:URL_list[0].find('&')] + str(cont.tbody.find_all('tr')[s].find_all('td')[3].find('a').get_text().encode('utf-8').strip()))
                    Dates.append(URL_list[i][52:URL_list[0].find('&')])
            print(round(i/len(URL_list)*100, 2)," finished")
                        
        for i in range(0, len(Age)):
            try:
                cur.execute("INSERT INTO ranking_master VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (Ident[i], 0, Link[i], int(Rank[i]), Dates[i], str(Name[i]), int(Age[i]), int(Points[i]), int(Tourn[i])))
                conn.commit()
            except:
                pass
           
        t1 = datetime.now()
        print(t1)
        print("It actually took ",(t1-t0)," seconds.")        

    def Update_IDs(self, db):
        t0 = datetime.now()
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        
        #1.1 Get data from DB
        #get data from players list
        self.cur.execute("SELECT * from players_master")
        ID = [tup[0] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from players_master")
        Name = [tup[1] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from players_master")
        Birthday = [tup[3] for tup in self.cur.fetchall()]
        df_play = pd.DataFrame({'ID':ID, 'Name':Name, 'Birthday':Birthday})
        df_play.set_index('ID')
        
        
        #1.2 get data for DBs where to adjust values
        #odds
        self.cur.execute("SELECT * from odds_master")
        primary_odds = [tup[0] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from odds_master")
        dates_odds = [tup[1] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from odds_master")
        ID_odds = [tup[2] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from odds_master")
        p1_odds = [tup[3] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from odds_master")
        p2_odds = [tup[4] for tup in self.cur.fetchall()]
        df_odds = pd.DataFrame({'Primary':primary_odds, 'Dates':dates_odds, 'ID':ID_odds, 'ID_check':ID_odds, 'Player1':p1_odds, 'Player2':p2_odds})
        
        #reg
        self.cur.execute("SELECT * from reg_master")
        primary_reg = [tup[0] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from reg_master")
        dates_reg = [tup[1] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from reg_master")
        ID_reg = [tup[2] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from reg_master")
        p1_reg = [tup[3] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from reg_master")
        p2_reg = [tup[4] for tup in self.cur.fetchall()]
        df_reg = pd.DataFrame({'Primary':primary_reg, 'Dates':dates_reg, 'ID':ID_reg, 'ID_check':ID_reg, 'Player1':p1_reg, 'Player2':p2_reg})
        df_reg = df_reg[df_reg['ID'].isnull()]
        df_reg.reset_index(inplace = True)
        df_reg.drop('index', axis=1, inplace = True)
        
        #ranking
        
        #2. Calculate Match IDs: P1**2 * P2**2 + timestamp
        # odds_master
        try:
            missing_odds = df_odds.loc[df_odds['ID'] == 0]
            print('In the odds DB there are ', len(missing_odds),' match IDs missing')
            for i in range(0, len(df_odds['ID'])):
                if df_odds.at[i, 'ID'] == 0:
                    df_odds.at[i, 'ID'] = int(df_play[df_play['Name'] == df_odds.at[i, 'Player1']]['ID'].iloc[0]**2 * df_play[df_play['Name'] == df_odds.at[i, 'Player2']]['ID'].iloc[0]**2 + int(time.mktime(datetime.strptime(df_odds.at[i, 'Dates'], "%d.%m.%Y").timetuple())))
        except:
            pass
        
        #reg_master
        
        p1_reg = df_reg['Player1'].tolist()
        p2_reg = df_reg['Player2'].tolist()
        for player in p2_reg:
            p1_reg.append(player)
            
        df_reg_alt = pd.DataFrame({'Player': p1_reg})
        df_reg_alt.drop_duplicates('Player', inplace = True)
        df_reg_alt.reset_index(inplace = True)
        df_reg_alt.drop('index', axis=1, inplace = True)
        
        
        #check if name is already in DB
        for i in range(0, len(df_reg_alt['Player'])):
            try:
                df_reg_alt.at[i, 'PID'] = int(df_play[df_reg_alt.at[i, 'Player'] == df_play['Name_Reg']]['ID'])
            except:
                pass   
        for i in range(0, len(df_reg_alt['Player'])):
            if i == 0:
                print('Initiating the word comparison...')
            if i%100 == 0:
                print('Progress: ', i/len(df_reg_alt['Player']) *100, '%')
            if math.isnan(df_reg_alt.at[i, 'PID']) == True:
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
        #odds_master
        for i in range(0, len(df_odds['ID'])):
            if df_odds.at[i, 'ID_check'] == 0:
                self.cur.execute("UPDATE odds_master SET MatchID = ? WHERE Link = ?", (int(df_odds.at[i, 'ID']), df_odds.at[i, 'Primary']))
                self.conn.commit()
        print('Match IDs for odds_master updated, step 1/3')
        
        #reg
        for i in range(0, len(df_reg['ID'])):
            if i == 0:
                print('Initiating the time intense upload to the db...')
            if i%100 == 0:
                print('Progress: ', i/len(df_reg_alt['Player']) *100, '%')
            if math.isnan(df_reg.at[i, 'ID_check']):
                if df_reg.at[i, 'ID'] != 0:
                    try:
                        self.cur.execute("UPDATE reg_master SET MatchID = ? WHERE Link = ?", (int(df_reg.at[i, 'ID']), df_reg.at[i, 'Primary']))
                        self.cur.execute("UPDATE reg_master SET Dates = ? WHERE Link = ?", (df_reg.at[i, 'Dates'], df_reg.at[i, 'Primary']))
                        self.conn.commit()        
                    except:
                        pass
        print('Match IDs for reg_master updated, step 2/3')
        
        #ranking (P ID)
        
        
        
        t1 = datetime.now()
        print(t1)
        print("It actually took ",(t1-t0)," seconds.")
        
    def Update_Players(self, db):
        #extract data from DB odds
        t0 = datetime.now()
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT * from odds_master")
        Player1 = [tup[3] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from odds_master")
        Player2 = [tup[4] for tup in self.cur.fetchall()]  
        self.cur.execute("SELECT * from odds_master")
        Birthday1 = [tup[10] for tup in self.cur.fetchall()] 
        self.cur.execute("SELECT * from odds_master")
        Birthday2 = [tup[11] for tup in self.cur.fetchall()]  
        self.cur.execute("SELECT * from odds_master")
        Hand1 = [tup[16] for tup in self.cur.fetchall()]  
        self.cur.execute("SELECT * from odds_master")
        Hand2 = [tup[17] for tup in self.cur.fetchall()]  
        #extract data from DB player masters
        self.cur.execute("SELECT * from players_master")
        Player_Mas = [tup[1] for tup in self.cur.fetchall()] 
        self.cur.execute("SELECT * from players_master")
        ID_Mas = [tup[0] for tup in self.cur.fetchall()] 
        self.cur.execute("SELECT * from players_master")
        Hand_Mas = [tup[2] for tup in self.cur.fetchall()] 
        self.cur.execute("SELECT * from players_master")
        Birthday_Mas = [tup[3] for tup in self.cur.fetchall()] 
        
        #get one list
        for player in Player2:
            Player1.append(player)
        for birthday in Birthday2:
            Birthday1.append(birthday)
        for hand in Hand2:
            Hand1.append(hand)
        
        #get DF with individual players
        df = pd.DataFrame({'Player':Player1, 'Birthday':Birthday1, 'Hand':Hand1})
        df.drop_duplicates('Player', inplace = True)
        df.reset_index(inplace = True)
        
        #check if player in DB, if not, append
        for i in range(0, len(df['Player'])):
            try:
                if not df.at[i, 'Player'] in Player_Mas:
                    Player_Mas.append(df.at[i, 'Player'])
                    try:
                        ID_Mas.append(ID_Mas[-1] + 1)
                    except:
                        ID_Mas.append(1)
                    Hand_Mas.append(df.at[i, 'Hand'])
                    Birthday_Mas.append(df.at[i, 'Birthday'])
                else:
                    pass
            except:
                pass
        df_mas = pd.DataFrame({'ID':ID_Mas, 'Name':Player_Mas, 'Hand':Hand_Mas, 'Birthday':Birthday_Mas})
        for i in range(0, len(df_mas['Birthday'])):
            try:
                df_mas.at[i, 'Birthday'] = datetime.strptime(df_mas.at[i, 'Birthday'], "%Y-%m-%d").strftime("%d.%m.%Y")
            except:
                pass
        
        #Update in DB
        for i in range(0, len(df_mas['ID'])):
            try:
                self.cur.execute("INSERT INTO players_master VALUES (?, ?, ?, ?)", (int(df_mas.at[i, 'ID']), df_mas.at[i, 'Name'], df_mas.at[i, 'Hand'], df_mas.at[i, 'Birthday']))
                self.conn.commit()
            except:
                pass        
        
        print('The update took', datetime.now() - t0, ' seconds. There are ', len(df_mas['ID']), ' Players in the DB.')
        
    def Map_Out(self, db):
        print('Function in progress...')
        print(db)
    
    def __del__(self):
        self.conn.close()
        print('Bye bye, connection to DB is closed.')