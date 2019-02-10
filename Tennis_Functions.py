from bs4 import BeautifulSoup as soup
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import sqlite3
from fuzzywuzzy import fuzz, process

class Tennis_Downloads:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.cur.execute("CREATE TABLE IF NOT EXISTS links_odds (Dates DATE PRIMARY KEY, link TEXT)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS links_reg (Dates DATE PRIMARY KEY, link TEXT)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS links_odds_match (Link TEXT PRIMARY KEY, Dates date)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS links_reg_match (Link TEXT PRIMARY KEY, Dates date)")
        #self.cur.execute("DROP TABLE odds_master")
        self.cur.execute("CREATE TABLE IF NOT EXISTS odds_master (Link TEXT PRIMARY KEY, Dates date, MatchID INTEGER, Player1 TEXT, Player2 TEXT, Info TEXT, Location TEXT, Score TEXT, Ranking1 INTEGER, Ranking2 INTEGER, Birthday1 Date, Birthday2 DATE, Height1 REAL, Height2 REAL, Weight1 REAL, Weight2 REAL, Hand1 TEXT, Hand2 TEXT, Pro1 INTEGER, Pro2 INTEGER, O_2_5_sets REAL, U_2_5_sets REAL,  O_3_sets REAL,  U_3_sets REAL,  O_3_5_sets REAL,  U_3_5_sets REAL,  O_4_sets REAL,  U_4_sets REAL,  O_4_5_sets REAL,  U_4_5_sets REAL,  O_14_5_games REAL,  U_14_5_games REAL,  O_15_games REAL,  U_15_games REAL,  O_15_5_games REAL,  U_15_5_games REAL,  O_16_games REAL,  U_16_games REAL,  O_16_5_games REAL,  U_16_5_games REAL,  O_17_games REAL,  U_17_games REAL,  O_17_5_games REAL,  U_17_5_games REAL,  O_18_games REAL,  U_18_games REAL,  O_18_5_games REAL,  U_18_5_games REAL,  O_19_games REAL,  U_19_games REAL,  O_19_5_games REAL,  U_19_5_games REAL,  O_20_games REAL,  U_20_games REAL,  O_20_5_games REAL,  U_20_5_games REAL,  O_21_games REAL,  U_21_games REAL,  O_21_5_games REAL,  U_21_5_games REAL,  O_22_games REAL,  U_22_games REAL,  O_22_5_games REAL,  U_22_5_games REAL,  O_23_games REAL,  U_23_games REAL,  O_23_5_games REAL,  U_23_5_games REAL,  O_24_games REAL,  U_24_games REAL,  O_24_5_games REAL,  U_24_5_games REAL,  O_25_games REAL,  U_25_games REAL,  O_25_5_games REAL,  U_25_5_games REAL,  O_26_games REAL,  U_26_games REAL,  O_26_5_games REAL,  U_26_5_games REAL,  O_27_games REAL,  U_27_games REAL,  O_27_5_games REAL,  U_27_5_games REAL,  O_28_games REAL,  U_28_games REAL,  O_28_5_games REAL,  U_28_5_games REAL,  O_29_games REAL,  U_29_games REAL,  O_29_5_games REAL,  U_29_5_games REAL,  O_30_games REAL,  U_30_games REAL,  O_30_5_games REAL,  U_30_5_games REAL,  O_31_games REAL,  U_31_games REAL,  O_31_5_games REAL,  U_31_5_games REAL,  O_32_games REAL,  U_32_games REAL,  O_32_5_games REAL,  U_32_5_games REAL,  O_33_games REAL,  U_33_games REAL,  O_33_5_games REAL,  U_33_5_games REAL,  O_34_games REAL,  U_34_games REAL,  O_34_5_games REAL,  U_34_5_games REAL,  O_35_games REAL,  U_35_games REAL,  O_35_5_games REAL,  U_35_5_games REAL,  O_36_games REAL,  U_36_games REAL,  O_36_5_games REAL,  U_36_5_games REAL,  O_37_games REAL,  U_37_games REAL,  AH__1_5_sets_1 REAL,  AH__1_5_sets_2 REAL,  AH__1_5_games_1 REAL,  AH__1_5_games_2 REAL,  AH_1_5_sets_1 REAL,  AH_1_5_sets_2 REAL,  AH__7_games_1 REAL,  AH__7_games_2 REAL,  AH__6_5_games_1 REAL,  AH__6_5_games_2 REAL,  AH__6_games_1 REAL,  AH__6_games_2 REAL,  AH__5_5_games_1 REAL,  AH__5_5_games_2 REAL,  AH__5_games_1 REAL,  AH__5_games_2 REAL,  AH__4_5_games_1 REAL,  AH__4_5_games_2 REAL,  AH__4_games_1 REAL,  AH__4_games_2 REAL,  AH__3_5_games_1 REAL,  AH__3_5_games_2 REAL,  AH__3_games_1 REAL,  AH__3_games_2 REAL,  AH__2_5_games_1 REAL,  AH__2_5_games_2 REAL,  AH__2_games_1 REAL,  AH__2_games_2 REAL,  AH__1_games_1 REAL,  AH__1_games_2 REAL,  AH__0_5_games_1 REAL,  AH__0_5_games_2 REAL,  AH_0_games_1 REAL,  AH_0_games_2 REAL,  AH_0_5_games_1 REAL,  AH_0_5_games_2 REAL,  AH_1_games_1 REAL,  AH_1_games_2 REAL,  AH_1_5_games_1 REAL,  AH_1_5_games_2 REAL,  AH_2_games_1 REAL,  AH_2_games_2 REAL,  AH_2_5_games_1 REAL,  AH_2_5_games_2 REAL,  AH_3_games_1 REAL,  AH_3_games_2 REAL,  AH_3_5_games_1 REAL,  AH_3_5_games_2 REAL,  AH_4_games_1 REAL,  AH_4_games_2 REAL,  AH_4_5_games_1 REAL,  AH_4_5_games_2 REAL,  AH_5_games_1 REAL,  AH_5_games_2 REAL,  AH_5_5_games_1 REAL,  AH_5_5_games_2 REAL,  AH_6_games_1 REAL,  AH_6_games_2 REAL,  AH_6_5_games_1 REAL,  AH_6_5_games_2 REAL,  AH_7_games_1 REAL,  AH_7_games_2 REAL,  AH_7_5_games_1 REAL,  AH_7_5_games_2 REAL,  Correct_Score_3_0 REAL,  Correct_Score_2_0 REAL,  Correct_Score_2_1 REAL,  Correct_Score_3_1 REAL,  Correct_Score_3_2 REAL,  Correct_Score_0_3 REAL,  Correct_Score_0_2 REAL,  Correct_Score_1_2 REAL,  Correct_Score_1_3 REAL,  Correct_Score_2_3 REAL)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS reg_master (Link TEXT PRIMARY KEY, Dates date, MatchID INTEGER, Player1 TEXT, Player2 TEXT, Round TEXT, Score TEXT, Location TEXT, Surface TEXT, First_Serve_P1 TEXT, First_Serve_Points_Won_P1 TEXT, Second_Serve_Points_Won_P1 TEXT, Break_Points_Won_P1 TEXT, Total_Return_Points_Won_P1 TEXT, Total_Points_Won_P1 TEXT, Double_Faults_P1 INTEGER, Aces_P1 INTEGER, First_Serve_P2 TEXT, First_Serve_Points_Won_P2 TEXT, Second_Serve_Points_Won_P2 TEXT, Break_Points_Won_P2 TEXT, Total_Return_Points_Won_P2 TEXT, Total_Points_Won_P2 TEXT, Double_Faults_P2 INTEGER, Aces_P2 INTEGER)")
        #self.cur.execute("CREATE TABLE IF NOT EXISTS matched_master (MatchID INTEGER PRIMARY KEY, Dates date)")
        #self.cur.execute("CREATE TABLE IF NOT EXISTS players_list (PlayerID INTEGER PRIMARY KEY, Dates date)")
        #This is filled with user input, need initialize with values from the most comprehensive download, then update function to get newest players in
        self.cur.execute("CREATE TABLE IF NOT EXISTS players_master (PlayerID INTEGER PRIMARY KEY, Name TEXT, Hand TEXT, Birthdate DATE, Country TEXT)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS ranking_master (Ident INTEGER PRIMARY KEY, PlayerID INTEGER, link TEXT, rank INTEGER, Dates DATE, Name TEXT, Age INTEGER, Points INTEGER, Tourn_Played INTEGER)")
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
            link_cont = html_soup.find_all('a', title='Click for match detail')
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
            URL.append("http://www.tennisergebnisse.net/herren/" + str(self.StDt.year) + "-" + str(self.StDt.month) + "-" + str(self.StDt.day) + "/")
        self.df = pd.DataFrame({'Link':URL,'Dates':datesXL})
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        #write to database links_odds
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
        if Days != 0:
            self.Days_to_Down = int(Days)
        self.conn = sqlite3.connect(db)
        #self.conn.row_factory = lambda cursor, row: row[0] THIS ALLOWS TO ONLY GET THE DATA NEEDED, NOT THE TUPLE!
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT * from links_reg_match")
        #links = self.cur.fetchall()
        #df = pd.DataFrame(columns=['Dates','Link'])
        #or: Dates = [tup[1] for tup in self.cur.fetchall()] and Link = [tup[0] for tup in self.cur.fetchall()]
        Dates = [tup[1] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from links_reg_match")
        links = [tup[0] for tup in self.cur.fetchall()]        
        df = pd.DataFrame({'Dates':Dates, 'Link':links})
        #THIS IS OLD
        # for i in range(0, len(links)):
        #     df.at[i, 'Dates'] = links[i][1]
        #     df.at[i, 'Link'] = links[i][0]

        # self.cur.execute("SELECT Link from links_reg_match")
        # links = self.cur.fetchall()
        # self.cur.execute("SELECT Dates from links_reg_match")
        # dates = self.cur.fetchall() 
        # df = pd.DataFrame({'Dates':dates,'Link':links})
        # df.sort_values(['Dates'], axis=0, ascending=[0], inplace=True)
        df['Dates'] = pd.to_datetime(df['Dates'], format='%d.%m.%Y')
        df.sort_values('Dates', inplace=True, axis=0)
        df.set_index('Dates', inplace = True)
        df = df.loc[EndDt:StDt]
        df.reset_index(inplace = True)
        time_to_download = 0.3528
        print('Matches to download: ',len(df['Link']))
        print('Estimated finish time: ',(t0 + timedelta(seconds=(time_to_download*len(df['Link'])))))
        for f in range(0,len(df['Link'])):  
            page = requests.get(df.at[f, 'Link'])
            html_soup = soup(page.text,'html.parser')
            if html_soup.find('tr', class_='tour_head unpair') is not None:
                df.at[f, 'Date'] = html_soup.find('tr', class_='tour_head unpair').find_all('td')[0].text
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
        print('Start time: ',t0)
        self.StDt = datetime.strptime(str(StDt), '%d.%m.%Y').date()
        if EndDt != 0:
            self.EndDt = datetime.strptime(str(EndDt), '%d.%m.%Y').date()
            self.Days_between = self.StDt - self.EndDt
            self.Days_to_Down = self.Days_between.days
        if Days != 0:
            self.Days_to_Down = int(Days)
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT * from links_odds_match")
        Dates = [tup[1] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from links_odds_match")
        links = [tup[0] for tup in self.cur.fetchall()]        
        df = pd.DataFrame({'Dates':Dates, 'Link':links})
        df['Dates'] = pd.to_datetime(df['Dates'], format='%d.%m.%Y')
        df.sort_values('Dates', inplace=True)

        df.set_index('Dates', inplace = True)
        df = df.loc[EndDt:StDt]
        df.reset_index(inplace = True)
        time_to_download = 0.75
        print('Downloading from ',EndDt,' to ',StDt)
        print('Matches to download: ',len(df['Link']))
        print('Estimated finish time: ',(t0 + timedelta(seconds=(time_to_download*len(df['Link'])))))      

        for i in range(0,len(df['Link'])): 
            page = requests.get(df.at[i, 'Link'])
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
                    df.at[i, 'Pro'] = TblTr[5].find_all('td')[0].text
                    df.at[i, 'Pro'] = TblTr[5].find_all('td')[1].text  
                    #Home/Away
                if not '(0)' in html_soup.find('li',id='oddsMenu-1').a.text:
                    df.at[i, 'Home'] = html_soup.find('tr', class_='average').find('td', class_='k1').text
                    df.at[i, 'Away'] = html_soup.find('tr', class_='average').find('td', class_='k2').text
                    #Over/Under
                if not '(0)' in html_soup.find('li',id='oddsMenu-2').a.text:
                    for s in range(0,len(html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type'))):
                        try:
                            if html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 2.5 sets':
                                df.at[i,'O 2.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 2.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 3 sets':
                                df.at[i,'O 3 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 3 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 3.5 sets':
                                df.at[i,'O 3.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 3.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 4 sets':
                                df.at[i,'O 4 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 4 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 4.5 sets':
                                df.at[i,'O 4.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 4.5 sets'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 14.5 games':
                                df.at[i,'O 14.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 14.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 15 games':
                                df.at[i,'O 15 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 15 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 15.5 games':
                                df.at[i,'O 15.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 15.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 16 games':
                                df.at[i,'O 16 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 16 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 16.5 games':
                                df.at[i,'O 16.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 16.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 17 games':
                                df.at[i,'O 17 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 17 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 17.5 games':
                                df.at[i,'O 17.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 17.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 18 games':
                                df.at[i,'O 18 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 18 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 18.5 games':
                                df.at[i,'O 18.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 18.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 19 games':
                                df.at[i,'O 19 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 19 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 19.5 games':
                                df.at[i,'O 19.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 19.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 20 games':
                                df.at[i,'O 20 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 20 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 20.5 games':
                                df.at[i,'O 20.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 20.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 21 games':
                                df.at[i,'O 21 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 21 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 21.5 games':
                                df.at[i,'O 21.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 21.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 22 games':
                                df.at[i,'O 22 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 22 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 22.5 games':
                                df.at[i,'O 22.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 22.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 23 games':
                                df.at[i,'O 23 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 23 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 23.5 games':
                                df.at[i,'O 23.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 23.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 24 games':
                                df.at[i,'O 24 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 24 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 24.5 games':
                                df.at[i,'O 24.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 24.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 25 games':
                                df.at[i,'O 25 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 25 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 25.5 games':
                                df.at[i,'O 25.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 25.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 26 games':
                                df.at[i,'O 26 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 26 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 26.5 games':
                                df.at[i,'O 26.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 26.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 27 games':
                                df.at[i,'O 27 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 27 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 27.5 games':
                                df.at[i,'O 27.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 27.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 28 games':
                                df.at[i,'O 28 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 28 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 28.5 games':
                                df.at[i,'O 28.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 28.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 29 games':
                                df.at[i,'O 29 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 29 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 29.5 games':
                                df.at[i,'O 29.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 29.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 30 games':
                                df.at[i,'O 30 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 30 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 30.5 games':
                                df.at[i,'O 30.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 30.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 31 games':
                                df.at[i,'O 31 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 31 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 31.5 games':
                                df.at[i,'O 31.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 31.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text   
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 32 games':
                                df.at[i,'O 32 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 32 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text     
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 32.5 games':
                                df.at[i,'O 32.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 32.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text     
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 33 games':
                                df.at[i,'O 33 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 33 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 33.5 games':
                                df.at[i,'O 33.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 33.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 34 games':
                                df.at[i,'O 34 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 34 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 34.5 games':
                                df.at[i,'O 34.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 34.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 35 games':
                                df.at[i,'O 35 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 35 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 35.5 games':
                                df.at[i,'O 35.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 35.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 36 games':
                                df.at[i,'O 36 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 36 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 36.5 games':
                                df.at[i,'O 36.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 36.5 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                            elif html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Over/Under 37 games':
                                df.at[i,'O 37 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'U 37 games'] = html_soup.find('div', id='oddsMenu-2-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text 
                        except:
                            pass
                    #Over/Under
                if not '(0)' in html_soup.find('li',id='oddsMenu-3').a.text:
                    for s in range(0,len(html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type'))):
                        try:
                            if html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -1.5 sets':
                                df.at[i,'AH -1.5 sets 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -1.5 sets 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -1.5 games':
                                df.at[i,'AH -1.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -1.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 1.5 games':
                                df.at[i,'AH 1.5 sets 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 1.5 sets 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -7 games':
                                df.at[i,'AH -7 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -7 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -6.5 games':
                                df.at[i,'AH -6.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -6.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -6 games':
                                df.at[i,'AH -6 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -6 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -5.5 games':
                                df.at[i,'AH -5.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -5.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -5 games':
                                df.at[i,'AH -5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -4.5 games':
                                df.at[i,'AH -4.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -4.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -4 games':
                                df.at[i,'AH -4 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -4 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -3.5 games':
                                df.at[i,'AH -3.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -3.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -3 games':
                                df.at[i,'AH -3 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -3 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -2.5 games':
                                df.at[i,'AH -2.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -2.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -2 games':
                                df.at[i,'AH -2 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -2 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -1 games':
                                df.at[i,'AH -1 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -1 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap -0.5 games':
                                df.at[i,'AH -0.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH -0.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 0 games':
                                df.at[i,'AH 0 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 0 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 0.5 games':
                                df.at[i,'AH 0.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 0.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 1 games':
                                df.at[i,'AH 1 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 1 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 1.5 games':
                                df.at[i,'AH 1.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 1.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 2 games':
                                df.at[i,'AH 2 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 2 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 2.5 games':
                                df.at[i,'AH 2.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 2.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 3 games':
                                df.at[i,'AH 3 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 3 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 3.5 games':
                                df.at[i,'AH 3.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 3.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 4 games':
                                df.at[i,'AH 4 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 4 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 4.5 games':
                                df.at[i,'AH 4.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 4.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 5 games':
                                df.at[i,'AH 5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 5.5 games':
                                df.at[i,'AH 5.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 5.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 6 games':
                                df.at[i,'AH 6 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 6 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 6.5 games':
                                df.at[i,'AH 6.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 6.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 7 games':
                                df.at[i,'AH 7 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 7 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                            elif html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Asian Handicap 7.5 games':
                                df.at[i,'AH 7.5 games 1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                                df.at[i,'AH 7.5 games 2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k2').text
                        except:
                            pass                                
                    #CS
                if not '(0)' in html_soup.find('li',id='oddsMenu-4').a.text:
                    for s in range(0,len(html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type'))):
                        try:
                            if html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 3:0':
                                df.at[i,'Correct Score 3:0'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                            elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 2:0':
                                df.at[i,'Correct Score 2:0'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                            elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 2:1':
                                df.at[i,'Correct Score 2:1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                            elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 3:1':
                                df.at[i,'Correct Score 3:1'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                            elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 3:2':
                                df.at[i,'Correct Score 3:2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                            elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 0:3':
                                df.at[i,'Correct Score 0:3'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                            elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 0:2':
                                df.at[i,'Correct Score 0:2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                            elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 1:2':
                                df.at[i,'Correct Score 1:2'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                            elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 1:3':
                                df.at[i,'Correct Score 1:3'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                            elif html_soup.find('div', id='oddsMenu-4-data').find('table',class_='result').tbody.find_all('tr',class_='odds-type')[s].text.strip() == 'Correct Score 2:3':
                                df.at[i,'Correct Score 2:3'] = html_soup.find('div', id='oddsMenu-3-data').find('table',class_='result').tbody.find_all('tr',class_='average')[s].find('td',class_='k1').text
                        except:
                            pass




        for i in range(0,len(df['Link'])):
            try:
                self.cur.execute("INSERT INTO odds_master VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (df.at[i, 'Link'], df.at[i, 'Date'], df.at[i, 'MatchID'], df.at[i, 'Player1'], df.at[i, 'Player2'], df.at[i, 'Info'], df.at[i, 'Location'], df.at[i, 'Score'], df.at[i, 'Ranking1'], df.at[i, 'Ranking2'], df.at[i, 'Birthday1'], df.at[i, 'Birthday2'], df.at[i, 'Height1'], df.at[i, 'Height2'], df.at[i, 'Weight1'], df.at[i, 'Weight2'], df.at[i, 'Hand1'], df.at[i, 'Hand2'], df.at[i, 'Pro1'], df.at[i, 'Pro2'], df.at[i, 'O 2.5 sets'], df.at[i, 'U 2.5 sets'], df.at[i, 'O 3 sets'], df.at[i, 'U 3 sets'], df.at[i, 'O 3.5 sets'], df.at[i, 'U 3.5 sets'], df.at[i, 'O 4 sets'], df.at[i, 'U 4 sets'], df.at[i, 'O 4.5 sets'], df.at[i, 'U 4.5 sets'], df.at[i, 'O 14.5 games'], df.at[i, 'U 14.5 games'], df.at[i, 'O 15 games'], df.at[i, 'U 15 games'], df.at[i, 'O 15.5 games'], df.at[i, 'U 15.5 games'], df.at[i, 'O 16 games'], df.at[i, 'U 16 games'], df.at[i, 'O 16.5 games'], df.at[i, 'U 16.5 games'], df.at[i, 'O 17 games'], df.at[i, 'U 17 games'], df.at[i, 'O 17.5 games'], df.at[i, 'U 17.5 games'], df.at[i, 'O 18 games'], df.at[i, 'U 18 games'], df.at[i, 'O 18.5 games'], df.at[i, 'U 18.5 games'], df.at[i, 'O 19 games'], df.at[i, 'U 19 games'], df.at[i, 'O 19.5 games'], df.at[i, 'U 19.5 games'], df.at[i, 'O 20 games'], df.at[i, 'U 20 games'], df.at[i, 'O 20.5 games'], df.at[i, 'U 20.5 games'], df.at[i, 'O 21 games'], df.at[i, 'U 21 games'], df.at[i, 'O 21.5 games'], df.at[i, 'U 21.5 games'], df.at[i, 'O 22 games'], df.at[i, 'U 22 games'], df.at[i, 'O 22.5 games'], df.at[i, 'U 22.5 games'], df.at[i, 'O 23 games'], df.at[i, 'U 23 games'], df.at[i, 'O 23.5 games'], df.at[i, 'U 23.5 games'], df.at[i, 'O 24 games'], df.at[i, 'U 24 games'], df.at[i, 'O 24.5 games'], df.at[i, 'U 24.5 games'], df.at[i, 'O 25 games'], df.at[i, 'U 25 games'], df.at[i, 'O 25.5 games'], df.at[i, 'U 25.5 games'], df.at[i, 'O 26 games'], df.at[i, 'U 26 games'], df.at[i, 'O 26.5 games'], df.at[i, 'U 26.5 games'], df.at[i, 'O 27 games'], df.at[i, 'U 27 games'], df.at[i, 'O 27.5 games'], df.at[i, 'U 27.5 games'], df.at[i, 'O 28 games'], df.at[i, 'U 28 games'], df.at[i, 'O 28.5 games'], df.at[i, 'U 28.5 games'], df.at[i, 'O 29 games'], df.at[i, 'U 29 games'], df.at[i, 'O 29.5 games'], df.at[i, 'U 29.5 games'], df.at[i, 'O 30 games'], df.at[i, 'U 30 games'], df.at[i, 'O 30.5 games'], df.at[i, 'U 30.5 games'], df.at[i, 'O 31 games'], df.at[i, 'U 31 games'], df.at[i, 'O 31.5 games'], df.at[i, 'U 31.5 games'], df.at[i, 'O 32 games'], df.at[i, 'U 32 games'], df.at[i, 'O 32.5 games'], df.at[i, 'U 32.5 games'], df.at[i, 'O 33 games'], df.at[i, 'U 33 games'], df.at[i, 'O 33.5 games'], df.at[i, 'U 33.5 games'], df.at[i, 'O 34 games'], df.at[i, 'U 34 games'], df.at[i, 'O 34.5 games'], df.at[i, 'U 34.5 games'], df.at[i, 'O 35 games'], df.at[i, 'U 35 games'], df.at[i, 'O 35.5 games'], df.at[i, 'U 35.5 games'], df.at[i, 'O 36 games'], df.at[i, 'U 36 games'], df.at[i, 'O 36.5 games'], df.at[i, 'U 36.5 games'], df.at[i, 'O 37 games'], df.at[i, 'U 37 games'], df.at[i, 'AH -1.5 sets 1'], df.at[i, 'AH -1.5 sets 2'], df.at[i, 'AH -1.5 games 1'], df.at[i, 'AH -1.5 games 2'], df.at[i, 'AH 1.5 sets 1'], df.at[i, 'AH 1.5 sets 2'], df.at[i, 'AH -7 games 1'], df.at[i, 'AH -7 games 2'], df.at[i, 'AH -6.5 games 1'], df.at[i, 'AH -6.5 games 2'], df.at[i, 'AH -6 games 1'], df.at[i, 'AH -6 games 2'], df.at[i, 'AH -5.5 games 1'], df.at[i, 'AH -5.5 games 2'], df.at[i, 'AH -5 games 1'], df.at[i, 'AH -5 games 2'], df.at[i, 'AH -4.5 games 1'], df.at[i, 'AH -4.5 games 2'], df.at[i, 'AH -4 games 1'], df.at[i, 'AH -4 games 2'], df.at[i, 'AH -3.5 games 1'], df.at[i, 'AH -3.5 games 2'], df.at[i, 'AH -3 games 1'], df.at[i, 'AH -3 games 2'], df.at[i, 'AH -2.5 games 1'], df.at[i, 'AH -2.5 games 2'], df.at[i, 'AH -2 games 1'], df.at[i, 'AH -2 games 2'], df.at[i, 'AH -1 games 1'], df.at[i, 'AH -1 games 2'], df.at[i, 'AH -0.5 games 1'], df.at[i, 'AH -0.5 games 2'], df.at[i, 'AH 0 games 1'], df.at[i, 'AH 0 games 2'], df.at[i, 'AH 0.5 games 1'], df.at[i, 'AH 0.5 games 2'], df.at[i, 'AH 1 games 1'], df.at[i, 'AH 1 games 2'], df.at[i, 'AH 1.5 games 1'], df.at[i, 'AH 1.5 games 2'], df.at[i, 'AH 2 games 1'], df.at[i, 'AH 2 games 2'], df.at[i, 'AH 2.5 games 1'], df.at[i, 'AH 2.5 games 2'], df.at[i, 'AH 3 games 1'], df.at[i, 'AH 3 games 2'], df.at[i, 'AH 3.5 games 1'], df.at[i, 'AH 3.5 games 2'], df.at[i, 'AH 4 games 1'], df.at[i, 'AH 4 games 2'], df.at[i, 'AH 4.5 games 1'], df.at[i, 'AH 4.5 games 2'], df.at[i, 'AH 5 games 1'], df.at[i, 'AH 5 games 2'], df.at[i, 'AH 5.5 games 1'], df.at[i, 'AH 5.5 games 2'], df.at[i, 'AH 6 games 1'], df.at[i, 'AH 6 games 2'], df.at[i, 'AH 6.5 games 1'], df.at[i, 'AH 6.5 games 2'], df.at[i, 'AH 7 games 1'], df.at[i, 'AH 7 games 2'], df.at[i, 'AH 7.5 games 1'], df.at[i, 'AH 7.5 games 2'], df.at[i, 'Correct Score 3:0'], df.at[i, 'Correct Score 2:0'], df.at[i, 'Correct Score 2:1'], df.at[i, 'Correct Score 3:1'], df.at[i, 'Correct Score 3:2'], df.at[i, 'Correct Score 0:3'], df.at[i, 'Correct Score 0:2'], df.at[i, 'Correct Score 1:2'], df.at[i, 'Correct Score 1:3'], df.at[i, 'Correct Score 2:3']))
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
        for i in range(0, len(URL_list)):
            if URL_list[i] in Links:
                URL_list.pop[i]

        Rank = []
        Name = []
        Link = []
        Points = []
        Tourn = []
        Age = []
        Ident = []
        Dates = []

        for i in range(0, len(URL_list)):
            page=requests.get(URL_list[i])
            html_soup = soup(page.text,'html.parser')
            cont = html_soup.find('table', class_='mega-table')
            for s in range(0, len(cont.tbody.find_all('tr'))):
                Rank.append(s+1)
                Name.append(cont.tbody.find_all('tr')[s].find_all('td')[3].find('a').get_text().encode('utf-8').strip())
                Points.append(str(cont.tbody.find_all('tr')[s].find_all('td')[5].get_text()[1:]))
                Tourn.append(str(cont.tbody.find_all('tr')[s].find_all('td')[6].get_text())[1:-1])
                Link.append(URL_list[i])
                Age.append(str(cont.tbody.find_all('tr')[s].find_all('td')[4].get_text()[2:-4]))
                Ident.append(str(self.StDt) + cont.tbody.find_all('tr')[s].find_all('td')[3].find('a').get_text().encode('utf-8').strip())
                Dates.append(self.StDt)
                        
        for i in range(0, len(Age)):
            try:
                cur.execute("INSERT INTO ranking_master VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (Ident[i], 0, Link[i], Rank[i], Dates[i], Name[i], Age[i], Points[i], Tourn[i]))
                conn.commit()
            except:
                pass
           
        t1 = datetime.now()
        print(t1)
        print("It actually took ",(t1-t0)," seconds.")        

    def Update_Players(self):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT * from links_odds_match")
        Dates = [tup[1] for tup in self.cur.fetchall()]
        self.cur.execute("SELECT * from links_odds_match")
        links = [tup[0] for tup in self.cur.fetchall()]        
        df = pd.DataFrame({'Dates':Dates, 'Link':links})
        df['Dates'] = pd.to_datetime(df['Dates'], format='%d.%m.%Y')
        df.sort_values('Dates', inplace=True)       

        t0 = datetime.now()
        for element in P2[5:15]:
            closest.append(process.extractOne(str(element), P1))
        print(datetime.now() - t0)

    def __del__(self):
        
        self.conn.close()