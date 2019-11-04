#New Tennis Functions to download Odds and Stats, put them in a DB and analyze

#imports
from bs4 import BeautifulSoup as soup
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import sqlite3
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import math
from sqlalchemy import create_engine

class DB_operations:
    '''
    This Class contains all database functions
    '''
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        print('Connected to the database at ' + str(datetime.now() + ' .'))
        
    def write_dataframe(self, db, tbl, df):
        '''
        Write a dataframe to the database
        '''
        engine = create_engine('sqlite:///' + db)
        df.to_sql(tbl, engine, if_exists='append')      
        
    def check_for_duplicates(self, db, tbl, df):
        '''
        Check if some entries of df are already in db tbl, if so, drop them
        '''
        comps = DB_operations.retrieve_data(db, tbl, columns = [0])
        comps = comps.to_list()
        to_check = df.index.tolist()
        for comp in comps:
            if comp in to_check:
                df = df.drop(comp)
        return df
        
        
    def show_tables(self, db):
        '''
        Print a list of all the tables in a database
        '''
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        info = self.cur.fetchall()
        tbl_names = [table for table in info]
        return tbl_names

    def show_columns(self, db, tbl):
        '''
        Print a list of the columns in a database
        '''
        self.cur.execute("SELECT * from " + str(tbl))
        clm_names = [description[0] for description in self.cur.description]
        return clm_names
        
        
    def retrieve_data(self, db, tbl, columns = 'all'):
        '''
        Retrieve data from a database
        Input:
            db: String, adress and name of db
            tbl: String, table in the db
            columns: String, all, select or list of numbers of columns
        Output:
            df: DataFrame with all information retrieved
        '''
        df = pd.DataFrame()
        self.cur.execute("SELECT * from " + str(tbl))
        all_info = self.cur.fetchall()
        if columns == 'all':
            header = DB_operations.show_columns(db, tbl)
            i = 0
            for head in header:
                df[head] = [tup[i] for tup in all_info]
                i += 1
        elif columns == 'select':
            columns_all = DB_operations.show_columns(db, tbl)
            print('Columns in table: ' + str(columns_all))
            down_cols = input('''Which columns do you want to retrieve?
                              Please enter a list of numbers that correspond
                              to the order of the columns printed above,
                              starting with 0''')
            for col in down_cols:
                df[columns_all[col]] = [tup[col] for tup in all_info]
        else:
            columns_all = DB_operations.show_columns(db, tbl)
            for col in columns:
                df[columns_all[col]] = [tup[col] for tup in all_info]           
        return df
    
    def save_excel(df, name):
        '''
        Save DataFrame as excel, giving the DF and a name.
        '''
        writer=pd.ExcelWriter(name + '.xlsx', engine='xlsxwriter')
        df.to_excel(writer,'Sheet1',index=False)
        writer.save()
    
    def __del__(self):
        self.conn.close()
        print('Bye bye, connection to DB is closed.')

class Tennis_Downloads:
    '''
    This Class will contain all the functions needed to download data from
    the web, connect and retrieve data from the DB.
    '''
    def __init__(self):
        print('You can now download data for tennis statistics or odds')

    def download_link_odd(self, link):
        '''
        Download the links for individual matches from the link for all 
        the matches for the odd.
        Input:
            link: String, day URL
        Output:
            List with specific links
        '''
        page = requests.get(link)
        html_soup = soup(page.text,'html.parser')
        link_count = html_soup.find('table', class_ = 'result').find_all(
                'a', title='Click for match detail')
        control = 0
        links = []
        for link in link_count:
            links.append('http://www.tennisexplorer.com' + 
                         link_count[control].get('href'))
            control += 1
        return links
            

    def download_links_odds(self, db, tbl, in_up = 'update'):
        t0 = datetime.now()
        print('''Downloading links for the odds.
              Start at time: ''', t0)
        urls = []
        col1 = []
        col2 = []
        col3 = []
        start_date = datetime.now().date() - timedelta(days=1)
        df = pd.DataFrame()
        if in_up == 'initiate':
            end_date = datetime(1990, 1, 1).date()
            number_days = (start_date - end_date).days
            dates = [start_date - timedelta(days = n) for n in 
                     range(number_days)]
            urls = ["http://www.tennisexplorer.com/results/?year=" + 
                    str(date.year) + 
                    "&month=" + str(date.month) + "&day=" + 
                    str(date.day) for date in dates
                    ]
        elif in_up == 'update':
            df = DB_operations.retrieve_data(db, 
                                        'links_statistics', columns = [2])
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date', axis = 1)
            last_date = df.loc['Date'][-1:]
            number_days = (start_date - last_date).days - 1
            dates = [start_date - timedelta(days = n) for n in 
                     range(number_days)]
            urls = ["http://www.tennisexplorer.com/results/?year=" + 
                    str(date.year) + 
                    "&month=" + str(date.month) + "&day=" + 
                    str(date.day) for date in dates
                    ]            
        else:
            spec_date = datetime.strptime(in_up, '%d-%m-%Y').date()
            urls = ["http://www.tennisexplorer.com/results/?year=" + 
                    str(spec_date.year) + 
                    "&month=" + str(spec_date.month) + "&day=" + 
                    str(spec_date.day)
                    ]
            dates = [spec_date]
        i = 0
        for url in urls:
            col11 = []
            col22 = []
            col33 = []
            if urls.index(url) %10 == 0:
                print('Progress: ', str(round(urls.index(url)/len(urls) *
                                              100, 2)), '%')
            col11 = DB_operations.download_link_odd(url)
            col22 = [url] * len(col11)
            col33 = [dates[i]] * len(col11)
            col1.extend(col11)
            col2.extend(col22)
            col3.extend(col33)
            i += 1
        df = pd.DataFrame({'specific link':col1, 
                           'general link':col2,
                           'date':col3})            
        df.set_index('specific link', inplace = True, drop = True)            
        if in_up != 'initiate':
            df = DB_operations.check_for_duplicates(db, tbl, df)
        DB_operations.write_dataframe(db, tbl, df)
        print('Done.')
        
    def download_link_stat(self, link):
        '''
        Download the links for individual matches from the link for all 
        the matches for the statistics.
        Input:
            link: String, day URL
        Output:
            List with specific links
        '''
        page = requests.get(link)
        html_soup = soup(page.text,'html.parser')
        link_count = html_soup.find_all('div', class_='head2head')
        control = 0
        links = []
        for link in link_count:
            links.append(link_count[control].a.get('href'))
            control += 1
        return links
    
    def download_links_stats(db, tbl, in_up = 'update'):
        t0 = datetime.now()
        print('''Downloading links for the statistics.
              Start at time: ''', t0)
        urls = []
        col1 = []
        col2 = []
        col3 = []
        start_date = datetime.now().date() - timedelta(days=1)
        df = pd.DataFrame()
        if in_up == 'initiate':
            end_date = datetime(1990, 1, 1).date()
            number_days = (start_date - end_date).days
            dates = [start_date - timedelta(days = n) for n in 
                     range(number_days)]
            urls = ["http://www.tennisergebnisse.net/herren/" + 
                    str(date.year) + "-" + DB_operations.make_month(
                            date.month) + 
                    "-" + str(date.day) + "/" for date in dates
                    ]
        elif in_up == 'update':
            df = DB_operations.retrieve_data(db, 
                                        'links_odds', columns = [2])
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date', axis = 1)
            last_date = df.loc['Date'][-1:]
            number_days = (start_date - last_date).days - 1
            dates = [start_date - timedelta(days = n) for n in 
                     range(number_days)]
            urls = ["http://www.tennisergebnisse.net/herren/" + 
                    str(date.year) + "-" + DB_operations.make_month(
                            date.month) + 
                    "-" + str(date.day) + "/" for date in dates
                    ]          
        else:
            spec_date = datetime.strptime(in_up, '%d-%m-%Y').date()
            urls = ["http://www.tennisergebnisse.net/herren/" + 
                    str(spec_date.year) + "-" + DB_operations.make_month(
                            spec_date.month) + 
                    "-" + str(spec_date.day) + "/" 
                    ] 
            dates = [spec_date]
        i = 0
        for url in urls:
            col11 = []
            col22 = []
            col33 = []
            if urls.index(url) %10 == 0:
                print('Progress: ', str(round(urls.index(url)/len(urls) *
                                              100, 2)), '%')
            col11 = Tennis_Downloads.download_link_stat(url)
            col22 = [url] * len(col11)
            col33 = [dates[i]] * len(col11)
            col1.extend(col11)
            col2.extend(col22)
            col3.extend(col33)
            i += 1
        df = pd.DataFrame({'specific link':col1, 
                           'general link':col2,
                           'date':col3})            
        df.set_index('specific link', inplace = True, drop = True)            
        if in_up != 'initiate':
            df = DB_operations.check_for_duplicates(db, tbl, df)
        DB_operations.write_dataframe(db, tbl, df)
        print('Done.')




    def make_month(month):
        month = str(month)
        if len(month) == 1:
            month = '0' + month
        return month
    
    def download_statistic(link):
        '''
        Download the statistics for a given link.
        Input:
            link: string, link
        Output:
            df: Dataframe of statistics for link
        '''
        
    def download_statistics():
        '''
        Download the statistics for a list of links. Check if they are
        already in the DB.
        '''
        
    def download_odd(link):
        '''
        Download the odds for a given link.
        Input:
            link: string, link
        Output:
            df: Dataframe of odds for link
        '''
        
    def download_odds():
        '''
        Download the odds for a list of links. Check if they are
        already in the DB.
        '''
    
    

    def __del__(self):
        print('Bye bye.')


#db = 'C:\\Users\\alex1\\Documents\\Tennis\\New\\tennis.db'
#download_links_stats(db, 'links_statistics', 'initiate')
#download_links_odds(db, 'links_odds', 'initiate')



