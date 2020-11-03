from os import path, listdir
from shutil import move
from datetime import datetime,timedelta
import time
import requests
from stem import Signal
from stem.control import Controller
import re
import pandas as pd
import numpy as np
import random
import pytz
import json
import csv
import sys

class DataProvider:
    
    def __init__(self):
        self.COUNTER = 0
        self.LOCAL_TZ = 'Asia/Almaty'
        self.SERVER_TZ = 'UTC'
        self.DATA_PATH='data/'
        self.DAYS_RAW_PATH='raw/days/'
        self.MATCHES_RAW_PATH='raw/matches/'
        self.SERVER_ERROR=False
        self.DATA=[]
        self.PROXY = {'https': 'socks5://127.0.0.1:9051'}
        self.API_URL='https://api.sofascore.com/api/v1/'
        self.TYPE='days'
        self.AVOID_STATUS_CODES=[60,70]
        self.CONTROL_PORT=9052
        self.EXCLUDE_COUNTRIES=['india', 'peru', 'south-africa', 'germany-amateur', 'saudi-arabia', 'united-arab-emirates']
    
    def _generate_headers(self,referer):
        return {
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.5",
                "Connection": "keep-alive",
                "Host": "api.sofascore.com",
                "Origin": "https://www.sofascore.com",
                "Referer": referer,
                "TE": "Trailers",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0" }
    
    
    def _get_dates(self, ds,de):
        dates=[]
        d= ds
        while d<=de:
            dates.append(d)
            d+=timedelta(days=1)
        dates=np.array(dates) 
        np.random.shuffle(dates)
        return dates

    def _tor_new_identity(self):
        with Controller.from_port(port=self.CONTROL_PORT) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            controller.close()

    def _parse_day(self, data,source):
        matches=[x for x in data['events'] if 'coverage' in x.keys()]
        matches=[x for x in matches if x['coverage']>-1]
        matches=[x for x in matches if not x['status']['code'] in self.AVOID_STATUS_CODES]
        matches=[x for x in matches if not x['tournament']['category']['slug'] in self.EXCLUDE_COUNTRIES]
        matches=[{
            'tournament' : x['tournament']['slug'],
            'country' : x['tournament']['category']['slug'],
            'round' : x['roundInfo']['round'] if 'roundInfo' in x.keys() else np.NaN,
            'status' : x['status']['code'],
            'homeTeam' : x['homeTeam']['name'],
            'homeTeamShort' : x['homeTeam']['shortName'],
            'awayTeam' : x['awayTeam']['name'],
            'awayTeamShort' : x['awayTeam']['shortName'],
            'homeScoreFT' : x['homeScore']['normaltime'] if 'normaltime' in x['homeScore'].keys() else np.NaN,
            'homeScoreET' : x['homeScore']['current'] if 'current' in x['homeScore'].keys() else np.NaN,
            'homeScoreHT' : x['homeScore']['period1'] if 'period1' in x['homeScore'].keys() else np.NaN,
            'awayScoreFT' : x['awayScore']['normaltime'] if 'normaltime' in x['homeScore'].keys() else np.NaN,
            'awayScoreET' : x['awayScore']['current'] if 'current' in x['homeScore'].keys() else np.NaN,
            'awayScoreHT' : x['awayScore']['period1'] if 'period1' in x['homeScore'].keys() else np.NaN,
            'id' : x['id'],
            'startTimestamp' : x['startTimestamp'],
            'coverage' : x['coverage'],
            'winnerCode' : x['winnerCode'] if 'winnerCode' in x.keys() else np.NaN
            } for x in matches]

        if len(matches)>0:
            
            #keys = matches[0].keys()
            #with open(self.DATA_PATH+'matches.csv', 'a', newline='', encoding='utf8')  as f:
            #    dict_writer = csv.DictWriter(f, keys)
            #    dict_writer.writerows(matches)

            file_name=self.DATA_PATH+'matches.csv'
            df_matches_new=pd.DataFrame(data=matches)
            df_matches_new['ts']=pd.DatetimeIndex(pd.to_datetime(df_matches_new['startTimestamp'], unit='s')).tz_localize(self.SERVER_TZ)
            
            if path.exists(file_name):
                df_matches=pd.read_csv(file_name, index_col=None).set_index('id')
                df_matches_new=df_matches_new.set_index('id')
                df_matches.update(df_matches_new[['awayScoreHT','awayScoreET','homeScoreET','awayScoreFT','homeScoreFT','status','winnerCode','homeScoreHT']])
                df_matches['id'] = df_matches.index
                df_matches_new['id'] = df_matches_new.index
                df_matches_new=pd.concat([df_matches,df_matches_new]).drop_duplicates(subset='id', keep='first').reset_index(drop=True)
                df_matches_new['done'].fillna(0, inplace = True)
            else:
                df_matches_new['done']=0
            df_matches_new['done']=0
            
            self.df_matches=pd.concat([self.df_matches,df_matches_new]).drop_duplicates(subset='id', keep='first').reset_index(drop=True)
            df_matches_new.to_csv(file_name, index=False)
        move( source, source.replace('raw','raw/done'))

    def _load_match_info(self, data):
        mid,stage,d=data
        dstr=d.split(' ')[0]
        print(f'Loading {dstr} {mid}: votes...', end='')
        referer=f'https://www.sofascore.com/football/{dstr}'
        self.HEADERS=self._generate_headers(referer)
        is_loaded=self._load_json('votes',data)
        if stage>0 and is_loaded: # Match started
            print(', graph...', end='')
            self._load_json('graph',data)
            print(', statistics...', end='')
            self._load_json('statistics',data)
            print(', lineups...', end='')
            self._load_json('lineups',data)
            print(', incidents...', end='')
            self._load_json('incidents',data)
        if stage>89: # Match completed
            self.df_matches.loc[self.df_matches['id']==mid,'done']=1
        print(' done.')

    def _load_json(self,fn, data):
        mid, stage,_=data
        file_name='{}{}_{}_{:%Y-%m-%d-%H%M}.json'.format(self.MATCHES_RAW_PATH,fn, mid, datetime.now()) if fn=='votes' and stage==0 else f'{self.MATCHES_RAW_PATH}{fn}_{mid}.json'
        if path.exists(file_name):
            print('***', end='')
            if stage>89: # Match completed
                self.df_matches.loc[self.df_matches['id']==mid,'done']=1
            return True
        else:
            script='' if fn=='event' else '/provider/1/'+fn if fn=='winning-odds' else '/'+fn
            link=f'{self.API_URL}event/{mid}{script}'
            try:
                r = requests.get(link, headers=self.HEADERS, proxies=self.PROXY)
                if r.status_code==200:
                    with open(file_name, 'w+', encoding='utf8') as f:
                        f.write(r.text)
                    print('+++', end='')
                    return True
                elif r.status_code==403:
                    print(f'ERROR {r.status_code}!!!', end='')
                    self.SERVER_ERROR=True
                    self._load_data(data)
            except:
                e = sys.exc_info()[0]
                print(f'ERROR {e}!!!')
                self.df_matches.to_csv(self.DATA_PATH+'matches.csv', index=False)
                raise Exception('Stop execution.')
                self.SERVER_ERROR=True
                #self._load_data(self.SERVER_ERROR=True)
            
            


    def _load_day(self, d):
        dstr='{:%Y-%m-%d}'.format(d)
        file_name=f'{self.DAYS_RAW_PATH}{dstr}.json'
        self.PAUSE=True
        if path.exists(file_name):
            self.PAUSE=False
            with open(file_name, 'r', encoding='utf8') as f:
                print(f'Found {file_name} on the disk.')
                data=json.load(f)
            self._parse_day(data,file_name)
        else:
            print('not path.exists')
            link=f'{self.API_URL}sport/football/scheduled-events/{dstr}'
            referer=f'https://www.sofascore.com/football/{dstr}'
            print(f'Loading {dstr} from {len(self.DATA)}...', end='')
            self.HEADERS=self._generate_headers(referer)
            r = requests.get(link, headers=self.HEADERS, proxies=self.PROXY)
            if r.status_code==200:
                with open(file_name, 'w+', encoding='utf8') as f:
                    f.write(r.text)
                    data=json.loads(r.text)
                self._parse_day(data,file_name)
                print(f' done #{self.COUNTER}!')
            else:
                print(f'ERROR {r.status_code}!!!', end='')
                self.SERVER_ERROR=True
                self._load_data()
        

    def load_matches(self):
        self.COUNTER=0
        self.PAUSE=True
        file_name=self.DATA_PATH+'matches.csv'
        self.df_matches=pd.read_csv(file_name, index_col=None)
        self.df_matches = self.df_matches.sample(frac=1, axis=1).reset_index(drop=True)
        self.DATA=self.df_matches.loc[(self.df_matches['done']==0) & (self.df_matches['status']>89)][['id', 'status', 'ts']].values
        np.random.shuffle(self.DATA)
        self.TYPE='matches'
        for data in self.DATA:
            #print('LOOP:', data)
            self._load_data(data)
        self.df_matches.to_csv(file_name, index=False)

    def load_days(self, ds=None,de=None):
        self.df_matches=pd.read_csv(self.DATA_PATH+'matches.csv', index_col=None)
        if ds==None:
            dates = [datetime.strptime(f.replace('.json', ''), '%Y-%m-%d') for f in listdir('raw/')] 
        else:
            d = datetime.strptime(ds, '%Y-%m-%d')
            de = datetime.strptime(de, '%Y-%m-%d')
            dates=[]
            while d<=de:
                dates.append(d)
                d+=timedelta(days=1)

        dates=np.array(dates) 
        np.random.shuffle(dates)
        self.COUNTER=0
        self.TYPE='days'
        self.DATA = dates
        for data in self.DATA:
            #print('LOOP:'+data)
            self._load_data(data)


    def _load_data(self,data):
        if self.SERVER_ERROR: # There was a server error
            print('Obtaining new identity.')
            self.COUNTER=0
            self._tor_new_identity()
            time.sleep(random.uniform(5, 10))
            self.SERVER_ERROR=False

        if self.TYPE=='days':
            self._load_day(data)
        else:
            self._load_match_info(data)

        # We are here if everything was ok. If error load_data have be restarted with a new tor identity
        self.COUNTER+=1
        if self.COUNTER>random.randint(30, 50):
            print('Reached maximum loads on this proxy. Changing...', end='')
            self._tor_new_identity()
            time.sleep(random.uniform(5, 15))
            print('Saving df_matches...', end='')
            self.df_matches.to_csv(self.DATA_PATH+'matches.csv', index=False)
            print('done')
            self.COUNTER=0
        else:
            if self.PAUSE:
                time.sleep(random.uniform(0, 1))
