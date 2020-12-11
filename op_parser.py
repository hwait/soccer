from os import path, listdir
from shutil import move
from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import re
import json
from tqdm import tqdm

class OpParser:
    def __init__(self):
        self.DATA_PATH='data/op/'
        self.RAW_PATH='raw/op/'
        self.DONE_PATH='raw/done/op/'
        self.DAYS_RAW_PATH=self.RAW_PATH+'days/'
        self.MATCHES_RAW_PATH=self.RAW_PATH+'matches/'
        self.DAYS_RAW_PATH_OUT=self.DONE_PATH+'days/'
        self.MATCHES_RAW_PATH_OUT=self.DONE_PATH+'matches/'
        self.DATA=[]

    def _append_save(self,df, f):
        if not path.exists(f):
            df.to_csv(f, index=False)
            return
        dfo=pd.read_csv(f)
        dfr=pd.concat([dfo,df], axis=0)
        dfr=dfr.drop_duplicates(subset=['link'],keep='first')
        dfr.to_csv(f, index=False)

    def parse_days(self):
        pCaption=r'<a href="([^"]+)">NEXT MATCHES'
        pMatch=r'<tr[^>]+><td class="table-time datet t(\d+)-[^>]+>[^<]+</td><td class="name table-participant"><a href="([^"]+)">([^<]+)</a></td><td class="center bold table-odds table-score">([^<]+)</td><td[^>]+><a[^>]+>([^<]+)</a></td><td[^>]+><a[^>]+>([^<]+)</a></td><td[^>]+><a[^>]+>([^<]+)</a></td><td class="center info-value">([^<]+)</td></tr>'
        files=listdir(self.DAYS_RAW_PATH)
        print(len(files))
        for file in files:
            if file=='.empty':
                continue
            with open(self.DAYS_RAW_PATH+file, 'r', encoding='utf8') as f:
                html=f.read()
            html=html.replace('/results/">RESULTS','')
            m=re.search(pCaption, html)
            caption='' if not m else m.groups()[0]
            _,_,country,liga,_=caption.split('/')
            pSeason=r'<a href="'+caption+'results/">([^<]+)</a>'
            m=re.search(pSeason, html)
            season='' if not m else m.groups()[0]
            html=html.replace('<span class="bold">','').replace('</span>','')
            print(file,country,liga,season)
            mm=re.findall(pMatch, html)
            if len(mm)>0:
                for x in mm:
                    t,link,name,result, odds1, oddsdraw, odds2, bn=x
                    t1,t2=name.split(' - ')
                    scores=result.split(':')
                    if len(scores)<2:
                        continue
                    sc1,sc2=result.split(':')
                    self.DATA.append({
                        'ds':datetime.utcfromtimestamp(int(t)),
                        'country':country,
                        'liga':liga,
                        'season':season,
                        't1':t1,
                        't2':t2,
                        'sc1':sc1,
                        'sc2':sc2,
                        'odds1':odds1,
                        'oddsdraw':oddsdraw,
                        'odds2':odds2,
                        'bn':bn,
                        'link':link                          
                    })
            #print(file, len(html))
            move(self.DAYS_RAW_PATH+file,self.DAYS_RAW_PATH_OUT+file)
            #break
        
        df=pd.DataFrame(self.DATA)
        df['done']=0
        self._append_save(df, self.DATA_PATH+'matches.csv')
        
    def parse_matches(self):
        # 0 - w1
        # 1 - x
        # 2 - w2
        files=listdir(self.MATCHES_RAW_PATH)
        for file in files:
            if file=='.empty':
                continue
            with open(self.MATCHES_RAW_PATH+file, 'r', encoding='utf8') as f:
                html=f.read()
            js=json.loads(html)
            mid=file.replace('.json','')
            odds=js['d']['oddsdata']['back']['E-1-2-0-0-0']['odds']
            movement=js['d']['oddsdata']['back']['E-1-2-0-0-0']['movement']
            opening_odds=js['d']['oddsdata']['back']['E-1-2-0-0-0']['opening_odds']
            opening_change_time=js['d']['oddsdata']['back']['E-1-2-0-0-0']['opening_change_time']
            #opening_volume=js['d']['oddsdata']['back']['E-1-2-0-0-0']['opening_volume']
            #volume=js['d']['oddsdata']['back']['E-1-2-0-0-0']['volume']
            change_time=js['d']['oddsdata']['back']['E-1-2-0-0-0']['change_time']
            bookies={}
            
            for x in odds:
                bookies[x]={
                    'mid':mid,
                    'bid':x,
                    'w1':odds[x]['0'],
                    'wx':odds[x]['1'],
                    'w2':odds[x]['2']
                }

            for x in movement:
                bookies[x]['move_1']=str(movement[x]['0'])[0].upper()
                bookies[x]['move_x']=str(movement[x]['1'])[0].upper()
                bookies[x]['move_2']=str(movement[x]['2'])[0].upper()

            for x in opening_odds:
                bookies[x]['open_1']=opening_odds[x]['0']
                bookies[x]['open_x']=opening_odds[x]['1']
                bookies[x]['open_2']=opening_odds[x]['2']
            
            for x in opening_change_time:
                if opening_change_time[x]['0'] and opening_change_time[x]['1'] and opening_change_time[x]['2']:
                    bookies[x]['time_open']=max([opening_change_time[x]['0'],opening_change_time[x]['1'],opening_change_time[x]['2']])

            for x in change_time:
                bookies[x]['time_close']=max([change_time[x]['0'],change_time[x]['1'],change_time[x]['2']])
            #move(self.MATCHES_RAW_PATH+file,self.MATCHES_RAW_PATH_OUT+file)
            self.DATA.append(pd.DataFrame([bookies[x] for x in bookies]))
            break
        
        df=pd.concat(self.DATA, axis=0)
        self._append_save(df, self.DATA_PATH+'odds.csv')  
    
