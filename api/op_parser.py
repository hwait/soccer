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
        self.TODAY=False
        self.DATA_PATH='data/op/'
        self.RAW_PATH='raw/op/'
        self.DONE_PATH='raw/done/op/'
        self.DAYS_RAW_PATH=self.RAW_PATH+'days/'
        self.MATCHES_RAW_PATH=self.RAW_PATH+'matches/'
        self.DAYS_RAW_PATH_OUT=self.DONE_PATH+'days/'
        self.MATCHES_RAW_PATH_OUT=self.DONE_PATH+'matches/'
        self.DATALIST=[]
        self.EXCLUDE_COUNTRIES=['africa','nicaragua','republic-of-the-congo','costa-rica','ghana','bahrain','oman','morocco','northern-ireland','mauritania','malta','cyprus','gambia','iceland','el-salvador','iraq','saudi-arabia','tunisia','ethiopia','guatemala','kuwait','bangladesh','zambia','andorra','albania','kenya','nigeria']
        self.BIDS=[  1,   2,   3,   5,   9,  14,  15,  16,  18,  21,  24,  26,  27,
             30,  32,  33,  43,  44,  46,  49,  56,  57,  73,  75,  76, 128,
            147, 149, 157, 164, 381, 383]

    def _append_save_matches(self,df, f):
        if not path.exists(f):
            df.to_csv(f, index=False)
            return
        dfo=pd.read_csv(f, index_col=None)
        dfr=pd.concat([dfo,df], axis=0)
        dfr=dfr.drop_duplicates(keep='last')
        dfr.to_csv(f, index=False)

    def _append_save_odds(self,df, f):
        df['bid']=df['bid'].astype(int)
        df=df.loc[df['bid'].isin(self.BIDS)]
        df['time_open']=df['time_open'].astype(float)
        if not path.exists(f):
            df.to_csv(f, index=False)
            return
        dfo=pd.read_csv(f, index_col=None)
        dfr=pd.concat([dfo,df], axis=0)
        dfr=dfr.drop_duplicates(keep='last')
        dfr.to_csv(f, index=False)

    
    def parse_days(self):
        #pMatch=r'<tr[^>]+><td class="table-time datet t(\d+)-[^>]+>[^<]+</td><td class="name table-participant"><a href="([^"]+)">([^<]+)</a></td><td class="center bold table-odds table-score">([^<]+)</td><td[^>]+><a[^>]+>([^<]+)</a></td><td[^>]+><a[^>]+>([^<]+)</a></td><td[^>]+><a[^>]+>([^<]+)</a></td><td class="center info-value">([^<]+)</td></tr>'
        pMatch=r'<tr[^>]+><td class="table-time datet t(\d+)-[^>]+>[^<]+</td><td class="name table-participant"><a href="([^"]+)">([^<]+)</a></td><td class="center bold table-odds table-score">([^<]+)</td><td[^>]+><a[^>]+>([^<]+)</a></td><td[^>]+><a[^>]+>([^<]+)</a></td><td[^>]+><a[^>]+>([^<]+)</a></td><td class="center info-value">([^<]+)</td></tr>'
        files=listdir(self.DAYS_RAW_PATH)
        print(len(files))
        for file in files:
            if file=='.empty':
                continue
            with open(self.DAYS_RAW_PATH+file, 'r', encoding='utf8') as f:
                html=f.read()
            html=html.replace('/results/">RESULTS','')
            #m=re.search(pCaption, html)
            #caption='' if not m else m.groups()[0]
            #_,_,country,liga,_=caption.split('/')
            #pSeason=r'<a href="'+caption+'results/">([^<]+)</a>'
            #m=re.search(pSeason, html)
            #season='' if not m else m.groups()[0]
            html=html.replace('<span class="bold">','').replace('</span>','')
            #print(file,country,liga,season)
            mm=re.findall(pMatch, html)
            if len(mm)>0:
                for x in mm:
                    t,link,name,result, odds1, oddsdraw, odds2, bn=x
                    _,_,country,liga,_,_=link.split('/')
                    if country in self.EXCLUDE_COUNTRIES:
                        continue
                    t1,t2=name.split(' - ')
                    scores=result.split(':')
                    if len(scores)<2:
                        continue
                    sc1,sc2=result.split(':')
                    self.DATALIST.append({
                        'ds':datetime.utcfromtimestamp(int(t)),
                        'country':country,
                        'liga':liga,
                        'season':'2020/2021',
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
            #move(self.DAYS_RAW_PATH+file,self.DAYS_RAW_PATH_OUT+file)
            #break
        
        df=pd.DataFrame(self.DATALIST)
        df['done']=0
        self._append_save_matches(df, self.DATA_PATH+'matches.csv')

    def parse_today(self):
        pMatch=r'<tr[^>]+><td class="table-time datet t(\d+)-[^>]+>[^<]+</td><td class="name table-participant" colspan="2"><a href="([^"]+)">([^<]+)</a></td><td[^>]+><a[^>]+>([^<]+)</a></td><td[^>]+><a[^>]+>([^<]+)</a></td><td[^>]+><a[^>]+>([^<]+)</a></td><td[^>]+>([^<]+)</td></tr>'
        files=listdir(self.DAYS_RAW_PATH)
        print(len(files))
        for file in files:
            if file=='.empty':
                continue
            with open(self.DAYS_RAW_PATH+file, 'r', encoding='utf8') as f:
                html=f.read()
            #html=html.replace('/results/">RESULTS','')
            #html=html.replace('<span class="bold">','').replace('</span>','')
            html=re.sub('<span[^>]+>[^<]+</span>','',html)
            html=re.sub('<a href="javascript[^>]+>&nbsp;</a>','',html)
            mm=re.findall(pMatch, html)
            print(len(mm))
            if len(mm)>0:
                for x in mm:
                    t,link,name,odds1, oddsdraw, odds2, bn=x
                    _,_,country,liga,_,_=link.split('/')
                    if country in self.EXCLUDE_COUNTRIES:
                        continue
                    t1,t2=name.split(' - ')
                    self.DATALIST.append({
                        'ds':datetime.utcfromtimestamp(int(t)),
                        'country':country,
                        'liga':liga,
                        'season':'2020/2021',
                        't1':t1,
                        't2':t2,
                        'odds1':odds1,
                        'oddsdraw':oddsdraw,
                        'odds2':odds2,
                        'bn':bn,
                        'link':link                          
                    })
            #print(file, len(html))
            move(self.DAYS_RAW_PATH+file,self.DAYS_RAW_PATH_OUT+file)
            #break
        
        df=pd.DataFrame(self.DATALIST)
        df['done']=0
        self._append_save_matches(df, self.DATA_PATH+'matches_today.csv')
        
    def parse_matches(self, today=False):
        # 0 - w1
        # 1 - x
        # 2 - w2
        files=listdir(self.MATCHES_RAW_PATH)
        for file in tqdm(files):
            if file=='.empty':
                continue
            with open(self.MATCHES_RAW_PATH+file, 'r', encoding='utf8') as f:
                html=f.read()
            print(file)
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
                if len(odds[x])==3:
                    bookies[x]={
                        'mid':mid,
                        'bid':x,
                        'w1':odds[x]['0'] if '0' in odds[x] else odds[x][0],
                        'wx':odds[x]['1'] if '1' in odds[x] else odds[x][1],
                        'w2':odds[x]['2'] if '2' in odds[x] else odds[x][2]
                    }

            for x in movement:
                if len(movement[x])==3:
                    bookies[x]['move_1']=str(movement[x]['0'])[0].upper() if '0' in movement[x] else 'N' if movement[x][0]==None else movement[x][0][0].upper()
                    bookies[x]['move_x']=str(movement[x]['1'])[0].upper() if '1' in movement[x] else 'N' if movement[x][1]==None else  movement[x][1][0].upper()
                    bookies[x]['move_2']=str(movement[x]['2'])[0].upper() if '2' in movement[x] else 'N' if movement[x][2]==None else  movement[x][2][0].upper()

            for x in opening_odds:
                if len(opening_odds[x])==3:
                    bookies[x]['open_1']=opening_odds[x]['0'] if '0' in opening_odds[x] else opening_odds[x][0]
                    bookies[x]['open_x']=opening_odds[x]['1'] if '1' in opening_odds[x] else opening_odds[x][1]
                    bookies[x]['open_2']=opening_odds[x]['2'] if '2' in opening_odds[x] else opening_odds[x][2]
            
            for x in opening_change_time:
                if len(opening_change_time[x])>0 and x in bookies:
                    bookies[x]['time_open']=opening_change_time[x]['0'] if '0' in opening_change_time[x] else opening_change_time[x][0]

            for x in change_time:
                if len(change_time[x])==3:
                    bookies[x]['time_close']=max([change_time[x]['0'],change_time[x]['1'],change_time[x]['2']]) if '0' in change_time[x] else max([change_time[x][0],change_time[x][1],change_time[x][2]])
            move(self.MATCHES_RAW_PATH+file,self.MATCHES_RAW_PATH_OUT+file)
            self.DATALIST.append(pd.DataFrame([bookies[x] for x in bookies]))
            #print(self.DATALIST)
            #break
        #print(self.DATALIST)
        df=pd.concat(self.DATALIST, axis=0)
        if today:
            self._append_save_odds(df, self.DATA_PATH+'odds_today.csv')
        else:
            self._append_save_odds(df, self.DATA_PATH+'odds.csv')
    
