from os import path, listdir
from shutil import move
from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import re
import json
from tqdm import tqdm

class FbrefParser:
    def __init__(self):
        self.DATA_PATH='data/fbref/'
        self.RAW_PATH='raw/fbref/'
        self.DONE_PATH='raw/done/fbref/'
        self.DAYS_RAW_PATH=self.RAW_PATH+'days/'
        self.MATCHES_RAW_PATH=self.RAW_PATH+'matches/'
        self.ROUNDS_RAW_PATH=self.RAW_PATH+'rounds/'
        self.DAYS_RAW_PATH_OUT=self.DONE_PATH+'days/'
        self.MATCHES_RAW_PATH_OUT=self.DONE_PATH+'matches/'
        self.ROUNDS_RAW_PATH_OUT=self.DONE_PATH+'rounds/'
        self.DATA=[]

    def _append_save(self,df, f, dup=['link']):
        if not path.exists(f):
            df.to_csv(f, index=False)
            return
        dfo=pd.read_csv(f, index_col=None)
        dfr=pd.concat([dfo,df], axis=0)
        dfr=dfr.drop_duplicates(subset=dup,keep='first')
        dfr.to_csv(f, index=False)

    def parse_days(self):
        pCountry=r'<span class=\'f-i [^>]+>([^<]+)</span>'
        pChamp=r'<a[^>]+>([^<]+)</a></h2>'
        pMatch=r'<a[^>]+>([^<]+)</a>\s*</td><td class="center " data-stat="score"\s*><a href="([^"]+)">(\d+)&ndash;(\d+)</a></td><td[^>]+>\s*<a[^>]+>([^<]+)</a>'
#                <a[^>]+>([^<]+)</a>\s*</td><td class="center " data-stat="score"\s*><a href="([^"]+)">(\d+)&ndash;(\d+)</a></td><td[^>]+>\s*<a[^>]+>([^<]+)</a>
        pSpan=r'<span[^>]+>[^<]*</span>'
        for file in tqdm(listdir(self.DAYS_RAW_PATH)):
            if len(file)>25 or file=='.empty':
                continue
            with open(self.DAYS_RAW_PATH+file, 'r', encoding='utf8') as f:
                html=f.read()
            chunks=html.split('<h2>')[1:]
            for ch in chunks:
                txt=ch.split('</tbody></table>')[0]
                m=re.search(pCountry, txt)
                country=m.groups()[0] if m else ''
                m=re.search(pChamp, txt)
                if m:
                    champ=m.groups()[0]
                    txt=re.sub(pSpan, '', txt)
                    txt=re.sub(pSpan, '', txt)
                    mm=re.findall(pMatch, txt)
                    if len(mm)>0:
                        for x in mm:
                            self.DATA.append({
                                'ds':file.replace('.htm',''),
                                'country':country,
                                'champ':champ,
                                't1':x[0],
                                't2':x[4],
                                'sc1':x[2],
                                'sc2':x[3],
                                'link':x[1]                          
                            })
            #print(file, len(html))
            move(self.DAYS_RAW_PATH+file,self.DAYS_RAW_PATH_OUT+file)
            #break
        
        df=pd.DataFrame(self.DATA)
        df['done']=0
        self._append_save(df, self.DATA_PATH+'matches.csv')

    def _getone(self,pattern, string):
        m=re.search(pattern, string)
        return '' if not m else m.groups()[0].replace('&nbsp;&nbsp;',' ').replace('&nbsp;',' ').strip()

    def _inbetween(self,txt,st,end):
        return txt.split(st)[1].split(end)[0]

    def _gk(self,matches, tid, mid):
        pid, pname, nationality,age,minutes,shots_on_target_against,goals_against_gk,saves= matches.groups()
        return {
            'mid':mid,
            'tid':tid, 
            'pid':pid,
            'pname':pname,
            'nationality':nationality,
            'age':age,
            'minutes':minutes,
            'shots_on_target_against':shots_on_target_against,
            'goals_against_gk':goals_against_gk,
            'saves':saves
        }

    def _pl(self,matches, tid, mid):
        players=[]
        for x in matches:
            players.append({
                'mid':mid, 
                'tid':tid, 
                'pid':x[0],
                'nameen':x[1],
                'name':x[2],
                'position':x[3],
                'nationality':x[4],
                'age':x[5],
                'goals':x[6],
                'assists':x[7],
                'pens_made':x[8],
                'pens_att':x[9],
                'shots_total':x[10],
                'shots_on_target':x[11],
                'cards_yellow':x[12],
                'cards_red':x[13],
                'fouls':x[14],
                'fouled':x[15],
                'offsides':x[16],
                'crosses':x[17],
                'tackles_won':x[18],
                'interceptions':x[19],
                'own_goals':x[20],
                'pens_won':x[21],
                'pens_conceded':x[22]
            })
        return players

    def parse_matches(self):
        pPlayer='<a href="/en/players/([^/]+)/([^"]+)">([^<]+)</a></th><td class="right[^"]*" data-stat="shirtnumber"\s*>[^<]+</td><td class="left poptip" data-stat="nationality"[^>]+><a[^>]+><span style="white-space: nowrap">([^<]+)</span></a></td><td class="center " data-stat="position"\s*>([^<]+)</td><td class="center[^"]+" data-stat="age"\s*>([^<]+)</td><td class="right[^"]*" data-stat="minutes"\s*>([^<]+)</td><td class="right[^"]*" data-stat="goals"\s*>([^<]+)</td><td class="right[^"]*" data-stat="assists"\s*>([^<]+)</td><td class="right[^"]*" data-stat="pens_made"\s*>([^<]*)</td><td class="right[^"]*" data-stat="pens_att"\s*>([^<]*)</td><td class="right[^"]*" data-stat="shots_total"\s*>([^<]*)</td><td class="right[^"]*" data-stat="shots_on_target"\s*>([^<]*)</td><td class="right[^"]*" data-stat="cards_yellow"\s*>([^<]*)</td><td class="right[^"]*" data-stat="cards_red"\s*>([^<]*)</td><td class="right[^"]*" data-stat="fouls"\s*>([^<]*)</td><td class="right[^"]*" data-stat="fouled"\s*>([^<]*)</td><td class="right[^"]*" data-stat="offsides"\s*>([^<]*)</td><td class="right[^"]*" data-stat="crosses"\s*>([^<]*)</td><td class="right[^"]*" data-stat="tackles_won"\s*>([^<]*)</td><td class="right[^"]*" data-stat="interceptions"\s*>([^<]*)</td><td class="right[^"]*" data-stat="own_goals"\s*>([^<]*)</td><td class="right[^"]*" data-stat="pens_won"\s*>([^<]*)</td><td class="right[^"]*" data-stat="pens_conceded"\s*>([^<]*)</td></tr>'
        pKeeper='<a href="/en/players/([^/]+)/">([^<]+)</a></th><td class="left poptip" data-stat="nationality"[^>]+><a[^>]+><span style="white-space: nowrap">([^<]+)</span></a></td><td class="center " data-stat="age"\s*>([^<]+)</td><td class="right " data-stat="minutes"[^>]+>(\d*)</td><td class="right[^"]*" data-stat="shots_on_target_against"\s*>(\d*)</td><td class="right[^"]*" data-stat="goals_against_gk"\s*>(\d*)</td><td class="right[^"]*" data-stat="saves"\s*>(\d*)</td>'
        pSpan=r'<span class[^>]+>[^<]*</span>'
        matches=[]
        players=[]
        goalkeepers=[]
        

        for file in tqdm(listdir(self.MATCHES_RAW_PATH)):
            if file=='.empty':
                continue
            match={}
            match['mid']=file.replace('.htm','')
            with open(self.MATCHES_RAW_PATH+file, 'r', encoding='utf8') as f:
                html=f.read()
            html=html.replace('&nbsp;&nbsp;&nbsp;','')
            html=self._inbetween(html,'<div id="content" role="main" class="box">','<h4>About FBref.com</h4>')
            scoreboards=self._inbetween(html,'<div class="scorebox">','<div class="scorebox_meta">')
            parts=scoreboards.split('<div itemscope="" itemprop="performer"')[1:]
            match['manager1']=self._getone(r'<strong>Manager</strong>: ([^<]+)</div>', parts[0])
            match['manager2']=self._getone(r'<strong>Manager</strong>: ([^<]+)</div>', parts[1])
            match['squad_link1']=self._getone(r'<a href="([^"]+)" itemprop="name">', parts[0])
            match['tid1']=match['squad_link1'].split('/')[3]
            match['squad_link2']=self._getone(r'<a href="([^"]+)" itemprop="name">', parts[1])
            match['tid2']=match['squad_link2'].split('/')[3]
            match['team1']=self._getone(r' itemprop="name">([^<]+)</a>', parts[0])
            match['team2']=self._getone(r' itemprop="name">([^<]+)</a>', parts[1])

            match['sc1']=self._getone(r'<div class="score">([^<]+)</div>', parts[0])
            match['sc2']=self._getone(r'<div class="score">([^<]+)</div>', parts[1])

            match['form1']=self._getone(r'</div><div>([^<]+)</div>', parts[0])
            match['form2']=self._getone(r'</div><div>([^<]+)</div>', parts[1])

            scorebox=self._inbetween(html,'<div class="scorebox_meta">','<div class="event" id="a">')
            match['ds_venue']=self._getone(r'data-venue-epoch="(\d+)">', scorebox)
            match['country_id']=self._getone(r'<a href="/en/comps/(\d+)/\d+/[^"]+">', scorebox)
            match['competition']=self._getone(r'<a href="/en/comps/\d+/\d+/([^"]+)">', scorebox)
            match['attendance']=self._getone(r'<small>Attendance</small></strong>: <small>(\d+)</small>', scorebox)
            match['venue']=self._getone(r'<small>Venue</small></strong>: <small>([^<]+)', scorebox)
            match['referee']=self._getone(r'<span style="display:inline-block">([^\()]+) \(Referee\)</span>', scorebox)
            match['ar1']=self._getone(r'<span style="display:inline-block">([^\()]+) \(AR1\)</span>', scorebox)
            match['ar2']=self._getone(r'<span style="display:inline-block">([^\()]+) \(AR2\)</span>', scorebox)
            match['ar3']=self._getone(r'<span style="display:inline-block">([^\()]+) \(4th\)</span>', scorebox)

            matches.append(match)

            # html=re.sub(pSpan, '', html)
            # # Team 1
            # tstats=self._inbetween(html,'all_stats_'+match['tid1'],'all_stats_'+match['tid2'])
            
            # keeper_str='<div id="all_keeper_stats'
            # if keeper_str in tstats:
            #     tstats,keeper=tstats.split(keeper_str)
            #     m=re.search(pKeeper, keeper)
            #     if m:
            #         goalkeepers.append(self._gk(m,match['tid1'],match['mid']))

            # mm=re.findall(pPlayer, tstats)
            # if len(mm)>0:
            #     players+=self._pl(mm,match['tid1'],match['mid'])
            
            # # Team 2
            # tstats=self._inbetween(html,'all_stats_'+match['tid2'],'</tbody></table>')
            # if keeper_str in tstats:
            #     tstats,keeper=tstats.split(keeper_str)
            #     m=re.search(pKeeper, keeper)
            #     if m:
            #         goalkeepers.append(self._gk(m,match['tid2'],match['mid']))

            # mm=re.findall(pPlayer, tstats)
            # if len(mm)>0:
            #     players+=self._pl(mm,match['tid2'],match['mid'])
            
            #print(file, len(html))
            move(self.MATCHES_RAW_PATH+file,self.MATCHES_RAW_PATH_OUT+file)
            #break
        df_matches=pd.DataFrame(matches)
        self._append_save(df_matches, self.DATA_PATH+'matches_full.csv', dup=['mid','tid1','tid2'])
        # df_players=pd.DataFrame(players)
        # self._append_save(df_players, self.DATA_PATH+'players.csv', dup=['mid','tid','pid'])
        # df_goalkeepers=pd.DataFrame(goalkeepers)
        # self._append_save(df_goalkeepers, self.DATA_PATH+'goalkeepers.csv', dup=['mid','tid','pid'])
    
    
