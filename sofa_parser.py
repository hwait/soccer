from os import path, listdir
from shutil import move
from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import json
from tqdm import tqdm

class SofaScoreParser:
    def __init__(self):
        self.DATA_PATH='data/sofa/'
        self.RAW_PATH='raw/sofa/'
        self.DONE_PATH='raw/done/sofa/'
        self.DAYS_RAW_PATH=self.RAW_PATH+'days/'
        self.MATCHES_RAW_PATH=self.RAW_PATH+'matches/'
        self.DAYS_RAW_PATH_OUT=self.DONE_PATH+'days/'
        self.MATCHES_RAW_PATH_OUT=self.DONE_PATH+'matches/'

    def _parse_votes(self,data, mid):
        row=data['vote']
        row['mid']=mid
        return row

    def _parse_graph(self, data, mid):
        rows=[{'mid':mid,'minute':x['minute'],'value':x['value'] } for x in data['graphPoints']]
        return rows

    def _parse_incidents(self, data, mid):
        rows=[]
        for x in data[::-1]:
            row={'mid':mid}
            if 'player' in x.keys():
                row['player1']=x['player']['name']
                if 'position' in x['player'].keys():
                    row['position1']=x['player']['position']
            if 'assist1' in x.keys():
                row['player2']=x['assist1']['name']
                if 'position' in x['assist1'].keys():
                    row['position2']=x['assist1']['position']
            if 'playerIn' in x.keys():
                row['player1']=x['playerIn']['name']
                if 'position' in x['playerIn'].keys():
                    row['position1']=x['playerIn']['position']
            if 'playerOut' in x.keys():
                row['player2']=x['playerOut']['name']
                if 'position' in x['playerOut'].keys():
                    row['position2']=x['playerOut']['position']
            if  'incidentClass' in x.keys() and x['incidentType']=='card':
                row['incidentType']=x['incidentClass']
            else:
                row['incidentType']=x['incidentType']
            if 'addedTime' in x.keys():
                row['addedTime']=x['length'] if x['incidentType']=='injuryTime' else x['addedTime']
            if 'isHome' in x.keys():
                row['isHome']=x['isHome'] 
            if 'time' in x.keys():
                row['time']=x['time']
            rows.append(row)
        return rows

    def _parse_team(self, data, home, mid):
        rows= [  {
            'mid':mid,
            'home':home,
            'name' : x['player']['name'],
            'slug' : x['player']['slug'],
            'position' : x['position'] if 'position' in x.keys() else '',
            'substitute' : x['substitute'] if 'substitute' in x.keys() else False,
            'totalPass' : x['statistics']['totalPass'] if 'statistics' in x.keys() and 'totalPass' in x['statistics'].keys() else np.NaN,
            'accuratePass' : x['statistics']['accuratePass'] if 'statistics' in x.keys() and 'accuratePass' in x['statistics'].keys() else np.NaN,
            'totalLongBalls' : x['statistics']['totalLongBalls'] if 'statistics' in x.keys() and 'totalLongBalls' in x['statistics'].keys() else np.NaN,
            'accurateLongBalls' : x['statistics']['accurateLongBalls'] if 'statistics' in x.keys() and 'accurateLongBalls' in x['statistics'].keys() else np.NaN,
            'aerialLost' : x['statistics']['aerialLost'] if 'statistics' in x.keys() and 'aerialLost' in x['statistics'].keys() else np.NaN,
            'duelLost' : x['statistics']['duelLost'] if 'statistics' in x.keys() and 'duelLost' in x['statistics'].keys() else np.NaN,
            'totalClearance' : x['statistics']['totalClearance'] if 'statistics' in x.keys() and 'totalClearance' in x['statistics'].keys() else np.NaN,
            'goodHighClaim' : x['statistics']['goodHighClaim'] if 'statistics' in x.keys() and 'goodHighClaim' in x['statistics'].keys() else np.NaN,
            'saves' : x['statistics']['saves'] if 'statistics' in x.keys() and 'saves' in x['statistics'].keys() else np.NaN,
            'totalKeeperSweeper' : x['statistics']['totalKeeperSweeper'] if 'statistics' in x.keys() and 'totalKeeperSweeper' in x['statistics'].keys() else np.NaN,
            'accurateKeeperSweeper' : x['statistics']['accurateKeeperSweeper'] if 'statistics' in x.keys() and 'accurateKeeperSweeper' in x['statistics'].keys() else np.NaN,
            'minutesPlayed' : x['statistics']['minutesPlayed'] if 'statistics' in x.keys() and 'minutesPlayed' in x['statistics'].keys() else np.NaN,
            'touches' : x['statistics']['touches'] if 'statistics' in x.keys() and 'touches' in x['statistics'].keys() else np.NaN,
            'rating' : x['statistics']['rating'] if 'statistics' in x.keys() and 'rating' in x['statistics'].keys() else np.NaN,
            'totalCross' : x['statistics']['totalCross'] if 'statistics' in x.keys() and 'totalCross' in x['statistics'].keys() else np.NaN,
            'aerialWon' : x['statistics']['aerialWon'] if 'statistics' in x.keys() and 'aerialWon' in x['statistics'].keys() else np.NaN,
            'duelWon' : x['statistics']['duelWon'] if 'statistics' in x.keys() and 'duelWon' in x['statistics'].keys() else np.NaN,
            'interceptionWon' : x['statistics']['interceptionWon'] if 'statistics' in x.keys() and 'interceptionWon' in x['statistics'].keys() else np.NaN,
            'totalTackle' : x['statistics']['totalTackle'] if 'statistics' in x.keys() and 'totalTackle' in x['statistics'].keys() else np.NaN,
            'fouls' : x['statistics']['fouls'] if 'statistics' in x.keys() and 'fouls' in x['statistics'].keys() else np.NaN,
            'shotOffTarget' : x['statistics']['shotOffTarget'] if 'statistics' in x.keys() and 'shotOffTarget' in x['statistics'].keys() else np.NaN,
            'wasFouled' : x['statistics']['wasFouled'] if 'statistics' in x.keys() and 'wasFouled' in x['statistics'].keys() else np.NaN,
            'onTargetScoringAttempt' : x['statistics']['onTargetScoringAttempt'] if 'statistics' in x.keys() and 'onTargetScoringAttempt' in x['statistics'].keys() else np.NaN,
            'blockedScoringAttempt' : x['statistics']['blockedScoringAttempt'] if 'statistics' in x.keys() and 'blockedScoringAttempt' in x['statistics'].keys() else np.NaN,
            'wonContest' : x['statistics']['wonContest'] if 'statistics' in x.keys() and 'wonContest' in x['statistics'].keys() else np.NaN,
            'totalContest' : x['statistics']['totalContest'] if 'statistics' in x.keys() and 'totalContest' in x['statistics'].keys() else np.NaN,
            'challengeLost' : x['statistics']['challengeLost'] if 'statistics' in x.keys() and 'challengeLost' in x['statistics'].keys() else np.NaN,
            'dispossessed' : x['statistics']['dispossessed'] if 'statistics' in x.keys() and 'dispossessed' in x['statistics'].keys() else np.NaN,
            'savedShotsFromInsideTheBox' : x['statistics']['savedShotsFromInsideTheBox'] if 'statistics' in x.keys() and 'savedShotsFromInsideTheBox' in x['statistics'].keys() else np.NaN,
            'totalOffside' : x['statistics']['totalOffside'] if 'statistics' in x.keys() and 'totalOffside' in x['statistics'].keys() else np.NaN,
            'bigChanceCreated' : x['statistics']['bigChanceCreated'] if 'statistics' in x.keys() and 'bigChanceCreated' in x['statistics'].keys() else np.NaN,
            'goals' : x['statistics']['goals'] if 'statistics' in x.keys() and 'goals' in x['statistics'].keys() else np.NaN }  
        for x in data['players'] ]
        return rows

    def _parse_lineups(self, data, mid):
        home=data['home']
        away=data['away']
        formations= { 
            'mid':mid, 
            'formation_h': home['formation'] if 'formation' in home.keys() else '', 
            'formation_a': away['formation'] if 'formation' in away.keys() else '' }
        rows=self._parse_team(home, 1, mid)
        rows+=self._parse_team(away, 0, mid)
        return rows, formations

    def _parse_htft(self, x, mid):
        home_row= { 'mid':mid, 'period':x['period'], 'ishome':1 }
        for i in x['groups']:
            for j in i['statisticsItems']:
                col=j['name']
                home_row[col]= j['home']
        away_row= { 'mid':mid, 'period':x['period'], 'ishome':0 }
        for i in x['groups']:
            for j in i['statisticsItems']:
                col=j['name']
                away_row[col]= j['away']
        return home_row,away_row

    def parse_matches(self):
        arr = {'votes':[],
            'graph':[],
            'incidents':[],
            'lineups':[],
            'statistics':[],
            'formations':[]  }
        row={}
        for file in tqdm(listdir(self.MATCHES_RAW_PATH)):
            if len(file)>25 or file=='.empty':
                continue
            mid=int(file.split('_')[1][:-5])
            with open(self.MATCHES_RAW_PATH+file, 'r', encoding='utf8') as f:
                data=json.load(f)
            isFound=True
            if file.startswith('votes'):
                row=self._parse_votes(data,mid)
                arr['votes'].append(row)
            elif file.startswith('graph'):
                rows=self._parse_graph(data,mid)
                arr['graph']+=rows
            elif file.startswith('incidents'):
                rows=self._parse_incidents(data['incidents'],mid)
                arr['incidents']+=rows
            elif file.startswith('lineups'):
                rows, formations=self._parse_lineups(data,mid)
                if formations['formation_h']!='' and formations['formation_a']!='':
                    arr['formations'].append(formations) 
                arr['lineups']+=rows
            elif file.startswith('statistics'):
                for x in data['statistics']:
                    home_row,away_row=self._parse_htft(x,mid)
                    arr['statistics'].append(home_row)
                    arr['statistics'].append(away_row)
            else:
                isFound=False
            if isFound:
                move( self.MATCHES_RAW_PATH+file, self.MATCHES_RAW_PATH_OUT+file)
        
        name='votes'
        file_name=self.DATA_PATH+name+'.csv'
        if path.exists(file_name):
            pd.concat([pd.read_csv(file_name, index_col=None), pd.DataFrame(data=arr[name])]).to_csv(file_name, index=False)
        else:
            pd.DataFrame(data=arr[name]).to_csv(file_name, index=False)
        
        name='graph'
        file_name=self.DATA_PATH+name+'.csv'
        if path.exists(file_name):
            pd.concat([pd.read_csv(file_name, index_col=None), pd.DataFrame(data=arr[name])]).to_csv(file_name, index=False)
        else:
            pd.DataFrame(data=arr[name]).to_csv(file_name, index=False)
        
        name='incidents'
        file_name=self.DATA_PATH+name+'.csv'
        if path.exists(file_name):
            pd.concat([pd.read_csv(file_name, index_col=None), pd.DataFrame(data=arr[name])]).to_csv(file_name, index=False)
        else:
            pd.DataFrame(data=arr[name]).to_csv(file_name, index=False)
        
        name='lineups'
        file_name=self.DATA_PATH+name+'.csv'
        if path.exists(file_name):
            pd.concat([pd.read_csv(file_name, index_col=None), pd.DataFrame(data=arr[name])]).to_csv(file_name, index=False)
        else:
            pd.DataFrame(data=arr[name]).to_csv(file_name, index=False)
        
        name='formations'
        file_name=f'data/{name}.csv'
        if path.exists(file_name):
            pd.concat([pd.read_csv(file_name, index_col=None), pd.DataFrame(data=arr[name])]).to_csv(file_name, index=False)
        else:
            pd.DataFrame(data=arr[name]).to_csv(file_name, index=False)
        
        name='statistics'
        file_name=f'data/{name}.csv'
        if path.exists(file_name):
            pd.concat([pd.read_csv(file_name, index_col=None), pd.DataFrame(data=arr[name])]).to_csv(file_name, index=False)
        else:
            pd.DataFrame(data=arr[name]).to_csv(file_name, index=False)
    
