import os
from os import path
import glob
import pandas as pd
import numpy as np
import seaborn as sns
import pickle
import pytz
from datetime import timezone,datetime,timedelta
from sklearn.preprocessing import LabelEncoder,OneHotEncoder

import api.util
from api.op_dp import OpDataProvider
#from op_dp import OpDataProvider
from api.sofa_dp import SofaDataProvider
#from sofa_dp import SofaDataProvider

class DataCollector:
    def __init__(self, today=False):
        self.LOCAL_TZ = 'Asia/Almaty'
        self.SERVER_TZ = 'UTC'
        self.DATA_PATH='data/'
        self.ELO_DATA_PATH='data/elo/'
        self.PREREQUISITES_PATH='prerequisites/'
        self.COL_CAT=[]
        self.COL_NUM=[]
        self.COL_LBL=[]
        self.COL_INF=[]
        self.TODAY=today
    
    def _load_prerequisites(self,name):
        with open(os.path.join(self.PREREQUISITES_PATH, name),'rb') as f:
            encoder = pickle.load(f)
        return encoder
    
    def _save_prerequisite(self, name, data):
        os.makedirs(self.PREREQUISITES_PATH, mode=0o777, exist_ok=True)
        with open(os.path.join(self.PREREQUISITES_PATH, name), mode='wb') as f:
            pickle.dump(data, f) 

    def _ff(self, columns):
        if len(self.INCLUDE)>0:
            return [x for x in columns if x in self.INCLUDE]
        else:
            return [x for x in columns if x not in self.EXCLUDE]
    
    def _encode(self, enctype, features, outs, df):
        if (len(self.INCLUDE)>0 and outs[0] in self.INCLUDE) or outs[0] in self.EXCLUDE:
            return df
        name='_'.join(features)
        if self.LOAD:
            encoder=self._load_prerequisites(f'{enctype}_{features[0]}')
        else:
            if enctype=='sc':
                encoder = MinMaxScaler()
            elif enctype=='le':
                encoder = LabelEncoder()
            elif enctype=='ohe':
                encoder = OneHotEncoder()
            if len(features)==1:
                encoder.fit(df[features].values)
            else:
                encoder.fit(pd.concat([pd.DataFrame(df[features[0]].unique(), columns=[name]),pd.DataFrame(df[features[1]].unique(), columns=[name])])[name])
            self._save_prerequisite(f'{enctype}_{name}', encoder)
        if  enctype=='ohe':
            return encoder.transform(df[features].values).toarray()
        if len(features)==1:
            df[outs[0]] = encoder.transform(df[features].values)
        else:
            df[outs[0]] = encoder.transform(df[features[0]])
            df[outs[1]] = encoder.transform(df[features[1]])
        return df

    def _encode_teams(self, df):
        teams_name=self.ELO_DATA_PATH+'teams.csv'
        teams_saved=pd.read_csv(teams_name, index_col=None)
        teams=df[['team']].dropna().drop_duplicates()
        teams_new=teams[~teams.team.isin(teams_saved.team)]
        print(teams_new)
        if not teams_new.empty:
            print('New teams!')
            id=teams_saved.id.max()+1
            #id=0
            teams_list=[]
            for row in teams_new.itertuples():
                if len(row.team)>1:
                    teams_list.append({'team':row.team, 'id':id})
                    id+=1
                    #break
            teams_saved=pd.concat([teams_saved,pd.DataFrame(teams_list)])
            teams_saved.id=teams_saved.id.astype(int)
            teams_saved.to_csv(teams_name, index=False)
        df=df.merge(teams_saved, on='team', how='left')
        return df
    
    def _add_elo(self, df_src,df_elo):
        df_teams=pd.read_csv(self.DATA_PATH+'teams.csv', index_col=None)
        df_elo_merged=df_elo.merge(df_teams[['id','tid']], on='id', how='left').drop_duplicates()
        #df_elo_merged=df_elo_merged.dropna()
        df_src['de']=df_src.ds.apply(lambda x: x.strftime('%Y-%m-%d'))
        df_elo_merged=df_elo_merged.rename(columns={'tid':'tid1', 'elo':'elo1'})
        df_src=df_src.merge(df_elo_merged[['tid1','de','elo1']], on=['tid1','de'], how='left')
        df_elo_merged=df_elo_merged.rename(columns={'tid1':'tid2', 'elo1':'elo2'})
        df_src=df_src.merge(df_elo_merged[['tid2','de','elo2']], on=['tid2','de'], how='left')
        return df_src

    def _provide_elo(self):
        if self.TODAY:
            df = pd.read_csv(self.DATA_PATH+'elo/elo_{:%Y-%m-%d}.csv'.format(datetime.today()-timedelta(days=1)), index_col=None)
        else:
            df = pd.concat(map(pd.read_csv, glob.glob(os.path.join(self.DATA_PATH+'elo/', 'elo_*.csv'))))
        df=df[['Club', 'Country', 'Level', 'Elo', 'From', 'To']]
        df.columns=['team', 'country', 'level', 'elo', 'ds', 'de']
        df=self._encode_teams(df)
        return df

    def _provide_sofa(self):
        dp=SofaDataProvider(load=True, today=self.TODAY)
        df=dp._load_data()
        print(len(df))
        return df.drop_duplicates(subset='mid', keep='last')

    def _provide_op(self):
        dp=OpDataProvider(load=True, today=self.TODAY)
        df=dp._load_data()
        return df

    def _bind_today(self,df):
        df_teams=pd.read_csv('data/teams.csv', index_col=None)
        df_teams=df_teams[['tid','op_tid']].drop_duplicates()

        df=df.merge(df_teams, left_on='tid1', right_on='tid')
        df=df.rename(columns={'op_tid':'op_tid1'})
        df=df.drop(columns=['tid'])
        df=df.merge(df_teams, left_on='tid2', right_on='tid')
        df=df.rename(columns={'op_tid':'op_tid2'})
        df=df.drop(columns=['tid'])

        df_op=self._provide_op()
        df_op=df_op.rename(columns={'tid1':'op_tid1','tid2':'op_tid2'})
        df=df.merge(df_op[['op_tid1','op_tid2', 'odds_away','odds_draw','odds_home', 'oddsprob_home', 'oddsprob_draw', 'oddsprob_away', 'drift_home', 'drift_away', 'drift_draw']], on=['op_tid1','op_tid2'], how='left')
        return df

    def _bind_sofa_op(self,df):
        df_op=self._provide_op()
        df_binds=pd.read_csv('data/binds_ss_op.csv', index_col=None)
        df_op=df_op.merge(df_binds[['op_mid','mid']], left_on='mid', right_on='op_mid')
        return df.merge(df_op[['mid_y','odds_away','odds_draw','odds_home','oddsprob_home','oddsprob_draw','oddsprob_away','drift_home','drift_away','drift_draw']], left_on='mid', right_on='mid_y', how='left')

    def _load_data(self):
        df_sofa=self._provide_sofa()
        df_elo=self._provide_elo()
        df_sofa=self._add_elo(df_sofa,df_elo)
        if self.TODAY:
            df_sofa=self._bind_today(df_sofa)
        #df_sofa=self._bind_sofa_op(df_sofa)
        return df_sofa
    
    def provide_today(self, double=True):
        df=self._load_data()
        df['psft']=0
        df['psht']=0
        df['w1']=0
        df['wx']=0
        df['w2']=0
        df_home=df.copy()
        df_home=df_home.rename(columns={'homeScoreHT':'ht1','awayScoreHT':'ht2','sc1':'ft1','sc2':'ft2','vote_home':'vote1','vote_draw':'votex','vote_away':'vote2','home_formation':'form1','away_formation':'form2','oddsprob_home':'oddsprob1','oddsprob_draw':'oddsprobx','oddsprob_away':'oddsprob2','drift_home':'drift1','drift_draw':'driftx','drift_away':'drift2'})
        if double:
            df_home['side']=1
            df_away=df.copy()
            df_away['side']=0
            df_away=df_away.rename(columns={'vote_home':'vote2','vote_draw':'votex','vote_away':'vote1',
                                            'home_formation':'form2','away_formation':'form1','elo1':'elo2','elo2':'elo1','t1':'t2','t2':'t1',
                                            'tid1':'tid2','tid2':'tid1','odds_away':'odds_home','odds_home':'odds_away','oddsprob1':'oddsprob2',
                                            'oddsprob2':'oddsprob1','drift1':'drift2','drift2':'drift1'})
            df_away['psft']=df_away['psft']*-1
            df_away['psht']=df_away['psht']*-1

            df_home=pd.concat([df_home,df_away], axis=0)

        return df_home.reset_index(drop=True)

    def provide_data(self, double=True):
        df=self._load_data()
        df['psft']=df.sc1-df.sc2
        df['psht']=df.homeScoreHT-df.awayScoreHT
        df['w1']=np.where(df.winner=='home',1,0)
        df['wx']=np.where(df.winner=='draw',1,0)
        df['w2']=np.where(df.winner=='away',1,0)
        df_home=df.copy()
        df_home=df_home.rename(columns={'homeScoreHT':'ht1','awayScoreHT':'ht2','sc1':'ft1','sc2':'ft2','vote_home':'vote1','vote_draw':'votex','vote_away':'vote2','home_formation':'form1','away_formation':'form2','oddsprob_home':'oddsprob1','oddsprob_draw':'oddsprobx','oddsprob_away':'oddsprob2','drift_home':'drift1','drift_draw':'driftx','drift_away':'drift2'})
        if double:
            df_home['side']=1
            df_away=df.copy()
            df_away['side']=0
            df_away=df_away.rename(columns={'homeScoreHT':'ht2','awayScoreHT':'ht1','sc1':'ft2','sc2':'ft1','vote_home':'vote2','vote_draw':'votex','vote_away':'vote1',
                                            'home_formation':'form2','away_formation':'form1','w1':'w2','w2':'w1','elo1':'elo2','elo2':'elo1','t1':'t2','t2':'t1',
                                            'tid1':'tid2','tid2':'tid1','odds_away':'odds_home','odds_home':'odds_away','oddsprob1':'oddsprob2',
                                            'oddsprob2':'oddsprob1','drift1':'drift2','drift2':'drift1',
                                            'possession1':'possession2', 'shont1':'shont2', 'shofft1':'shofft2', 'corners1':'corners2', 
                                            'offsides1':'offsides2', 'fouls1':'fouls2', 'cards1':'cards2', 'gksaves1':'gksaves2',
                                            'possession2':'possession1', 'shont2':'shont1', 'shofft2':'shofft1', 'corners2':'corners1', 
                                            'offsides2':'offsides1', 'fouls2':'fouls1', 'cards2':'cards1', 'gksaves2':'gksaves1'})
            df_away['psft']=df_away['psft']*-1
            df_away['psht']=df_away['psht']*-1

            df_home=pd.concat([df_home,df_away], axis=0)

        return df_home.reset_index(drop=True)