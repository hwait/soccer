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
#from api.op_dp import OpDataProvider
from op_dp import OpDataProvider
#from api.sofa_dp import SofaDataProvider
from sofa_dp import SofaDataProvider

class DataCollector:
    def __init__(self):
        self.LOCAL_TZ = 'Asia/Almaty'
        self.SERVER_TZ = 'UTC'
        self.DATA_PATH='data/'
        self.ELO_DATA_PATH='data/elo/'
        self.PREREQUISITES_PATH='prerequisites/'
        self.COL_CAT=[]
        self.COL_NUM=[]
        self.COL_LBL=[]
        self.COL_INF=[]
    
    def _load_prerequisites(self,name):
        with open(os.path.join(self.PREREQUISITES_PATH, name),'rb') as f:
            encoder = pickle.load(f)
        return encoder
    
    def _save_prerequisite(self, name, data):
        folder='prerequisites/'
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
        df_elo_merged=df_elo_merged.dropna()
        df_src['de']=df_src.ds.apply(lambda x: x.strftime('%Y-%m-%d'))
        df_elo_merged=df_elo_merged.rename(columns={'tid':'tid1', 'elo':'elo1'})
        df_src=df_src.merge(df_elo_merged[['tid1','de','elo1']], on=['tid1','de'], how='left')
        df_elo_merged=df_elo_merged.rename(columns={'tid1':'tid2', 'elo1':'elo2'})
        df_src=df_src.merge(df_elo_merged[['tid2','de','elo2']], on=['tid2','de'], how='left')
        return df_src

    def _provide_elo(self):
        df = pd.concat(map(pd.read_csv, glob.glob(os.path.join(self.DATA_PATH+'elo/', 'elo_*.csv'))))
        df=df[['Club', 'Country', 'Level', 'Elo', 'From', 'To']]
        df.columns=['team', 'country', 'level', 'elo', 'ds', 'de']
        df=self._encode_teams(df)
        return df

    def _provide_sofa(self):
        dp=SofaDataProvider(load=True)
        df=dp._load_data()
        return df.drop_duplicates(subset='mid', keep='last')

    def _provide_op(self):
        dp=OpDataProvider(load=True)
        df=dp._load_data()
        return df

    def _bind_sofa_op(self):
        df_sofa=self._provide_sofa()
        df_op=self._provide_op()
        return None

    def _load_data(self):
        df_sofa=self._provide_sofa()
        df_elo=self._provide_elo()
        df_sofa=self._add_elo(df_sofa,df_elo)
        return df_sofa
    
    def provide_data(self):
        
        return None