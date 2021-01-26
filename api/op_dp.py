import os
import pandas as pd
import numpy as np
import pickle
from datetime import timezone
import api.util
from sklearn.preprocessing import LabelEncoder,OneHotEncoder,MinMaxScaler

class OpDataProvider:
    def __init__(self, include=[],exclude=[], load=False):
        self.LOCAL_TZ = 'Asia/Almaty'
        self.SERVER_TZ = 'UTC'
        self.DATA_PATH='data/op/'
        self.PREREQUISITES_PATH='prerequisites/op/'
        self.INCLUDE=include
        self.EXCLUDE=exclude
        self.COL_CAT=[]
        self.COL_NUM=[]
        self.COL_LBL=[]
        self.COL_INF=[]
        self.LOAD=load
    
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
    
    def _encode_teams(self, df):
        teams_name=self.DATA_PATH+'teams.csv'
        teams_saved=pd.read_csv(teams_name, index_col=None)
        teams=pd.concat([pd.DataFrame(df['t1'].unique(), columns=['name']),pd.DataFrame(df['t2'].unique(), columns=['name'])]).drop_duplicates()
        teams_new=teams[~teams.name.isin(teams_saved.name)]
        if not teams_new.empty:
            print('New teams!')
            id=teams_saved.id.max()+1
            teams_list=[]
            for row in teams_new.itertuples():
                if len(row.name)>1:
                    teams_list.append({'name':row.name, 'id':id})
                    id+=1
            teams_saved=pd.concat([teams_saved,pd.DataFrame(teams_list)])
            teams_saved.to_csv(teams_name, index=False)
        teams_saved.columns=['t1','tid1']
        df=df.merge(teams_saved, on='t1', how='left')
        teams_saved.columns=['t2','tid2']
        df=df.merge(teams_saved, on='t2', how='left')
        return df

    def _encode(self, enctype, features, outs, df):
        if (len(self.INCLUDE)>0 and outs[0] in self.INCLUDE) or outs[0] in self.EXCLUDE:
            return df
        name='_'.join(features)
        if self.LOAD:
            encoder=self._load_prerequisites(f'{enctype}_{name}')
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

    def _provide_odds(self, df_src):
        self.COL_NUM+=['drift_home','drift_draw','drift_away']
        #self.COL_NUM+=['oddsprob_home','oddsprob_draw','oddsprob_away','drift_home','drift_draw','drift_away']
        df=pd.read_csv(self.DATA_PATH+'odds.csv', index_col=False)
        df=df.dropna()
        df['w1']=1/df['w1']
        df['w2']=1/df['w2']
        df['wx']=1/df['wx']
        df['open_1']=1/df['open_1']
        df['open_2']=1/df['open_2']
        df['open_x']=1/df['open_x']
        df['margin']=df['w1']+df['w2']+df['wx']
        df['oddsprob_home']=df['w1']/df['margin']
        df['oddsprob_away']=df['w2']/df['margin']
        df['oddsprob_draw']=df['wx']/df['margin']
        df=df[(df['margin']>1.01) & (df['margin']<3)]
        df['margin']=df['open_1']+df['open_2']+df['open_x']
        df['open_1']=df['open_1']/df['margin']
        df['open_2']=df['open_2']/df['margin']
        df['open_x']=df['open_x']/df['margin']
        df=df[(df['margin']>1.01) & (df['margin']<3)]
        df['drift_home']=(df['open_1']-df['oddsprob_home'])/df['open_1']
        df['drift_away']=(df['open_2']-df['oddsprob_away'])/df['open_2']
        df['drift_draw']=(df['open_x']-df['oddsprob_draw'])/df['open_x']
        #df.to_csv('data/op/odds2.csv', index=False)
        df=df.groupby('mid')[['oddsprob_home','oddsprob_draw','oddsprob_away','drift_home','drift_away','drift_draw']].mean().reset_index()
        df_src=df_src.merge(df, on='mid', how='left')
        df_src=df_src.dropna(subset=['oddsprob_home'])
        return df_src

    def _provide_matches(self):
        info_colums=['mid','ds','country','liga','t1','t2','tid1','tid2','sc1','sc2', 'odds_home','odds_draw','odds_away',]
        #num_colums=['odds_prob_home','odds_prob_draw','odds_prob_away','bn']
        num_colums=['bn']
        cat_colums=['country_id']
        label_colums=['winner']
        self.COL_NUM+=num_colums
        self.COL_INF+=info_colums
        self.COL_CAT+=cat_colums
        self.COL_LBL+=label_colums
        cols=np.unique(info_colums+num_colums+cat_colums+label_colums)
        df=pd.read_csv(self.DATA_PATH+'matches_done.csv', index_col=False)
        df = df.rename(columns={'odds1': 'odds_home','oddsdraw': 'odds_draw','odds2': 'odds_away'})
        df['t1']=df['t1'].replace('[^a-zA-Z0-9 ]', '', regex=True).str.lower()
        df['t2']=df['t2'].replace('[^a-zA-Z0-9 ]', '', regex=True).str.lower()
        df=df[~df['t1'].str.contains(' u2')]
        df=df[~df['t2'].str.contains(' u2')]
        df.loc[df.odds_home=='-','odds_home']='1.01'
        df.loc[df.odds_away=='-','odds_away']='1.01'
        df.odds_home=df.odds_home.astype(float)
        df.odds_draw=df.odds_draw.astype(float)
        df.odds_away=df.odds_away.astype(float)
        df['ds']=pd.to_datetime(df['ds'])
        df['ds']=df['ds'].dt.tz_localize(timezone.utc)
        df['sc2']=df['sc2'].apply(lambda x: str(x).replace('&nbsp;pen.','').replace('&nbsp;ET',''))
        df.sc2=df.sc2.astype(int)
        df['winner']='home'
        df.loc[df['sc1']== df['sc2'],'winner']='draw'
        df.loc[df['sc1'] < df['sc2'],'winner']='away'
        df['mid'] = df.link.apply(lambda x: x[-9:-1])
        df['season'] = df.season.str.replace('/','-')
        df['liga'] = df.apply(lambda x: x.liga.replace('-'+x.season, ''), axis=1)
        
        df=self._encode('sc', ['bn'], ['bn'], df)
        df=self._encode('le', ['country'], ['country_id'], df)
        #df=self._encode('le', ['t1','t2'], ['home_tid','away_tid'], df)
        df=self._encode_teams(df)
        return df[self._ff(cols)]

    def _load_data(self):
        df=self._provide_matches()
        df=self._provide_odds(df)
        return df
    
    def provide_data(self):
        df=self._load_data()
        df.reset_index(drop=True, inplace=True)
        data=df[self._ff(self.COL_NUM)].values
        for col in self._ff(self.COL_CAT):
            data=np.hstack([data,self._encode('ohe', [col], [col], df)])
       
        labels=self._encode('ohe', self.COL_LBL, self.COL_LBL, df)
        info=df[self.COL_INF]
        return data, labels, info, df