import os
import pandas as pd
import numpy as np
import pickle
import api.util
from sklearn.preprocessing import LabelEncoder,OneHotEncoder,MinMaxScaler

class SofaDataProvider:
    def __init__(self, include=[],exclude=[], load=False, today=False):
        self.LOCAL_TZ = 'Asia/Almaty'
        self.SERVER_TZ = 'UTC'
        self.DATA_PATH='data/sofa/'
        self.PREREQUISITES_PATH='prerequisites/sofa/'
        self.INCLUDE=include
        self.EXCLUDE=exclude
        self.COL_CAT=[]
        self.COL_NUM=[]
        self.COL_LBL=[]
        self.COL_INF=[]
        self.TODAY=today
        self.LOAD=True if today else load
    
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
    
    def _encode_teams(self, df):
        teams_name=self.DATA_PATH+'teams.csv'
        teams_saved=pd.read_csv(teams_name, index_col=None)
        teams=pd.concat([pd.DataFrame(df['t1'].unique(), columns=['name']),pd.DataFrame(df['t2'].unique(), columns=['name'])]).drop_duplicates()
        teams_new=teams[~teams.name.isin(teams_saved.name)]
        if not teams_new.empty:
            print('New teams!')
            id=teams_saved.id.max()+1
            #id=0
            teams_list=[]
            for row in teams_new.itertuples():
                if len(row.name)>1:
                    teams_list.append({'name':row.name, 'id':id})
                    id+=1
                #break
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
                df1=pd.DataFrame(df[features[0]].unique(), columns=[name])
                df2=pd.DataFrame(df[features[1]].unique(), columns=[name])
                if enctype=='sc':
                    encoder.fit(pd.concat([df1,df2], axis=1)[name])
                else:
                    encoder.fit(pd.concat([df1,df2])[name])
            self._save_prerequisite(f'{enctype}_{name}', encoder)
        if  enctype=='ohe':
            return encoder.transform(df[features].values).toarray()
        if len(features)==1:
            df[outs[0]] = encoder.transform(df[features].values)
        else:
            if enctype=='sc':
                df[outs] = encoder.transform(df[features])
            else:
                df[outs[0]] = encoder.transform(df[[features[0]]])
                df[outs[1]] = encoder.transform(df[[features[1]]])
        return df

    def _provide_statistics(self, df_src, period='ALL'):
        df=pd.read_csv(self.DATA_PATH+'statistics.csv', index_col=False)
        #nulls=pd.DataFrame(df.isna().sum(), columns=['n'])
        #drop_cols=['Blocked shots', 'Duels won', 'Shots inside box', 'Shots outside box', 'Passes', 'Accurate passes', 'Aerials won', 'Big chances', 'Clearances', 'Big chances missed', 'Long balls', 'Dribbles', 'Crosses', 'Interceptions', 'Tackles', 'Possession lost', 'Hit woodwork', 'Red cards', 'Counter attacks', 'Counter attack shots', 'Counter attack goals', 'Total shots']
        #cols_to_keep=[x for x in df.columns if not x in drop_cols]
        cols_to_keep=['mid', 'period', 'ishome', 'Ball possession', 'Shots on target', 'Shots off target', 'Corner kicks', 'Offsides', 'Fouls', 'Yellow cards', 'Goalkeeper saves']
        df=df[cols_to_keep]
        df=df.reset_index(drop=True)
        df['precision']=np.where(df['Shots on target']>0, df['Shots off target']/df['Shots on target'], 0)        
        for col in df.columns[4:]:
            df=self._encode('sc', [col], [col], df)
        #scaler = MinMaxScaler()
        #df_scaled = scaler.fit_transform(df[df.columns[4:]])
        #df=pd.concat([df[df.columns[:4]],pd.DataFrame(df_scaled, columns=df.columns[4:])], axis=1)

        cols_stats=['possession', 'shont', 'shofft', 'corners', 'offsides', 'fouls', 'cards', 'gksaves','precision']
        df1=df[df['ishome']==1].reset_index(drop=True).sort_values(by='mid')
        df1=df1.drop(columns=['period', 'ishome'])
        df1.columns=['mid']+[x+'1' for x in cols_stats]
        df0=df[df['ishome']==0].reset_index(drop=True).sort_values(by='mid')
        df0=df0.drop(columns=['mid','period', 'ishome'])
        df0.columns=[x+'2' for x in cols_stats]
        df=pd.concat([df1,df0], axis=1)
        df=df.dropna()
        df['possession1']=df['possession1'].str[:-1].astype(float)/100
        df['possession2']=df['possession2'].str[:-1].astype(float)/100

        df=df.drop_duplicates()
        df_src=df_src.merge(df, on='mid', how='left')
        return df_src

    def _provide_lineups(self):
        df=pd.read_csv(self.DATA_PATH+'lineups.csv', index_col=False)
        return df

    def _provide_formations(self, df_src):
        self.COL_CAT+=['home_formation','away_formation']
        if self.TODAY:
            df=pd.read_csv(self.DATA_PATH+'formations_today.csv', index_col=False)
        else:
            df=pd.read_csv(self.DATA_PATH+'formations.csv', index_col=False)

        df=self._encode('le', ['formation_h','formation_a'], ['home_formation','away_formation'], df)
       
        df_src=df_src.merge(df, on='mid', how='left')
        if not self.TODAY:
            df_src=df_src.dropna(subset=['home_formation'])
            df_src['home_formation'] = df_src['home_formation'].astype(int)
            df_src['away_formation'] = df_src['away_formation'].astype(int)
        return df_src

    def _provide_incidents(self):
        df=pd.read_csv(self.DATA_PATH+'incidents.csv', index_col=False)
        return df

    def _provide_graph(self, df_src):
        df_graph=pd.read_csv(self.DATA_PATH+'graph.csv', index_col=False)
        df_graph=df_graph.loc[(df_graph['minute']>0) & (df_graph['minute']<91)]
        df_graph.columns=['mid','time','graph1']
        df_graph=df_graph.drop_duplicates()
        df_graph=df_graph.groupby('mid').graph1.sum().reset_index()
        df_graph['graph2']=df_graph['graph1']*-1
        df_graph=self._encode('sc', ['graph1','graph2'], ['graph1','graph2'], df_graph)
        df_src=df_src.merge(df_graph, on='mid', how='left')
        return df_src

    def _provide_votes(self, df_src):
        self.COL_NUM+=['vote_home','vote_draw','vote_away']
        self.COL_CAT+=['pop_r']
        if self.TODAY:
            df=pd.read_csv(self.DATA_PATH+'votes_today.csv', index_col=False)
        else:
            df=pd.read_csv(self.DATA_PATH+'votes.csv', index_col=False)
        df=df.dropna()
        df['votes']=df[['vote1','vote2','voteX']].sum(axis=1)
        df['vote_home']=df['vote1']/df['votes']
        df['vote_draw']=df['voteX']/df['votes']
        df['vote_away']=df['vote2']/df['votes']
        df=df[['mid','vote_home','vote_draw','vote_away','votes']]

        df_src=df_src.merge(df, on='mid', how='left')
        if not self.TODAY:
            df_src=df_src.dropna(subset=['votes'])
        df_src['y']=df_src.ds.dt.year

        name='r_votes'
        if self.LOAD:
            intervals=self._load_prerequisites(name)
        else:
            intervals={}
            for y in range(2015,2022):
                _,intervals[y]=pd.qcut(df_src[df_src.y==y].votes, 5, retbins=True, labels=False)
            self._save_prerequisite(name, intervals)

        for key in intervals:
            df_src.loc[df_src.y==key, 'pop_r']=pd.cut(df_src[df_src.y==key]['votes'], bins=intervals[key], labels=False, include_lowest=True)
        #df_src.pop_r=df_src.pop_r.astype(int)
        if not self.TODAY:
            df_src.drop(columns=['votes','y'], inplace=True)
        return df_src

    def _provide_matches(self):
        info_colums=[ 'mid', 'ds', 'country', 'liga','tid1','tid2', 't1', 'homeScoreHT', 'sc1', 't2', 'awayScoreHT','sc2', 'winner']
        cat_colums=['country_id', 'round']
        label_colums=['winner']
        self.COL_INF+=info_colums
        self.COL_CAT+=cat_colums
        self.COL_LBL+=label_colums
        cols=np.unique(info_colums+cat_colums+label_colums)

        chars0=['ó','é','í','ş','ã','İ','ğ','ç','ü','É','â','Ç','õ','ł','ą','Ś','ø','ń','ț','å','Å','ß', 'æ', 'Ž','ş', 'ə','Ö','ı','á','î','ñ','ö','ź','ú','è','Ł','ę','Ş','ä','ë','ô','ș','ū','č','Š','Þ','ė','Ä','ă','ì','š','i','ć','ň','ž','ư','ơ','ê','à','ð','ő','Ü','ý','ď','Á','ř','Č','Ú']
        chars1=['o','e','i','s','a','I','g','c','u','E','a','C','o','l','a','s','o','n','t','a','A','ss','ae','Z','sh','a','O','i','a','i','n','o','z','u','e','L','e','S','a','e','o','s','u','c','S','P','e','A','a','i','s','i','c','n','z','u','o','e','a','d','o','U','y','d','A','r','C','U']
        dicUnicode2En=dict(zip(chars0, chars1))
        
        df_countries=pd.read_csv(self.DATA_PATH+'countries.csv', index_col=None)
        df_countries['Name']=df_countries['Name'].str.lower()
        df_countries.columns=['country','countryCode']
        
        if self.TODAY:
            df=pd.read_csv(self.DATA_PATH+'matches_today.csv', index_col=False)
            df['winnerCode']=0
        else:
            df=pd.read_csv(self.DATA_PATH+'matches_done.csv', index_col=False)
        df['round']=df['round'].fillna(0).astype(int)
        df['ts']=pd.to_datetime(df['ts'])
        df['winner']=df['winnerCode'].apply(lambda x: 'home' if x==1.0 else 'away' if x==2.0 else 'draw')
        df = df.rename(columns={'id': 'mid','tournament': 'liga','ts': 'ds','homeScoreFT': 'sc1','awayScoreFT': 'sc2'})
        df=df.merge(df_countries, on='country', how='left')
        df.loc[df['country']=='england','countryCode']='GB'
        df.loc[df['country']=='scotland','countryCode']='GB'
        df.loc[df['country']=='czech-republic','countryCode']='CZ'
        df.loc[df['country']=='russia','countryCode']='RU'
        df.loc[df['country']=='usa','countryCode']='US'
        df['t1']=df['homeTeam'].replace(dicUnicode2En, regex=True).replace('[^a-zA-Z0-9 ]', '', regex=True).str.lower()
        df['t2']=df['awayTeam'].replace(dicUnicode2En, regex=True).replace('[^a-zA-Z0-9 ]', '', regex=True).str.lower()
        df.loc[df['t1']=='','t1']='AEK Athens'
        df.loc[df['t2']=='','t2']='AEK Athens'

        df=self._encode('le', ['country'], ['country_id'], df)
        df=self._encode_teams(df)
        return df[cols]

    def _load_data(self):
        df=self._provide_matches()
        df=self._provide_formations(df)
        df=self._provide_votes(df)
        if not self.TODAY:
            df=self._provide_graph(df)
            df=self._provide_statistics(df)
        return df
    
    def provide_data(self):
        df=self._load_data()
        data=df[self._ff(self.COL_NUM)].values
        for col in self._ff(self.COL_CAT):
            data=np.hstack([data,self._encode('ohe', [col], [col], df)])
       
        labels=self._encode('ohe', self.COL_LBL, self.COL_LBL, df)
        info=df[self.COL_INF]
        return data, labels, info, df
