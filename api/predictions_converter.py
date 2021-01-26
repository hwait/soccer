import pandas as pd
import numpy as np
from IPython.display import display

import api.util

class PredictionsConverter:
    def __init__(self, provider,  yhat, y, info, odds=True):
        self.CLASSES=['HOME','DRAW','AWAY']
        self.DATA_PATH=f'predictions/{provider}/'
        self.LABELS_PREDICTED=yhat
        self.LABELS=y
        self.INFO=info.copy()
        self.ODDS=odds

    def make_df(self, threshold=0.5):
        df_yhat=pd.DataFrame(data=self.LABELS_PREDICTED, columns=['prob_home', 'prob_draw', 'prob_away'])
        df_y=pd.DataFrame(data=self.LABELS, columns=['winner_home', 'winner_draw', 'winner_away'])
        df_i=self.INFO.reset_index(drop=True)
        df_preds=pd.concat([df_i,df_y,df_yhat], axis=1)
        if threshold=='max':
            a=df_yhat.rank(method='max', axis=1)
            df_preds['pred_home']=a['prob_home'].apply(lambda x: 1 if x>2 else 0)
            df_preds['pred_draw']=a['prob_draw'].apply(lambda x: 1 if x>2 else 0)
            df_preds['pred_away']=a['prob_away'].apply(lambda x: 1 if x>2 else 0)
        else:
            df_preds['pred_home']=np.where(df_preds['prob_home']>threshold,1,0)
            df_preds['pred_draw']=np.where(df_preds['prob_draw']>threshold,1,0)
            df_preds['pred_away']=np.where(df_preds['prob_away']>threshold,1,0)
        df_preds=df_preds[(df_preds['pred_home']==1) | (df_preds['pred_draw']==1) |(df_preds['pred_away']==1)]
        df_preds['winner_home']=df_preds['winner_home'].astype(int)
        df_preds['winner_draw']=df_preds['winner_draw'].astype(int)
        df_preds['winner_away']=df_preds['winner_away'].astype(int)
        df_preds['pred_home']=df_preds['pred_home'].astype(int)
        df_preds['pred_draw']=df_preds['pred_draw'].astype(int)
        df_preds['pred_away']=df_preds['pred_away'].astype(int)
        df_preds['win']=0
        df_preds.loc[(df_preds['winner_home']==df_preds['pred_home']) & (df_preds['winner_home']==1),'win']=1
        df_preds.loc[(df_preds['winner_draw']==df_preds['pred_draw']) & (df_preds['winner_draw']==1),'win']=1
        df_preds.loc[(df_preds['winner_away']==df_preds['pred_away']) & (df_preds['winner_away']==1),'win']=1
        if self.ODDS:
            df_preds.loc[df_preds['pred_home']==1,'odds']=df_preds['odds_home']
            df_preds.loc[df_preds['pred_draw']==1,'odds']=df_preds['odds_draw']
            df_preds.loc[df_preds['pred_away']==1,'odds']=df_preds['odds_away']
            df_preds.loc[df_preds['win']==0,'prf']=-1
            df_preds.loc[df_preds['odds']==0,'prf']=0
            
            df_preds['prf']=np.where(df_preds.win>0,df_preds.odds-1, df_preds['prf'])
        df_preds = df_preds.drop_duplicates()
        #df_preds = df_preds.rename(columns={'homeTeamShort': 't1','awayTeamShort': 't2','tournament': 'liga','ts': 'ds','homeScoreFT': 'sc1','awayScoreFT': 'sc2'})
        self.Y=df_preds[['winner_home','winner_draw','winner_away']].values
        self.YHAT=df_preds[['pred_home','pred_draw','pred_away']].values

        # homeTeamShort awayTeamShort tournament ts homeScoreFT awayScoreFT
        #"['sc1', 'sc2', 't2', 'liga', 't1', 'ds'] not in index"
        self.DF=df_preds[['ds', 'country', 'liga', 't1', 't2', 'sc1', 'sc2', 'odds_home', 'odds_draw', 'odds_away','winner_home', 'winner_draw', 'winner_away','pred_home','pred_draw','pred_away','prob_home', 'prob_draw', 'prob_away','win','prf']] if self.ODDS else df_preds[['ds', 'country', 'liga', 't1', 't2', 'sc1', 'sc2', 'winner_home', 'winner_draw', 'winner_away','pred_home','pred_draw','pred_away','prob_home', 'prob_draw', 'prob_away','win']]
    
    def performance_metrics(self):
        display(api.util.get_performance_metrics(self.Y, self.YHAT, self.CLASSES))
    
    def graph(self,mode='tpfp'):
        if mode == 'tpfp':
            api.util.get_curve(self.Y, self.YHAT, self.CLASSES)
        elif mode== 'prc':
            api.util.get_curve(self.Y, self.YHAT, self.CLASSES, curve='prc')
    
    def profit(self):
        df_=self.DF.loc[self.DF['odds_home']>0]
        print('WAG:{}; ACC: {}; PRF: {}; ROI: {}'.format(df_.shape[0],df_.win.mean(), df_.prf.sum(), df_.prf.sum()/df_.shape[0]))