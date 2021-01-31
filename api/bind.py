import glob
from os import path
import pandas as pd
import numpy as np
import pytz
from datetime import timezone,datetime,timedelta
import api.util

def bind_full(df_sofa,df_op):
    df_sofa_=df_sofa.copy()
    df_op_=df_op.copy()
    print(f'IN: Sofa={df_sofa_.shape}, OP={df_op_.shape}')
    df_sofa_['t1_first']=df_sofa_['t1'].apply(lambda x: x.split(' ')[0])
    df_sofa_['t2_first']=df_sofa_['t2'].apply(lambda x: x.split(' ')[0])
    df_op_['t1_first']=df_op_['t1'].apply(lambda x: x.split(' ')[0])
    df_op_['t2_first']=df_op_['t2'].apply(lambda x: x.split(' ')[0])
    
    # Both teams step:
    df_op_=df_op_.rename(columns={'tid1':'op_tid1','tid2':'op_tid2','t1':'op_t1','t2':'op_t2','mid':'op_mid'})
    df_merged=df_sofa_.merge(df_op_[['op_mid','op_tid1','op_tid2','op_t1','op_t2', 'ds']], left_on=['ds','t1', 't2'], right_on=['ds','op_t1', 'op_t2'], how='left')
    df_sofa_full=df_merged[~df_merged['op_mid'].isna()]
    df_sofa_=df_merged[df_merged['op_mid'].isna()][df_sofa_.columns]
    print(f'BOTH teams step: Binded={df_sofa_full.shape}, Total={df_sofa_full.shape}, Rest={df_sofa_.shape}')
    
    # First team step:
    teams_exclude=['inter','racing','liverpool','nacional','arsenal','san jose']
    df_sofa_none=df_sofa_[df_sofa_['t1'].isin(teams_exclude)]
    df_sofa_=df_sofa_[~df_sofa_['t1'].isin(teams_exclude)]
    df_merged=df_sofa_.merge(df_op_[['op_mid','op_tid1','op_tid2','op_t1','op_t2', 'ds', 't1_first', 'country']], left_on=['ds','t1_first', 't2','country'], right_on=['ds','t1_first', 'op_t2','country'], how='left')
    df_binded=df_merged[~df_merged['op_mid'].isna()]
    df_sofa_full=pd.concat([df_sofa_full,df_binded], axis=0)
    df_sofa_=df_merged[df_merged['op_mid'].isna()][df_sofa_.columns]
    print(f'First team step: Binded={df_binded.shape}, Total={df_sofa_full.shape}, Rest={df_sofa_.shape}, Excluded={df_sofa_none.shape}')

    # Second team step:
    teams_exclude=['racing','arsenal']
    df_sofa_none=pd.concat([df_sofa_none,df_sofa_[df_sofa_['t2'].isin(teams_exclude)]], axis=0)
    df_sofa_=df_sofa_[~df_sofa_['t2'].isin(teams_exclude)]
    df_merged=df_sofa_.merge(df_op_[['op_mid','op_tid1','op_tid2','op_t1','op_t2', 'ds', 't2_first', 'country']], left_on=['ds','t2_first', 't1','country'], right_on=['ds','t2_first', 'op_t1','country'], how='left')
    df_binded=df_merged[~df_merged['op_mid'].isna()]
    df_sofa_full=pd.concat([df_sofa_full,df_binded], axis=0)
    df_sofa_=df_merged[df_merged['op_mid'].isna()][df_sofa_.columns]
    df_sofa_=pd.concat([df_sofa_,df_sofa_none], axis=0)
    print(f'Second team step: Binded={df_binded.shape}, Total={df_sofa_full.shape}, Rest={df_sofa_.shape}, Excluded={df_sofa_none.shape}')

    return (df_sofa_full, df_sofa_)

def check_teams(df):
    a=df[['country','tid1','t1','op_tid1','op_t1']]
    b=df[['country','tid2','t2','op_tid2','op_t2']]
    a.columns=b.columns=['country','tid','t','op_tid','op_t']
    teams=pd.concat([a,b], axis=0).drop_duplicates().sort_values(by='tid')
    #mask = teams.tid.duplicated(keep=False)
    #display(teams[mask])
    return teams

def save(df, teams):
    fn=f'data/teams_ss_op.csv'
    if path.exists(fn):
        teams_old=pd.read_csv(fn, index_col=None)
        teams=pd.concat([teams_old,teams], axis=0).drop_duplicates()
    teams.to_csv(fn, index=False)

    fn=f'data/binds_ss_op.csv'
    cols=['country', 'ds', 'mid','tid1','tid2','t1','t2','op_mid','op_tid1','op_tid2','op_t1','op_t2']
    if path.exists(fn):
        df_old=pd.read_csv(fn, index_col=None).drop_duplicates()
        df=pd.concat([df_old[cols],df[cols]], axis=0)
        print('save',df.shape)
        df=df.drop_duplicates(subset=['mid','op_mid'])
        print('save',df.shape)
    df[cols].to_csv(fn, index=False)

def filter_tids(df, teams):
    teams_=teams.rename(columns={'tid':'tid1','op_tid':'op_tid1'})
    df_=df.merge(teams_[['tid1','op_tid1', 'country']], left_on=['tid1', 'country'], right_on=['tid1','country'], how='left')
    print('T1 merged: ', df_.shape)
    teams_=teams.rename(columns={'tid':'tid2','op_tid':'op_tid2'})
    df_=df_.merge(teams_[['tid2','op_tid2', 'country']], left_on=['tid2', 'country'], right_on=['tid2','country'], how='left')
    print('T2 merged: ',df_.shape)
    df_both=df_[~(df_['op_tid1'].isna() | df_['op_tid2'].isna())]
    df_1=df_[~df_['op_tid1'].isna() & df_['op_tid2'].isna()]
    df_2=df_[df_['op_tid1'].isna() & ~df_['op_tid2'].isna()]
    df_none=df_[(df_['op_tid1'].isna()) & (df_['op_tid2'].isna())]
    print('IN: {}, BOTH: {}, ONLY T1: {}, ONLY T2: {}, NO BINDS: {}, OUT: {}'.format(len(df.index),len(df_both.index),len(df_1.index),len(df_2.index),len(df_none.index), len(df_both.index)+len(df_1.index)+len(df_2.index)+len(df_none.index)))
    return df_both, df_1,df_2,df_none


def process_by_tid(df_ss, df_op, type='both'):
    df_op_=df_op.copy()
    df_op_=df_op_.rename(columns={'tid1':'op_tid1','tid2':'op_tid2','t1':'op_t1','t2':'op_t2','mid':'op_mid'})
    print(f'IN: Sofa={df_ss.shape}, OP={df_op_.shape}')
    df_ss['date']=df_ss.ds.apply(lambda x: x.strftime('%d-%m-%Y'))
    df_op_['date']=df_op_.ds.apply(lambda x: x.strftime('%d-%m-%Y'))

    if type=='both':
        # By Both teams
        df_merged=df_ss.merge(df_op_[['op_mid','op_tid1','op_tid2','op_t1','op_t2', 'ds']], on=['ds','op_tid1','op_tid2'], how='left')
        df_binded=df_merged[~df_merged['op_mid'].isna()]
        df_none=df_merged[df_merged['op_mid'].isna()][df_ss.columns]
        print(f'Both teams step, exact dates: Binded={df_binded.shape}, Total={df_binded.shape}, Rest={df_none.shape}')
        df_merged=df_none.merge(df_op_[['op_mid','op_tid1','op_tid2','op_t1','op_t2','date', 'country']], on=['date','op_tid1','op_tid2', 'country'], how='left')
        df_binded1=df_merged[~df_merged['op_mid'].isna()]
        df_binded=pd.concat([df_binded,df_binded1], axis=0).drop_duplicates()
        df_ss=df_merged[df_merged['op_mid'].isna()][df_ss.columns]
        print(f'Both teams step, within a day: Binded={df_binded1.shape}, Total={df_binded.shape}, Rest={df_ss.shape}')

    if type=='first':
        # By First team
        df_merged=df_ss[[x for x in df_ss.columns if x!='op_tid2']].merge(df_op_[['op_mid','op_tid1','op_tid2','op_t1','op_t2', 'ds']], on=['ds','op_tid1'], how='left')
        df_binded=df_merged[~df_merged['op_mid'].isna()]
        df_none=df_merged[df_merged['op_mid'].isna()][df_ss.columns]
        print(f'First team step, exact dates: Binded={df_binded.shape}, Total={df_binded.shape}, Rest={df_none.shape}')
        df_merged=df_none[[x for x in df_none.columns if x!='op_tid2']].merge(df_op_[['op_mid','op_tid1','op_tid2','op_t1','op_t2','date', 'country']], on=['date','op_tid1', 'country'], how='left')
        df_binded1=df_merged[~df_merged['op_mid'].isna()]
        df_binded=pd.concat([df_binded,df_binded1], axis=0).drop_duplicates()
        df_ss=df_merged[df_merged['op_mid'].isna()][df_ss.columns]
        print(f'First team step, within a day: Binded={df_binded1.shape}, Total={df_binded.shape}, Rest={df_ss.shape}')
    
    if type=='second':
        # By Second team
        df_merged=df_ss[[x for x in df_ss.columns if x!='op_tid1']].merge(df_op_[['op_mid','op_tid1','op_tid2','op_t1','op_t2', 'ds']], on=['ds','op_tid2'], how='left')
        df_binded=df_merged[~df_merged['op_mid'].isna()]
        #df_binded=pd.concat([df_binded,df_binded1], axis=0).drop_duplicates()
        df_none=df_merged[df_merged['op_mid'].isna()][df_ss.columns]
        print(f'Second team step, exact dates: Binded={df_binded.shape}, Total={df_binded.shape}, Rest={df_none.shape}')
        df_merged=df_none[[x for x in df_none.columns if x!='op_tid1']].merge(df_op_[['op_mid','op_tid1','op_tid2','op_t1','op_t2','date', 'country']], on=['date','op_tid2', 'country'], how='left')
        df_binded1=df_merged[~df_merged['op_mid'].isna()]
        df_binded=pd.concat([df_binded,df_binded1], axis=0).drop_duplicates()
        df_ss=df_merged[df_merged['op_mid'].isna()][df_ss.columns]
        print(f'Second team step, within a day: Binded={df_binded1.shape}, Total={df_binded.shape}, Rest={df_ss.shape}')

    return df_binded.drop(columns='date'),df_ss.drop(columns='date')


def bind_iteration(n,df, df_ss, df_op):
    print(f'**** {n} ITERATION ****')
    teams=check_teams(df)
    save(df,teams)

    df_both, df_1,df_2,df_none=filter_tids(df_ss, teams)

    df_binded,df_both=process_by_tid(df_both, df_op, type='both')
    df=pd.concat([df,df_binded], axis=0).drop_duplicates()
    print(df.shape)

    df_binded,df_1=process_by_tid(df_1, df_op, type='first')
    df=pd.concat([df,df_binded], axis=0).drop_duplicates()
    print(df.shape)

    df_binded,df_2=process_by_tid(df_2, df_op, type='second')
    df=pd.concat([df,df_binded], axis=0).drop_duplicates()
    print(df.shape)
    teams=check_teams(df)
    save(df,teams)
    return df