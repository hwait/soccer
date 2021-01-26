import sys
from os import path,listdir
from datetime import datetime,timedelta
import time
import re
import pandas as pd
import numpy as np
import random
import pytz
from tqdm import tqdm
import bz2
import json

BASE_PATH='data/bf'
IN_PATH='raw/bf/BASIC'
OUT_PATH='data/bf/days'

def read_match(eventId, path):
    di={}
    for f in listdir('{}/{}/'.format(path,eventId)):
        process_odds=False
        if not f.startswith('1.'):
            continue
        found=False
        with bz2.open('{}/{}/{}'.format(path, eventId, f), "rt") as bz_file:
            suspendedTime=0
            inplay=False
            for line in bz_file:
                if not found and not 'MATCH_ODDS' in line and not 'OVER_UNDER_25' in line and not 'HALF_TIME' in line:
                    break
                #print(eventId,f)
                found=True
                j=json.loads(line)
                odi={
                    'eventId':eventId,
                    'clk':datetime.fromtimestamp(j['pt']//1000)
                }
                for x in j['mc']:
                    if 'marketDefinition' in x.keys():
                        node=x['marketDefinition']
                        if node['inPlay'] and not inplay:
                            inplay=node['inPlay']
                            inplayTime=datetime.fromtimestamp(j['pt']//1000)
                        if node['marketType']=='MATCH_ODDS':
                            process_odds=True
                            di['eventId']=eventId
                            di['clk']=datetime.fromtimestamp(j['pt']//1000)
                            di['marketTime']= datetime.strptime(node['marketTime'][:-5],'%Y-%m-%dT%H:%M:%S')
                            di['countryCode']= node['countryCode'] if 'countryCode' in node.keys() else ''
                            di['openDate']= datetime.strptime(node['openDate'][:-5],'%Y-%m-%dT%H:%M:%S')
                            di['eventName']= node['eventName']
                            di['home_id']= node['runners'][0]['id']
                            di['home_name']= node['runners'][0]['name']
                            di['home_sp']= node['runners'][0]['sortPriority']
                            di['away_id']= node['runners'][1]['id']
                            di['away_name']= node['runners'][1]['name']
                            di['away_sp']= node['runners'][1]['sortPriority']
                            di['draw_id']= node['runners'][2]['id']
                            di['draw_name']= node['runners'][2]['name']
                            di['draw_sp']= node['runners'][2]['sortPriority']
                            if inplay:
                                di['inplayTime']=inplayTime
                        if node['marketType']=='OVER_UNDER_25':
                            process_odds=True
                            di['under_id']= node['runners'][0]['id']
                            di['under_name']= node['runners'][0]['name']
                            di['over_id']= node['runners'][1]['id']
                            di['over_name']= node['runners'][1]['name']
                        if node['marketType']=='HALF_TIME':
                            process_odds=False
                            if node['status']=="SUSPENDED":
                                suspendedTime=datetime.fromtimestamp(j['pt']//1000)
                            elif node['status']=="CLOSED":
                                di['halfTime']=suspendedTime
                    if 'rc' in x.keys() and process_odds:
                        for rc in x['rc']:
                            odi['ltp']= rc['ltp']
                            odi['id']= rc['id']
                            odi['inplay']=inplay
                            odds_changes.append(odi)
    match_changes.append(di)

ID_UNDER=47972
ID_OVER=47973
ID_DRAW=58805

def append_save(df, f):
    dfo=pd.read_csv(f)
    dfr=pd.concat([dfo,df], axis=0)
    dfr=dfr.drop_duplicates(keep='first')
    dfr.to_csv(f, index=False)

def convert_odds(df_odds, df_matches, y):
    cols=['eventId','clk','ltp','id','ip']
    cols_noid=['eventId','clk','ltp','ip']
    df=df_odds[df_odds['eventId'].isin(df_matches['eventId'])]
    df['ip']=0
    df.loc[df['inplay'],'ip']=1
    df=df[cols]
    df_un=df.loc[df['id']==ID_UNDER][cols_noid]
    df_ov=df.loc[df['id']==ID_OVER][cols_noid]
    df_draw=df.loc[df['id']==ID_DRAW][cols_noid]
    df_home=pd.merge(df,df_matches, left_on=['eventId','id'], right_on=['eventId','home_id'])[cols_noid]
    df_away=pd.merge(df,df_matches, left_on=['eventId','id'], right_on=['eventId','away_id'])[cols_noid]
    append_save(df_un,f'{BASE_PATH}/bf_un_{y}.csv')
    append_save(df_ov,f'{BASE_PATH}/bf_ov_{y}.csv')
    append_save(df_draw,f'{BASE_PATH}/bf_draw_{y}.csv')
    append_save(df_home,f'{BASE_PATH}/bf_home_{y}.csv')
    append_save(df_away,f'{BASE_PATH}/bf_away_{y}.csv')

def convert_matches(df_matches):
    df=df_matches.loc[~df_matches['home_id'].isna()]
    df=df[['eventId','countryCode','openDate','eventName','home_id','home_name','away_id','away_name','draw_id','draw_name','inplayTime','halfTime']]
    df=df[df['eventName'].str.contains(' v ')]
    df[['ht','at']]=df.apply(lambda x: x.eventName.split(' v '), axis=1, result_type="expand")
    df=df.reset_index(drop=True)
    df1=df.loc[(df['draw_name']!='The Draw')]
    df2=df.loc[(df['draw_name']=='The Draw')]
    df1.loc[(df1['away_name']=='The Draw') & (df1['ht'].str.lower()==df1['home_name'].str.lower()),'away_id']=df1['draw_id']
    df1.loc[(df1['away_name']=='The Draw') & (df1['ht'].str.lower()==df1['home_name'].str.lower()),'away_name']=df1['at']
    df1.loc[(df1['away_name']=='The Draw') & (df1['at'].str.lower()==df1['home_name'].str.lower()),'away_id']=df1['home_id']
    df1.loc[(df1['away_name']=='The Draw') & (df1['at'].str.lower()==df1['home_name'].str.lower()),'home_id']=df1['draw_id']
    df1.loc[(df1['away_name']=='The Draw') & (df1['at'].str.lower()==df1['home_name'].str.lower()),'home_name']=df1['ht']
    df1.loc[(df1['away_name']=='The Draw') & (df1['ht'].str.lower()==df1['home_name'].str.lower()),'away_name']=df1['at']
    df1.loc[(df1['home_name']=='The Draw') & (df1['at'].str.lower()==df1['away_name'].str.lower()),'home_id']=df1['draw_id']
    df1.loc[(df1['home_name']=='The Draw') & (df1['at'].str.lower()==df1['away_name'].str.lower()),'home_name']=df1['ht']
    df1.loc[(df1['home_name']=='The Draw') & (df1['ht'].str.lower()==df1['away_name'].str.lower()),'home_id']=df1['away_id']
    df1.loc[(df1['home_name']=='The Draw') & (df1['ht'].str.lower()==df1['away_name'].str.lower()),'away_id']=df1['draw_id']
    df1.loc[(df1['home_name']=='The Draw') & (df1['ht'].str.lower()==df1['away_name'].str.lower()),'away_name']=df1['at']
    df1.loc[(df1['home_name']=='The Draw') & (df1['at'].str.lower()==df1['away_name'].str.lower()),'home_name']=df1['ht']
    df=pd.concat([df1,df2], axis=0)
    df.dropna(subset=['inplayTime'], inplace=True)
    df=df[~((df['home_name']=='test1') | (df['away_name']=='test1') | (df['home_name']=='test2') | (df['away_name']=='test2'))]
    df=df[['eventId','countryCode','openDate','eventName','home_id','home_name','away_id','away_name','inplayTime','halfTime']]
    append_save(df,'data/bf/bf_matches.csv')

if __name__ == '__main__':
    if len(sys.argv) == 3:
        matches=[]
        odds=[]
        ds=sys.argv[1]
        de=sys.argv[2]
        d = datetime.strptime(ds, '%Y-%m-%d')
        end_date = datetime.strptime(de, '%Y-%m-%d')
        while d<=end_date:
            print(d)
            in_path='{}/{dt:%Y/%b/}{dt.day}/'.format(IN_PATH, dt=d)
            if path.isdir(in_path):# and d.day<10:
                match_changes=[]
                odds_changes=[]
                for f in tqdm(listdir(in_path)):
                    #print(f)
                    read_match(int(f),in_path)
                    #break
                out_path='{}/{:%Y-%b-%d}'.format(OUT_PATH, d)
                df_matches=pd.DataFrame(match_changes)
                df_odds=pd.DataFrame(odds_changes)
                df_matches.to_csv(out_path+'_matches.csv', index=False)
                df_odds.drop_duplicates().to_csv(out_path+'_odds.csv', index=False)
                matches.append(df_matches)
                odds.append(df_odds)
            d+=timedelta(days=1)
        df_matches=pd.concat(matches, axis=0)[['eventId','countryCode','openDate','eventName','home_id','home_name','away_id','away_name','draw_id','draw_name','inplayTime','halfTime']]
        df_odds=pd.concat(odds, axis=0)
        convert_matches(df_matches)
        convert_odds(df_odds, df_matches, 2020)
        
        