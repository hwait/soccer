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


if __name__ == '__main__':
    if len(sys.argv) == 3:
        ds=sys.argv[1]
        de=sys.argv[2]
        d = datetime.strptime(ds, '%Y-%m-%d')
        end_date = datetime.strptime(de, '%Y-%m-%d')
        while d<end_date:
            print(d)
            in_path='{}/{dt:%Y/%b/}{dt.day}/'.format(IN_PATH, dt=d)
            if path.isdir(in_path):# and d.day<10:
                match_changes=[]
                odds_changes=[]
                for f in tqdm(listdir(in_path)):
                    read_match(int(f),in_path)
                    #break
                out_path='{}/{:%Y-%b-%d}'.format(OUT_PATH, d)
                pd.DataFrame(match_changes).to_csv(out_path+'_matches.csv', index=False)
                pd.DataFrame(odds_changes).drop_duplicates().to_csv(out_path+'_odds.csv', index=False)
            d+=timedelta(days=1)

        
        