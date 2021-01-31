import sys
import json
from os import path,listdir
from shutil import move
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from api.data_provider import DataProvider
from api.sofa_parser import SofaScoreParser
from api.fbref_parser import FbrefParser
from api.op_parser import OpParser
from tqdm import tqdm

dp=DataProvider()

if __name__ == '__main__':
    if len(sys.argv) == 2:
        ds=de='{:%Y-%m-%d}'.format(datetime.today()-timedelta(days=1))
    elif len(sys.argv) == 3:
        ds=de=sys.argv[2]
    elif len(sys.argv) == 4:
        ds=sys.argv[2]
        de=sys.argv[3]
    else:
        df=pd.read_csv('data/sofa/matches_done.csv')
        ds=df.ts.max()[:10]
        de='{:%Y-%m-%d}'.format(datetime.today()-timedelta(days=1))
        #de='2020-12-02'

    if sys.argv[1]=='d':
        dp.load_days(ds, de)
    elif sys.argv[1]=='df':
        dp.load_days()
    elif sys.argv[1]=='m':
        dp.load_matches()
    elif sys.argv[1]=='p':
        ssp=SofaScoreParser()
        ssp.parse_matches()
    elif sys.argv[1]=='fm':
        dp.load_fbref_matches()
    elif sys.argv[1]=='fd':
        dp.load_fbref_days(ds, de)
    elif sys.argv[1]=='fdp':    
        p=FbrefParser()
        p.parse_days()
    elif sys.argv[1]=='fmp':
        p=FbrefParser()
        p.parse_matches()
    elif sys.argv[1]=='od':
        dp.load_op_days(ds, de)
    elif sys.argv[1]=='odp':
        p=OpParser()
        p.parse_days()
    elif sys.argv[1]=='om':
        dp.load_op_matches()
    elif sys.argv[1]=='a':
        ssp=SofaScoreParser()
        fbp=FbrefParser()
        opp=OpParser()
        # print('*'*20)
        # print('  LOAD DAYS')
        # print('*'*20)
        print('-'*5,' Sofa ','-'*5)
        dp.load_days(ds, de)
        # print('-'*5,' Fbref ','-'*5)
        # dp.load_fbref_days(ds, de)
        # print('-'*5,' OP ','-'*5)
        # dp.load_op_days(ds, de)
        # print('-'*5,' ELO ','-'*5)
        # dp.load_elos(ds, de)

        # print('*'*20)
        # print('  PARSE DAYS')
        # print('*'*20)

        # print('-'*5,' Fbref ','-'*5)
        # fbp.parse_days()
        # print('-'*5,' OP ','-'*5)
        # opp.parse_days()
    
        # print('*'*20)
        # print('  LOAD MATCHES')
        # print('*'*20)
        print('-'*5,' Sofa ','-'*5)
        dp.load_matches()
        # print('-'*5,' Fbref ','-'*5)
        # dp.load_fbref_matches()
        # print('-'*5,' OP ','-'*5)
        # dp.load_op_matches()

        # print('*'*20)
        # print('  PARSE MATCHES')
        # print('*'*20)
        print('-'*5,' Sofa ','-'*5)
        ssp.parse_matches()
        # print('-'*5,' Fbref ','-'*5)
        # fbp.parse_matches()
        # print('-'*5,' OP ','-'*5)
        # opp.parse_matches()
    elif sys.argv[1]=='t':
        ds=de='{:%Y-%m-%d}'.format(datetime.today())
        ssp=SofaScoreParser()
        fbp=FbrefParser()
        opp=OpParser()
        print('*'*20)
        print('  TODAY MATCHES')
        print('*'*20)
        # dp.load_days(ds, de)
        # dp.load_op_days(ds, de)
        # opp.parse_today()
        # dp.load_matches()
        # dp.load_op_matches_today()
        # opp.parse_matches(today=True)
        ssp.parse_matches(today='_today')