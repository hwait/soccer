import sys
import json
from os import path,listdir
from shutil import move
import pandas as pd
import numpy as np
from datetime import datetime
from data_provider import DataProvider
from sofa_parser import SofaScoreParser
from fbref_parser import FbrefParser
from tqdm import tqdm

dp=DataProvider()

if __name__ == '__main__':
    if len(sys.argv) == 3:
        ds=sys.argv[1]
        de=sys.argv[2]
    elif len(sys.argv) == 4:
        ds=sys.argv[2]
        de=sys.argv[3]
    elif len(sys.argv) == 2:
        ds=de=sys.argv[1]

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
        ssp=SofaScoreParser()
        ssp.parse_matches()
    elif sys.argv[1]=='om':
        dp.load_op_matches()
        