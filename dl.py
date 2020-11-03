import sys
from datetime import datetime
from data_provider import DataProvider

dp=DataProvider()

if __name__ == '__main__':
    if len(sys.argv) == 3:
        ds=sys.argv[1]
        de=sys.argv[2]
    elif len(sys.argv) == 2:
        ds=de=sys.argv[1]

    if sys.argv[1]=='d':
        dp.load_days(ds, de)
    elif sys.argv[1]=='df':
        dp.load_days()
    elif sys.argv[1]=='m':
        dp.load_matches()
        