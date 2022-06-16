"""
DB_TABLE_NAMEで指定されるテーブルをBigQueryにLoad
test
"""

import pandas as pd
import sqlalchemy as sa
import time
import datetime as dt
import math
import sys
import numpy as np

import os
from os.path import join, dirname
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import boto3

import logging
formatter = '%(asctime)s : %(levelname)s : %(message)s'
logging.basicConfig(filename='example.log',level=logging.INFO)

logging.critical('CRITICAL MESSAGE')
logging.error('ERROR MESSAGE')
logging.warning('WARNING MESSAGE')
logging.info('ログをログファイルに出力します。')
logging.debug('DEBUG MESSAGE')


dotenv_path = join(dirname(__file__), '1.env')
load_dotenv(dotenv_path)

HOST = os.environ["HOST"]
PORT = os.environ["PORT"]
DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_NAME = os.environ["DB_NAME"]
DB_TABLE_NAME =  os.environ["TABLE_NAME"] #移行元DBの対象テーブル名

BATCH_SIZE = 100000 #DBから一度に取ってくるレコードの行数。あまり大きすぎるとメモリに乗らなくなりそう
BQ_PROJECT_NAME = os.environ["BQ_PROJECT_NAME"]
BQ_DATASET_NAME = os.environ["BQ_DATASET_NAME"]
BQ_TABLE_NAME = os.environ["TABLE_NAME"]
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './client_credentials.json'

url = f'mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{HOST}:{PORT}/{DB_NAME}?charset=utf8'
engine = sa.create_engine(url, echo=False)
process_start_time = time.time()

# pandasでSQL実行+実行結果を取得
TABLE_NAME = "advice_logs"

# pandasでSQL実行+実行結果を取得
query="select * from advice_logs where DATE(created_at) = CURRENT_DATE() - INTERVAL 15 DAY limit 5000;"

df = pd.read_sql(query, url)
print(df.head())
print(df.dtypes)

df['advice_loggable_id'] = df['advice_loggable_id'].astype(str)
print(df.dtypes)


# CSV 保存
now = dt.datetime.now()
time = now.strftime('%Y%m%d')
file_title = f'{time}_{TABLE_NAME}_l'
file_title_1 = f'{TABLE_NAME}'
df.to_parquet('result/{}.parquet'.format(file_title))
df.to_csv('result/{}.csv'.format(file_title))
