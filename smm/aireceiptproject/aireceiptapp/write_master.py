import os, datetime
import pandas as pd
from sqlalchemy import create_engine
l = ['s.csv', 't.csv', 'y.csv']
for filename in l:
	df = pd.read_csv(filename,  engine='python', encoding='cp932', names=['item_code', 'item_name', 'points'], usecols=[2,4,11])
	df.insert(3,'start_date', datetime.date(2020, 4, 1))
	df.insert(4,'end_date', datetime.date(2022, 3, 31))
	con = create_engine('mysql://root:administrator@127.0.0.1/receiptdb?charset=utf8')
	df.to_sql('item_master', con=con, if_exists='append', index=None)
