import MySQLdb
import os
import datetime
import pandas as pd
from sqlalchemy import create_engine
import os
from dateutil.relativedelta import relativedelta
import shutil
import get_hanyo
from pathlib import Path

def get_unique_month():
	connection = MySQLdb.connect(
		host='127.0.0.1',
		user='root',
		passwd='administrator',
		db='aireceiptdb',
		charset='utf8'
	)
	cursor = connection.cursor()
	request_sql = "SELECT DISTINCT SUBSTRING(MOVE_DATETIME, 1, 7) FROM aireceiptapp_movement_info"
	sql = (request_sql)
	cursor.execute(sql)
	rows = cursor.fetchall()
	connection.close()
	return [i[0] for i in rows]

def delete_old_rows(request_sql):
	connection=MySQLdb.connect(
		host='127.0.0.1',
		user='root',
		passwd='administrator',
		db='aireceiptdb',
		charset='utf8'
		)
	cursor = connection.cursor()
	sql = (request_sql)
	cursor.execute(sql)
	connection.commit()
	connection.close()

def write_movement(input_path):
	con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
	# 過去半年分より昔のレコード削除
	while True:
		result = get_unique_month()
		if len(result) < 7:
			break
		else:
			request_sql = f"DELETE FROM aireceiptapp_movement_info WHERE MOVE_DATETIME LIKE '%{min(result)}%'"
			delete_old_rows(request_sql)
	input_path = Path(input_path)
	in_out_file_t = get_hanyo.get_hanyo("select file_name from aireceiptapp_karte_file where file_code like 'mv%%' limit 1")
	in_out_col_t = get_hanyo.get_hanyo("SELECT COLUMN_NAME, BEFORE_COLUMN_NAME, DATA_TYPE FROM aireceiptapp_karte_file_column WHERE FILE_CODE LIKE 'mv%'")
	rename_col_d = {item['BEFORE_COLUMN_NAME']: item['COLUMN_NAME'] for item in in_out_col_t}
	usecols = [item['BEFORE_COLUMN_NAME'] for item in in_out_col_t]
	in_out_file = in_out_file_t[0]['file_name']
	import aireceipt_karte_pp
	list_df = []
	for file in input_path.glob('**/*.csv'):
		if in_out_file in file.name:
			df = aireceipt_karte_pp.read_creaned_data(file)
			in_df = df[usecols]
			in_df = in_df.rename(columns=rename_col_d)
			list_df.append(in_df)
	all_df = pd.concat(list_df)
	all_df['MOVE_DATETIME'] = pd.to_datetime(all_df['MOVE_DATETIME']).dt.strftime('%Y/%m/%d')
	all_df = all_df.sort_values(by="MOVE_DATETIME")
	all_df = all_df.dropna(subset=['MOVE_DATETIME'])
	all_df = all_df[all_df['MOVE_DATETIME']!=""]
	new_datas = []
	for i, v in all_df.iterrows():
		patient_id = v['PATIENT_ID']
		move_datetime = v['MOVE_DATETIME']
		move_category = v['MOVE_CATEGORY']
		ward = v['WARD']
		dept = v['DEPT']
		row_df = pd.read_sql(sql=f"select * from aireceiptapp_movement_info where patient_id = '{patient_id}' and move_datetime like '{move_datetime}%%' and move_category = '{move_category}'", con=con)
		if len(row_df) > 0:
			with con.connect() as connection:
				connection.execute(f"update aireceiptapp_movement_info set ward = '{ward}', dept = '{dept}' where patient_id = '{patient_id}' and move_datetime like '{move_datetime}%%' and move_category = '{move_category}'")
		else:
			new_datas.append(list(v))
	append_df = pd.DataFrame(new_datas, columns=list(all_df.columns))
	append_df.to_sql('aireceiptapp_movement_info', con=con, if_exists='append', index=None)

class Ctime:
  def __init__(self):
    self.CTIME = ""
    self.TARGET_PATH = ""


def get_move_path(input_path):
	try:
		folders = os.listdir(input_path)
		folders = [f for f in folders if os.path.isdir(os.path.join(input_path, f))]
		results = []
		for folder in folders:
			result = Ctime()
			result.CTIME = os.path.getctime(os.path.join(input_path, folder))
			result.TARGET_PATH = os.path.join(input_path, folder)
			results.append(result)

		ctime_list = []

		for r in results:
			ctime_list.append(r.CTIME)

		new_folder = max(ctime_list)

		for r in results:
			if new_folder == r.CTIME:
				move_path = r.TARGET_PATH
		return(move_path)
	except:
		return(input_path)


def move_csv(input_path, output_path):
	# 移動情報のファイル名称を取得
	in_out_file_t = get_hanyo.get_hanyo("select file_name from aireceiptapp_karte_file where file_code like 'mv%%' limit 1")
	in_out_file = in_out_file_t[0]['file_name']
	files = os.listdir(input_path)
	file = [f for f in files if os.path.isfile(os.path.join(input_path, f)) and in_out_file in f]
	target_file = ""
	for f in file:
		target_file = f
	input_path = os.path.join(input_path, target_file)
	output_path = os.path.join(output_path, target_file)
	if os.path.isdir(output_path):
		os.remove(output_path)
	shutil.move(input_path, output_path)

if __name__ == '__main__':
	import folder
	input_path = folder.user_karte_folder('predict')
	write_movement(input_path)
	output_path = folder.admin_karte_backup()
	move_csv(input_path, output_path)
