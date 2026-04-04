from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine

import pandas as pd
import os
import MySQLdb
from sqlalchemy import create_engine
import datetime
import time
import sys

def ent(seikyunengetsu, df, truncate=False):
	import get_hanyo
	# 検知結果テーブルの請求年月を取得
	result_table_month = get_hanyo.get_hanyo("SELECT SEIKYUNENGETSU, COUNT(*) as COUNT FROM no_calculation_results GROUP BY SEIKYUNENGETSU ORDER BY COUNT desc")[0]['SEIKYUNENGETSU']
	if seikyunengetsu == result_table_month:
		request_sql = "SELECT USER_CHECK, PATIENT_ID, MEDICAL_CARE_DATE, DEPT, CODE, KENCHI50 FROM no_calculation_results WHERE USER_CHECK <> '未'"
		con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
		ent_df = pd.read_sql_query(sql=request_sql, con=con)
		# ent_dfの情報を使用してdfを更新
		for _, row in ent_df.iterrows():
			patient_id = row["PATIENT_ID"]
			medical_care_date = row["MEDICAL_CARE_DATE"]
			dept = row["DEPT"]
			code = row["CODE"]
			user_check = row["USER_CHECK"]
			kenchi50 = row["KENCHI50"]

			mask = (
					(df["PATIENT_ID"] == patient_id) &
					(df["MEDICAL_CARE_DATE"] == medical_care_date) &
					(df["DEPT"] == dept) &
					(df["CODE"] == code)
			)

			df.loc[mask, "USER_CHECK"] = user_check
			df.loc[mask, "KENCHI50"] = kenchi50

	return(df)

# DB情報
db_passwd = 'administrator'
db_name = 'aireceiptdb'

def write_result(input_path, seikyunengetsu):
	# result.csvファイルリストを作る
	files = os.listdir(input_path)
	files_file = [f for f in files if os.path.isfile(os.path.join(input_path,f))]

	import get_kenchi
	code_name_dict = get_kenchi.get_code_name_dict()

	# csvファイルの結合
	list_file = []
	for file in files_file:
		item_code = file[:-4]
		# 難病患者等入院診療加算、退院時リハビリテーション指導料、障害者等加算（人工腎臓）、障害者等加算（持続緩徐式血液濾過）以外に対して
		if not item_code in ['190101770', '190056910', '140033770', '140053670', '190193110', '190193210', '190193510', '190193610']:
			d = pd.read_csv(input_path + file, engine='python', skiprows=1, encoding='cp932', header=None, usecols=range(0,30))
			d[5] = code_name_dict[item_code]
			d.insert(5,'',item_code)
			list_file.append(d)

	# pandasのdataframeに変換
	df_all = pd.concat(list_file)
	# 欠損値補完
	df_all.insert(0,'USER_CHECK','未')
	df_all.insert(1,'SEIKYUNENGETSU',seikyunengetsu)
	df_all.insert(4,'PATIENT_NAMEK',' ')
	df_all.insert(8,'WARD',' ')
	df_all.insert(11,'CODE2',' ')
	df_all.insert(12,'CODE2NAME',' ')
	df_all.insert(13,'POINTS',0)
	df_all.insert(38,'KENCHI23','')
	df_all.insert(39,'KENCHI24','')
	df_all.insert(40,'KENCHI25','')
	df_all.insert(41,'KENCHI26','')
	df_all.insert(42,'KENCHI27','')
	df_all.insert(43,'KENCHI28','')
	df_all.insert(44,'KENCHI29','')
	df_all.insert(45,'KENCHI30','')
	df_all.insert(46,'KENCHI31','')
	df_all.insert(47,'KENCHI32','')
	df_all.insert(48,'KENCHI33','')
	df_all.insert(49,'KENCHI34','')
	df_all.insert(50,'KENCHI35','')
	df_all.insert(51,'KENCHI36','')
	df_all.insert(52,'KENCHI37','')
	df_all.insert(53,'KENCHI38','')
	df_all.insert(54,'KENCHI39','')
	df_all.insert(55,'KENCHI40','')
	df_all.insert(56,'KENCHI41','')
	df_all.insert(57,'KENCHI42','')
	df_all.insert(58,'KENCHI43','')
	df_all.insert(59,'KENCHI44','')
	df_all.insert(60,'KENCHI45','')
	df_all.insert(61,'KENCHI46','')
	df_all.insert(62,'KENCHI47','')
	df_all.insert(63,'KENCHI48','')
	df_all.insert(64,'KENCHI49','')
	df_all.insert(65,'KENCHI50','')
	df_all.insert(66,'OUTPUTDATE',datetime.datetime.now())

	df_all.columns = ['USER_CHECK','SEIKYUNENGETSU','PATIENT_ID','PATIENT_NAMEJ','PATIENT_NAMEK','MEDICAL_CARE_DATE', 'IN_OUT', 'DEPT','WARD','CODE','CODENAME','CODE2','CODE2NAME','POINTS','PREDICT_PROBA','ZISSEKI','KENCHI01','KENCHI02','KENCHI03','KENCHI04','KENCHI05','KENCHI06','KENCHI07','KENCHI08','KENCHI09','KENCHI10','KENCHI11','KENCHI12','KENCHI13','KENCHI14','KENCHI15','KENCHI16','KENCHI17','KENCHI18','KENCHI19','KENCHI20','KENCHI21','KENCHI22','KENCHI23','KENCHI24','KENCHI25','KENCHI26','KENCHI27','KENCHI28','KENCHI29','KENCHI30','KENCHI31','KENCHI32','KENCHI33','KENCHI34','KENCHI35','KENCHI36','KENCHI37','KENCHI38','KENCHI39','KENCHI40','KENCHI41','KENCHI42','KENCHI43','KENCHI44','KENCHI45','KENCHI46','KENCHI47','KENCHI48','KENCHI49','KENCHI50','OUTPUTDATE']
	df_all['PATIENT_ID'] = df_all['PATIENT_ID'].astype('str').str.zfill(8)
	work_int = 0
	work_str = ''
	while True:
		work_int += 1
		work_str = str(work_int).zfill(2)
		work_str = 'KENCHI' + work_str
		try:
			if df_all[work_str].dtypes == 'float64':
				df_all[work_str] = df_all[work_str].astype(str)
			if work_int < 11:
				if work_int % 2 != 0:
					df_all = df_all.replace({work_str:{-1.0:''}})
				else:
					df_all = df_all.replace({work_str:{-1:''}})
					df_all = df_all.replace({work_str:{-1.0:''}})
					df_all = df_all.replace({work_str:{'-1.0':''}})
			else:
				df_all = df_all.replace({work_str:{-1:''}})
				df_all = df_all.replace({work_str:{-1.0:''}})
				df_all = df_all.replace({work_str:{'-1.0':''}})
		except:
			pass

		if work_int > 21:
			break
	return(df_all)


if __name__ == '__main__':
    df = write_result('C:/aibrain_main/aireceiptproject/data/result/', '2021-03')
    ent('2021-03', df)