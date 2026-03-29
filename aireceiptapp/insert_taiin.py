# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
import MySQLdb
from sqlalchemy import create_engine
import datetime
import sys
from pathlib import Path

# DB情報
db_passwd = 'administrator'
db_name = 'aireceiptdb'

def conectdb(request_sql):
	engine = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
	connection = engine.connect()
	result = connection.execute(request_sql)
	df = pd.DataFrame(result.fetchall(), columns=result.keys())
	connection.close()
	return (df)

def write_result(input_path, seikyunengetsu):
	# result.csvファイルリストを作る
	files = Path(input_path).glob('**/*.csv')

	import get_kenchi
	code_name_dict = get_kenchi.get_code_name_dict()

	# csvファイルの結合
	list_file = []
	for file in files:
		# ファイル名からレセ電コードを取得
		item_code = file.name[:-4]
		# 190056910 退院時リハビリテーション指導料なら該当病名の有無データに書き換える
		if item_code == '190056910':
			usecols = ['カルテ番号等','氏名','_date','入外','診療科名','算定項目名称','算定漏れ確率','算定実績','算定漏れ根拠_1','算定漏れ根拠_寄与度_1','算定漏れ根拠_2','算定漏れ根拠_寄与度_2','算定漏れ根拠_3','算定漏れ根拠_寄与度_3','算定漏れ根拠_4','算定漏れ根拠_寄与度_4','算定漏れ根拠_5','算定漏れ根拠_寄与度_5','緊急入院','緊急入院（2以外の場合）','救急医療管理加算2','JCS値','トリアージレベル_フラグ','入院中算定回数','karute_all_弾性ストッキング','karute_all_フットポンプ','判定(ｈｂｓａｇ)+','判定(ｈｃｖ)+','カルテ_腹臥位','karute_all_退院前カンファレンス']
			d = pd.read_csv(file, engine='python', usecols=usecols, encoding='cp932', dtype='object')
			list_file.append(d)
	# pandasのdataframeに変換
	if len(list_file) > 0:
		df_all = pd.concat(list_file)
		# 検知結果の日付が退院日でないレコードの確率を0%にする
		request_sql = "SELECT PATIENT_ID, DATE_FORMAT(DATE(MOVE_DATETIME), '%%Y/%%m/%%d') AS MOVE_DATE, MOVE_CATEGORY FROM aireceiptapp_movement_info WHERE MOVE_CATEGORY = '退院確定'"
		move_df = conectdb(request_sql)
		move_df = move_df.rename(columns={'PATIENT_ID':'カルテ番号等', 'MOVE_DATE':'_date'})
		move_df = move_df.drop_duplicates()
		move_df.to_csv('移動情報.csv', index=False, encoding='cp932')
		merged_data = pd.merge(df_all, move_df, on=['カルテ番号等', '_date'], how='left')
		merged_data.loc[merged_data['MOVE_CATEGORY'] != '退院確定', '算定漏れ確率'] = '0.0'
		merged_data = merged_data.drop(['MOVE_CATEGORY'], axis=1)
		rename_dict = {
			'カルテ番号等':'PATIENT_ID',
			'氏名':'PATIENT_NAMEJ',
			'_date':'MEDICAL_CARE_DATE',
			'入外':'IN_OUT',
			'診療科名':'DEPT',
			'算定項目名称':'CODENAME',
			'算定漏れ確率':'PREDICT_PROBA',
			'算定実績':'ZISSEKI',
			'算定漏れ根拠_1':'KENCHI01',
			'算定漏れ根拠_寄与度_1':'KENCHI02',
			'算定漏れ根拠_2':'KENCHI03',
			'算定漏れ根拠_寄与度_2':'KENCHI04',
			'算定漏れ根拠_3':'KENCHI05',
			'算定漏れ根拠_寄与度_3':'KENCHI06',
			'算定漏れ根拠_4':'KENCHI07',
			'算定漏れ根拠_寄与度_4':'KENCHI08',
			'算定漏れ根拠_5':'KENCHI09',
			'算定漏れ根拠_寄与度_5':'KENCHI10',
			'緊急入院':'KENCHI11',
			'緊急入院（2以外の場合）':'KENCHI12',
			'救急医療管理加算2':'KENCHI13',
			'JCS値':'KENCHI14',
			'トリアージレベル_フラグ':'KENCHI15',
			'入院中算定回数':'KENCHI16',
			'karute_all_弾性ストッキング':'KENCHI17',
			'karute_all_フットポンプ':'KENCHI18',
			'判定(ｈｂｓａｇ)+':'KENCHI19',
			'判定(ｈｃｖ)+':'KENCHI20',
			'カルテ_腹臥位':'KENCHI21',
			'karute_all_退院前カンファレンス':'KENCHI22'
		}
		df_all = df_all.rename(columns=rename_dict)
		for i in range(23, 51):
			df_all[f"KENCHI{str(i).zfill(2)}"] = ""
		import get_hanyo
		patient_id_length_ = get_hanyo.get_hanyo("SELECT CONTROLINT1 FROM aireceiptapp_hanyo WHERE CODE1 = 'hos_info' AND CODE2 = 'patient_id_length'")
		patient_id_length = patient_id_length_[0]['CONTROLINT1'] if len(patient_id_length_) > 0 else 8
		df_all['PATIENT_ID'] = df_all['PATIENT_ID'].astype('str').str.zfill(patient_id_length)
		df_all['SEIKYUNENGETSU'] = seikyunengetsu
		df_no_calculation_results = merged_data[df_all['ZISSEKI'] == 0.0]
		result = (get_hanyo.get_hanyo("SELECT DISTINCT OUTPUTDATE FROM no_calculation_results"))
		outputdate = result[0]['OUTPUTDATE'] if len(result) > 0 else datetime.datetime.now()
		df_no_calculation_results['OUTPUTDATE'] = outputdate
		df_calculation_exists_results = df_all[df_all['ZISSEKI'] == 1.0]
		result = (get_hanyo.get_hanyo("SELECT DISTINCT OUTPUTDATE FROM calculation_exists_results"))
		outputdate = result[0]['OUTPUTDATE'] if len(result) > 0 else datetime.datetime.now()
		df_calculation_exists_results['OUTPUTDATE'] = outputdate

		con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
		df_no_calculation_results.to_sql('no_calculation_results', con=con, if_exists='append', index=None)
		df_calculation_exists_results.to_sql('calculation_exists_results', con=con, if_exists='append', index=None)

if __name__ == '__main__':
	args = sys.argv
	input_path = args[1]
	seikyunengetsu = args[2]
	output_datetime = ''
	write_result(input_path, seikyunengetsu)

