import update_inspection
import pandas as pd
import os, datetime, pathlib
import MySQLdb
from sqlalchemy import create_engine

def write_karte_all():
	import folder
	karte = folder.karte('predict')
	files = os.listdir(karte)
	files = [f for f in files if '.csv' in f]
	file_info_dict = {}
	for file in files:
		file_info_dict[pathlib.Path(os.path.join(karte,file)).stat().st_mtime] = file
		
	target_file = os.path.join(karte, file_info_dict[max(file_info_dict)])
	
	df = pd.read_csv(target_file, engine='python', encoding='cp932', dtype={'カルテ番号等':'object'})
	df = df.rename(columns={'カルテ番号等':'patient_id', '_date':'medical_care_date', '文書種別':'doctype', '体位':'taii', '検査項目':'kensakoumoku', '受付初再診区分':'uketukesaisyosinkubun', '診療科':'dept', 'カルテ内容':'karte_text', '手術開始時間':'surgery_start_time', '診療内容':'care_detail', '介護支援連携指導_文書':'kaigosien','受付診療科':'first_dept'})
	cols = ['patient_id', 'medical_care_date', 'doctype', 'taii', 'kensakoumoku', 'uketukesaisyosinkubun', 'dept', 'karte_text', 'surgery_start_time', 'care_detail', 'kaigosien','first_dept']
	usecols = []
	for c in cols:
		if c in df.columns:
			usecols.append(c)
	df = df[usecols]

	con = create_engine('mysql://root:administrator@127.0.0.1/inspectiondb?charset=utf8')
	df.to_sql('karte_all', con=con, if_exists='append', index=None)

if __name__ == '__main__':
	request_sql = "DELETE FROM karte_all"
	update_inspection.update_inspectiondb(request_sql)
	write_karte_all()