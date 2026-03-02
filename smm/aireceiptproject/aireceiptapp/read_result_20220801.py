import pandas as pd
import os, datetime
from pathlib import Path
import MySQLdb
from sqlalchemy import create_engine
import get_kenchi

def read_result():
	con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
	# 検知結果のユニーク請求年月リスト
	active_result_df = pd.read_sql(sql="SELECT DISTINCT SEIKYUNENGETSU FROM aireceiptapp_kenchikekka", con=con)
	active_result_dict = {}
	for i, v in active_result_df.iterrows():
		request_sql = f"SELECT DISTINCT OUTPUTDATE FROM aireceiptapp_kenchikekka WHERE SEIKYUNENGETSU = '{v['SEIKYUNENGETSU']}'"
		unique_updatetime_df = pd.read_sql(sql=request_sql, con=con)
		target_outputdate = max(unique_updatetime_df['OUTPUTDATE'])
		active_result_dict[v['SEIKYUNENGETSU']] = target_outputdate
	# バックアップ検知結果の請求年月リスト
	backup_result_df = pd.read_sql(sql="SELECT DISTINCT SEIKYUNENGETSU FROM aireceiptapp_backup_kenchikekka", con=con)
	backup_result_dict = {}
	for i, v in backup_result_df.iterrows():
		request_sql = f"SELECT DISTINCT OUTPUTDATE FROM aireceiptapp_backup_kenchikekka WHERE SEIKYUNENGETSU = '{v['SEIKYUNENGETSU']}'"
		unique_updatetime_df = pd.read_sql(sql=request_sql, con=con)
		target_outputdate = max(unique_updatetime_df['OUTPUTDATE'])
		backup_result_dict[v['SEIKYUNENGETSU']] = target_outputdate
	return(active_result_dict, backup_result_dict)

def count_predict_proba(predict_proba, outdir, active_result_dict, backup_result_dict):
	con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
	# 引数predict_probaに設定した確率以上の件数カウント
	list_df = []
	for k, v in active_result_dict.items():
		request_sql = f"SELECT SEIKYUNENGETSU, COUNT(*) AS C FROM aireceiptapp_kenchikekka WHERE SEIKYUNENGETSU = '{k}' AND OUTPUTDATE = '{v}' AND ZISSEKI = 0 AND PREDICT_PROBA >= {predict_proba}"
		df = pd.read_sql(sql=request_sql, con=con)
		list_df.append(df)

	for k, v in backup_result_dict.items():
		request_sql = f"SELECT SEIKYUNENGETSU, COUNT(*) AS C FROM aireceiptapp_backup_kenchikekka WHERE SEIKYUNENGETSU = '{k}' AND OUTPUTDATE = '{v}' AND ZISSEKI = 0 AND PREDICT_PROBA >= {predict_proba}"
		df = pd.read_sql(sql=request_sql, con=con)
		list_df.append(df)

	all_df = pd.concat(list_df)
	outfile = outdir/f'alldf{predict_proba}.csv'
	all_df.to_csv(outfile, index=False, encoding='cp932')	

def setting_predict_proba(outdir, active_result_dict, backup_result_dict):
	display_predict_proba_dict = get_kenchi.get_kenchi_predict_proba()
	add_proba_sql = ""
	if display_predict_proba_dict:
		add_proba_sql += " AND ("
		for k, v in display_predict_proba_dict.items():
			where_sql = " (CODE = '" + k + "' AND PREDICT_PROBA >= " + str(v) + ") OR"
			add_proba_sql += where_sql
		if add_proba_sql:
			add_proba_sql = add_proba_sql[:-3]
			add_proba_sql += ")"
	con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
	# 引数predict_probaに設定した確率以上の件数カウント
	list_df = []
	for k, v in active_result_dict.items():
		request_sql = f"SELECT SEIKYUNENGETSU, COUNT(*) AS C FROM aireceiptapp_kenchikekka WHERE ZISSEKI = 0 AND SEIKYUNENGETSU = '{k}' AND OUTPUTDATE = '{v}'{add_proba_sql}"
		df = pd.read_sql(sql=request_sql, con=con)
		list_df.append(df)

	for k, v in backup_result_dict.items():
		request_sql = f"SELECT SEIKYUNENGETSU, COUNT(*) AS C FROM aireceiptapp_backup_kenchikekka WHERE SEIKYUNENGETSU = '{k}' AND OUTPUTDATE = '{v}' AND ZISSEKI = 0{add_proba_sql}"
		df = pd.read_sql(sql=request_sql, con=con)
		list_df.append(df)

	all_df = pd.concat(list_df)
	outfile = outdir/f'settingprobadf.csv'
	all_df.to_csv(outfile, index=False, encoding='cp932')

def count_predict_proba_solo(predict_proba, outdir, active_result_dict, backup_result_dict, code):
	con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
	# 引数predict_probaに設定した確率以上の件数カウント
	list_df = []
	for k, v in active_result_dict.items():
		request_sql = f"SELECT SEIKYUNENGETSU, COUNT(*) AS C FROM aireceiptapp_kenchikekka WHERE SEIKYUNENGETSU = '{k}' AND OUTPUTDATE = '{v}' AND ZISSEKI = 0 AND PREDICT_PROBA >= {predict_proba}"
		df = pd.read_sql(sql=request_sql, con=con)
		list_df.append(df)

	for k, v in backup_result_dict.items():
		request_sql = f"SELECT SEIKYUNENGETSU, COUNT(*) AS C FROM aireceiptapp_backup_kenchikekka WHERE SEIKYUNENGETSU = '{k}' AND OUTPUTDATE = '{v}' AND ZISSEKI = 0 AND PREDICT_PROBA >= {predict_proba}"
		df = pd.read_sql(sql=request_sql, con=con)
		list_df.append(df)

	all_df = pd.concat(list_df)
	outfile = outdir/f'{code}-{predict_proba}.csv'
	all_df.to_csv(outfile, index=False, encoding='cp932')	

def setting_predict_proba_solo(outdir, active_result_dict, backup_result_dict, code):
	display_predict_proba_dict = get_kenchi.get_kenchi_predict_proba()
	add_proba_sql = ""
	if display_predict_proba_dict:
		add_proba_sql += " AND ("
		for k, v in display_predict_proba_dict.items():
			where_sql = " (CODE = '" + k + "' AND PREDICT_PROBA >= " + str(v) + ") OR"
			add_proba_sql += where_sql
		if add_proba_sql:
			add_proba_sql = add_proba_sql[:-3]
			add_proba_sql += ")"
	con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
	# 引数predict_probaに設定した確率以上の件数カウント
	list_df = []
	for k, v in active_result_dict.items():
		request_sql = f"SELECT SEIKYUNENGETSU, COUNT(*) AS C FROM aireceiptapp_kenchikekka WHERE CODE = '{code}' AND ZISSEKI = 0 AND SEIKYUNENGETSU = '{k}' AND OUTPUTDATE = '{v}'{add_proba_sql}"
		df = pd.read_sql(sql=request_sql, con=con)
		list_df.append(df)

	for k, v in backup_result_dict.items():
		request_sql = f"SELECT SEIKYUNENGETSU, COUNT(*) AS C FROM aireceiptapp_backup_kenchikekka WHERE CODE = '{code}' AND SEIKYUNENGETSU = '{k}' AND OUTPUTDATE = '{v}' AND ZISSEKI = 0{add_proba_sql}"
		df = pd.read_sql(sql=request_sql, con=con)
		list_df.append(df)

	all_df = pd.concat(list_df)
	outfile = outdir/f'{code}settingproba.csv'
	all_df.to_csv(outfile, index=False, encoding='cp932')

def solo_count(predict_proba, outdir, active_result_dict, backup_result_dict,):
	for k, v in get_kenchi.get_itemcode_dict().items():
		if k:
			count_predict_proba_solo(predict_proba, outdir, active_result_dict, backup_result_dict, k)

def solo_setting(outdir, active_result_dict, backup_result_dict,):
	for k, v in get_kenchi.get_itemcode_dict().items():
		if k:
			setting_predict_proba_solo(outdir, active_result_dict, backup_result_dict, k)


if __name__ == '__main__':
	active_result_dict, backup_result_dict = read_result()
	outdir = Path('C:/Users/Administrator/Desktop/count')
	for predict_proba in [50, 60, 70, 80, 90]:
		count_predict_proba(predict_proba, outdir, active_result_dict, backup_result_dict)
		solo_count(predict_proba, outdir, active_result_dict, backup_result_dict)
	setting_predict_proba(outdir, active_result_dict, backup_result_dict)
	solo_setting(outdir, active_result_dict, backup_result_dict)
