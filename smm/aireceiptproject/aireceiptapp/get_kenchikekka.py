import MySQLdb
import datetime, time
import pandas as pd
import os
from sqlalchemy import create_engine
import datetime
import time
from aireceiptapp import get_kenchi, get_hanyo
# import get_kenchi, get_hanyo

# 検知結果テーブルからユニークな請求年月を取得する
def get_unique_seikyunengetsu():
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT DISTINCT SEIKYUNENGETSU FROM no_calculation_results")
	cursor.execute(sql)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

# バックアップテーブルからユニークな請求年月を取得する
def get_unique_backup_seikyunengetsu():
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT DISTINCT SEIKYUNENGETSU FROM aireceiptapp_backup_kenchikekka")
	cursor.execute(sql)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

# 引数に指定した請求年月の更新日のユニーク値を取得
def get_unique_outputdate(seikyunengetsu, table_name):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	request_sql = "SELECT DISTINCT OUTPUTDATE FROM " + table_name + " WHERE SEIKYUNENGETSU = '" + seikyunengetsu + "'"
	sql = (request_sql)
	cursor.execute(sql)
	rows = cursor.fetchall()
	connection.close()
	l = []
	for row in rows:
		l.append(row['OUTPUTDATE'])
	result = None
	if l:
		result = max(l)
		result = result.strftime('%Y-%m-%d %H:%M:%S')
	return(result)

# サマリー画面
def get_chart(request_sql):
	start = time.time()
	# データベースへの接続とカーソルの生成
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
	objects = cursor.fetchall()
	endtime = time.time() - start
	return(objects)


class Obj:
	def __init__(self):
		self.CODENAME = ""
		self.COUNT = 0
		self.POINTS = 0
		self.RANC = None
		self.DETAIL = ""

def chart_obj(chart_category, table_name, max_seikyunengetsu, max_outputdate):
	results = []
	con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
	request_sql = "SELECT " + chart_category + ", COUNT(*) AS CO, SUM(POINTS) AS TOTAL FROM "
	request_sql += table_name + " WHERE (ZISSEKI = 0)"
	request_sql += " AND (SEIKYUNENGETSU = '" + max_seikyunengetsu + "')"
	if max_outputdate:
		request_sql += " AND (OUTPUTDATE LIKE '" + max_outputdate + "%%')"
	if chart_category == 'WARD':
		request_sql += "AND (IN_OUT = '入院')"
	# マスタで設定した確率
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
	if add_proba_sql:
		request_sql += add_proba_sql
	else:
		request_sql += " AND PREDICT_PROBA >= 50"
	request_sql += " GROUP BY " + chart_category
	request_sql += " ORDER BY CO DESC"
	obj = get_chart(request_sql)
	if chart_category == 'CODENAME':
		count_field = 'DEPT'
		t, check_list = get_hanyo.get_dept()
	elif chart_category == 'WARD' or chart_category == 'DEPT':
		count_field = 'CODENAME'
		t, check_list = get_kenchi.get_kenchi()
	else:
		count_field = 'DEPT'
		t, check_list = get_hanyo.get_dept()
	for i in obj:
		request_sql = "SELECT " + count_field
		request_sql += " FROM aireceiptapp_kenchikekka WHERE (ZISSEKI = 0)"
		request_sql += " AND (" + chart_category +" = '" + i[0] + "')"
		if add_proba_sql:
			request_sql += add_proba_sql
		else:
			request_sql += " AND PREDICT_PROBA >= 50"
		df = pd.read_sql_query(sql=request_sql, con=con)
		count = df[count_field].value_counts()
		num = 0
		total_num = 0
		ranc_dict = {}
		detail = ""
		for index, v in count.items():
			if index in check_list:
				ranc_dict[index] = v
				detail += index + '（' + str(v) + '）、'
				total_num += v
				num += 1
				if num > 2:
					break
		sonota = int(i[1]) - total_num
		if sonota:
			ranc_dict['その他'] = sonota
			detail += 'その他（' + str(sonota) + '）、'
		result = Obj()
		result.CODENAME = i[0]
		result.COUNT = i[1]
		result.POINTS = i[2]
		result.RANC = ranc_dict
		result.DETAIL = detail[:-1]
		results.append( result )
	return(results)

def get_all_seikyunengetsu_list():
	seikyunengetsu_list = []

	# 検知結果テーブルとバックアップ検知結果テーブルにあるユニークな請求年月を取得する
	for i in get_unique_seikyunengetsu():
		seikyunengetsu_list.append(i['SEIKYUNENGETSU'])
	for i in get_unique_backup_seikyunengetsu():
		if not i['SEIKYUNENGETSU'] in seikyunengetsu_list:
			seikyunengetsu_list.append(i['SEIKYUNENGETSU'])
	return(seikyunengetsu_list)

def line_chart_obj(seikyunengetsu_list):
	count_list = []
	table_name = 'aireceiptapp_kenchikekka'

	for i in seikyunengetsu_list:
		# 最新の更新日時
		request_sql = "SELECT MAX(OUTPUTDATE) AS MO FROM aireceiptapp_kenchikekka WHERE SEIKYUNENGETSU = '" + i + "'"
		obj = get_chart(request_sql)
		max_outputdate = None
		for o in obj:
			max_outputdate = o[0]
		if not max_outputdate:
			table_name = 'aireceiptapp_backup_kenchikekka'
			request_sql = "SELECT MAX(OUTPUTDATE) AS MO FROM aireceiptapp_backup_kenchikekka WHERE SEIKYUNENGETSU = '" + i + "'"
			obj = get_chart(request_sql)
			for o in obj:
				max_outputdate = o[0]
		str_max_outputdate = max_outputdate.strftime('%Y-%m-%d %H:%M:%S')
		request_sql = "SELECT COUNT(*) FROM " + table_name + " WHERE SEIKYUNENGETSU = '" + i + "' AND OUTPUTDATE LIKE '" + str_max_outputdate + "%%'"
		request_sql += " AND ZISSEKI = 0"
		# マスタで設定した確率
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
		if add_proba_sql:
			request_sql += add_proba_sql
		else:
			request_sql += " AND PREDICT_PROBA >= 50"
		obj = get_chart(request_sql)
		count_ = None
		if obj:
			for o in obj:
				count_ = o[0]
		count_list.append(count_)
	print(f'count_list = {count_list}')
	return(count_list)






# # 単体テスト
# if __name__ == '__main__':
# 	seikyunengetsu_list, count_list = line_chart_obj()
# 	print(f'seikyunengetsu_list = {seikyunengetsu_list}')
# 	print(f'count_list = {count_list}')