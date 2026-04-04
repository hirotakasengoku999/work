import MySQLdb
import os
import datetime

# 有効な算定項目リストを取得する
def get_kenchi(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT NAME FROM aireceiptapp_kenchi WHERE USE_FLAG = '1' AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY CODE, CODE2")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	results_tupple = []
	results_list = []
	for i in rows:
		results_tupple.append((i['NAME'],i['NAME']))
		results_list.append(i['NAME'])
	return(results_tupple, results_list)

# 有効な算定項目リストを取得する
def get_select_kenchi(request_sql):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = (request_sql)
	cursor.execute(sql)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

def test_ward():
	results = (
		('全て','全て'),
		('ICU','ICU'),
		('SCU','SCU'),
		('HCU','HCU'),
		('３Ａ病棟','３Ａ病棟'),
		('３Ｂ病棟','３Ｂ病棟'),
		('４Ａ病棟','４Ａ病棟'),
		('４Ｂ病棟','４Ｂ病棟'),
		('５Ａ病棟','５Ａ病棟'),
		('５Ｂ病棟','５Ｂ病棟'),
		)
	return(results)

# 有効な算定項目辞書を取得する
def get_kenchi_dict(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT NAME FROM aireceiptapp_kenchi WHERE USE_FLAG = '1' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	results = {}
	results[''] = '全て'
	if rows:
		for i in rows:
			results[i['NAME']] = i['NAME']
	return(results)

# 結果紹介で表示する確率を取得
def get_kenchi_predict_proba(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT CODE, CODE2, PREDICT_PROBA FROM aireceiptapp_kenchi WHERE USE_FLAG = '1' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	results = {}
	if rows:
		for row in rows:
			if row['CODE2']:
				results[row['CODE2']] = row['PREDICT_PROBA']
			else:
				results[row['CODE']] = row['PREDICT_PROBA']
	return(results)

# 有効な算定項目コード辞書を取得する
def get_itemcode_dict(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT CODE, CODE2 FROM aireceiptapp_kenchi WHERE USE_FLAG = '1' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	results = {}
	results[''] = '全て'
	if rows:
		for i in rows:
			if i['CODE2']:
				results[i['CODE2']] = i['CODE2']
			else:
				results[i['CODE']] = i['CODE']
	return(results)

# 有効な算定項目コード辞書を取得する
def get_code_name_dict(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT CODE, CODE2, NAME FROM aireceiptapp_kenchi WHERE USE_FLAG = '1' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	results = {}
	if rows:
		for i in rows:
			if i['CODE2']:
				results[i['CODE2']] = i['NAME']
			else:
				results[i['CODE']] = i['NAME']
	return(results)


# if __name__ == '__main__':
# 	results = get_code_name_dict()
# 	for k, v in results.items():
# 		print('"{}"  "{}"'.format(k,v))

