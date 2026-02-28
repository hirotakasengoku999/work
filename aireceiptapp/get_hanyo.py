import MySQLdb
import os
import datetime

# 引数に指定した日付に対して有効なヘッダーレコードを取得する
def get_active_header(user_id='9', criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT id,CODE2, NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'header' AND CODE2 <> 'user' AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY CODE1")

	user_authority = get_user_authority(user_id)
	if user_authority == '2':
		sql = ("SELECT id,CODE2, NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'header' AND AUTHORITY != '9' AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY CODE1")
	
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

# 引数に指定したヘッダーコードの名称を取得する
def get_active_header_name_id(header_code, criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT id,NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'header' AND CODE2=%s AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (header_code, criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result_name = ""
	result_id = ""
	for i in rows:
		result_name = i['NAME']
		result_id = i['id']
	return(result_id, result_name)

def get_active_header_detail(header_code, criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT id,AUTHORITY, NAME FROM aireceiptapp_hanyo WHERE CODE1='header' AND CODE2=%s AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (header_code, criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

def get_active_detail(header_code, user_id='9', criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	user_authority = get_user_authority(user_id)

	sql = ("SELECT * FROM aireceiptapp_hanyo WHERE CODE1 = %s AND STARTDATE <= %s AND ENDDATE >= %s")
	if header_code == 'user' and user_authority == '2':
		sql = ("SELECT * FROM aireceiptapp_hanyo WHERE CODE1 = %s AND AUTHORITY != '9' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (header_code, criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

def get_active_login(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT CODE2, CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1 = 'login' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

def get_msg_number(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT CODE2, CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1 = 'msg_number' AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY CODE2")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

def get_msg_number2(line_number, criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT CODE2, CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1 = 'msg_number' AND CODE2 = %s AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (line_number, criteria_date, criteria_date, )
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = ""
	for i in rows:
		result = i['CODE2']
		if i['CONTROLTEXT1']:
			result += '：　' + i['CONTROLTEXT1']
	return(result)

def get_user_authority(user_id, criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql=("SELECT `AUTHORITY` FROM `aireceiptapp_hanyo` WHERE `CODE1`='user' AND CODE2=%s AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (user_id, criteria_date, criteria_date, )
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = ""
	for i in rows:
		result = str(i['AUTHORITY'])
	return(result)

def get_menu_info(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql=("SELECT CODE2,CONTROLTEXT1,NAME FROM aireceiptapp_hanyo WHERE CODE1='menu' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date, )
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	menu_code = ""
	menu_name = ""
	menu_auth = []
	params = {}
	result = []
	for i in rows:
		menu_code = i['CODE2']		
		menu_name = i['NAME']
		menu_auth = list(str(i['CONTROLTEXT1']))
		params = {
			'menu_code':menu_code,
			'menu_name':menu_name,
			'menu_auth':menu_auth,
		}
		result.append(params)
	return(result)

def get_authority(user_id='', criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	user_authority = get_user_authority(user_id)
	sql=("SELECT CONTROLINT1,NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'user_category' AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY CONTROLINT1 ASC")
	params = (criteria_date, criteria_date, )
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = []
	w = ""
	if rows:
		result.append(('0',' '))
		for i in rows:
			if user_authority == '9':
				w = str(i['CONTROLINT1']) + ":" + i['NAME']
				result.append((str(i['CONTROLINT1']),w))
			elif i['CONTROLINT1'] != 9:
				w = str(i['CONTROLINT1']) + ":" + i['NAME']
				result.append((str(i['CONTROLINT1']),w))
	return(result)

def get_authority_edit(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql=("SELECT CONTROLINT1,NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'user_category' AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY CONTROLINT1 ASC")
	params = (criteria_date, criteria_date, )
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = []
	w = ""
	if rows:
		for i in rows:
			w = str(i['CONTROLINT1']) + ":" + i['NAME']
			result.append((str(i['CONTROLINT1']),w))
	return(result)

# 診療科リストを取得
def get_dept(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	d_today1 = criteria_date
	d_today2 = criteria_date # SQLで当日日付を２回使用するが、１つの変数を使いまわせなかったため、２つ用意しました
	sql=("SELECT CODE2,NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'dept' AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY CONTROLINT1 ASC")
	params = (d_today1, d_today2, )
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result_tuple = []
	result_list = []
	for i in rows:
		result_tuple.append((i['NAME'],i['NAME']))
		result_list.append(i['NAME'])
	result_tuple.append(('',''))
	result_list.append('')
	return(result_tuple, result_list)

# 診療科リストを取得
def get_ward(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	d_today1 = criteria_date
	d_today2 = criteria_date # SQLで当日日付を２回使用するが、１つの変数を使いまわせなかったため、２つ用意しました
	sql=("SELECT NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'ward' AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY CONTROLINT1 ASC")
	params = (d_today1, d_today2, )
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result_tuple = []
	result_list = []
	for i in rows:
		result_tuple.append((i['NAME'],i['NAME']))
		result_list.append(i['NAME'])
	result_tuple.append(('',''))
	result_list.append('')
	return(result_tuple, result_list)

# 診療科リストを取得
def get_dept_dict(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql=("SELECT NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'dept' AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY CONTROLINT1 ASC")
	params = (criteria_date, criteria_date, )
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	results = {}
	results[''] = '全て'
	if rows:
		for i in rows:
			results[i['NAME']] = i['NAME']
	return(results)


# モデル期間
def get_model_period(criteria_date=datetime.datetime.now()):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	sql = ("SELECT CONTROLINT1 FROM aireceiptapp_hanyo WHERE CODE1='hos_info' AND CODE2='model' AND STARTDATE <= %s AND ENDDATE >= %s")
	parmas = (criteria_date, criteria_date)
	cursor.execute(sql, parmas)
	rows = cursor.fetchone()
	connection.close()
	result = ""
	if rows:
		for i in rows:
			result = i
	return(result)

# 引数に指定した日付に対して有効なヘッダーレコードを取得する
def get_seikyugetu(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT CONTROLTEXT1,CONTROLTEXT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'seikyugetu' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = []
	if rows:
		for i in rows:
			result.append((i['CONTROLTEXT1'],i['CONTROLTEXT2']))
	return(result)

# 引数に指定した日付に対して有効なヘッダーレコードを取得する
def update_proc_flag(proc, update_flag, criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	flag_text = ''
	if update_flag == 0:
		flag_text = 'stop'
	elif update_flag == 1:
		flag_text = 'run'
	elif update_flag == 2:
		flag_text = 'wait'

	sql = ("UPDATE aireceiptapp_hanyo SET CONTROLTEXT1 = %s, CONTROLINT1 = %s WHERE CODE1 = 'proc' AND CODE2 = %s AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (flag_text, update_flag, proc, criteria_date, criteria_date,)
	cursor.execute(sql, params)
	connection.commit()
	connection.close()

def get_proc_name(proc, criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	if proc == 'createmodel':
		proc = 'model'
	sql = ("SELECT NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'proc' AND CODE2 = %s AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (proc, criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = ''
	if rows:
		for i in rows:
			result = i['NAME']
	return(result)

def get_receipt_file_list(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1 = 'receipt_file_chk' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = []
	if rows:
		for i in rows:
			result.append(i["CONTROLTEXT1"])
	return(result)

def get_user_list(request_sql):
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

def get_message(criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1 = 'message' AND CODE2 = 'result_backup' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = ""
	if rows:
		for i in rows:
			result = i["CONTROLTEXT1"]
	return(result)

def delete_message():
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = ("UPDATE aireceiptapp_hanyo SET CONTROLTEXT1 = '' WHERE CODE1 = 'message' AND CODE2 = 'result_backup'")
	cursor.execute(sql)
	connection.commit()
	connection.close()

def get_hanyo(request_sql):
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

# データの更新をします
def update_hanyo(request_sql):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	cursor.execute(request_sql)
	connection.commit()
	connection.close()

# データの更新をします
def update_aireceiptdb(request_sql):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	cursor.execute(request_sql)
	connection.commit()
	connection.close()

# 単体テスト
# if __name__ == '__main__':
	# request_sql = "UPDATE aireceiptapp_hanyo SET CONTROLINT1 = 1 WHERE CODE1 = 'hos_info' AND CODE2 = 'chart_update_flag'"
	# update_hanyo(request_sql)
	# print(f'get_active_header = {get_active_header()}')# done
	# print(f'get_active_header_name_id = {get_active_header_name_id("proc")}')# done
	# print(f'get_active_header_detail = {get_active_header_detail("menu")}') # done
	# print(f'get_active_detail = {get_active_detail("proc")}')# done
	# print(f'get_active_login = {get_active_login()}')# done
	# print(f'get_msg_number = {get_msg_number()}')# done
	# print(f'get_msg_number2 = {get_msg_number2()}')# done
	# print(f'get_user_authority = {get_user_authority("testuser")}')# done
	# print(f'get_menu_info = {get_menu_info()}')# done
	# print(f'get_authority = {get_authority()}')# done
	# print(f'get_authority_edit = {get_authority_edit()}')# done
	# print(f'get_dept = {get_dept()}')# done
	# print(f'get_ward = {get_ward()}')# done
	# print(f'get_dept_dict = {get_dept_dict()}')# done
	# print(f'get_model_period = {get_model_period()}')# done
	# print(f'get_seikyugetu = {get_seikyugetu()}')# done
	# print(f'update_proc_flag = {update_proc_flag("predict", 0)}') #done
	# print(f'get_proc_name = {get_proc_name("predict")}') #done
	# print(f'get_receipt_file_list = {get_receipt_file_list()}')# done
	# print(f'get_user_list = {get_user_list("SELECT * FROM aireceiptapp_hanyo")}')# done
	# print(f'get_message = {get_message()}') # done
	# print(f'delete_message = {delete_message()}') # done

# # 単体テスト メニュー権限
# if __name__ == '__main__':
# 	result = get_menu_info()
# 	for i in result:
# 		print(i)
# 	print('')
# 	print(result[0]['menu_code'])

# # 単体テスト　詳細取り出し
# if __name__ == '__main__':
# 	result = get_active_detail('proc')
# 	print(result)