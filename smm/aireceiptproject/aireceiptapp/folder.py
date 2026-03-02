# 参考にしたサイト　https://www.sejuku.net/blog/82657
"""
hanyoテーブルからフォルダーの値を取得し、定義します。
"""
import MySQLdb
import os
import datetime

# ベースフォルダー
def base(criteria_date=datetime.date.today()):
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='base' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	base = ""
	if rows:
		for row in rows:
			base = row['CONTROLTEXT1']
		base += '/'
	return(base)

def data_dir():
	result = base() + "data/"
	return(result)

def receipt_pre(proc):
	if proc == 'model':
		proc = 'createmodel'
	receipt_pre = base() + "data/" + proc + "_in/receipt_pre/"
	return(receipt_pre)

def karte_pre(proc):
	if proc == 'model':
		proc = 'createmodel'
	karte_pre = base() + "data/" + proc + "_in/karte_pre/"
	return(karte_pre)

def receipt(proc):
	if proc == 'model':
		proc = 'createmodel'
	receipt = base() + "data/" + proc + "_in/receipt/"
	return(receipt)

def karte(proc):
	if proc == 'model':
		proc = 'createmodel'
	karte = base() + "data/" + proc + "_in/karte/"
	return(karte)

def model():
	model = base() + "data/model/"
	return(model)

def result():
	result = base() + "data/result/"
	return(result)

# カルテ前処理前のファイル名をDBから取得する
def karte_pre_file(criteria_date=datetime.date.today()):
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='file_chk' AND CODE2 REGEXP'kartepre' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	file_list = []
	if rows:
		for row in rows:
			file_list.append(row['CONTROLTEXT1'])
	return(file_list)

# カルテ前処理後のファイル名をDBから取得する
def karte_file(criteria_date=datetime.date.today()):
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='file_chk' AND CODE2='karte' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date, )
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = ""
	if rows:
		for row in rows:
			result = row['CONTROLTEXT1']
	return(result)

# 入院情報ファイル名をDBから取得する
def movement_file(criteria_date=datetime.date.today()):
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='file_chk' AND CODE2='movement' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date, )
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = ""
	if rows:
		for row in rows:
			result = row['CONTROLTEXT1']
	return(result)

def dept_list():
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	cursor.execute("SELECT DISTINCT `dept` FROM `no_calculation_results`")
	rows = cursor.fetchall()
	connection.close()
	l = []
	for i in rows:
		l.append(i)
	return(l)

# 利用者用学習モデル作成フォルダー
def user_model_folder(criteria_date=datetime.date.today()):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='user_mode' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	connection.close()
	result = ""
	if rows:
		for dir in rows:
			result = dir
		result += '/'
		if not os.path.isdir(result):
			os.makedirs(result)
	return(result)

# 利用者用検知データ作成フォルダー
def user_predict_folder(criteria_date=datetime.date.today()):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='user_pre' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	connection.close()
	result = ""
	if rows:
		for dir in rows:
			result = dir
		result += '/'
		if not os.path.isdir(result):
			os.makedirs(result)
	return(result)

# 利用者用レセプトフォルダー
def user_receipt_folder(proc='predict', criteria_date=datetime.date.today()):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='user_rece' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	connection.close()
	result = ""
	for dir in rows:
		result += dir + "/"
		if proc == 'createmodel':
			result = user_model_folder() + result
		else:
			result = user_predict_folder() + result
		if not os.path.isdir(result):
			os.makedirs(result)
	return(result)

# 利用者用カルテフォルダー
def user_karte_folder(proc='predict', criteria_date=datetime.date.today()):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='user_karte' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	connection.close()
	result = ""
	for dir in rows:
		result += dir + "/"
		if proc == 'createmodel':
			result = user_model_folder() + result
		else:
			result = user_predict_folder() + result
		if not os.path.isdir(result):
			os.makedirs(result)
	return(result)

# ﾊﾞｯｸｱｯﾌﾟﾌｫﾙﾀﾞｰ
def BK_folder(criteria_date=datetime.date.today()):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='BK' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	result = ""
	if rows:
		for row in rows:
			result = row['CONTROLTEXT1']
		result += '/'
	return(result)

def receipt_pre_BK_folder(proc, criteria_date=datetime.date.today()):
	if proc == 'model':
		proc = 'createmodel'
	result = BK_folder() + proc + '_in/receipt_pre'
	return(result)

def receipt_BK_folder(proc, criteria_date=datetime.date.today()):
	if proc == 'model':
		proc = 'createmodel'
	result = BK_folder() + proc + '_in/receipt'
	return(result)

# 変換前レセプトﾊﾞｯｸｱｯﾌﾟﾌｫﾙﾀﾞｰ
def receipt_original_bk_folder(proc, criteria_date=datetime.date.today()):
	if proc == 'model':
		proc = 'createmodel'
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='receipt_original' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	connection.close()
	result = ''
	if rows:
		for row in rows:
			result += row + "/"
	result = BK_folder() + proc + '_in/' + result
	if not os.path.isdir(result):
		os.makedirs(result)
	return(result)

# カルテﾊﾞｯｸｱｯﾌﾟﾌｫﾙﾀﾞｰ
def karte_pre_BK_folder(proc, criteria_date=datetime.date.today()):
	if proc == 'model':
		proc = 'createmodel'
	result = BK_folder() + proc + '_in/karte_pre'
	return(result)

# カルテﾊﾞｯｸｱｯﾌﾟﾌｫﾙﾀﾞｰ
def karte_BK_folder(proc, criteria_date=datetime.date.today()):
	if proc == 'model':
		proc = 'createmodel'
	result = BK_folder() + proc + '_in/karte'
	return(result)

# 変換前レセプトﾊﾞｯｸｱｯﾌﾟﾌｫﾙﾀﾞｰ
def karte_original_bk_folder(proc, criteria_date=datetime.date.today()):
	if proc == 'model':
		proc = 'createmodel'
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='karte_original' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	connection.close()
	result = ''
	if rows:
		for row in rows:
			result += row + "/"
	result = BK_folder() + proc + '_in/' + result
	if not os.path.isdir(result):
		os.makedirs(result)
	return(result)


# モデルﾊﾞｯｸｱｯﾌﾟﾌｫﾙﾀﾞｰ
def model_BK_folder():
	result = os.path.join(BK_folder(),'model')
	return(result)

# 検知ﾊﾞｯｸｱｯﾌﾟﾌｫﾙﾀﾞｰ
def result_BK_folder():
	result = os.path.join(BK_folder(),'result')
	return(result)

# ipアドレス取得
def get_ip_address(criteria_date=datetime.date.today()):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='ip_address' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	connection.close()
	result = ''
	for dir in rows:
		result = dir
	result = "\\\\" + result + "\\c$"
	return(result)

# 利用者用フォルダー（ディスプレイ表示用）
def display_user_folder(proc='predict', target_data='receipt', criteria_date=datetime.date.today()):
	# データベースへの接続とカーソルの生成
	if target_data == "receipt":
		result = get_ip_address() +'\\' + user_receipt_folder(proc)[3:]
	else:
		result = get_ip_address() + '\\' + user_karte_folder(proc)[3:]
	result = result.replace('/','\\')
	return(result)

# 利用者用カルテバックアップフォルダー
def admin_karte_backup(criteria_date=datetime.date.today()):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='admin_backup' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	result = ""
	if rows:
		for dir in rows:
			result = dir
		result += '/'
		if not os.path.isdir(result):
			os.makedirs(result)
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='user_karte' AND STARTDATE <= %s AND ENDDATE >= %s")
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	if rows:
		for dir in rows:
			add_dir = dir
		result += dir + '/'
		if not os.path.isdir(result):
			os.makedirs(result)

	return(result)

# 利用者用レセプトファイルバックアップフォルダー
def admin_rece_backup(criteria_date=datetime.date.today()):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='admin_backup' AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (criteria_date, criteria_date)
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	result = ""
	if rows:
		for dir in rows:
			result = dir
		result += '/'
		if not os.path.isdir(result):
			os.makedirs(result)
	sql = ("SELECT CONTROLTEXT1 FROM aireceiptapp_hanyo WHERE CODE1='fol_chk' AND CODE2='user_rece' AND STARTDATE <= %s AND ENDDATE >= %s")
	cursor.execute(sql, params)
	rows = cursor.fetchone()
	if rows:
		for dir in rows:
			add_dir = dir
		result += dir + '/'
		if not os.path.isdir(result):
			os.makedirs(result)

	return(result)

def conf_dir():
	return(base() + 'config/')

# 単体テスト
if __name__ == '__main__':
	print(admin_karte_backup())
	print(admin_rece_backup())
