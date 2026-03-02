'''
aireceiptapp_hanyo
CODE1:proc
の
CONTROLTEXT1
CONTROLINT1
を更新する
参考にしたサイト　https://www.sejuku.net/blog/82657
'''
import MySQLdb
import os
import datetime

def proc_status_update(proc, status):
	result_messe = ""
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()

	# 制御テキストの状態を確認
	sql = ("SELECT CONTROLINT1 FROM aireceiptapp_hanyo WHERE CODE1='proc'")
	cursor.execute(sql)
	rows = cursor.fetchall()
	status_flag = 0
	for row in rows:
		if row[0] == 1:
			status_flag = row[0]

	if status == 0:
		sql = ("UPDATE aireceiptapp_hanyo SET CONTROLTEXT1='stop', CONTROLINT1=0 WHERE CODE1='proc' AND (CODE2='receipt' OR CODE2='karte' OR CODE2=%s)")
		params = (proc,)
		cursor.execute(sql,params)
		connection.commit()
		connection.close()
		result_messe = 'statusをstopに更新しました'

	elif status_flag == 1:
		result_messe = "処理中です。"
	else:
		sql = ("UPDATE aireceiptapp_hanyo SET CONTROLTEXT1='run', CONTROLINT1=1 WHERE CODE1='proc' AND (CODE2='receipt' OR CODE2='karte' OR CODE2=%s)")
		params = (proc,)
		cursor.execute(sql,params)
		connection.commit()
		connection.close()	
		result_messe = "statusをrunに更新しました"
	return(result_messe)

# 処理ステータス確認
def proc_status_now():
	result_messe = ""
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("SELECT NAME, CONTROLINT1 from aireceiptapp_hanyo WHERE CODE1='proc' AND (CODE2='model' OR CODE2='predict')")
	cursor.execute(sql)
	rows = cursor.fetchall()
	active_proc_list = []
	wait_proc_list = []
	for row in rows:
		if row['CONTROLINT1'] == 1:
			active_proc_list.append(row['NAME'])
		if row['CONTROLINT1'] == 2:
			wait_proc_list.append(row['NAME'])
	return(active_proc_list, wait_proc_list)


# 処理名を取得する
def get_proc_name(proc, criteria_date=datetime.date.today()):
	if proc == 'createmodel':
		proc = 'model'
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = ("SELECT NAME, CONTROLINT1 from aireceiptapp_hanyo WHERE CODE1='proc' AND CODE2=%s AND STARTDATE <= %s AND ENDDATE >= %s")
	params = (proc, criteria_date, criteria_date,)
	cursor.execute(sql,params)
	rows = cursor.fetchall()
	proc_name = ""
	for row in rows:
		proc_name = row['NAME']
	return(proc_name)

# # 単体テスト呼び出しコマンド
# if __name__ == '__main__':
# 	proc = 'model'
# 	status = 0
# 	result = proc_status_update(proc, status)
# 	print(result)

# if __name__ == '__main__':
# 	result=proc_status_now()
# 	print(type(result))


