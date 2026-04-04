import MySQLdb
import os
import datetime

# アクティブユーザー取得
def get_active_user(user_id='9', criteria_date=datetime.date.today()):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = ("SELECT * FROM aireceiptapp_hanyo WHERE CODE1 = 'user' AND CODE2 = %s AND STARTDATE <= %s AND ENDDATE >= %s")

	params = (user_id, criteria_date, criteria_date,)
	cursor.execute(sql, params)
	rows = cursor.fetchall()
	connection.close()
	return(rows)