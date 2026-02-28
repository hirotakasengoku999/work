"""
引数-------------
table          テーブル名称
page_num       ページ番号
display_record 一画面に何件のデータを表示させるか
"""
import MySQLdb
import os
from . import database_info
import time

def get_page(request_sql):
	start = time.time()
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd=database_info.database_password(),
	  db=database_info.database_name(),
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = (request_sql)
	cursor.execute(sql)
	objects = cursor.fetchall()
	endtime = time.time() - start
	print('データの取り出しにかかった時間＝{}'.format(endtime))
	return(objects)

def update_usercheck(request_sql):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd=database_info.database_password(),
	  db=database_info.database_name(),
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = (request_sql)
	cursor.execute(sql)
	connection.commit()
	connection.close()
	return('チェックを更新しました')

