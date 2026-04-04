import MySQLdb
import os

# 検知結果をバックアップ
def write_backup_result():
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	cursor.callproc('insert_backup')
	connection.commit()
	connection.close()

# 検知結果を空にする
def delete_result():
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	sql = ("DELETE FROM aireceiptapp_kenchikekka")

	cursor.execute(sql)
	connection.commit()
	connection.close()

if __name__ == '__main__':
	write_backup_result()
	delete_result()