import MySQLdb

# chartデータベースのデータをアップデートする
def update_inspectiondb(request_sql: object) -> object:
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='inspectiondb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = (request_sql)
	cursor.execute(sql)
	connection.commit()
	connection.close()


# if __name__ == '__main__':
# 	request_sql = "DELETE FROM karte_all"
# 	update_inspectiondb(request_sql)