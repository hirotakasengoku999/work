import MySQLdb, datetime, sys

# FB情報
db_name = 'aireceiptdb'
db_passwd = 'administrator'

# 日付が入っていないレコードに「/01」を追加する
def add_date(code_name, table_name):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd=db_passwd,
	  db=db_name,
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	request_sql = f"UPDATE {table_name} SET MEDICAL_CARE_DATE = CONCAT(MEDICAL_CARE_DATE, '/01') WHERE CODENAME='{code_name}'"
	sql = (request_sql)
	cursor.execute(sql)
	connection.commit()
	# 今月
	t = datetime.datetime.today()
	m = t.strftime('%Y/%m')
	m += '/01'
	print(m)
	request_sql = "UPDATE aireceiptapp_kenchikekka SET MEDICAL_CARE_DATE = '" + m +"' WHERE MEDICAL_CARE_DATE LIKE '%-1%'"
	sql = (request_sql)
	cursor.execute(sql)
	connection.commit()
	connection.close()

def write_tensu(table_name):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd=db_passwd,
	  db=db_name,
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	request_sql = f"UPDATE {table_name} A , aireceiptapp_kenchi B SET A.POINTS = B.POINTS WHERE A.CODE = B.CODE AND A.MEDICAL_CARE_DATE >= B.STARTDATE AND A.MEDICAL_CARE_DATE <= B.ENDDATE"
	sql = (request_sql)
	cursor.execute(sql)
	connection.commit()
	request_sql = f"UPDATE {table_name} A , aireceiptapp_kenchi B SET A.POINTS = B.POINTS WHERE A.CODE = B.CODE2 AND A.MEDICAL_CARE_DATE >= B.STARTDATE AND A.MEDICAL_CARE_DATE <= B.ENDDATE"
	sql = (request_sql)
	cursor.execute(sql)
	connection.commit()
	connection.close()

def delete_date(code_name, table_name):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd=db_passwd,
	  db=db_name,
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	request_sql = f"UPDATE {table_name} SET MEDICAL_CARE_DATE = SUBSTRING(MEDICAL_CARE_DATE, 1, CHAR_LENGTH(MEDICAL_CARE_DATE)-3) WHERE CODENAME = '{code_name}'"
	sql = (request_sql)
	cursor.execute(sql)
	connection.commit()
	connection.close()

# 日付が入っていないレコードを抽出
def get_date_only_list(table_name):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd=db_passwd,
	  db=db_name,
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	request_sql = f"SELECT DISTINCT CODENAME FROM {table_name} WHERE CHAR_LENGTH(MEDICAL_CARE_DATE) < 8"
	sql = (request_sql)
	cursor.execute(sql)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

if __name__ == '__main__':
	argvs = sys.argv
	table_name = argvs[1]
	result = get_date_only_list(table_name)
	for i in result:
		add_date(i['CODENAME'], table_name)

	write_tensu(table_name)

	for i in result:
		delete_date(i['CODENAME'], table_name)

