# 参考にしたサイト　https://www.sejuku.net/blog/82657
"""
messageテーブル値を取得します。
"""
import MySQLdb
import os
import datetime
from . import database_info, now_time
# import database_name
# import now_time, database_name

# 開始日と終了日が逆かをチェック
def start_end_date(start_date, end_date):
	result = None 
	# 引数を日付型に変換
	try:
		# "-"区切り
		if start_date[4] == "-":
			start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
			end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
			if start_date > end_date:
				result = '終了日が開始日より前です。'

		# "/"区切り
		if start_date[4] == "/":
			start = datetime.datetime.strptime(start_date, '%Y/%m/%d')
			end = datetime.datetime.strptime(end_date, '%Y/%m/%d')
			if start_date > end_date:
				result = '終了日が開始日より前です。'
	except:
		pass
	return(result)

# 開始日が過去日かをチェック
def past_date(start_date):
	result = None
	# "-"区切り
	if start_date[4] == "-":
		start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()

	# "/"区切り
	elif start_date[4] == "/":
		start_date = datetime.datetime.strptime(start_date, '%Y/%m/%d').date()
		
	if start_date < now_time.date_today():
		result = "開始日が過去になっています。"
	return(result)

# 開始日～終了日の期間重複チェック
def line_no_duplicate(line_no, input_start_date, input_end_date, create, num=None):
	# "-"区切り
	if input_start_date[4] == "-":
		input_start_date = datetime.datetime.strptime(input_start_date, '%Y-%m-%d').date()
		input_end_date = datetime.datetime.strptime(input_end_date, '%Y-%m-%d').date()

	# "/"区切り
	elif input_start_date[4] == "/":
		input_start_date = datetime.datetime.strptime(input_start_date, '%Y/%m/%d').date()
		input_end_date = datetime.datetime.strptime(input_end_date, '%Y/%m/%d').date()

	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd=database_info.database_password(),
	  db=database_info.database_name(),
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)

	# 行番号が同じレコードを取得する
	sql = ("SELECT * FROM aireceiptapp_message WHERE LINENUMBER=%s")
	params = (line_no,)
	cursor.execute(sql,params)
	rows = cursor.fetchall()
	connection.close()
	result = ""
	duplication_message = None
	for i in rows:
		if create:
			if (i['STARTDATE'] <= input_start_date <= i['ENDDATE']) or (input_start_date <= i['STARTDATE'] <= input_end_date):
				result = '期限が以下のメッセージと重複しています。'
				duplication_message = i
		else:
			if i['id'] != num:
				if (i['STARTDATE'] <= input_start_date <= i['ENDDATE']) or (input_start_date <= i['STARTDATE'] <= input_end_date):
					result = '期限が他のメッセージと重複しています。'
					duplication_message = i
	return(result, duplication_message)


# # 単体テスト呼び出しコマンド
# if __name__ == '__main__':
# 	start_date = '2020/12/12'
# 	end_date = '2020/12/11'
# 	result = start_end_date(start_date,end_date)
# 	print(result)

# # line_no_duplicate単体テスト呼び出しコマンド
# if __name__ == '__main__':
# 	line_no = 2
# 	start_date = datetime.date(2020,12,21)
# 	end_date = datetime.date(2020,12,30)
# 	result = line_no_duplicate(line_no, start_date, end_date)
# 	print(result)