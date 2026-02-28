import datetime
import time

# 現在時刻
def nt():
	n_time = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
	return(n_time)

# 今日
def date_today():
	result = datetime.date.today()
	return(result)

# 明日
def tommorrow():
	today = date_today()
	result = today + datetime.timedelta(days=1)
	return(result)
	
# # 単体テスト呼び出しコマンド
# if __name__ == '__main__':
# 	print(date_today())