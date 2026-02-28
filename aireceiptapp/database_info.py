'''
使用するデータベースの名称を定義する
'''

def database_name():
	db_name = 'aireceiptdb'
	return(db_name)

def database_password():
	db_password = 'administrator'
	return(db_password)

# # 単体テスト呼び出しコマンド
# if __name__ == '__main__':
# 	print(database_name())