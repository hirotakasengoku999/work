# 引数に指定したフォルダ内のファイルの更新日時をリストで取得します。
# file_listにファイル名を指定すると、そのファイル名と同じファイルの更新日時を取得します。

import os

def update_time(file_list,path):
	update_time_list = []
	files = os.listdir(path)
	for f in files:
		print(f)
		if f in file_list:
			p = path + f
			time = os.stat(p).st_mtime
			update_time_list.append(time)
	print(type(update_time_list))
	return(update_time_list)

# # 単体テスト用
# if __name__ == '__main__':
# 	file_list = ['カルテ_全結合.csv']
# 	path = 'C:/aiplane/aireceiptproject/data/2018/predict_in/karte/'
# 	result = update_time(file_list,path)
# 	print(result)


