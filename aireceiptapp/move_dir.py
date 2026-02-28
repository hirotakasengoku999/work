import os
import shutil
import datetime
import time
from dateutil.relativedelta import relativedelta

def move_dir(path_dir, move_dir, csvfiles):

	# 移動先のフォルダが存在しなかったら作成する
	if not os.path.isdir(move_dir):
		os.makedirs(move_dir)

	# ファイルを移動する	
	for file in csvfiles:
		input_path = os.path.join(path_dir, file)
		output_path = os.path.join(move_dir, file)
		if os.path.isdir(output_path):
			shutil.rmtree(output_path)
		shutil.move(input_path, output_path)

def copy_file(path_dir, move_dir, csvfiles):

	# 移動先のフォルダが存在しなかったら作成する
	if not os.path.isdir(move_dir):
		os.makedirs(move_dir)

	# ファイルを移動する	
	for file in csvfiles:
		input_path = os.path.join(path_dir, file)
		output_path = os.path.join(move_dir, file)
		shutil.copy(input_path, output_path)

def copy_karte_file(path_dir, move_dir, csvfiles, karte_pre_files):
	result = '管理者用カルテフォルダーからkarte_preフォルダーへファイルをコピー出来ませんでした'
	# 移動先のフォルダが存在しなかったら作成する
	if not os.path.isdir(move_dir):
		os.makedirs(move_dir)

	# ファイルを移動する
	for file in csvfiles:
		input_path = os.path.join(path_dir, file)
		out_file_name = file
		for karte_pre_file in karte_pre_files:
			if karte_pre_file[:-4] in file:
				out_file_name = karte_pre_file
				output_path = os.path.join(move_dir,out_file_name)
				shutil.copy(input_path, output_path)
				result = '管理者用カルテフォルダーからkarte_preフォルダーへファイルをコピーしました'
				break
	return(result)

def move_karte_file(path_dir, move_dir, csvfiles, karte_pre_files):
	result = '管理者用カルテフォルダーからkarte_preフォルダーへファイルをコピー出来ませんでした'
	# 移動先のフォルダが存在しなかったら作成する
	if not os.path.isdir(move_dir):
		os.makedirs(move_dir)

	# ファイルを移動する
	for file in csvfiles:
		input_path = os.path.join(path_dir, file)
		out_file_name = file
		for karte_pre_file in karte_pre_files:
			if karte_pre_file[:-4] in file:
				out_file_name = karte_pre_file
				output_path = os.path.join(move_dir,out_file_name)
				shutil.move(input_path, output_path)
				result = '管理者用カルテフォルダーからkarte_preフォルダーへファイルをコピーしました'
				break
	return(result)

def copy_dir(path_dir, move_dir, dirs):

	# 移動先のフォルダが存在しなかったら作成する
	if not os.path.isdir(move_dir):
		os.makedirs(move_dir)

	# ファイルを移動する	
	for folder in dirs:
		input_path = os.path.join(path_dir, folder)
		output_path = os.path.join(move_dir, folder)
		shutil.copytree(input_path, output_path)


def make_BKfolder(BKpath):

	# BKpathが存在しなかったら作成する
	if not os.path.isdir(BKpath):
		os.makedirs(BKpath)

	# 現在の日時フォルダを生成	
	before_converting = datetime.datetime.now()
	list_datetime = str(before_converting).split(' ')
	list_seconds = list_datetime[1].split('.')
	str_date = list_datetime[0].replace('-','')
	str_seconds = list_seconds[0].replace(':','')
	result = str_date + '_' + str_seconds

	new_BKpath = os.path.join(BKpath, result)
	if not os.path.isdir(new_BKpath):
		os.makedirs(new_BKpath)

	return(new_BKpath)

def make_period_BKfolder(receipt_folder, BKpath):

	print(BKpath)

	# BKpathが存在しなかったら作成する
	if not os.path.isdir(BKpath):
		os.makedirs(BKpath)

	import folder
	import receipt_pre_chk

	seikyunengetsu_list, seikyugetsu = receipt_pre_chk.get_seikyugetsu(receipt_folder)
	if seikyunengetsu_list:
		start_period = min(seikyunengetsu_list) + relativedelta(months=-1)
		start_period_str = start_period.strftime('%Y/%m')
		start_period_str_list = start_period_str.split('/')
		start_period_str = start_period_str_list[0] + '年' + str(int(start_period_str_list[1])) + '月'
		end_period = max(seikyunengetsu_list) + relativedelta(months=-1)
		end_period_str = end_period.strftime('%Y/%m')
		end_period_str_list = end_period_str.split('/')
		end_period_str = end_period_str_list[0] + '年' + str(int(end_period_str_list[1])) + '月'
		period_str = start_period_str + '_' + end_period_str
		new_BKpath = os.path.join(BKpath, period_str)
		if os.path.isdir(new_BKpath):
			shutil.rmtree(new_BKpath)
		os.makedirs(new_BKpath)
	else:
		new_BKpath = make_BKfolder(BKpath)
	return(new_BKpath)

 
