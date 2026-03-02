import pandas as pd
from sqlalchemy import create_engine
import MySQLdb
import datetime, sys
from logging import config, getLogger
from pathlib import Path
try:
  base = Path.cwd()
  conf_dir = base/'config/logging.conf'
  config.fileConfig(conf_dir)
  logger = getLogger(__name__)
except:
	base = Path.cwd().parent
	conf_dir = base/'config/logging.conf'
	config.fileConfig(conf_dir)
	logger = getLogger(__name__)
	# print('ログファイルの作成に失敗しました。', file=(sys.stderr))
	# exit(1)

# try:
#   base = Path.cwd().parent
#   conf_dir = base/'config/logging.conf'
#   config.fileConfig(conf_dir)
#   logger = getLogger(__name__)
# except:
#   print('ログファイルの作成に失敗しました。', file=(sys.stderr))
#   exit(1)

def age(birthday):
    today = datetime.date.today()
    birthday = datetime.date(birthday.year, birthday.month, birthday.day)
    return (int(today.strftime("%Y%m%d")) - int(birthday.strftime("%Y%m%d"))) // 10000

def get_inspection(request_sql):
	con = create_engine('mysql://root:administrator@127.0.0.1/inspectiondb?charset=utf8')
	df = pd.read_sql_query(sql=request_sql, con=con)
	return(df)

def get_inspection_dict(request_sql):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='inspectiondb',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = request_sql
	cursor.execute(sql)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

def create_receiptview(rece_obj):

	# コードから名称に変換
	def converting_to_name(tablename, convert_type, code):
		name_obj = get_inspection(f"SELECT name FROM {tablename} WHERE code = '{code}'")
		try:
			disease_name = name_obj['name'][0]
		except Exception as e:
			disease_name = code
			logger.warning(f'{convert_type}コード「{code}」が変換できませんでした')
			logger.warning(e)
		return(disease_name)

	# 修飾語作成
	def makediseasename(modifiercode):
		modifiercode_list = [modifiercode[i:i + 4] for i in range(0, len(modifiercode), 4)]
		prefix_list = []
		suffix_list = []
		for m in modifiercode_list:
			if int(m) < 8000:
				prefix_list.append(converting_to_name('m_modifier', '修飾語', m))
			else:
				suffix_list.append(converting_to_name('m_modifier', '修飾語', m))
		prefix = ''.join(prefix_list) if prefix_list else ''
		suffix = ''.join(suffix_list) if suffix_list else ''
		return (prefix, suffix)

	profile = {}
	obj = {}
	table_key_list = []
	for index, v in enumerate(rece_obj):
		rows = v['receipt_text'].split('\n')
		in_obj = {}
		submission_target = '社保' if '社保' in v['file_type'] else '国保'
		rece_type = '出来高' if 'RECEIPTC' in v['file_type'] else 'DPC'
		in_obj['file_type'] = f'レセプトファイル：{submission_target} {rece_type}'
		sy_list = []
		sb_list = []
		si_list = []
		bu_list = []
		cd_list = []
		for row in rows:
			row_list = row.split(',')
			if row_list[0] == 'RE':
				profile['patient_id'] = row_list[13]
				profile['patient_name'] = row_list[4]
				profile['birthday'] = datetime.datetime.strptime(row_list[6], '%Y/%m/%d').strftime('%Y/%m/%d')
				profile['age'] = f"{age(datetime.datetime.strptime(row_list[6], '%Y/%m/%d'))}歳"
				profile['sex'] = '男性' if row_list[5] == '1' else '女性'
				in_obj['receipt_type'] = row_list[2]
				table_key_list.append(f'table_{index}')
				in_obj['receipt_num'] = row_list[1]
				in_obj['medical_month'] = datetime.datetime.strptime(row_list[3], '%Y%m').strftime('%Y/%m')
				in_obj['patient_id'] = row_list[13]
				in_obj['patient_name'] = row_list[4]
				in_obj['sex'] = '男性' if row_list[5] == '1' else '女性'
				in_obj['age'] = datetime.datetime.strptime(row_list[6], '%Y/%m/%d').strftime('%Y/%m/%d')
				in_obj['inout'] = '外来' if int(row_list[2]) % 2 == 0 else '入院'
				summary_type = ''
				if row_list[18] == '0':
					summary_type = 'DPCレセプト'
				elif row_list[18] == '1':
					summary_type = '総括レセプト'
				elif row_list[18] == '2':
					summary_type = '総括対象DPCレセプト'
				elif row_list[18] == '3':
					summary_type = '総括対象医科入院レセプト'
				in_obj['soukatu'] = summary_type
			elif row_list[0] == 'SY':
				main_disease_name = '〇' if row_list[6] == '01' else ''
				if row_list[5]:
					disease_name = row_list[5]
				else:
					disease_name = converting_to_name('m_disease_name', '病名', row_list[1])
				start_date = f"{row_list[2][:4]}年{row_list[2][4:6]}月{row_list[2][6:]}日"
				outcom = row_list[3]
				prefix, suffix = makediseasename(row_list[4])
				reaf_sy_dict = {
					'main_disease_name': main_disease_name,
					'disease_name': f'{prefix}{disease_name}{suffix}',
					'start_date': start_date,
					'outcome': outcom
				}
				sy_list.append(reaf_sy_dict)
			elif row_list[0] == 'SB':
				if row_list[3]:
					disease_name = row_list[3]
				else:
					disease_name = converting_to_name('m_disease_name', '病名', row_list[1])
				prefix, suffix = makediseasename(row_list[2])
				reaf_sb_dict = {
					'disease_name': f'{prefix}{disease_name}{suffix}',
					'icd10':row_list[4],
					'disease_category':converting_to_name('m_diseasename_category', '傷病名区分', row_list[5]),
					'case_of_death': row_list[6],
					'comment': row_list[7]
				}
				sb_list.append(reaf_sb_dict)
			elif row_list[0] == 'SI':
				medical_identification = row_list[1]
				hutan = row_list[2]
				medical_act = converting_to_name('m_medical_act', '診療行為', row_list[3])
				quantity = row_list[4]
				points_num = f'{row_list[5]} x {row_list[6]}'
				reaf_si_dict = {
					'medical_identification': medical_identification,
					'hutan': hutan,
					'medical_act': medical_act,
					'quantity': quantity,
					'points_num': points_num
				}
				for i in range(1,32):
					reaf_si_dict[str(i)] = row_list[i + 12]
				si_list.append(reaf_si_dict)
			elif row_list[0] == 'BU':
				if len(row_list) == 6:
					shindanbunrui = row_list[1]
					nyuin_date = row_list[2]
					taiin_date = row_list[3] if row_list[3] != 'nan' else '入院中'
					dpctenkikubun = row_list[4]
					shiin = row_list[5]
					reaf_bu_dict = {
						'shindanbunrui': shindanbunrui,
						'nyuin_date': nyuin_date,
						'taiin_date': taiin_date,
						'dpctenkikubun': dpctenkikubun,
						'shiin': shiin
					}
					bu_list.append(reaf_bu_dict)
			elif row_list[0] == 'CD':
				date_of_execution = row_list[1]
				medical_category = row_list[2]
				receipt_code = converting_to_name('m_medical_act', '診療行為', row_list[5])
				count = row_list[9]
				reaf_cd_dict = {
					'date_of_execution': date_of_execution,
					'medical_category': medical_category,
					'receipt_code': receipt_code,
					'count': count
				}
				cd_list.append(reaf_cd_dict)
		in_obj['SY'] = sy_list
		in_obj['SB'] = sb_list
		in_obj['SI'] = si_list
		in_obj['BU'] = bu_list
		in_obj['CD'] = cd_list
		obj[index] = in_obj

	table_key_dict = {}
	table_key_dict['key_list'] = table_key_list

	return(profile, obj, table_key_dict)


if __name__ == '__main__':
	request_sql = "SELECT file_type, receipt_num, receipt_text FROM receipt_detail WHERE patient_id = '04433075'"
	d = get_inspection_dict(request_sql)
	profile, obj, table_key_dict = create_receiptview(d)
	print(profile)
	print(table_key_dict)
	print(obj)
	for i, v in obj.items():
		print(i)
		for ii in v:
			print(ii)
