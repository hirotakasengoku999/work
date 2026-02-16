import datetime, os
import get_kenchi, get_hanyo
from sqlalchemy import create_engine
import pandas as pd
import MySQLdb

# chartデータベースのデータをアップデートする
def update_add_calc(request_sql):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='chart',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = (request_sql)
	cursor.execute(sql)
	connection.commit()
	connection.close()

# chartデータベースのデータを取り出す
def get_add_calc(request_sql):
	# データベースへの()接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='chart',
	  charset='utf8'
	  )
	cursor = connection.cursor(MySQLdb.cursors.DictCursor)
	sql = (request_sql)
	cursor.execute(sql)
	rows = cursor.fetchall()
	connection.close()
	return(rows)

def write_add_calc(table_name, seikyunengetsu, chart_table='add_calc'):

	def get_proba():
		# マスタで設定した確率
		display_predict_proba_dict = get_kenchi.get_kenchi_predict_proba()
		add_proba_sql = ""
		if display_predict_proba_dict:
			for k, v in display_predict_proba_dict.items():
				if k == '111000110':
					where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院') OR"
				elif k == '113006910':
					where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND KENCHI16 = '0.0' AND(KENCHI17 = '1.0' OR KENCHI18 = '1.0')) OR"
				elif k == '190101770':
					where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND(KENCHI01 <> '' OR KENCHI03 <> '' OR KENCHI05 <> '' OR KENCHI07 <> '' OR KENCHI09 <> '')) OR"
				elif k == '140033770':
					where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND(KENCHI01 <> '' OR KENCHI03 <> '' OR KENCHI05 <> '' OR KENCHI07 <> '' OR KENCHI09 <> '')) OR"
				elif k == '140053670':
					where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND(KENCHI01 <> '' OR KENCHI03 <> '' OR KENCHI05 <> '' OR KENCHI07 <> '' OR KENCHI09 <> '')) OR"
				elif k == '820100017':
					where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND(KENCHI14 <> '1.0' AND KENCHI14 <> '0.0' AND KENCHI14 <> 'nan')) OR"
				elif k == '190128110':
					where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND CAST(KENCHI16 AS DECIMAL(10,2)) < 20.0) OR"
				else:
					where_sql = " (CODE = '" + k + "' AND PREDICT_PROBA >= " + str(v) + ") OR"
				add_proba_sql += where_sql
			if add_proba_sql:
				add_proba_sql = add_proba_sql[:-3]
		if not add_proba_sql:
			add_proba_sql += " PREDICT_PROBA >= 50"
		return(add_proba_sql)

	def count_calc(count_num, total_num):
		try:
			result = count_num + total_num
		except:
			result = total_num
		return(result)

	attribute_list = ['CODENAME', 'WARD', 'DEPT']
	t, codename_list = get_kenchi.get_kenchi()
	t, ward_list = get_hanyo.get_ward()
	t, dept_list = get_hanyo.get_dept()

	con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')


	# すでに同じ請求年月のデータが入っていたら、削除する
	seikyu_count = 0
	print(f'{seikyunengetsu} = {type(seikyunengetsu)}')
	request_sql = "SELECT COUNT(*) as seikyunengetsu_count FROM " + chart_table + " WHERE seikyunengetsu = '" + seikyunengetsu + "'"

	rows = get_add_calc(request_sql)
	for row in rows:
		seikyu_count = row['seikyunengetsu_count']

	if seikyu_count > 0:
		request_sql = "DELETE FROM " + chart_table + " WHERE seikyunengetsu = '" + seikyunengetsu + "'"
		update_add_calc(request_sql)

	for attribute in attribute_list:
		request_sql = "SELECT " + attribute + ", count(*) as total_count, sum(points) as total_amount FROM " + table_name + " WHERE "
		request_sql += "(" + get_proba() + ") AND ZISSEKI = 0 AND SEIKYUNENGETSU = '" + seikyunengetsu + "'"
		if chart_table == 'indicate':
			request_sql += " AND USER_CHECK = '算定可'"
		request_sql += " GROUP BY " + attribute + " ORDER BY total_count DESC"
		print(request_sql)
		df = pd.read_sql_query(sql=request_sql, con=con)
		if attribute == 'CODENAME':
			count_attribute = 'DEPT'
			chk_list = dept_list
		else:
			count_attribute = 'CODENAME'
			chk_list = codename_list
		first_count_key_list = []
		first_count_value_list = []
		first_amount_value_list = []
		second_count_key_list = []
		second_count_value_list = []
		second_amount_value_list = []
		third_count_key_list = []
		third_count_value_list = []
		third_amount_value_list = []
		others_count_key_list = []
		others_count_value_list = []
		others_amount_value_list = []
		for row, value in df.iterrows():
			attribute_row = value[attribute]
			request_sql = "SELECT " + count_attribute + ", COUNT(*) AS COU, SUM(POINTS) AS TOTALPO FROM "
			request_sql += table_name
			attribute_row = '' if attribute_row is None else attribute_row
			print(f"{attribute}  {attribute_row}")
			request_sql += " WHERE  " + attribute + " = '" + attribute_row + "'"
			request_sql += " AND (" + get_proba() + ") AND ZISSEKI = 0 AND SEIKYUNENGETSU = '" + seikyunengetsu + "'"
			if chart_table == 'indicate':
				request_sql += " AND USER_CHECK = '算定可'"
			request_sql += " GROUP BY " + count_attribute + " ORDER BY COU DESC"
			reaf_df = pd.read_sql_query(sql = request_sql, con=con)
			work_num = 1
			work_count_num = 0
			work_amount_num = 0
			first_count_key = None
			first_count_value = None
			first_count_amount = None
			second_count_key = None
			second_count_value = None
			second_count_amount = None
			third_count_key = None
			third_count_value = None
			third_count_amount = None
			for reaf_row, reaf_value in reaf_df.iterrows():
				if reaf_value[0] in chk_list:
					if work_num == 1:
						first_count_key = reaf_value[0]
						first_count_value = reaf_value[1]
						first_count_amount = reaf_value[2]
						work_count_num = count_calc(reaf_value[1], work_count_num)
						work_amount_num = count_calc(reaf_value[2], work_amount_num)
					elif work_num == 2:
						second_count_key = reaf_value[0]
						second_count_value = reaf_value[1]
						second_count_amount = reaf_value[2]
						work_count_num = count_calc(reaf_value[1], work_count_num)
						work_amount_num = count_calc(reaf_value[2], work_amount_num)
					elif work_num == 3:
						third_count_key = reaf_value[0]
						third_count_value = reaf_value[1]
						third_count_amount = reaf_value[2]
						work_count_num = count_calc(reaf_value[1], work_count_num)
						work_amount_num = count_calc(reaf_value[2], work_amount_num)
					work_num += 1

			first_count_key_list.append(first_count_key)
			first_count_value_list.append(first_count_value)
			first_amount_value_list.append(first_count_amount)
			second_count_key_list.append(second_count_key)
			second_count_value_list.append(second_count_value)
			second_amount_value_list.append(second_count_amount)
			third_count_key_list.append(third_count_key)
			third_count_value_list.append(third_count_value)
			third_amount_value_list.append(third_count_amount)
			others_count_key_list.append('その他')
			others_count_value_list.append(value.total_count - work_count_num)
			others_amount_value_list.append(value.total_amount - work_amount_num)

		df.insert(0, 'seikyunengetsu', seikyunengetsu)
		df.insert(1, 'attribute', attribute)
		df['first_key'] = first_count_key_list
		df['first_count'] = first_count_value_list
		df['first_amount'] = first_amount_value_list
		df['second_key'] = second_count_key_list
		df['second_count'] = second_count_value_list
		df['second_amount'] = second_amount_value_list
		df['third_key'] = third_count_key_list
		df['third_count'] = third_count_value_list
		df['third_amount'] = third_amount_value_list
		df['others_key'] = others_count_key_list
		df['others_count'] = others_count_value_list
		df['others_amount'] = others_amount_value_list
		df.insert(len(df.columns), 'date_joined', datetime.datetime.now())
		delete_low_list = []
		if attribute == 'CODENAME':
			delete_row_check_list = codename_list
		elif attribute == 'WARD':
			delete_row_check_list = ward_list
		elif attribute == 'DEPT':
			delete_row_check_list = dept_list
		for u in df[attribute].unique():
			if not u in delete_row_check_list:
				delete_low_list.append(u)
		for delete_low in delete_low_list:
			df = df[df[attribute] != delete_low]
		df = df.rename(columns={attribute:'item_name'})
		print(df)
		con_chart = create_engine('mysql://root:administrator@127.0.0.1/chart?charset=utf8')
		df.to_sql(chart_table, con=con_chart, if_exists='append', index=None)

def get_max_seikyunengetsu():
	request_sql = "SELECT MAX(SEIKYUNENGETSU) AS max_seikyunengetsu FROM no_calculation_results"
	rows = get_chart(request_sql)
	result = ''
	if rows:
		for row in rows:
			result = row[0]
	if not result:
		date_today = datetime.date.today()
		result = date_today.strftime('%Y-%m')
	return(result)

def get_max_outputdate(seikyunengetsu):
	request_sql = "SELECT MAX(OUTPUTDATE) AS max_outputdate FROM no_calculation_results WHERE SEIKYUNENGETSU = '"
	request_sql += seikyunengetsu + "'"
	rows = get_chart(request_sql)
	result = ''
	if rows:
		for row in rows:
			result = row[0]
	return(result)

def get_chart(request_sql):
	# データベースへの接続とカーソルの生成
	connection=MySQLdb.connect(
	  host='127.0.0.1',
	  user='root',
	  passwd='administrator',
	  db='aireceiptdb',
	  charset='utf8'
	  )
	cursor = connection.cursor()

	sql = (request_sql)
	cursor.execute(sql)
	objects = cursor.fetchall()
	connection.close()
	return(objects)

if __name__ == '__main__':
    max_seikyunengetsu = get_max_seikyunengetsu()
    table_name = 'no_calculation_results'
    # chart_table = 'indicate'
    chart_table = 'add_calc'
    write_add_calc(table_name, max_seikyunengetsu, chart_table)