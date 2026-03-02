from sqlalchemy import create_engine
import pandas as pd

def get_chart(request_sql):
	con_chart = create_engine('mysql://root:administrator@127.0.0.1/chart?charset=utf8')
	df = pd.read_sql_query(sql = request_sql, con=con_chart)
	return(df)

def df_dict_conversion(df):
  item_name_list = []
  total_count_list = []
  total_amount_list = []
  first_key_list = []
  first_count_list = []
  first_amount_list = []
  second_key_list = []
  second_count_list = []
  second_amount_list = []
  third_key_list = []
  third_count_list = []
  third_amount_list = []
  others_key_list = []
  others_count_list = []
  others_amount_list = []
  df = df.fillna({'item_name':'', 'total_count':0, 'total_amount':0, 'first_key':'', 'first_count':0, 'first_amount':0, 'second_key':'', 'second_count':0, 'second_amount':0, 'third_key':'', 'third_count':0, 'third_amount':0, 'others_key':'', 'others_count':0, 'others_amount':0})
  for key, value in df.iterrows():
    if not value.item_name == '-1':
      item_name_list.append(value.item_name)
      total_count_list.append(value.total_count)
      total_amount_list.append(value.total_amount)
      first_key_list.append(value.first_key)
      first_count_list.append(value.first_count)
      first_amount_list.append(value.first_amount)
      second_key_list.append(value.second_key)
      second_count_list.append(value.second_count)
      second_amount_list.append(value.second_amount)
      third_key_list.append(value.third_key)
      third_count_list.append(value.third_count)
      third_amount_list.append(value.third_amount)
      others_key_list.append(value.others_key)
      others_count_list.append(value.others_count)
      others_amount_list.append(value.others_amount)
  obj_dict = {}
  obj_dict['item_name_list'] = item_name_list
  obj_dict['total_count_list'] = total_count_list
  obj_dict['total_amount_list'] = total_amount_list
  obj_dict['first_key_list'] = first_key_list
  obj_dict['first_count_list'] = first_count_list
  obj_dict['first_amount_list'] = first_amount_list
  obj_dict['second_key_list'] = second_key_list
  obj_dict['second_count_list'] = second_count_list
  obj_dict['second_amount_list'] = second_amount_list
  obj_dict['third_key_list'] = third_key_list
  obj_dict['third_count_list'] = third_count_list
  obj_dict['third_amount_list'] = third_amount_list
  obj_dict['others_key_list'] = others_key_list
  obj_dict['others_count_list'] = others_count_list
  obj_dict['others_amount_list'] = others_amount_list
  return(df, obj_dict)