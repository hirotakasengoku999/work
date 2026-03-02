'''
カルテファイルの列名、日時を変換
'''
import pandas as pd
import os
import MySQLdb
import datetime


def get_column_conversion_dict(proc, criteria_date=datetime.date.today()):
    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)

    # 列名の変更が必要なカルテファイル名を取得
    if proc == 'predict':
        sql = (
            "SELECT FILE_NAME, FILE_CODE FROM aireceiptapp_karte_file WHERE CONVERSION_FLAG = 1 AND STARTDATE <= %s AND ENDDATE >= %s")
    else:
        sql = (
            "SELECT FILE_NAME, FILE_CODE FROM aireceiptapp_karte_file WHERE CONVERSION_FLAG = 1 AND FILE_CODE <> 'mv' AND STARTDATE <= %s AND ENDDATE >= %s")
    params = (criteria_date, criteria_date,)
    cursor.execute(sql, params, )
    rows = cursor.fetchall()
    connection.close()
    return (rows)


def conversion_columns(rows, input_path=os.getcwd()):
    criteria_date = datetime.date.today()
    files = os.listdir(input_path)
    csvfiles = [f for f in files if
                os.path.isfile(os.path.join(input_path, f)) and '.csv' in f]

    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)

    if rows:
        for row in rows:
            try:
                key = row['FILE_CODE']
                print(f'key = {key}')
                use_columns_list = []
                add_columns_dict = {}
                column_name_conversion_tuple = []
                date_column_name_list = []
                time_column_name_list = []
                patient_id_column_name = ''
                sql = (
                    "SELECT COLUMN_NAME, BEFORE_COLUMN_NAME, DATA_TYPE, ADD_COLUMN_FLAG, ADD_COLUMN_TEXT  FROM aireceiptapp_karte_file_column WHERE FILE_CODE = %s AND STARTDATE <= %s AND ENDDATE >= %s")
                params = (key, criteria_date, criteria_date,)
                cursor.execute(sql, params)
                leaf_rows = cursor.fetchall()
                print(f'leaf_rows = {leaf_rows}')
                print('row = {}'.format(row['FILE_NAME']))
                if leaf_rows:
                    for leaf_row in leaf_rows:
                        if not leaf_row['ADD_COLUMN_FLAG'] and leaf_row['BEFORE_COLUMN_NAME']:
                            use_columns_list.append(leaf_row['BEFORE_COLUMN_NAME'])
                            column_name_conversion_tuple.append((leaf_row[
                                                                     'BEFORE_COLUMN_NAME'],
                                                                 leaf_row[
                                                                     'COLUMN_NAME']))
                        else:
                            add_columns_dict[leaf_row['COLUMN_NAME']] = leaf_row[
                                'ADD_COLUMN_TEXT']
                        if leaf_row['DATA_TYPE'] == '日付':
                            date_column_name_list.append(leaf_row['COLUMN_NAME'])
                        if leaf_row['DATA_TYPE'] == '時刻':
                            time_column_name_list.append(leaf_row['COLUMN_NAME'])
                        if leaf_row['DATA_TYPE'] == '患者ID':
                            patient_id_column_name = leaf_row['BEFORE_COLUMN_NAME']
                    for csvfile in csvfiles:
                        if row['FILE_NAME'][:-4] in csvfile:
                            target_path = os.path.join(input_path, csvfile)
                            print(f'use_columns_list = {use_columns_list}')
                            df = pd.read_csv(target_path, engine='python',
                                             encoding='cp932',
                                             usecols=use_columns_list, dtype={
                                    patient_id_column_name: 'object'})
                            # 列名を変換
                            if column_name_conversion_tuple:
                                for column_name_conversion in column_name_conversion_tuple:
                                    df.rename(columns={column_name_conversion[0]:
                                                           column_name_conversion[
                                                               1]}, inplace=True)
                            # 列を追加
                            if add_columns_dict:
                                for k, v in add_columns_dict.items():
                                    df.insert(len(df.columns), k, v)
                            # 日付を変換
                            if date_column_name_list:
                                for date_column_name in date_column_name_list:
                                    df[date_column_name] = \
                                    df[date_column_name].str.split(expand=True)[0]
                                    df[date_column_name] = df[
                                        date_column_name].str.replace('-', '/')
                            if time_column_name_list:
                                for time_column_name in time_column_name_list:
                                    print(time_column_name)
                                    df[time_column_name] = \
                                    df[time_column_name].str.split(expand=True)[1]
                            output_path = os.path.join(input_path, csvfile)
                            print('file name = {}'.format(csvfile))
                            print(df)
                            df.to_csv(output_path, index=False, encoding='cp932')
            except:
                pass

    connection.close()


# # 単体テスト
# if __name__ == '__main__':
#     rows = get_column_conversion_dict('predict')
#     for row in rows:
#         print(f'{row["FILE_NAME"]}  {row["FILE_CODE"]}')
#     current = os.getcwd()
#     file_path = os.path.join(current, 'karte_pre')
#     conversion_columns(rows, file_path)
