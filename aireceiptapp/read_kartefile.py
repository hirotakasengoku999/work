import datetime

import MySQLdb


class KarteFileObj:
    def __init__(self):
        self.FILENAME = ""
        self.COLUMNLIST = ""
        self.DATECOL = ""


# カルテファイルリストを取得する
def get_karte_pre_file(process, criteria_date=datetime.date.today()):
    # データベースへの()接続とカーソルの生成
    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)

    sql = (
        "SELECT FILE_CODE, FILE_NAME FROM aireceiptapp_karte_file WHERE "
        "STARTDATE <= %s AND ENDDATE >= %s ORDER BY FILE_CODE")
    if process == 'createmodel':
        sql = (
            "SELECT FILE_CODE, FILE_NAME FROM aireceiptapp_karte_file WHERE "
            "FILE_CODE <> 'mv' AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY "
            "FILE_CODE")
    params = (criteria_date, criteria_date,)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    connection.close()
    return (rows)


# カルテファイルカラムリストを取得する
def get_karte_file_column(file_code, criteria_date=datetime.date.today()):
    # データベースへの()接続とカーソルの生成
    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)

    sql = (
        "SELECT COLUMN_NAME, BEFORE_COLUMN_NAME, DATA_TYPE, "
        "ADD_COLUMN_FLAG, ADD_COLUMN_TEXT FROM aireceiptapp_karte_file_column "
        "WHERE FILE_CODE = %s AND STARTDATE <= %s AND ENDDATE >= %s ORDER BY "
        "FILE_CODE"
    )
    params = (file_code, criteria_date, criteria_date,)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    connection.close()
    return (rows)


def create_karte_file_obj(process):
    karte_pre_file_list = get_karte_pre_file(process)
    results = []
    for karte_pre_file in karte_pre_file_list:
        column_obj = get_karte_file_column(karte_pre_file['FILE_CODE'])
        file_name = karte_pre_file['FILE_NAME']
        column_list = []
        data_col = ''
        for column in column_obj:
            column_list.append(column['COLUMN_NAME'])
            if column['DATA_TYPE'] == '日付':
                data_col = column['COLUMN_NAME']
        result = KarteFileObj()
        result.FILENAME = file_name
        result.COLUMNLIST = column_list
        result.DATECOL = data_col
        results.append(result)
    return (results)

def get_karte_pre_file_list():
    obj = get_karte_pre_file('createmodel')
    results = []
    for o in obj:
        results.append(o["FILE_NAME"])
    return(results)

if __name__ == "__main__":
    result = create_karte_file_obj('predict')
    for i in result:
        print(f'{i.FILENAME}   {i.COLUMNLIST}   {i.DATECOL}')
    result = get_karte_pre_file_list()
    print(result)
