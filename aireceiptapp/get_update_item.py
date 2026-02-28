# 検知処理を実行後、検知結果ファイルが更新されたかをチェックします

import os
import glob
import MySQLdb
from . import database_info

def connection():
    # データベースへの接続とカーソルの生成
    connection=MySQLdb.connect(
      host='127.0.0.1',
      user='root',
      passwd=database_info.database_password(),
      db=database_info.database_name(),
      charset='utf8'
      )
    cursor = connection.cursor()
    return(cursor)

def get_item_name( code ):
    cursor = connection()
    sql = ("SELECT NAME FROM aireceiptapp_kenchi WHERE USE_FLAG='1' AND CODE=%s")
    param = (code,)
    try:
        cursor.execute( sql, param )
        rows = cursor.fetchone()
        name = ""
        for n in rows:
            name = n 
        return(name)
    except:
        sql = ("SELECT NAME FROM aireceiptapp_kenchi WHERE USE_FLAG='1' AND CODE2=%s")
        cursor.execute( sql, param )
        rows = cursor.fetchone()
        name = ""
        for n in rows:
            name = n 
        return(name)

def gettime(path,start_time_unix,file_type):
    if file_type == "*result.sav":
        str_filetype = -11
    else:
        str_filetype = -4
    current = os.getcwd()
    os.chdir(path)
    file_list = glob.glob(file_type)
    d = {}
    for f in file_list:
        p = os.stat(f).st_mtime
        if p > start_time_unix:
            code = f[:str_filetype]
            name = get_item_name( code )
            d[ code ] = name
    os.chdir( current )
    return( d )

# # 単体テスト
# if __name__ == '__main__':
#     path = 'C:/airece_test/aireceiptproject/data/2018/result'
#     start_time_unix = 1505941172.8142288
#     file_type = "*csv"
#     print(gettime( path, start_time_unix, file_type ))