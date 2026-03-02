from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import os
import MySQLdb
import os, datetime

def get_request_amount(request_sql):
    # データベースへの()接続とカーソルの生成
    connection=MySQLdb.connect(
      host='127.0.0.1',
      user='root',
      passwd='administrator',
      db='receiptdb',
      charset='utf8'
      )
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(request_sql)
    rows = cursor.fetchall()
    connection.close()
    return(rows)


# 単体テスト用コマンド呼び出し関数
# if __name__ == "__main__":
#     """
#     引数
#         path : レセプトフォルダー
#     """
#     base_dir = 'C:/Users/Administrator/Desktop/work/'
#     dirs = os.listdir(base_dir)
#     folders_dirs = [f for f in dirs if os.path.isdir(os.path.join(base_dir, f))]
#     for f in folders_dirs:
#         target_dir = os.path.join(folders_dirs, f)
#         ngflg, billing_year_month = chk_billing_year_month(target_dir)
#         print('ngflg = {}  billing_year_month = {}'.format(ngflg, billing_year_month))

