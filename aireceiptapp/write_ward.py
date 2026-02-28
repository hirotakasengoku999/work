# 検知結果に病棟を追加する
import MySQLdb, datetime, sys
from dateutil.relativedelta import relativedelta


def write_ward(medical_care_month, table_name):
    print(medical_care_month)
    request_sql = f"UPDATE {table_name} KK \
  INNER JOIN (SELECT MI1.PATIENT_ID, \
  MI1.WARD, \
  MI1.MOVE_DATETIME \
  FROM aireceiptapp_movement_info MI1 \
  INNER JOIN(SELECT PATIENT_ID,\
  MAX(MOVE_DATETIME) AS MOVE_DATETIME \
  FROM aireceiptapp_movement_info \
  GROUP BY PATIENT_ID) MI2 \
  ON MI1.PATIENT_ID = MI2.PATIENT_ID \
  AND MI1.MOVE_DATETIME = MI2.MOVE_DATETIME) MI \
  on KK.PATIENT_ID = MI.PATIENT_ID \
  SET KK.WARD = MI.WARD, KK.DEPT = MI.DEPT WHERE KK.IN_OUT <> '外来'"

    # データベースへの接続とカーソルの生成
    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )
    cursor = connection.cursor()

    sql = (request_sql)
    cursor.execute(sql)

    connection.commit()
    # 接続を閉じる

    f"UPDATE {table_name} KK \
  INNER JOIN (SELECT MI1.PATIENT_ID, \
  MI1.WARD, \
  MI1.MOVE_DATETIME \
  FROM aireceiptapp_movement_info MI1 \
  INNER JOIN(SELECT PATIENT_ID, \
  MAX(MOVE_DATETIME) AS MOVE_DATETIME \
  FROM aireceiptapp_movement_info \
  WHERE MOVE_CATEGORY <> '退院' \
  AND PATIENT_ID IN (SELECT PATIENT_ID FROM aireceiptapp_kenchikekka WHERE WARD = "") \
  GROUP BY PATIENT_ID) MI2 \
  ON MI1.PATIENT_ID = MI2.PATIENT_ID \
  AND MI1.MOVE_DATETIME = MI2.MOVE_DATETIME) MI \
  ON KK.PATIENT_ID = MI.PATIENT_ID \
  SET KK.WARD = MI.WARD, KK.DEPT = MI.DEPT WHERE KK.IN_OUT <> '外来'"

    sql = (request_sql)
    cursor.execute(sql)
    connection.commit()

    connection.close()

def replace_missing(table_name):
    # データベースへの接続とカーソルの生成
    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )
    cursor = connection.cursor()
    sql = (f"UPDATE {table_name} SET IN_OUT = '入院' WHERE WARD <> '' AND IN_OUT = '-1'")
    cursor.execute(sql)
    sql = (f"UPDATE {table_name} SET IN_OUT = '入院' WHERE CODE <> '111000110' AND IN_OUT = '-1'")
    cursor.execute(sql)
    connection.commit()
    connection.close()

if __name__ == '__main__':
    import receipt_pre_chk
    import folder

    receipt = folder.receipt('predict')
    print(receipt)
    argvs = sys.argv
    table_name = argvs[1]
    # 診療日
    try:
        seikyunengetsu_list, seikyunengetsu = receipt_pre_chk.get_seikyugetsu(receipt)
        seikyunengetsu_date = datetime.datetime.strptime(seikyunengetsu,'%Y-%m')
        seikyunengetsu_date = seikyunengetsu_date - relativedelta(months=1)
        medical_care_month = seikyunengetsu_date.strftime('%Y-%m')
    except:
        seikyunengetsu = datetime.datetime.today()
        medical_care_month_date = seikyunengetsu - relativedelta(months=1)
        medical_care_month = medical_care_month_date.strftime('%Y-%m')
    # 病棟付与
    write_ward(medical_care_month, table_name)
    # -1に値を付与
    replace_missing(table_name)
