import subprocess
import os
import sys
import folder
import get_model_list, update_chart
import regist_backup_result
import time
import MySQLdb
import datetime
from logging import config, getLogger

try:
    import folder

    config.fileConfig(folder.conf_dir() + 'logging.conf')
    logger = getLogger(__name__)
except:
    print('ログファイルの作成に失敗しました。', file=(sys.stderr))
    exit(1)


def update_aireceiptdb(request_sql):
    # データベースへの接続とカーソルの生成
    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )

    cursor = connection.cursor()

    # ログに書き込み
    cursor.execute(request_sql)
    connection.commit()
    connection.close()


# 処理が実行されているか
def get_proc_status(criteria_date=datetime.date.today()):
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
        "SELECT CONTROLINT1, CONTROLTEXT1, NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'proc' AND STARTDATE <= %s AND ENDDATE >= %s")
    params = (criteria_date, criteria_date,)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    connection.close()
    status = 0
    process_name = ''
    if rows:
        for row in rows:
            if row['CONTROLINT1'] == 1 or row['CONTROLTEXT1'] == 'run':
                status = 1
                process_name = row['NAME']
                break
    return (status, process_name)


# 処理フラグを更新する
def update_proc_status(update_status, criteria_date=datetime.date.today()):
    # データベースへの()接続とカーソルの生成
    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)

    update_text = 'stop'
    if update_status == 1:
        update_text = 'run'

    sql = (
        "UPDATE aireceiptapp_hanyo SET CONTROLINT1 = %s WHERE CODE1 = 'proc' AND CODE2 = 'predict' AND STARTDATE <= %s AND ENDDATE >= %s")
    params = (update_status, criteria_date, criteria_date,)
    cursor.execute(sql, params)
    sql = (
        "UPDATE aireceiptapp_hanyo SET CONTROLTEXT1 = %s WHERE CODE1 = 'proc' AND CODE2 = 'predict' AND STARTDATE <= %s AND ENDDATE >= %s")
    params = (update_text, criteria_date, criteria_date,)
    cursor.execute(sql, params)
    connection.commit()
    connection.close()
    result = '処理ステータスを「' + update_text + '」に更新しました'
    logger.info(result)

    return (result)


# 処理中で開始出来なかった時のログを更新する
def update_cannot_start_log(process_name, criteria_date=datetime.date.today()):
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
        "SELECT NAME FROM aireceiptapp_hanyo WHERE CODE1 = 'menu' AND CODE2 = '02' AND STARTDATE <= %s AND ENDDATE >= %s")
    params = (criteria_date, criteria_date)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    proc_name = '検知'
    if rows:
        for row in rows:
            proc_name = row['NAME']

    start_time = datetime.datetime.now()
    rcode = 1
    if not process_name:
        process_name = 'モデル作成、または検知データ作成'
    log_comment = '「' + process_name + '」が処理中だったため、開始できませんでした。'
    cursor.execute(
        "INSERT INTO aireceiptapp_userlog (CODE,STARTDATE,ENDDATE,STATUS_FLAG,COMMENT) VALUES(%s,%s,%s,%s,%s)",
        (proc_name, start_time, start_time, rcode, log_comment))
    connection.commit()
    connection.close()
    result = 'ログを更新しました'

    return (result)


# 処理の戻り値を０で初期化
def update_CONTROLINT2():
    # データベースへの接続とカーソルの生成
    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )
    cursor = connection.cursor()
    sql = ("UPDATE aireceiptapp_hanyo SET CONTROLINT2 = 0 WHERE CODE1 = 'proc'")
    cursor.execute(sql)
    connection.commit()
    connection.close()


# 処理を実行する
def execution_program(program_name, codes):
    logger.info('{}を呼び出します'.format(program_name))
    if codes:
        args = ["python", program_name, "predict", codes]
    else:
        args = ["python", program_name, "predict"]
    execution_process = subprocess.Popen(args)
    while True:
        time.sleep(10)
        if execution_process.poll() is not None:
            break

    logger.info('{}が完了しました'.format(program_name))


# 処理が正常に終わっているかをデータベースから取得する
def get_CONTROLINT2():
    # データベースへの接続とカーソルの生成
    connection = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='administrator',
        db='aireceiptdb',
        charset='utf8'
    )
    cursor = connection.cursor()
    sql = ("SELECT CONTROLINT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'proc'")
    cursor.execute(sql)
    rows = cursor.fetchall()
    connection.close()
    result = 0
    for i in rows:
        if i[0] != 0:
            result = 1
            break
    return (result)


if __name__ == '__main__':
    max_seikyunengetsu = update_chart.get_max_seikyunengetsu()
    chart_table = 'add_calc'
    table_name = 'no_calculation_results'
    update_chart.write_add_calc(table_name, max_seikyunengetsu, chart_table)
