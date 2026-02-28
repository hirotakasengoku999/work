import os
import sys
import time
import datetime
import subprocess
import MySQLdb
import folder
import pandas as pd
from sqlalchemy import create_engine
from logging import config, getLogger
try:
    config.fileConfig(folder.conf_dir() + 'logging.conf')
    logger = getLogger(__name__)
except:
    print('ログファイルの作成に失敗しました。', file=(sys.stderr))
    exit(1)

def update_aireceiptdb(request_sql):
  # データベースへの接続とカーソルの生成
  connection=MySQLdb.connect(
    host='127.0.0.1',
    user='root',
    passwd='administrator',
    db='aireceiptdb',
    charset='utf8'
    )

  cursor=connection.cursor()

  # ログに書き込み
  cursor.execute(request_sql)
  connection.commit()
  connection.close()

def run_receipt_pp(process):
  logger.info('レセプト前処理を開始します')
  try:
    base = folder.base()
    os.chdir(base)

    tdatetime = datetime.datetime.now()
    start_time = tdatetime.strftime('%Y-%m-%d %H:%M:%S')
    request_sql = "INSERT INTO aireceiptapp_userlog (CODE,STARTDATE,STATUS_FLAG,COMMENT)"
    request_sql += " VALUES('レセプト処理','" + start_time + "',0,'レセプト前処理を実行中です')"
    update_aireceiptdb(request_sql)

    input_path = folder.receipt_pre(process)
    output_path = folder.receipt(process)
    bk_path = folder.receipt_BK_folder(process)

    # バックアップ
    import move_dir
    if process != 'createmodel':
      logger.info('バックアップを作成します')
      receipt_folders = os.listdir(output_path)
      receipt_folders_dir = [f for f in receipt_folders if os.path.isdir(os.path.join(output_path, f))]
      move_dir.move_dir(output_path, bk_path, receipt_folders_dir)
      logger.info('バックアップを作成しました')

    rcode = 1
    current = os.getcwd()
    rece_pp = base + "/aireceiptapp/aireceipt_receipt_pp.py"
    logger.info('{}を呼び出します'.format(rece_pp))
    os.chdir(base + "/aireceiptapp/")

    args = ["python", rece_pp, input_path,output_path]
    proc_receipt_pp = subprocess.Popen(args)
    while True:
      time.sleep(3)
      if proc_receipt_pp.poll() is not None:
        break
    rcode = proc_receipt_pp.returncode

    os.chdir(current)

    log_comment = ''
    if rcode == 0:
      logmesse = 'レセプト前処理　正常終了'
      log_comment = '正常終了'
      logger.info('レセプト前処理が完了しました')
    else:
      logmesse = 'レセプト前処理　異常終了'
      log_comment = '異常終了'
      logger.warning('レセプト前処理が異常終了しました')

    if process == 'createmodel':
      logger.info('バックアップを作成します')
      receipt_folders = os.listdir(output_path)
      receipt_folders_dir = [f for f in receipt_folders if os.path.isdir(os.path.join(output_path, f))]
      # モデル作成の期間をマスターから取得
      con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
      df = pd.read_sql_query(sql="select controlint1 from aireceiptapp_hanyo where code1 = 'hos_info' and code2 = 'model'", con=con)
      try:
        period = int(df.at[0, 'controlint1'])
      except:
        period = 6
      # 一番過去の月のレセプトデータをバックアップに移動
      if len(receipt_folders_dir) > period:
        min_dirs = [min(receipt_folders_dir)]
        move_dir.move_dir(output_path, bk_path, min_dirs)
      logger.info('バックアップを作成しました')

    tdatetime = datetime.datetime.now()
    end_time = tdatetime.strftime('%Y-%m-%d %H:%M:%S')
    request_sql = "UPDATE aireceiptapp_userlog SET ENDDATE = '"
    request_sql += end_time + "', STATUS_FLAG = "
    request_sql += str(rcode) + ", COMMENT = '"
    request_sql += log_comment + "' WHERE STARTDATE = '"
    request_sql += start_time + "' AND CODE = 'レセプト処理'"

    update_aireceiptdb(request_sql)

    request_sql = "UPDATE aireceiptapp_hanyo SET CONTROLINT2 = "
    request_sql += str(rcode) + " WHERE CODE1 = 'proc' AND CODE2 = 'receipt'"
    update_aireceiptdb(request_sql)

  # 異常終了の場合
  except Exception as e:
    print(e)
    exit(1)

  # 正常終了の場合  
  exit(0)

if __name__ == '__main__':
  # 引数を１つずつ表示
  argvs = sys.argv

  process = ""
  for i in range(len(argvs)):
    if i == 0:
      pass
    elif i == 1:
      process = argvs[1]
  if process:
    result = run_receipt_pp(process)
  else:
    result = run_receipt_pp('predict') 