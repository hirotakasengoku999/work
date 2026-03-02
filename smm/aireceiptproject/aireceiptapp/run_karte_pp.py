import os
import sys
import time
import datetime
import subprocess
import MySQLdb
import folder
import pandas as pd
from sqlalchemy import create_engine
import pathlib
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


def run_karte_pp(process):

  logger.info("カルテ前処理開始します。")
  base = folder.base()
  os.chdir(base)

  tdatetime = datetime.datetime.now()
  start_time = tdatetime.strftime('%Y-%m-%d %H:%M:%S')
  request_sql = "INSERT INTO aireceiptapp_userlog (CODE,STARTDATE,STATUS_FLAG,COMMENT)"
  request_sql += " VALUES('カルテ処理','" + start_time + "',0,'カルテ前処理を実行中です')"
  update_aireceiptdb(request_sql)

  input_path = folder.karte_pre(process)
  output_path = folder.karte(process)
  bk_path = folder.karte_BK_folder(process)

  # input_pathにカルテファイルが無ければ処理はしない
  input_files = list(pathlib.Path(input_path).glob('**/*.csv'))
  rcode = 0
  log_comment = '正常終了'
  if len(input_files) > 0:
    # バックアップ
    logger.info("バックアップを作成します。")
    karte_file = os.listdir(output_path)
    karte_csvfile = [f for f in karte_file if '.csv' in f]
    import move_dir
    new_bk_path = move_dir.make_BKfolder(bk_path)
    move_dir.copy_file(output_path, new_bk_path, karte_csvfile)
    logger.info('バックアップを作成しました')
    if process == 'predict':
      logger.info(f"{output_path}フォルダを空にします")
      for file in pathlib.Path(output_path).glob('**/*.csv'):
        try:
          file.unlink()
        except:
          print(f'{file.name}が削除出来ませんでした')

    rcode = 1
    current = os.getcwd()
    karte_pp = base + "/aireceiptapp/aireceipt_karte_pp.py"
    logger.info('{}を呼び出します'.format(karte_pp))
    os.chdir(base + "/aireceiptapp/")

    args = ["python", karte_pp, input_path,output_path]
    proc_receipt_pp = subprocess.Popen(args)
    while True:
      time.sleep(3)
      if proc_receipt_pp.poll() is not None:
        break
    rcode = proc_receipt_pp.returncode
    os.chdir(current)

    log_comment = ''
    if rcode == 0:
      logmesse = 'カルテ前処理　正常終了'
      log_comment = '正常終了'
      logger.info('カルテ前処理が完了しました')
    else:
      logmesse = 'カルテ前処理　異常終了'
      log_comment = '異常終了'
      logger.warning('カルテ前処理が異常終了しました')

    end_time = datetime.datetime.now()

    # karteフォルダ内にファイルが[マスタで決められた期間n]個以上あったら削除する
    class FileInfo:
      def __init__(self):
        self.FILENAME = ''
        self.UPDATETIME = None

    files = os.listdir(output_path)
    files = [f for f in files if os.path.isfile(os.path.join(output_path,f)) and '.csv' in f]
    # モデル作成の期間をマスターから取得
    con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
    df = pd.read_sql_query(
      sql="select controlint1 from aireceiptapp_hanyo where code1 = 'hos_info' and code2 = 'model'", con=con)
    try:
      period = int(df.at[0, 'controlint1'])
    except:
      period = 6
    if len(files) > period:
      file_info = []
      for file in files:
        fileinfo = FileInfo()
        fileinfo.FILENAME = file
        fileinfo.UPDATETIME = pathlib.Path(os.path.join(output_path,file)).stat().st_mtime
        file_info.append(fileinfo)
      updatetime_list = []
      old_file = ''
      for i in file_info:
        updatetime_list.append(i.UPDATETIME)
      min_updatetime = min(updatetime_list)
      for i in file_info:
        if i.UPDATETIME == min_updatetime:
          old_file = i.FILENAME
      rmfile = os.path.join(output_path, old_file)
      os.remove(rmfile)
      logger.info('{} を削除しました'.format(rmfile))

  else:
    logger.info(f'{input_path}にファイルがなかったためkarte_pp.pyをスキップしました')

  # カルテ前処理の完了ログ更新
  tdatetime = datetime.datetime.now()
  end_time = tdatetime.strftime('%Y-%m-%d %H:%M:%S')
  request_sql = "UPDATE aireceiptapp_userlog SET ENDDATE = '"
  request_sql += end_time + "', STATUS_FLAG = "
  request_sql += str(rcode) + ", COMMENT = '"
  request_sql += log_comment + "' WHERE STARTDATE = '"
  request_sql += start_time + "' AND CODE = 'カルテ処理'"

  update_aireceiptdb(request_sql)

  request_sql = "UPDATE aireceiptapp_hanyo SET CONTROLINT2 = "
  request_sql += str(rcode) + " WHERE CODE1 = 'proc' AND CODE2 = 'karte'"
  update_aireceiptdb(request_sql)

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
    result = run_karte_pp(process)
  else:
    result = run_karte_pp('predict')