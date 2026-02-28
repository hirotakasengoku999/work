import sys
import os
import time
import datetime
import glob
import subprocess
import MySQLdb
import get_uke_billing_yearmonth
import write_db_csv
import shutil
from dateutil.relativedelta import relativedelta
import traceback
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

# 検知処理を実行する
def run_predict(codes):
  logger.info('検知を開始します。')
    
  # 検知が開始されたことをログに書き込む
  tdatetime = datetime.datetime.now()
  start_time = tdatetime.strftime('%Y-%m-%d %H:%M:%S')
  request_sql = "INSERT INTO aireceiptapp_userlog (CODE,STARTDATE,STATUS_FLAG,COMMENT)"
  request_sql += " VALUES('検知データ作成','" + start_time + "',0,'検知データ作成を実行中です')"
  update_aireceiptdb(request_sql)

  import folder
  base = folder.base()
  receipt = folder.receipt('predict')
  karte = folder.karte('predict')
  model = folder.model()
  result = folder.result()

  # resultフォルダを空にする
  logger.info('resultフォルダを空にします')
  files = os.listdir(result)
  csvfiles = [f for f in files if os.path.isfile(os.path.join(result, f)) and '.csv' in f]
  for file in csvfiles:
    rm_file = os.path.join(result, file)
    os.remove(rm_file)
  logger.info('resultフォルダを空にしました')

  predict_rcode = 1
  current = os.getcwd()
  proc_dir = base + "/aireceipt/src/predict_receipt.pyc"
  logger.info('{}を呼び出します'.format(proc_dir))
  try:
    os.chdir(base + "/aireceipt/src")
    args = ["python", proc_dir, receipt, karte, model, result, codes]
    proc_model = subprocess.Popen(args)
    while True:
      time.sleep(10)
      if proc_model.poll() is not None:
        break
    predict_rcode = proc_model.returncode
  except:
    logger.warning(sys.exc_info())
    logger.warning(traceback.format_exc())

  os.chdir(current)

  log_comment = ''
  if predict_rcode == 0:
    log_comment = "正常終了"
    logger.info('検知処理が終了しました')
    logger.info('バックアップを作成します')
    import folder
    import move_dir
    bk_path = folder.result_BK_folder()
    receipt_folder = folder.receipt('predict')
    new_bk_folder = move_dir.make_period_BKfolder(receipt_folder, bk_path)
    logger.info('「{}」にresultファイルをコピーします'.format(new_bk_folder))
    shutil.copytree(folder.result(), os.path.join(new_bk_folder, 'result'))
  else:
    log_comment = "異常終了"
    logger.warning('検知処理　{}'.format(log_comment))

  if predict_rcode == 0:

    # レセプト請求年月を取得する
    import receipt_pre_chk
    seikyugetsu_list, seikyunengetsu = receipt_pre_chk.get_seikyugetsu(receipt)
      
    #検知結果をDBに保存する
    logger.info('検知結果をDBに保存します')
    logger.info('請求年月を「{}」で保存します'.format(seikyunengetsu))
    request_sql = "UPDATE aireceiptapp_hanyo SET CONTROLTEXT2 = 'write_db_csv' WHERE CODE1 = 'proc' AND CODE2 = 'predict'"
    update_aireceiptdb(request_sql)
    try:
      args = ["python", "write_db_csv.py", result, seikyunengetsu]
      proc_model = subprocess.Popen(args)
      while True:
        time.sleep(1)
        if proc_model.poll() is not None:
          break
      rcode = proc_model.returncode
      logger.info('write_db_csv rcode={0}'.format(rcode))
      logger.info('検知結果をDBに保存しました')
    except:
      logger.warning(sys.exc_info())
      logger.warning(traceback.format_exc())

    def update_points_ward(table_name):
      # 点数を同期
      logger.info('検知結果に点数を付与します')
      request_sql = "UPDATE aireceiptapp_hanyo SET CONTROLTEXT2 = 'write_tensu' WHERE CODE1 = 'proc' AND CODE2 = 'predict'"
      update_aireceiptdb(request_sql)
      try:
        args = ["python", 'write_tensu.py', table_name]
        proc_model = subprocess.Popen(args)
        while True:
          time.sleep(1)
          if proc_model.poll() is not None:
            break
        rcode = proc_model.returncode
        if rcode == 0:
          logger.info(f'検知結果「{table_name}」に点数を付与しました')
        else:
          logger.warning(f'検知結果「{table_name}」に点数が付与されませんでした')
      except:
        logger.warning(sys.exc_info())
        logger.warning(traceback.format_exc())

      # 病棟付与
      logger.info('病棟の更新を開始します')
      request_sql = "UPDATE aireceiptapp_hanyo SET CONTROLTEXT2 = 'write_ward' WHERE CODE1 = 'proc' AND CODE2 = 'predict'"
      update_aireceiptdb(request_sql)
      try:
        args = ["python", 'write_ward.py', table_name]
        proc_model = subprocess.Popen(args)
        while True:
          time.sleep(1)
          if proc_model.poll() is not None:
            break
        rcode = proc_model.returncode
        logger.info('write_ward rcode={0}'.format(rcode))
        if rcode == 0:
          logger.info(f'「{table_name}」の病棟の更新が完了しました')
        else:
          logger.warning(f'「{table_name}」の病棟が更新されませんでした')
      except:
        logger.warning(sys.exc_info())
        logger.warning(traceback.format_exc())
    update_points_ward('no_calculation_results')
    update_points_ward('calculation_exists_results')

  tdatetime = datetime.datetime.now()
  end_time = tdatetime.strftime('%Y-%m-%d %H:%M:%S')
  request_sql = "UPDATE aireceiptapp_userlog SET ENDDATE = '"
  request_sql += end_time + "', STATUS_FLAG = "
  request_sql += str(predict_rcode) + ", COMMENT = '"
  request_sql += log_comment + "' WHERE STARTDATE = '"
  request_sql += start_time + "' AND CODE = '検知データ作成'"

  update_aireceiptdb(request_sql)

if __name__ == '__main__':
  from pathlib import Path
  import folder
  model_folder = Path(folder.model())/'model'
  pickles = [i.name.replace('.pickle', '') for i in model_folder.glob('**/*.pickle')]
  savs = [i.name.replace('_result.sav', '') for i in model_folder.glob('**/*.sav')]
  csvs = [i.name.replace('_importance.csv', '') for i in model_folder.glob('**/*.csv')]
  codes = ""
  for s in savs:
    if s in csvs and s in pickles:
      codes += f"{s},"
  codes += "111000110,150371590,150371490,150371290"
  run_predict(codes)
