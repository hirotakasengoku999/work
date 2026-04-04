import sys
import os
import time
import datetime
import subprocess
import MySQLdb
import shutil
from pathlib import Path
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

  # workフォルダ削除
  work_dir = Path(base)/'aireceipt/work'
  if work_dir.exists():
    shutil.rmtree(work_dir, ignore_errors=True)
  work_dir.mkdir(parents=True)

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
      write_db_csv_path = os.path.join(base, "aireceiptapp", "write_db_csv.py")
      args = ["python", write_db_csv_path, result, seikyunengetsu]
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
        write_tensu_path = os.path.join(base, "aireceiptapp", "write_tensu.py")
        args = ["python", write_tensu_path, table_name]
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
        write_ward_path = os.path.join(base, "aireceiptapp", "write_ward.py")
        args = ["python", write_ward_path, table_name]
        proc_model = subprocess.Popen(args)
        print('proc_type=',type(proc_model))
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

  try:
    write_inspection_path = os.path.join(base, "aireceiptapp", "write_inspection.py")
    args = ["python", write_inspection_path]
    proc_model = subprocess.Popen(args)
    while True:
      time.sleep(1)
      if proc_model.poll() is not None:
        break
    rcode = proc_model.returncode
    logger.info('write_inspection rcode={0}'.format(rcode))
    if rcode == 0:
      logger.info('カルテデータの更新が完了しました')
    else:
      logger.warning('カルテデータが更新されませんでした')
  except:
    logger.warning(sys.exc_info())
    logger.warning(traceback.format_exc())

  try:
    write_receipt_path = os.path.join(base, "aireceiptapp", "write_receipt.py")
    args = ["python", write_receipt_path]
    proc_model = subprocess.Popen(args)
    while True:
      time.sleep(1)
      if proc_model.poll() is not None:
        break
    rcode = proc_model.returncode
    logger.info('write_inspection rcode={0}'.format(rcode))
    if rcode == 0:
      logger.info('レセプトデータの更新が完了しました')
    else:
      logger.warning('レセプトデータが更新されませんでした')
  except:
    logger.warning(sys.exc_info())
    logger.warning(traceback.format_exc())

  tdatetime = datetime.datetime.now()
  end_time = tdatetime.strftime('%Y-%m-%d %H:%M:%S')
  request_sql = "UPDATE aireceiptapp_userlog SET ENDDATE = '"
  request_sql += end_time + "', STATUS_FLAG = "
  request_sql += str(predict_rcode) + ", COMMENT = '"
  request_sql += log_comment + "' WHERE STARTDATE = '"
  request_sql += start_time + "' AND CODE = '検知データ作成'"

  update_aireceiptdb(request_sql)

if __name__ == '__main__':
  
  # 引数を１つずつ表示
  argvs = sys.argv

  process = ""
  codes = ""

  for i in range(len(argvs)):
    if i == 0:
      pass
    elif i == 1:
      process = argvs[1]
    else:
      codes += argvs[i] + ","
  if codes != "":
    codes = codes[:-1]
  run_predict(codes)


# # 単体テスト用
# codes = "111000110"
# run_predict(codes)
