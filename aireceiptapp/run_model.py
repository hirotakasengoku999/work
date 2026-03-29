import traceback
import os
import time
import datetime
import subprocess
import MySQLdb
import sys
import shutil
from pathlib import Path
try:
    # Djangoプロジェクト内での実行時のインポート
    from . import remove_old_data
except ImportError:
    # 単体実行時のインポート
    import remove_old_data
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


# モデル作成を実行する
def run_createmodel():
    logger.info('モデル作成を開始します。')

    # モデル作成が開始されたことをログに書き込む
    tdatetime = datetime.datetime.now()
    start_time = tdatetime.strftime('%Y-%m-%d %H:%M:%S')
    request_sql = "INSERT INTO aireceiptapp_userlog (CODE,STARTDATE,STATUS_FLAG,COMMENT)"
    request_sql += " VALUES('モデル作成','" + start_time + "',0,'モデル作成を実行中です')"
    update_aireceiptdb(request_sql)

    import folder
    base = folder.base()
    receipt = folder.receipt("createmodel")
    karte = folder.karte("createmodel")
    model = folder.model()
    rcode = 1

    # workフォルダ削除
    work_dir = Path(base) / 'aireceipt/work'
    if work_dir.exists():
        shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True)

    current = os.getcwd()

    try:
        os.chdir(base + "/aireceipt/src")
        args = ["python", base + "/aireceipt/src/create_model.pyc", receipt,
                karte, model]
        proc_model = subprocess.Popen(args)
        while True:
            time.sleep(10)
            if proc_model.poll() is not None:
                break
        rcode = proc_model.returncode
    except:
        print(sys.exc_info())
        print(traceback.format_exc())

    os.chdir(current)

    log_comment = ''
    if rcode == 0:
        log_comment = '正常終了'
        logger.info('バックアップを作成します')
        import folder
        import move_dir
        bk_path = folder.model_BK_folder()
        remove_old_data.rm_old(Path(bk_path), 3)
        receipt_folder = folder.receipt('createmodel')
        new_bk_folder = move_dir.make_period_BKfolder(receipt_folder, bk_path)
        logger.info('「{}」にモデルファイルをコピーします'.format(new_bk_folder))
        shutil.copytree(folder.model(), os.path.join(new_bk_folder, 'model'))
        logger.info('バックアップが完了しました')
    else:
        log_comment = '異常終了'
        logger.info('モデル作成が異常終了しました')

    tdatetime = datetime.datetime.now()
    end_time = tdatetime.strftime('%Y-%m-%d %H:%M:%S')
    request_sql = "UPDATE aireceiptapp_userlog SET ENDDATE = '"
    request_sql += end_time + "', STATUS_FLAG = "
    request_sql += str(rcode) + ", COMMENT = '"
    request_sql += log_comment + "' WHERE STARTDATE = '"
    request_sql += start_time + "' AND CODE = 'モデル作成'"

    update_aireceiptdb(request_sql)


if __name__ == '__main__':
    run_createmodel()
