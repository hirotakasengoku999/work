import os
import os.path
from pathlib import Path
from datetime import datetime
import shutil, subprocess, pandas as pd, sys
from sqlalchemy import create_engine, text

from logging import config, getLogger
try:
    import folder
    base = folder.base()
    log_path = os.path.join(base, 'config', 'batch_logging.conf')
    config.fileConfig(log_path)
    logger = getLogger(__name__)
except:
    print('ログファイルの作成に失敗しました。', file=(sys.stderr))
    exit(1)

def import_karte(ip_address, username, password, file_path):
    try:
        logger.info(f'カルテ受け渡しフォルダに接続します {ip_address}')
        command = f"net use \\\\{ip_address} /user:{username} {password}"
        subprocess.run(command, shell=True)

        # aiブレーンサーバの受け取りフォルダ
        import folder
        receive_dir = Path(folder.user_karte_folder('predict'))
        if receive_dir.exists():
            shutil.rmtree(receive_dir)
        receive_dir.mkdir(parents=True)
        for file in file_path.glob('*.csv'):
            logger.info(f"{file}を取り込みます")
            destination_path = receive_dir/file.name
            shutil.copy(file, destination_path)
            logger.info(f"{file}を取り込みました")
    except:
        logger.warning('カルテファイルの取り込みに失敗しました')


if __name__ == '__main__':
    logger.info('カルテファイルの取り込みを開始します')
    ip_address = '192.168.2.68'
    username = 'aibrain'
    password = 'aibrain!'
    file_path = Path(f"\\\\{ip_address}\\aibrain")
    import_karte(ip_address, username, password, file_path)
    logger.info('カルテファイルの取り込みを完了しました')