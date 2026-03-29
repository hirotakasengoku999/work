import os, sys
import folder
import pandas as pd
from sqlalchemy import create_engine
from logging import config, getLogger
try:
  import folder
  config.fileConfig(folder.conf_dir() + 'logging.conf')
  logger = getLogger(__name__)
except:
  print('ログファイルの作成に失敗しました。', file=(sys.stderr))
  exit(1)

logger.info('レセプト集計を開始します')
receipt = folder.receipt_pre('createmodel')
fols = os.listdir(receipt)
fols = [f for f in fols if os.path.isdir(os.path.join(receipt, f))]
con = create_engine('mysql://root:administrator@127.0.0.1/receiptdb?charset=utf8')
for fol in fols:
    target_dir = os.path.join(receipt, fol)
    files = os.listdir(target_dir)
    files = [f for f in files if 'RECEIPTD' in f or 'RECEIPTC' in f]
    seikyunengetsu = ""
    item_list = []
    logger.info(f'{seikyunengetsu} の集計を開始します')
    amount = 0
    for file in files:
        target_file = os.path.join(target_dir, file)
        with open(target_file, encoding='cp932') as f:
            l = f.readlines()

        in_flag = False
        for row in l:
            row_list = row.split(',')
            if row_list[0] == 'IR':
                seikyunengetsu = row_list[7][0:4] + '-' + row_list[7][4:7]

            # DPCの場合、GOレコードの3列目の「総点数」を取得する
            if "RECEIPTD" in file:
                if row_list[0] == 'GO':
                    amount += int(row_list[2])

            elif 'RECEIPTC' in file:
                # REレコードの3列目が奇数だったら入院
                if 'RE' in row:
                    work_list = row.split(',')
                    inout_code = work_list[2]
                    if work_list[0] != 'RE':
                        item_code = work_list[5]
                    inout_code_last = inout_code[len(inout_code)-1]

                    if int(inout_code_last) % 2 != 0:
                        in_flag = True
                    else:
                        in_flag = False

                # 入院レセプトの場合、HOレコードの5列目の請求点数を取得する
                if row_list[0] == 'HO' and in_flag:
                    amount += int(row_list[5])

        logger.info(f'{file} を集計をしました')

    # smmaryテーブルに同じ請求年月があったら削除
    request_sql = "SELECT COUNT(*) FROM summary WHERE seikyunengetsu = '" + seikyunengetsu + "'"
    equal_month = con.execute(request_sql)
    if equal_month:
        logger.info(f'請求年月「{seikyunengetsu}」を上書きします')
        request_sql = "DELETE FROM summary WHERE seikyunengetsu = '" + seikyunengetsu + "'"
        con.execute(request_sql)
    write_amount = str(amount)
    request_sql = "INSERT INTO summary VALUES (DEFAULT, '', 0, " + write_amount + ", '" + seikyunengetsu + "', default)"
    con.execute(request_sql)