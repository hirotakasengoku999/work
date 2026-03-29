from datetime import datetime
from typing import List, Any

from sqlalchemy import create_engine
from pathlib import Path
from dateutil.relativedelta import relativedelta
import pandas as pd
import sys
import get_hanyo
import update_inspection
import get_inspectiondb
from logging import config, getLogger

try:
    import folder

    config.fileConfig(folder.conf_dir() + 'logging.conf')
    logger = getLogger(__name__)
except:
    print('ログファイルの作成に失敗しました。', file=(sys.stderr))
    exit(1)


def delete_in_batches(table_name: str, condition: str, batch_size: int = 1000):
    """
    指定された条件に基づいて、テーブルからレコードをバッチで削除する関数。

    :param table_name: テーブル名
    :param condition: 削除条件 (例: "medical_month = '2023/01' AND file_type = 'example.csv'")
    :param batch_size: 一度に削除するレコード数 (デフォルトは1000)
    """
    # 削除対象のレコード数を取得
    request_sql = f"SELECT COUNT(*) FROM {table_name} WHERE {condition}"
    target_count_t = get_inspectiondb.get_inspection_dict(request_sql)
    target_count = int(target_count_t[0]['COUNT(*)'])

    # バッチでレコードを削除
    while target_count > 0:
        request_sql = f"DELETE FROM {table_name} WHERE {condition} LIMIT {batch_size}"
        update_inspection.update_inspectiondb(request_sql)
        target_count -= batch_size


def write_receipt():
    import folder
    files = Path(folder.receipt('predict')).glob('**/*.UKE')
    for file in files:
        print(file)
        with open(file, encoding='cp932') as f:
            rows = f.readlines()
        medical_month = ''
        list_df = []
        receipt_text = ''
        receipt_num = 0
        for row in rows:
            row_list = row.split(',')

            if row_list[0] == 'IR':
                seikyunengetsu = datetime.strptime(row_list[7], '%Y%m')
                one_month_ago = seikyunengetsu - relativedelta(months=1)
                medical_month = one_month_ago.strftime('%Y/%m')
            elif row_list[0] != 'GO':
                if row_list[0] == 'RE':
                    if receipt_text:
                        append_dict = {}
                        append_dict['file_type'] = file.name
                        append_dict['receipt_num'] = receipt_num
                        append_dict['medical_month'] = medical_month
                        append_dict['patient_id'] = patient_id
                        append_dict['receipt_text'] = receipt_text
                        list_df.append(pd.DataFrame(append_dict, index=['i', ]))
                    receipt_num = row_list[1]
                    receipt_text = row
                    patient_id = row_list[13]
                else:
                    receipt_text += row
            elif row_list[0] == 'GO':
                append_dict = {}
                append_dict['file_type'] = file.name
                append_dict['receipt_num'] = receipt_num
                append_dict['medical_month'] = medical_month
                append_dict['patient_id'] = patient_id
                append_dict['receipt_text'] = receipt_text
                list_df.append(pd.DataFrame(append_dict, index=['i', ]))

        df = pd.concat(list_df)

        # 同じ診療月、同じレセプトがある場合は削除する
        table_name = 'receipt_detail'
        condition = f"medical_month = '{medical_month}' AND file_type = '{file.name}'"
        delete_in_batches(table_name, condition)

        con = create_engine('mysql://root:administrator@127.0.0.1/inspectiondb?charset=utf8')
        df.to_sql('receipt_detail', con=con, if_exists='append', index=None)


def get_rece_save_duration() -> int:
    """
	レセプトの保存期間をマスタから取得する
	"""
    request_sql = "SELECT CONTROLINT1 FROM aireceiptapp_hanyo where CODE1 = 'hos_info' AND CODE2 = 'rece_save_duration' LIMIT 1"
    rece_save_duration_t = get_hanyo.get_hanyo(request_sql)
    try:
        data_controlint1: object = rece_save_duration_t[0]['CONTROLINT1']
        if data_controlint1 is not None and data_controlint1 != 0:
            rece_save_duration = int(rece_save_duration_t[0]['CONTROLINT1'])
        else:
            rece_save_duration = 1
    except:
        logger.warning("表示期間設定が見つかりません。マスタを確認してください。")
        rece_save_duration = 1
    return rece_save_duration


def del_old_rece(period_display: int) -> None:
    """
	古いレセプトデータを削除する
	"""
    request_sql = "select distinct medical_month from receipt_detail"
    selection = get_inspectiondb.get_inspection_dict(request_sql)
    list_medical_month = [item['medical_month'] for item in selection]
    sorted_dates = sorted(list_medical_month, key=lambda x: datetime.strptime(x, '%Y/%m'), reverse=True)

    count_month = len(list_medical_month)
    if count_month > period_display:
        drop_months: List[Any] = sorted_dates[period_display:]
        # サーバに負荷をかけないよう、1000件ずつ削除する
        for medical_month in drop_months:
            table_name = 'receipt_detail'
            condition = f"medical_month = '{medical_month}'"
            delete_in_batches(table_name, condition)


if __name__ == '__main__':
    write_receipt()
    period_display = get_rece_save_duration()
    del_old_rece(period_display)