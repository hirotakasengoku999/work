# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2021/12/06
import datetime
import pandas as pd
import numpy as np
import glob, shutil, time
import os
from pathlib import Path
import chardet
import sys, csv
csv.field_size_limit(1000000)
try:
    # Djangoプロジェクト内での実行時のインポート
    from . import get_hanyo
except ImportError:
    # 単体実行時のインポート
    import get_hanyo

# from logging import config, getLogger
# try:
#   import folder
#   config.fileConfig(folder.conf_dir() + 'logging.conf')
#   logger = getLogger(__name__)
# except:
#   print('ログファイルの作成に失敗しました。', file=(sys.stderr))
#   exit(1)

# 複数列を１つの列に結合
def Grouping(df, out_colum, target_list):
    if out_colum in target_list:
        target_list.remove(out_colum)
    else:
        df[out_colum] = ''

    for target_col in target_list:
        df[out_colum] = df[out_colum].str.cat([df[target_col].fillna('')], sep=' ')
    df.drop(target_list, axis=1, inplace=True)
    return df

def detect_encoding(file_path):
    # with open(file_path, 'rb') as f:
    #     rowdata = f.read()
    # result = chardet.detect(rowdata)
    # return result['encoding']
    return 'cp932'

def read_creaned_data(input_path):
    # read_csvで読み込めなかった場合
    rows = []
    cols = []
    encoding = detect_encoding(input_path)
    with open(input_path, 'r', errors='ignore', encoding=encoding) as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            row = [s.strip() for s in row]
            if i == 0:
                cols = row
            else:
                rows.append(row)

        f.close()
    df = pd.DataFrame(rows)
    df.columns = cols

    return df

def aireceipt_karte_pp(input_path, output_path):
    karute = 'カルテ番号等'
    data_column_name = "_date"

    # 列名変更
    def RenameColum(df, target_list):
        return df.rename(columns=target_list)


    # 入力ファイル
    path = str(input_path + '/*')
    file_listc = glob.glob(path)

    # 格納
    tmp_list = []
    tmp_df = pd.DataFrame()

    for path_1 in file_listc:
        print(f"{path_1}を読み込みます")
        basename = os.path.basename(path_1).replace('.csv', '')

        df = read_creaned_data(path_1)

        # 不要な列削除
        df = df.loc[:, ~(df.columns.str.startswith('Unnamed'))]

        # 共通
        df = RenameColum(df, {'患者番号': karute, '記事日時': data_column_name, '手術日付': data_column_name, '手術日': data_column_name, '科名': '診療科'})
        if basename.startswith('手術歴'):
            basename = '手術歴'
            df = Grouping(df, 'カルテ内容', ['手術項目名称', '使用機器'])
            df = RenameColum(df, {'開始時間': '手術開始時間'})
            df['手術開始時間'] = df['手術開始時間'].str.slice(0, 5)
            df = df[[karute, data_column_name, '手術開始時間', '体位', 'カルテ内容']]
            df['手術開始時間'] = df['手術開始時間'].str.strip()
            df['手術開始時間'] = pd.to_datetime(df['手術開始時間']).dt.strftime('%H:%M')
            df['手術開始時間'] = pd.to_datetime(df['手術開始時間'], format='%H:%M')

        elif basename.startswith('手術記録'):
            basename = '手術記録'
            df = Grouping(df, 'カルテ内容', ['病名', '記事日時', '記事内容（全文）'])
            df = df[[karute, data_column_name, 'カルテ内容']]

        elif basename.startswith('医師記録'):
            basename = '医師記録'
            df = RenameColum(df, {'記事内容（全文）': 'カルテ内容'})
            df = df[[karute, data_column_name, '診療科', 'カルテ内容']]

        elif basename.startswith('看護記録'):
            basename = '看護記録'
            df = RenameColum(df, {'記事内容（全文）': 'カルテ内容'})
            df = df[[karute, data_column_name, '診療科', 'カルテ内容']]

        df[data_column_name] = pd.to_datetime(df[data_column_name]).dt.strftime("%Y/%m/%d")
        patient_id_length_ = get_hanyo.get_hanyo(
            "SELECT CONTROLINT1 FROM aireceiptapp_hanyo WHERE CODE1 = 'hos_info' AND CODE2 = 'patient_id_length'")
        patient_id_length = patient_id_length_[0]['CONTROLINT1'] if len(patient_id_length_) > 0 else 8
        df[karute] = df[karute].astype('str').str.zfill(patient_id_length)

        # 2021/4/15 追加
        if basename == 'テンプレート':
            _tmp_df = df[~df.duplicated([karute, data_column_name, '文書種別'])]
        else:
            df = df[~df.duplicated()]
            df[data_column_name] = pd.to_datetime(df[data_column_name], format='%Y/%m/%d')
            df[data_column_name] = df[data_column_name].dt.strftime('%Y/%m/%d')

            # カルテ番号と日付でグルーピング
            target_colum = list(set(df.columns) - {karute, data_column_name})

            for i, target in enumerate(target_colum):
                t_df = df[[karute, data_column_name, target]]
                t_df = t_df[~t_df.duplicated()]

                if basename == '手術歴' and target == '手術開始時間':
                    result = t_df.groupby([karute, data_column_name])[target].max()
                    result = result.dt.strftime('%H:%M')
                else:
                    t_df[target] = t_df[target].astype("str")
                    result = (t_df.groupby([karute, data_column_name])[target].apply(list).apply(
                        lambda x: sorted(x)).apply(' '.join))
                result = result.reset_index()

                if i == 0:
                    _tmp_df = result.copy()
                else:
                    _tmp_df = pd.merge(_tmp_df, result, on=[karute, data_column_name])

            _tmp_df['文書種別'] = basename

        tmp_df = pd.concat([tmp_df, _tmp_df])
        # tmp_df = pd.concat([tmp_df, _tmp_df], sort=True)

    out_df = pd.DataFrame()
    target_colum = list(set(tmp_df.columns) - {karute, data_column_name, '文書種別'})
    tmp_df = tmp_df.fillna('')
    for target in target_colum:
        t_df = tmp_df[[karute, data_column_name, '文書種別', target]]
        t_df = t_df[~t_df.duplicated()]

        result = (t_df.groupby([karute, data_column_name, '文書種別'])[target].apply(list).apply(lambda x: sorted(x)).apply(' '.join))
        # result.name = target

        result = result.reset_index()

        # 改行削除
        result[target] = result[target].replace("\t", " ", regex=True)
        result[target] = result[target].replace("\n", " ", regex=True)

        result = result.set_index([karute, data_column_name, '文書種別'])

        out_df = pd.concat([out_df, result], axis=1)
        # a=result.reset_index([karute, data_column_name, '文書種別'])

    out_df = out_df.reset_index()

    # 日付チェック
    dt_check = pd.to_datetime(out_df[data_column_name], format='%Y/%m/%d')
    out_df[data_column_name] = dt_check.dt.strftime('%Y/%m/%d')

    # 出力パス
    # 月ごとの出現回数を計算
    month_counts = out_df['_date'].str[:7].value_counts()
    # 一番出現回数が多かった月を取得
    most_common_month = month_counts.idxmax()
    period = f"{most_common_month.replace('/', '')}"
    out_path = output_path + f'/カルテ_全結合_{period}.csv'

    # 出力
    out_df.to_csv(out_path, index=False, encoding='cp932')


# 単体テスト用コマンド呼び出し関数
if __name__ == "__main__":
    """
    引数
        input_path : 電子カルテフォルダー
        output_path: 加工済電子カルテフォルダー（レセプト診断AI APIの入力用フォルダー）
        type       : "iseikai" = 医誠会用の処理
                   : "shimane_fj" = 島根中央病院用の処理
    復帰値(result)
        0: 正常終了
        1: 異常終了
    """

    # input_path = "D:\data\shimane/pre_karute"
    # output_path = "D:\data\shimane/karte"

    args = sys.argv
    input_path = args[1]
    output_path = args[2]
    result = aireceipt_karte_pp(input_path, output_path)
