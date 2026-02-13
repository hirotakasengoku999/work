# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import csv
import pandas as pd
import numpy as np
import os
import glob


def call(params, in_dir, logger):
    """
    入力データを情報区分ごとに分割
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s1_table_processing"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    out_dir = files[0]
    outpath_date = files[1]
    if "infocode_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["infocode_list"]))
        raise Exception
    code_list = params["infocode_list"]

    # 指定の入力ファイル名
    in_file_list = ["RECEIPTC国保", "RECEIPTC社保", "RECEIPTD国保", "RECEIPTD社保"]

    # in_dir/レセプト/配下の日付フォルダのパス取得
    folder_path_all = in_dir + '*'
    folder_path_list = glob.glob(folder_path_all)

    # n_dir/レセプト/配下の日付フォルダが１つもない場合エラー
    if len(folder_path_list) == 0:
        # 入力ファイルの読み込みに失敗 フォルダがない
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003008", [in_dir + " (directory not found)"]))
        raise Exception

    date_list = []
    # 日付のフォルダごとに処理
    for folder_path in folder_path_list:
        target_month_origi = os.path.basename(folder_path)
        # inputフォルダの日付情報を取得、リストに格納
        date_list.append(target_month_origi)

        # 出力フォルダ　パス
        out_dir_ika = out_dir + target_month_origi + '/ika/'
        out_dir_dpc = out_dir + target_month_origi + '/dpc/'

        # 出力フォルダ作成
        os.makedirs(out_dir_ika, exist_ok=True)
        os.makedirs(out_dir_dpc, exist_ok=True)
        os.makedirs(os.path.dirname(outpath_date), exist_ok=True)

        file_ika = 'RECEIPTC'
        file_dpc = 'RECEIPTD'

        for file_name in in_file_list:

            # 入力ファイル
            file = folder_path + "/" + file_name + ".UKE"

            # ファイルがない場合、エラー
            if not os.path.isfile(file):
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003008", [file + " (file not found)"]))
                raise Exception

            basename = os.path.basename(file)
            # 1レコードずつ読み込んで、カラムの最大値を取得
            reader = csv.reader(open(file, 'r'))
            num = 0
            for row in reader:
                if len(row) > num:
                    num = len(row)
                    numa = row

            # num=0 のときファイルが空
            if num == 0:
                # データが空 処理をスキップ
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [file]))

            else:
                df = pd.read_csv(file, engine='python', names=range(num), dtype='object', encoding='cp932')

                # 文書識別 IR,GOは不要のため除外
                df_drop = df[~(df[0].isin(['IR', 'GO']))]
                df_new = df_drop.copy()
                df_new[num] = np.nan

                # ikaの場合
                if basename.startswith(file_ika):
                    # 紐づけ番号を作成
                    target_dir = out_dir_ika
                    df_new[num] = df_drop[1][df_drop[0] == 'RE']
                    df_new[num] = df_new[num].fillna(method='ffill')

                # dpcの場合
                elif basename.startswith(file_dpc):
                    # 紐づけ番号と結合キーを作成
                    target_dir = out_dir_dpc
                    df_new[num] = df_drop[1][df_drop[0] == 'RE']
                    tmp = pd.DataFrame()
                    tmp[num] = df_new.loc[df_new[0] == 'RE'][num]
                    tmp[num + 1] = range(1, len(df_new[1][df_new[0] == 'RE']) + 1)
                    tmp.drop(num, inplace=True, axis=1)
                    tmp[num + 1] = tmp[num + 1].astype('int').astype('str')

                    df_new = pd.merge(df_new, tmp, left_index=True, right_index=True, how='left')
                    df_new[num] = df_new[num].fillna(method='ffill')
                    df_new[num + 1] = df_new[num + 1].fillna(method='ffill')

                # ID列でテーブル分け
                id_list = df_new[0].value_counts().index

                for i in id_list:
                    # 情報識別以外は出力しない

                    # jsonファイル指定　if i in []
                    if i in code_list:
                        df_id = df_new[df_new[0] == i]
                        infile_name = file.replace('.UKE', '')
                        fil = os.path.basename(infile_name)

                        out_file = target_dir + fil + '_' + str(i) + '.csv'

                        try:
                            df_id.to_csv(out_file, index=False, encoding='cp932')
                        except:
                            import traceback, sys
                            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [out_file + ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                            raise

    # 日付データ出力
    out_date = pd.DataFrame(columns=date_list)

    try:
        out_date.to_csv(outpath_date, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath_date + ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s1_table_processing"]))
