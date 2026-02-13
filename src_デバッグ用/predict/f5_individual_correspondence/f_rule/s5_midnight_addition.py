# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
from time import strptime, strftime
import os
import glob
import re

def call(params, logger, karute_dir, in_targets):
    """
    深夜の手術記録を抽出。
    22：00~24：00、00：00~5:59　※曜日関係なし
    """
    # 開始ログ
    l = lambda x: x[0](x[1]);
    l(logger.format_log_message("AIR001001", ["s5_midnight_addition"]))

    if "files" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 1:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["files", 1]))
        raise Exception
    outpath = files[0]
    if "columns" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 1:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["columns", 1]))
        raise Exception
    columns1 = columns[0]
    if "target" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["target"]))
        raise Exception
    target = params["target"]

    if "rule_midnight_time" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["rule_midnight_time"]))
        raise Exception
    rule_midnight_time = params["rule_midnight_time"]
    if len(rule_midnight_time) < 2:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["rule_midnight_time", 2]))
        raise Exception
    rule_midnight_time_1 = rule_midnight_time[0]
    rule_midnight_time_2 = rule_midnight_time[1]

    # 出力フォルダの作成
    os.makedirs(outpath, exist_ok=True)

    # target：jsonで指定したtarget、in_targets：predicti_receiptで指定したtargets
    # in_targetsにtargetがあれば、処理を実施
    if target in in_targets:

        # 電子カルテ読み込み
        # karute_dir 配下のファイル名取得
        # predict_receipt.pyでkarute_dir配下にファイルが存在することは確認済み
        folder_path_list = glob.glob(karute_dir + '*')
        wkdf = pd.DataFrame()
        for karute_path in folder_path_list:
            # カルテファイル読み込み
            try:
                in_df = pd.read_csv(karute_path, engine='python', encoding="cp932",
                                    usecols=columns1, dtype={columns1[0]: 'object'})
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
                raise
            wkdf = pd.concat([wkdf, in_df])

        # 空白除外
        df_trilevel = wkdf.dropna(subset=[columns1[2]])
        df_trilevel=df_trilevel.reset_index()

        # 長さチェック
        if len(df_trilevel)!=0:
            df_trilevel.loc[:, columns1[2]] = df_trilevel[columns1[2]].astype('str')
            df_trilevel.loc[:, columns1[2]] = df_trilevel[columns1[2]].str.strip()
            df_trilevel = df_trilevel[df_trilevel[columns1[2]] != 'NaT']

            df_trilevel['開始時間_dt'] = pd.to_datetime(df_trilevel[columns1[2]], format='%H:%M')
            df_trilevel['算定漏れ確率'] = 0

            # 一行ずつ時間外判断
            def zikangai_check(l_df):
                flag = 0
                # 手術開始時間が　22：00~24：00、00：00~5:59

                t1 = strptime(rule_midnight_time_1[0], '%H:%M:%S')
                t2 = strptime(rule_midnight_time_1[1], '%H:%M:%S')

                t1_2 = strptime(rule_midnight_time_2[0], '%H:%M:%S')
                t2_2 = strptime(rule_midnight_time_2[1], '%H:%M:%S')
                nn = strptime(l_df[columns1[2]], '%H:%M')
                if (t1 <= nn <= t2) | (t1_2 <= nn <= t2_2):
                    flag = 1

                return flag

            df_trilevel['算定漏れ確率'] = df_trilevel.T.apply(lambda x: zikangai_check(x))

            # len(df_trilevel)!=0の場合に出力
            if len(df_trilevel) != 0:
                out_p=outpath + target + ".csv"
                try:
                    df_trilevel.to_csv(out_p, index=False, encoding='cp932')
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [out_p+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise
        else:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [columns1[2]]))

    # 終了ログ
    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["s5_midnight_addition"]))
