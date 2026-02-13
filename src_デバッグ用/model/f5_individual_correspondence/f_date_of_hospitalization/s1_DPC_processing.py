# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re

def call(params, logger):
    """
    DPCレセ
    同患者で同入院日の記録が存在する場合に日付が長いほうの退院日を選ぶ処理
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s1_DPC_processing"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    inpath = files[0]
    if not os.path.exists(inpath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath]))
        raise Exception
    outpath = files[1]
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 1]))
        raise Exception
    columns1 = columns[0]

    # inCSV
    try:
        df_in = pd.read_csv(inpath, engine='python', encoding='cp932', dtype={columns1[0]: 'object'},
                            usecols=columns1)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    # 読み込んだデータがカラの場合、空のデータフレーム出力
    if len(df_in) != 0:
        df_in.loc[:, '入院年月日_dt'] = pd.to_datetime(df_in[columns1[1]])
        df_in.loc[:, '退院年月日_dt'] = pd.to_datetime(df_in[columns1[2]])

        # 重複するレコードを削除
        df = df_in[~df_in.duplicated()].sort_values(columns1[0])
        # カルテ番号、入院年月日が重複している患者をすべて抽出
        df_drop = df[df.duplicated(subset=[columns1[0], columns1[1]], keep=False)].sort_values(columns1[0])
        # カルテ番号、入院年月日が重複していない患者を抽出
        df_dup = df[~df.duplicated(subset=[columns1[0], columns1[1]], keep=False)]

        # カルテ番号、入院年月日が重複している患者に対して、退院日を選択
        # 今回退院年月日の記録がない場合、退院記録のMAXを採用
        df_dup2 = pd.DataFrame()
        if len(df_drop) != 0:
            _df_drop = df_drop[[columns1[0], columns1[1]]][~df_drop.duplicated(subset=[columns1[0], columns1[1]])]
            df_drop[columns1[3]] = df_drop[columns1[3]].astype(str)
            for karute, nyuin in _df_drop.values:
                _tmp = df_drop[(df_drop[columns1[0]] == karute) & (df_drop[columns1[1]] == nyuin)]

                # 今回退院年月日の記録がある場合、その記録を採用
                if len(_tmp[_tmp[columns1[3]] != "nan"]) > 0:
                    __tmp = _tmp[_tmp[columns1[3]] != "nan"]
                    # 今回退院年月日の記録があるレコードが複数ある場合、退院年月日の日付が新しいほう
                    if len(__tmp) > 1:
                        __tmp = __tmp[__tmp['退院年月日_dt'] == max(__tmp['退院年月日_dt'])]

                # 今回退院年月日の記録がない場合、退院年月日の日付が新しいほう
                else:
                    __tmp = _tmp[_tmp['退院年月日_dt'] == max(_tmp['退院年月日_dt'])]

                df_dup2 = pd.concat([df_dup2, __tmp])

        df_new = pd.concat([df_dup, df_dup2])
    else:
        df_new = pd.DataFrame(columns=[columns1[0], '入院年月日_dt', '入院年月日_dt', columns1[3]])

    try:
        df_new.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s1_DPC_processing"]))
