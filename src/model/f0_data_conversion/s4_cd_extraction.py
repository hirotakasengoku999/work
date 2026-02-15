# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import re
import os

def call(params, logger):
    '''
    コメントコードを抽出　DPCのみ
    '''
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s4_cd_extraction"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 3:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 3]))
        raise Exception
    in_dir = files[0]
    if not os.path.exists(in_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_dir]))
        raise Exception
    outpath = files[1]
    input_date = files[2]
    if not os.path.exists(input_date):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [input_date]))
        raise Exception
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 2]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]

    # 出力フォルダ作成
    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    # date
    s_target_month_l = pd.read_csv(input_date, engine='python',
                                   encoding='cp932', dtype='object').columns.tolist()

    dummy_new = pd.DataFrame()
    # 月ごとに処理
    for s_target_month in s_target_month_l:
        # 国社
        for hoken in ['国保', '社保']:

            inFile_str = in_dir + '/' + s_target_month + '/dpc/RECEIPTD' + hoken
            # CD
            inpath_cd = inFile_str + '_CD.csv'

            # ファイルチェック：なければWARNIG、skip
            if not os.path.isfile(inpath_cd):
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [inpath_cd]))
                continue

            # CD読み込み
            try:
                df_cd_si = pd.read_csv(inpath_cd, engine='python',
                                    usecols=columns1,
                                    encoding='cp932', dtype='object')
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
                raise

            try:
                df_cd_si.loc[:, columns2[0]] = pd.to_datetime(df_cd_si[columns1[1]], format='%Y/%m/%d')
                df_cd_si.loc[:, columns2[0]] = df_cd_si[columns2[0]].dt.strftime('%Y/%m/%d')
            except ValueError:
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003013", [inpath_cd]))
                raise

            df_cd_si = df_cd_si.rename(columns={columns1[2]: columns2[1]})
            df_cd_si = df_cd_si.drop([columns1[1]], axis=1)
            dummy_new = pd.concat([dummy_new, df_cd_si])

    # 1レコードもなかった場合、空のデータフレームを作成
    if len(dummy_new) == 0:
        dummy_new = pd.DataFrame(columns=columns2 + [columns1[0]])

    try:
        dummy_new.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s4_cd_extraction"]))
