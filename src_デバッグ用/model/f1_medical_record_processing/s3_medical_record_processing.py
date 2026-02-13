# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re

def call(params, logger):
    """
    電子カルテ 文書種別から文書有無1/0データを作成
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s3_medical_record_processing"]))

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
    if len(columns) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 2]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]

    # カルテ読み込み
    try:
        header_df = pd.read_csv(inpath, engine='python', dtype='object', encoding='cp932',
                                usecols=[columns1[0], columns1[1], columns2[0]])
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
        raise

    # 文書種別のダミー変数作成　1/0データ
    df_dummy = pd.get_dummies(header_df[columns2[0]])

    tmp_df = pd.concat([header_df[[columns1[0], columns1[1]]], df_dummy], axis=1)
    group_df = tmp_df.groupby([columns1[0], columns1[1]]).sum()
    group_df[group_df > 1] = 1

    group_df = group_df.reset_index()

    try:
        group_df.to_pickle(outpath)
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise


    # 確認用csv
    # group_df.to_csv(outpathcsv, encoding='cp932', index=False)
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s3_medical_record_processing"]))
