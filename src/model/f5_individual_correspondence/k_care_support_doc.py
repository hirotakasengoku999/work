# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re

# def call(in_file, out_dir):
def call(params, logger):
    """
    介護支援指導料　個別対応
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["k_care_support_doc"]))

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

    # karuteCSV:カルテ全結合データ読み込み
    try:
        wkdf = pd.read_csv(inpath, engine='python', encoding='cp932', dtype='object',
                        usecols=[columns1[0], columns1[1], columns2[0]])
    # 読み込めない場合、エラー
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    wkdf = wkdf.dropna(subset=[columns2[0]])

    # 長さチェック
    if len(wkdf)!=0:
        df_sinryo = wkdf.set_index([columns1[0], columns1[1]])

        # ダミー変数 1/0
        df_sinryo = pd.get_dummies(df_sinryo[columns2[0]])
        df_sinryo = df_sinryo.groupby(level=[0, 1]).sum()
        df_sinryo[df_sinryo > 1] = 1
    else:
        df_sinryo = pd.DataFrame(columns=[columns1[0], columns1[1]])

    try:
        df_sinryo.to_csv(outpath, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" +traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["k_care_support_doc"]))
