# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import re
import os


def call(params, logger):
    """
    DPCレセのみ
    予定緊急入院区分1/0データ作成　入院初日のみフラグ
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s2_hospitalization_category"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 3:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 3]))
        raise Exception
    inpath = files[0]
    if not os.path.exists(inpath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath]))
        raise Exception
    inpath_si = files[1]
    if not os.path.exists(inpath_si):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_si]))
        raise Exception
    outpath = files[2]
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 3:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 3]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    columns3 = columns[2]

    # karuteCSV:カルテ番号、日付一覧
    # columns1 ["カルテ番号等", "_date"]
    try:
        df_si = pd.read_csv(inpath_si, engine='python', encoding="cp932", dtype={columns1[0]: 'object'},
                            usecols=[columns1[0], columns1[1]])
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    # dfCSV:予定・緊急入院区分コード
    # columns2 ["予定・緊急入院区分", "入院年月日", "退院年月日"],
    try:
        df = pd.read_csv(inpath, engine='python', usecols=[columns2[0], columns2[1], columns2[2], columns1[0]],
                        dtype='object', encoding="cp932")
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    df['入院年月日_dt'] = pd.to_datetime(df[columns2[1]], format='%Y/%m/%d')
    df['退院年月日_dt'] = pd.to_datetime(df[columns2[2]], format='%Y/%m/%d')

    # dfに存在するカルテ番号と一致するレコードをdf_siから抽出
    df_new_si = df_si[df_si[columns1[0]].isin(df[columns1[0]].tolist())].copy()

    # 入院区分ごとに処理
    for kubun in columns3:
        # kubun　と一致するレコードを抽出
        kubun_df = df[df[columns2[0]] == kubun]

        # 長さチェック
        if len(kubun_df)!=0:
            kubun_df_new = kubun_df[[columns1[0], columns2[1]]].copy()
            kubun_df_new.loc[:, kubun] = 1
            kubun_df_new = kubun_df_new.rename(columns={columns2[1]: columns1[1]})
            df_new_si = pd.merge(df_new_si, kubun_df_new, on=[columns1[0], columns1[1]], how='left')
        else:
            df_new_si.loc[:, kubun] = 0



    df_new = df_new_si.groupby([columns1[0], columns1[1]]).sum()
    df_new[df_new >= 1] = 1

    try:
        df_new.to_csv(outpath, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s2_hospitalization_category"]))
