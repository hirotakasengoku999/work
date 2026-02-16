# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re

def call(params, logger):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_job_extraction"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 4:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 4]))
        raise Exception
    in_path = files[0]
    if not os.path.exists(in_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_path]))
        raise Exception
    in_path2 = files[1]
    if not os.path.exists(in_path2):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_path2]))
        raise Exception
    out_dir = files[2]
    path = files[3]
    if not os.path.exists(path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [path]))
        raise Exception
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

    # 出力フォルダの作成
    os.makedirs(out_dir, exist_ok=True)

    # ChatName3は不使用
    # in_path2 = str(in_dir_x2 + '職種マスタ.csv')
    df_master = pd.read_csv(in_path2, engine='python', dtype='object', encoding='cp932', usecols=['CatCode', 'CatName'])
    df_master1 = df_master.copy()
    df_master1.columns = columns1
    df_master2 = df_master.copy()
    df_master2.columns = columns2

    # in_path = str(in_dir + 'EXDIDH_all.csv')
    usecols = [columns3[0], columns3[1], columns1[0], columns2[0]]
    try:
        in_df = pd.read_csv(in_path, engine='python', dtype='object', encoding='cp932', usecols=usecols)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
        raise

    in_df = in_df.rename(columns={columns3[1]: columns3[2]})

    in_df_dup = in_df.dropna(subset=[columns1[0], columns2[0]])
    in_df_dup = in_df_dup[~in_df_dup.duplicated(subset=[columns3[0], columns3[2], columns1[0], columns2[0]])]

    # path = str(work_dir + '/患者紐づけ.csv')
    try:
        df_w = pd.read_csv(path, engine='python', dtype='object', encoding='cp932', usecols=[columns3[3], columns3[0], columns3[2]])
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
        raise

    df = pd.merge(df_w, in_df_dup, on=[columns3[0], columns3[2]])

    df.drop(columns3[0], axis=1, inplace=True)
    df_dup = df[~df.duplicated(subset=[columns3[3], columns3[2], columns1[0], columns2[0]])]
    df_dup = pd.merge(df_dup, df_master1, on=[columns1[0]], how='left')
    df_dup = pd.merge(df_dup, df_master2, on=[columns2[0]], how='left')
    df_dup.drop([columns1[0], columns2[0]], axis=1, inplace=True)
    df_dup = df_dup.dropna(subset=[columns1[1], columns2[1]])


    df_dup_1 = df_dup[[columns3[3], columns3[2], columns1[1]]]
    df_dup_1 = df_dup_1[~df_dup_1.duplicated(subset=[columns3[3], columns3[2], columns1[1]])]
    df_dpc_group_1 = df_dup_1.groupby([columns3[3], columns3[2]])[columns1[1]].apply(
        lambda x: '/'.join(sorted(list(x))))

    df_dup_2 = df_dup[[columns3[3], columns3[2], columns2[1]]]
    df_dup_2 = df_dup_2[~df_dup_2.duplicated(subset=[columns3[3], columns3[2], columns2[1]])]
    df_dpc_group_2 = df_dup_2.groupby([columns3[3], columns3[2]])[columns2[1]].apply(
        lambda x: '/'.join(sorted(list(x))))

    df_out = pd.concat([df_dpc_group_1, df_dpc_group_2], axis=1)

    outpath = out_dir + '発生更新者職種.csv'
    try:
        df_out.to_csv(outpath, index=True, header=True, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_job_extraction"]))
