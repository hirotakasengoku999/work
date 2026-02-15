# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re

def call(params, logger):
    """
    文書のタイトル有無
    """

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s4_title_processing"]))

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
    if "bunsyo_syubetu" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["bunsyo_syubetu"]))
        raise Exception
    bunsyo_syubetu_list = params["bunsyo_syubetu"]

    # 指定
    title = columns2[0]
    bunsyo_syubetu = columns2[1]

    # カルテ全結合データ読み込み
    try:
        wk_df = pd.read_csv(inpath, engine='python', encoding='cp932', dtype='object',
                            usecols=[columns1[0], columns1[1], title, bunsyo_syubetu])
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
        raise

    # 指定の文書種別のレコードを抽出
    wk_df = wk_df[wk_df[bunsyo_syubetu].isin(bunsyo_syubetu_list)]
    # ”タイトル”が欠損の場合レコード削除
    wk_df = wk_df.dropna(subset=[title])

    # [文書種別名]_[タイトル名]が要素の列を生成
    wk_df[title] = wk_df[bunsyo_syubetu].str.cat(wk_df[title], sep='_')
    df_dummy = pd.get_dummies(wk_df.set_index([columns1[0], columns1[1]])[title])

    group_df = df_dummy.groupby([columns1[0], columns1[1]]).sum()
    group_df[group_df > 1] = 1

    group_df = group_df.reset_index()

    if len(group_df) != 0:
        try:
            group_df.to_pickle(outpath)
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise


    # 確認用csv
    # group_df.to_csv(outpathcsv, encoding='cp932', index=False)
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s4_title_processing"]))
