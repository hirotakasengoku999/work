# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os


def call(params, logger):
    '''
    傷病　SYとカルテ情報 REを結合
    日付　カルテ番号　傷病1/0 のデータに加工
    '''
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s6_sy_rese"]))

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
    out_dir = files[1]
    input_date = files[2]
    if not os.path.exists(input_date):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [input_date]))
        raise Exception
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 1]))
        raise Exception
    columns1 = columns[0]
    if "rese_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["rese_list"]))
        raise Exception
    rese_list = params["rese_list"]

    # date
    s_target_month_l = pd.read_csv(input_date, engine='python',
                                   encoding='cp932', dtype='object').columns.tolist()
    # 月毎に処理
    for s_target_month in s_target_month_l:
        # 区分毎に処理
        for kubun, kubun2 in zip(['ika', 'dpc'], ['RECEIPTC', 'RECEIPTD']):
            # 国社
            for hoken in ['社保', '国保']:
                for rese, rese_code, resekubun in rese_list:

                    # 出力フォルダ
                    name_2 = out_dir + kubun + '/' + rese + '/'
                    outpath = name_2 + hoken + '_' + s_target_month + '.csv'
                    os.makedirs(name_2, exist_ok=True)

                    # SY パス
                    inFile_str = in_dir + '/' + s_target_month + '/' + kubun + '/' + kubun2 + hoken
                    inpath_sy = inFile_str + '_' + resekubun + '.csv'

                    # ファイルチェック：なければWARNIG、skip（出力されない）
                    if not os.path.isfile(inpath_sy):
                        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [inpath_sy]))
                        continue

                    try:
                        df_sy = pd.read_csv(inpath_sy, engine='python', encoding='cp932'
                                            , usecols=[columns1[0], columns1[1], rese_code]
                                            , dtype=({columns1[0]: 'object', rese_code: 'str'}))
                    except KeyError as ke:
                        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", ke.args))
                        raise

                    df_index = df_sy.set_index([columns1[0]])

                    if kubun == 'ika':
                        df_index = df_index.dropna(subset=[columns1[1]])

                    dummy = pd.get_dummies(df_index[rese_code])

                    new_df_group = dummy.groupby(level=0).sum()
                    new_df_group[new_df_group >= 1] = 1

                    try:
                        new_df_group.to_csv(outpath, encoding='cp932')
                    except:
                        import traceback, sys
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" +traceback.format_exception(*sys.exc_info())[-1]]))
                        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s6_sy_rese"]))
