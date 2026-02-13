# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import re
import pandas as pd
import os
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta


def call(params, logger, pattern):
    """
    N月DPC　REの患者一覧の中に、N-1月のSYの記録がある患者がいれば、N月DPCに上書き
    予測の場合、前月に加工したものをどこかに格納しておく。
    傷病　SYとカルテ情報 REを結合
    日付　カルテ番号　傷病1/0 のデータに加工
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s7_last_month_sy"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 4:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 4]))
        raise Exception
    in_dir = files[0]
    if not os.path.exists(in_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_dir]))
        raise Exception
    in_dir_sy = files[1]
    if not os.path.exists(in_dir_sy):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_dir_sy]))
        raise Exception
    out_dir = files[2]
    input_date = files[3]
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

    # 予測とモデルで異なる
    if pattern == 1:
        loop = 0
    elif pattern == 0:
        loop = 1

    for rese in rese_list:
        for s_target_month in s_target_month_l:
            loop = loop + 1

            if loop != 1:
                try:
                    target_month = dt.strptime(s_target_month, '%Y%m')
                except ValueError:
                    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003013", [input_date]))
                    raise

                one_month_ago = target_month - relativedelta(months=1)
                s_target_month_one_ago = one_month_ago.strftime('%Y/%m/%d').replace('/', '')[0:6]

                for hoken in ['国保', '社保']:
                    # 出力パス
                    outpath = out_dir + 'dpc/' + rese + '/' + hoken + '_' + s_target_month + '.csv'

                    # 出力フォルダ
                    os.makedirs(out_dir + 'dpc/' + rese + '/', exist_ok=True)

                    # N月DPC患者がN-1月医科患者に存在した場合、N-1月の医科の傷病記録を引き継ぐ

                    # N月DPC)RE
                    inFile_str_dpc = in_dir + '/' + s_target_month + '/dpc/RECEIPTD' + hoken + '_RE.csv'

                    # ファイルがなければ処理をスキップ
                    if not os.path.isfile(inFile_str_dpc):
                        continue

                    try:
                        dpc_karutelist = pd.read_csv(inFile_str_dpc, engine='python', usecols=columns1, encoding='cp932',
                                                    dtype={columns1[0]: 'object'})[columns1[0]].tolist()
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
                        raise

                    # N-1月ika,DPC)SY
                    inFile_str_ika = in_dir_sy + 'dpc/' + rese + '/' + hoken + '_' + s_target_month_one_ago + '.csv'
                    inFile_str_dpc = in_dir_sy + 'ika/' + rese + '/' + hoken + '_' + s_target_month_one_ago + '.csv'
                    if os.path.isfile(inFile_str_ika):
                        df_N_1_ika_re = pd.read_csv(inFile_str_ika, engine='python', encoding='cp932',
                                                    dtype={columns1[0]: 'object'})
                        ika_karutelist = df_N_1_ika_re[columns1[0]].tolist()
                    else:
                        df_N_1_ika_re = pd.DataFrame(columns=[columns1[0]])
                        ika_karutelist = []

                    if os.path.isfile(inFile_str_dpc):
                        df_N_1_dpc_re = pd.read_csv(inFile_str_dpc, engine='python',
                                                    encoding='cp932', dtype={columns1[0]: 'object'})
                        dpc_n_1_karutelist = df_N_1_dpc_re[columns1[0]].tolist()
                    else:
                        df_N_1_dpc_re = pd.DataFrame(columns=[columns1[0]])
                        dpc_n_1_karutelist = []

                    df_N_1 = pd.concat([df_N_1_ika_re, df_N_1_dpc_re])

                    n_1_karute = list(set(ika_karutelist) | set(dpc_n_1_karutelist))
                    dpc_ika_list = list(set(n_1_karute) & set(dpc_karutelist))
                    new_df_N_1_ika_re = df_N_1[df_N_1[columns1[0]].isin(dpc_ika_list)]

                    # N月DPC)SY
                    inFile_str_dpc_2 = in_dir_sy + 'dpc/' + rese + '/' + hoken + '_' + s_target_month + '.csv'
                    if os.path.isfile(inFile_str_dpc_2):
                        df_N_dpc_sy = pd.read_csv(inFile_str_dpc_2, engine='python',
                                                  encoding='cp932', dtype={columns1[0]: 'object'})
                    else:
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [inFile_str_dpc_2]))
                        continue

                    new_df = pd.concat([new_df_N_1_ika_re, df_N_dpc_sy])

                    new_df_group = new_df.groupby([columns1[0]]).sum()
                    new_df_group[new_df_group >= 1] = 1
                    new_df_group = new_df_group.reset_index()

                    try:
                        new_df_group.to_csv(outpath, index=False, encoding='cp932')
                    except:
                        import traceback, sys
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s7_last_month_sy"]))
