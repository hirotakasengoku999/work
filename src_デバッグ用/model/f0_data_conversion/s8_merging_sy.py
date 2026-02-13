# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import re
import os

def call(params, logger):
    '''
    国保と社保をdpc、ikaごとに結合
    '''
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s8_merging_sy"]))

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
    out_dir = files[1]
    input_date = files[2]
    if not os.path.exists(input_date):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [input_date]))
        raise Exception
    input_nyuin = files[3]
    if not os.path.exists(input_nyuin):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [input_nyuin]))
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
    if "rese_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["rese_list"]))
        raise Exception
    rese_list = params["rese_list"]

    s_target_month_l = pd.read_csv(input_date, engine='python',
                                   encoding='cp932', dtype='object').columns.tolist()

    for rese in rese_list:
        new_df = pd.DataFrame()
        for s_target_month in s_target_month_l:
            for kubun in ['dpc', 'ika']:
                for hoken in ['国保', '社保']:
                    inpath = in_dir + kubun + '/' + rese + '/' + hoken + '_' + s_target_month + '.csv'
                    # ファイルがあれば処理
                    if os.path.isfile(inpath):
                        indf = pd.read_csv(inpath, engine='python', encoding='cp932', dtype={columns1[0]: 'object'})
                        indf['__d'] = s_target_month[0:4] + '/' + s_target_month[4:6]  # 年月

                        new_df = pd.concat([new_df, indf])

        outpath=out_dir + rese + '.pickle'
        # この段階で new_df の長さが0場合、warning(からのpickle出力あり)
        if len(new_df) == 0:
            # データが空のため処理をスキップ
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [outpath]))
            out_df = pd.DataFrame(columns=columns2)
        else:
            new_df = new_df.groupby([columns1[0], '__d']).sum()
            new_df[new_df >= 2] = 1
            new_df = new_df.reset_index()

            try:
                df_nyuin = pd.read_csv(input_nyuin, engine='python', encoding="cp932", usecols=columns2,
                                    dtype={columns2[0]: 'object'})
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                continue

            df_nyuin = df_nyuin[df_nyuin[columns2[2]] == 1][[columns2[0], columns2[1]]]
            df_nyuin["__d"] = df_nyuin[columns2[1]].str[0:7]  # 入力データの年月
            out_df = pd.merge(df_nyuin, new_df, on=[columns1[0], '__d'])

            out_df.drop("__d", axis=1, inplace=True)

        try:
            out_df.to_pickle(outpath)
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

        # csv出力する必要なし(確認用)
        # out_df.to_csv(out_dir + 'csv/' + rese + '.csv')
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s8_merging_sy"]))
