# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import re
import os


def call(params, logger, model_path, model_flag, in_targets):
    """
    特別食加算の電子カルテオーダのフラグを修正
    入院期間中、退院まで連続でフラグを付与
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_special_meal_flag"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    inpath_nyuin = files[1]
    if not os.path.exists(inpath_nyuin):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_nyuin]))
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
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    target_list = params["targets"]
    if "bunsyo" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["bunsyo"]))
        raise Exception
    bunsyo = params["bunsyo"]

    # モデルの場合と予測の場合
    if model_flag == 1:
        dir = model_path + files[0]
    elif model_flag == 0:
        dir = files[0]
        target_list = list(set(target_list) & set(in_targets))
        if len(target_list) < 0:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_special_meal_flag"]))
            return 0

    if not os.path.exists(dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [dir]))
        raise Exception

    # nyuinCSV:入退院
    try:
        df_nyuin = pd.read_csv(inpath_nyuin, engine='python', usecols=[columns2[0], columns2[1], columns1[2], columns1[3]],
                            dtype={columns1[0]: 'object'}, encoding="cp932")
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        return 0

    # 日付↔文字列　変換
    df_nyuin[columns1[2]] = pd.to_datetime(df_nyuin[columns1[2]], format='%Y/%m/%d')
    df_nyuin[columns1[3]] = pd.to_datetime(df_nyuin[columns1[3]], format='%Y/%m/%d')
    df_nyuin[columns1[0]] = df_nyuin[columns1[2]].dt.strftime('%Y/%m/%d')
    df_nyuin[columns1[1]] = df_nyuin[columns1[3]].dt.strftime('%Y/%m/%d')

    # カルテ番号と入退院日　一覧抽出
    __new_df = df_nyuin[[columns2[0]] + columns1][~df_nyuin.duplicated(subset=[columns2[0]] + columns1)].reset_index(
        drop=True)

    for target in target_list:
        # 特徴量　読み込み
        in_pickle = dir + "/" + target + '.pickle'
        # ファイルがない場合　target skip
        if not os.path.exists(in_pickle):
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [in_pickle]))
            continue

        df = pd.read_pickle(in_pickle)

        # kyusyokuCSV
        try:
            df_kyusyoku = df[[columns2[0], columns2[1], target]]
        except KeyError as ke:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002004", ke.args))
            continue

        df_D001 = df.drop(target, axis=1)

        # フラグ修正
        out_df = pd.DataFrame()
        # 1行ずつ確認
        for i in range(len(__new_df)):
            karute = df_nyuin.loc[i, columns2[0]]
            start = df_nyuin.loc[i, columns1[2]]
            end = df_nyuin.loc[i, columns1[3]]

            tmp_df = df_D001[df_D001[columns2[0]] == karute]

            date_df = pd.DataFrame(index=[pd.to_datetime(start), pd.to_datetime(end)])
            date_df['入院フラグ'] = 1
            date_df = date_df.resample('D').mean()
            date_df['入院フラグ'] = 1
            date_df = date_df.reset_index()
            date_df[columns2[1]] = date_df['index'].astype('str').str[0:10].str.replace('-', '/')

            df_karute = pd.merge(tmp_df, date_df, on=[columns2[1]])

            df_1 = df_karute[[columns2[0], columns2[1]]]
            df_2 = df_karute.drop([columns2[0], columns2[1]], axis=1)

            df_2.loc[df_2[bunsyo] == 1] = df_2.fillna(0)
            df_2_fill = df_2.fillna(method='ffill')

            df_karute_fill = pd.concat([df_1, df_2_fill], axis=1)
            out_df = pd.concat([out_df, df_karute_fill])

        out_df_all = pd.merge(df_kyusyoku, out_df, on=[columns2[0], columns2[1]], how='left')

        # 出力エラーチェック
        outpath=dir + "/" + target + '.pickle'
        try:
            out_df_all.to_pickle(outpath)
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

        # 確認用
        # out_df_all.to_csv(dir + 'csv/' + target + '.csv')
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_special_meal_flag"]))
