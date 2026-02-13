# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re


def call(params, logger, option_key, model_path, model_flag, in_targets):
    """
    個別対応結合
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["merging_option"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    option_path = files[1]
    if not os.path.exists(option_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [option_path]))
        raise Exception
    nyuin_path = files[2]
    if not os.path.exists(nyuin_path):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [nyuin_path]))
        raise Exception
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 1]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    target_list = params["targets"]
    if "option_pattern" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["option_pattern"]))
        raise Exception
    option_pattern = params["option_pattern"]
    if "items" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["items"]))
        raise Exception
    items = params["items"]

    # モデルの場合と予測の場合
    if model_flag == 1:
        dir = model_path + files[0]
    elif model_flag == 0:
        dir = files[0]
        target_list = list(set(target_list) & set(in_targets))
        if len(target_list) == 0:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["merging_option"]))
            # s_merging_option.pyの処理終了
            return 0

    if not os.path.exists(dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [dir]))
        raise Exception

    ### 個別関数
    # monthly 月でグルーピング
    def Month(df):
        df[columns1[1]] = df[columns1[1]].str[0:7]
        drop_col = [columns1[0], columns1[1]]

        if items == "max":
            out_df = df.groupby(drop_col, as_index=False).max()
        else:
            week_df_group = df.groupby(drop_col, as_index=False).sum()
            week_df_1 = week_df_group.loc[:, drop_col]
            week_df_2 = week_df_group.drop(drop_col, axis=1)
            week_df_2[week_df_2 >= 1] = 1
            out_df = pd.concat([week_df_1, week_df_2], axis=1)
        return out_df

    # weekly 週でグルーピング（日～土） #修正必要　年区別できていない
    def Week(df):
        df['index_dt'] = pd.to_datetime(df[columns1[1]])

        # 年
        df['YY'] = df[columns1[1]].str[0:4]  # 年の情報
        df['WW'] = (df['index_dt'] + pd.DateOffset(days=1)).dt.week  # 第何週か
        df['YOUBI'] = df['index_dt'].dt.dayofweek  # 曜日の情報
        # {0: '月', 1: '火', 2: '水', 3: '木', 4: '金', 5: '土', 6: '日'}

        # 週の最終日の日付取得
        sunday = df[['YY', 'WW', 'YOUBI', columns1[1]]].groupby(['WW', 'YY'], as_index=False).max()
        sunday = sunday[~sunday.duplicated()]

        # 第何週,年,カルテ番号でグルーピング
        if items=="max":
            week_df_group = df.groupby([columns1[0], 'YY', 'WW'], as_index=False).max()
        else:
            week_df_group = df.groupby([columns1[0], 'YY', 'WW'], as_index=False).sum()
        week_df_group.drop(columns1[1], axis=1, inplace=True, errors='ignore')

        week_df_group = pd.merge(week_df_group, sunday[[columns1[1], 'WW', 'YY']], on=['WW', 'YY'], how='left')
        week_df_group.drop(['WW', 'YOUBI', 'YY'], axis=1, inplace=True)

        if items == "max":
            out_df = week_df_group
        else:
            week_df_1 = week_df_group[[columns1[0], columns1[1]]]
            week_df_2 = week_df_group.drop([columns1[0], columns1[1]], axis=1)
            week_df_2[week_df_2 >= 1] = 1
            out_df = pd.concat([week_df_1, week_df_2], axis=1)
        return out_df

    # 入院期間中でまとめる calculation
    # para 初期値＝入院期間中　数値指定した場合入院初日からpara日
    def NyuinkikanChu(df):
        # 入院期間のデータフレーム
        try:
            df_in = pd.read_csv(nyuin_path, engine='python', encoding='cp932', dtype={columns1[0]: 'object'}, usecols=[columns1[0], columns1[1], columns2[2], columns2[3]])
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
            raise

        # 日付↔文字列　変換
        try:
            df_in[columns2[2]] = pd.to_datetime(df_in[columns2[2]], format='%Y/%m/%d')
            df_in[columns2[3]] = pd.to_datetime(df_in[columns2[3]], format='%Y/%m/%d')
            df_in[columns2[0]] = df_in[columns2[2]].dt.strftime('%Y/%m/%d')
            df_in[columns2[1]] = df_in[columns2[3]].dt.strftime('%Y/%m/%d')
        except ValueError:
            l = lambda x: x[0](x[1]);
            l(logger.format_log_message("AIR003013", [nyuin_path]))
            raise

        # カルテ番号と入退院日　一覧抽出
        __new_df = df_in[[columns1[0]] + columns2][~df_in.duplicated(subset=[columns1[0]] + columns2)].reset_index(drop=True)

        # 1行ずつ確認
        out_df=pd.DataFrame(columns=df.columns)
        for coli in range(len(__new_df)):

            # カルテ番号、入院日、退院日を取得
            karute = __new_df.loc[coli, columns1[0]]
            startstr = __new_df.loc[coli, columns2[0]]
            start = __new_df.loc[coli, columns2[2]]
            end = __new_df.loc[coli, columns2[3]]

            # カルテ番号、入院日、退院日を取得一致する入院期間を df_in から抽出
            date_df = df_in[[columns1[0], columns1[1]]][(df_in[columns1[0]] == karute) & (df_in[columns2[2]] == start) & (df_in[columns2[3]] == end)].reset_index(drop=True)
            fill_df = pd.merge(date_df, df, on=[columns1[0], columns1[1]])

            # 長さチェック
            if len(fill_df) == 0:
                continue

            group_df = fill_df.fillna(0)

            if items == "max":
                group_df = group_df.groupby([columns1[0]]).max()
            else:
                group_df = group_df.groupby([columns1[0]]).sum()
                group_df[group_df >= 1] = 1

            group_df = group_df.reset_index()
            group_df[columns1[1]] = startstr

            out_df.loc[coli, :] = group_df.loc[0, :]

        return out_df

    # 個別対応ファイル読み込み
    # 指定の列のみ読み込み
    if option_key in ["1", "2", "4", "7", "8"]:

        # optionCSV
        try:
            option_df = pd.read_csv(option_path, engine='python', encoding="cp932",
                                    usecols=columns1, dtype={columns1[0]: 'object'})
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
            # s_merging_option.pyの処理終了
            return 0

    # すべての列を読み込み
    elif option_key in ["3", "5", "9", "10"]:

        option_df = pd.read_csv(option_path, engine='python', encoding="cp932",
                                dtype={columns1[0]: 'object'})
    else:
        return 0

    for target,option in zip(target_list,option_pattern):

        if option == 'monthly':
            option_df = Month(option_df)
        elif option == 'weekly':
            option_df = Week(option_df)
        elif option == 'calculation':
            option_df = NyuinkikanChu(option_df)


        # 特徴量　読み込み
        in_pickle = dir + "/" + target + '.pickle'
        # ファイルがない場合　target skip
        if not os.path.exists(in_pickle):
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [in_pickle]))
            continue

        df = pd.read_pickle(in_pickle)

        if option_key == "7":
            # 入院期間をベースにhow=left
            if len(option_df) == 0:
                out_df = df.copy()
            else:
                out_df = pd.merge(option_df, df, on=[columns1[0], columns1[1]], how='left', copy=False)
        else:
            # 全期間 how="left"
            out_df = pd.merge(df, option_df, on=[columns1[0], columns1[1]], how='left', copy=False)

        # 確認用
        # out_df.to_csv(dir + 'csv/' + target + '.csv')

        # 出力エラーチェック
        outpath=dir + "/" + target + '.pickle'
        try:
            out_df.to_pickle(outpath)
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["merging_option"]))
