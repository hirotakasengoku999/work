# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import datetime
import os
import re

def call(params, logger, heap_dir, in_targets):
    """
    入院中の算定回数をカウント
     対象：'113011710', '190120610','190814010','190128110','113017610', '113017710'
     """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_calculations_count"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 4:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 4]))
        raise Exception
    inpath = files[0]
    if not os.path.exists(inpath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath]))
        raise Exception
    inpath_si = files[1]
    if not os.path.exists(inpath_si):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_si]))
        raise Exception
    out_dir = files[2]

    check_path = heap_dir + files[3]
    if not os.path.exists(check_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [check_path]))
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
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    _target_list = params["targets"]
    if "nyuin_kaisu_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["nyuin_kaisu_list"]))
        raise Exception
    nyuinkaisu_list = params["nyuin_kaisu_list"]

    # _target_list：jsonで指定したtargets、in_targets：predicti_receiptで指定したtargets
    # 共通する算定項目コードを抽出する。共通する算定項目コードがない場合、処理をスキップ。
    __target_list = list(set(_target_list) & set(in_targets))
    if len(__target_list) > 0:

        # 入退院日
        try:
            new_df = pd.read_csv(inpath, engine='python', dtype={columns1[0]: 'object'}, encoding="cp932",
                                usecols=columns1 + [columns2[2], columns2[3]])
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
            raise
        # 日付↔文字列　変換
        try:
            new_df[columns2[2]] = pd.to_datetime(new_df[columns2[2]], format='%Y/%m/%d')
            new_df[columns2[3]] = pd.to_datetime(new_df[columns2[3]], format='%Y/%m/%d')
            new_df[columns2[0]] = new_df[columns2[2]].dt.strftime('%Y/%m/%d')
            new_df[columns2[1]] = new_df[columns2[3]].dt.strftime('%Y/%m/%d')
        except ValueError:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003013", [inpath]))
            raise

        # カルテ番号と入退院日　一覧抽出
        __new_df = new_df[[columns1[0]] + columns2][~new_df.duplicated(subset=[columns1[0]] + columns2)].reset_index(
            drop=True)

        # 診療行為
        df_si = pd.read_pickle(inpath_si)

        # モデルで作成した中間ファイルを読み込み
        try:
            df_nyuin_ago = pd.read_csv(check_path, engine='python', dtype='object', encoding='cp932')
        except:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [check_path]))
            raise
        df_nyuin_ago=df_nyuin_ago.fillna(0)

        for target, nyuin_kaisu in zip(_target_list, nyuinkaisu_list):
            # in_targetsにtargetが含まれていれば実施
            if target in in_targets:
                # 診療行為　target列抽出
                use_col = [columns1[0], columns1[1]] + [target]
                target_df = df_si.loc[:, df_si.columns.intersection(use_col)].reindex(columns=use_col)

                out_df = pd.DataFrame()
                # 1行ずつ確認
                for coli in range(len(__new_df)):

                    # カルテ番号、入院日、退院日を取得
                    karute = __new_df.loc[coli, columns1[0]]
                    start = __new_df.loc[coli, columns2[2]]
                    end = __new_df.loc[coli, columns2[3]]

                    # カルテ番号、入院日、退院日を取得一致する入院期間をnew_dfから抽出
                    date_df = new_df[columns1][
                        (new_df[columns1[0]] == karute) & (new_df[columns2[2]] == start) & (
                                new_df[columns2[3]] == end)].reset_index(drop=True)
                    # 入院中算定回数を入院期間にフラグ1
                    date_df[columns3[2]] = 1

                    tmp_df = pd.merge(target_df, date_df, on=[columns1[0], columns1[1]])
                    tmp_df[target] = tmp_df[target].fillna(0)

                    df_count = tmp_df[tmp_df[target] >= 1]

                    # 入院期間中に算定のある患者であれば、
                    if len(df_count) >= 1:
                        # カルテ番号と入院開始日でチェックリストに算定歴があるか確認
                        check_ago = df_nyuin_ago[
                            (df_nyuin_ago[columns1[0]] == karute) & (
                                    df_nyuin_ago[columns2[0]] == __new_df.loc[coli, columns2[0]])]

                        # ない場合、前月度までの実績zisseki_ago=0
                        if len(check_ago) == 0:
                            zisseki = len(df_count)
                            zisseki_ago = 0

                        # ある場合、前月度までの実績zisseki_ago=N
                        else:
                            zisseki = int(len(df_count) + float(check_ago[target].values[0]))
                            zisseki_ago = int(float(check_ago[target].values[0]))

                        # 入院中1回以上の制限、かつ実績1回の場合、実績ありの日は0それ以外は1
                        if (nyuin_kaisu >= 1) and (zisseki == 1):
                            tmp_df.loc[tmp_df[target] >= 1, columns3[2]] = 0

                        # 入院中1回の制限、かつ実績が1回以上の場合、実績1回目の日は0それ以外は1
                        elif (nyuin_kaisu == 1) and (zisseki > 1):
                            a_df = tmp_df[tmp_df[target] >= 1]
                            a_df = a_df.reset_index(drop=True)
                            a_df.loc[0, columns3[2]] = 0
                            b_df = tmp_df[tmp_df[target] == 0]
                            tmp_df = pd.concat([a_df, b_df])

                        # 入院中2回以上の制限、かつ実績2回以上の場合、実績有りの日に今までの回数、それ以外は実績の値
                        elif (nyuin_kaisu >= 2) and (zisseki >= 2):
                            a_df = tmp_df[tmp_df[target] >= 1]
                            a_df = a_df.reset_index(drop=True)
                            a_df[columns3[2]] = range(0 + zisseki_ago, len(df_count) + zisseki_ago)
                            # 入院中算定回数制限より入院中算定回数の値が大きい場合、入院中制限の値を代入
                            a_df.loc[a_df[columns3[2]] > nyuin_kaisu, columns3[2]] = nyuin_kaisu

                            b_df = tmp_df[tmp_df[target] == 0]
                            b_df = b_df.reset_index(drop=True)
                            b_df.loc[:, columns3[2]] = nyuin_kaisu
                            tmp_df = pd.concat([a_df, b_df])

                    else:
                        # チェックリストに算定歴があるか確認
                        check_ago = df_nyuin_ago[
                            (df_nyuin_ago[columns1[0]] == karute) & (
                                    df_nyuin_ago[columns2[0]] == __new_df.loc[coli, columns2[0]])]

                        # チェックリストに算定歴がある場合、今までの実績値
                        if len(check_ago) != 0:
                            zisseki = check_ago[target].values[0]
                            tmp_df[columns3[2]] = zisseki
                        # チェックリストに算定歴がない場合なにもしない
                        else:
                            continue
                    out_df = pd.concat([out_df, tmp_df])

                out_df = out_df.reset_index(drop=True)
                outpath = out_dir + target + '_入院中算定回数.csv'

                try:
                    out_df.to_csv(outpath, encoding='cp932', index=False)
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_calculations_count"]))
