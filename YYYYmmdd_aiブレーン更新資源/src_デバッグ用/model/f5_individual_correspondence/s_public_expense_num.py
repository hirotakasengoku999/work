# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30
import pandas as pd
import glob
import os
import re


def call(params, logger):
    """
    KO 負担者番号を結合
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_public_expense_num"]))

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
    inpath_si = files[1]
    if not os.path.exists(inpath_si):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_si]))
        raise Exception
    outpath = files[2]
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 2]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]

    out_new_df = pd.DataFrame()

    # 日付のディレクトリ読み込み
    dirs_list = glob.glob(in_dir + "/*")
    for dirs in dirs_list:
        # dpc/ikaのディレクトリ読み込み
        dirs_list2 = glob.glob(dirs + "/*")
        for dir in dirs_list2:
            # KO.csvファイル読み込み
            filepath_list = glob.glob(dir + "/*_KO.csv")
            for ko_path in filepath_list:
                # koCSV:KO読み込み
                try:
                    ko_df = pd.read_csv(ko_path, engine='python', dtype='object', encoding="cp932",
                                        usecols=[columns1[0], columns2[0]])
                except ValueError as ve:
                    keys = []
                    for arg in ve.args:
                        keys.extend(re.findall(r"\'(.*)\'", arg))
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
                    raise

                # 年月の情報
                ko_df["__date_month"] = str(dirs).split('\\')[-1]
                out_new_df = pd.concat([out_new_df, ko_df])

    if len(out_new_df) != 0:
        # 重複削除
        df_drop_dup = out_new_df.drop_duplicates()
        df_drop_dup=df_drop_dup.reset_index()
        # 負担者番号columns2[0]の頭２ケタを抽出し列作成
        df_drop_dup.loc[:, '_ko_2桁'] = df_drop_dup[columns2[0]].str[0:2]
        # '_ko_2桁'が51,54のレコードを抽出（値固定）
        target_df = df_drop_dup[df_drop_dup['_ko_2桁'].isin(['51', '54'])]

        target_df_rest_index = target_df.reset_index(drop=True)
        # 負担者番号のれ湯を削除
        target_df_rest_index = target_df_rest_index.drop([columns2[0]], axis=1)
        # 重複削除
        target_df_rest_index = target_df_rest_index.drop_duplicates()

        # ダミーデータ作成
        dummy_df = target_df_rest_index.set_index([columns1[0], "__date_month"])
        dummy_df = pd.get_dummies(dummy_df['_ko_2桁'])
        dummy_df = dummy_df.rename(columns=lambda s: str(f'{columns2[0]}_' + s))
        out_df = dummy_df.reset_index()

        # カルテ番号/日付一覧取得
        si_df = pd.read_csv(inpath_si, engine='python', dtype={columns1[0]: 'object'}, encoding="cp932")
        si_df["__date_month"] = si_df[columns1[1]].str[:7].str.replace('/', '')
        # マージ
        mer_df = si_df.merge(out_df, how='left', on=[columns1[0], "__date_month"])

        # 不要列削除
        mer_df_drop = mer_df.drop(["__date_month"], axis=1)
    else:
        mer_df_drop = pd.DataFrame(columns=[columns1[0], columns1[0], columns2[0] + "_51", columns2[0] + "_54"])

    try:
        mer_df_drop.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise


    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_public_expense_num"]))
