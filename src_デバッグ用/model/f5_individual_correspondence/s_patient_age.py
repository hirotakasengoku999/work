# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import re
import os
import pandas as pd
import glob


def call(params, logger):
    """
    患者年齢 抽出
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_patient_age"]))

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
    si_path = files[1]
    if not os.path.exists(si_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [si_path]))
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

    # 出力フォルダ作成
    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    # 結合用データフレーム
    df_new = pd.DataFrame()

    # karuteCSV:カルテ番号、日付一覧
    try:
        df_si = pd.read_csv(si_path, engine='python', usecols=[columns1[0], columns1[1]], dtype='object', encoding="cp932")
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    # 日付のディレクトリ読み込み
    dirs_list = glob.glob(in_dir + "/*")
    for dirs in dirs_list:
        # dpc/ikaのディレクトリ読み込み
        dirs_list2 = glob.glob(dirs + "/*")
        for dir in dirs_list2:
            # RE.csvファイル読み込み
            filepath_list = glob.glob(dir + "/*_RE.csv")
            for inpath in filepath_list:

                # reCSV:columnsミスのデータフレーム読み込み失敗　エラー
                try:
                    df = pd.read_csv(inpath, engine='python', dtype='object',
                                    usecols=[columns2[0], columns2[1]], encoding="cp932")
                except ValueError as ve:
                    keys = []
                    for arg in ve.args:
                        keys.extend(re.findall(r"\'(.*)\'", arg))
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
                    raise

                # 結合
                df_new = pd.concat([df_new, df])

    if len(df_new) != 0:
        df_new = df_new[~df_new.duplicated()]

        # 生年月日が空白の患者を抽出、空白の場合は年齢を-100
        _drop = df_new[df_new[columns2[1]] == "nan"]
        _drop['年齢'] = -100
        # _df_si_drop：生年月日が空白の患者の年齢を-100にしたデータフレーム
        _df_si_drop = pd.merge(df_si, _drop, on=[columns1[0]])

        # 空白でない場合、西暦に変換
        _df_new = df_new[df_new[columns2[1]] != "nan"]
        df_new_w = _df_new.reset_index(drop=True)

        # 重複削除
        df_new_drop = df_new_w[~df_new_w.duplicated()]
        _df_si = pd.merge(df_si, df_new_drop, on=[columns1[0]])

        # 日付と生年月日から年齢を計算
        _df_si['index_2'] = _df_si[columns1[1]].str.replace('/', '')
        _df_si['生年月日_西暦_2'] = _df_si[columns2[1]].str.replace('/', '')
        # (20200714-19940926)/10000=年齢
        _df_si['年齢'] = ((_df_si['index_2'].astype('float') - _df_si['生年月日_西暦_2'].astype('float')) / 10000).astype('int')

        # 結合
        df_age = pd.concat([_df_si_drop, _df_si])

        # 個別の処理
        # # -1を0に上書き
        # columns1= ["カルテ番号等", "_date", "年齢"]
        df_age.loc[df_age["年齢"] <= -1, "年齢"] = 0
        df_age = df_age[~df_age.duplicated(keep='last', subset=[columns1[0], columns1[1]])]

    else:
        df_age=pd.DataFrame(columns=[columns1[0], columns1[1], "年齢"])

    try:
        df_age.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise



    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_patient_age"]))
