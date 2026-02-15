# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re


def call(params, logger, model_flag, in_targets=[]):
    """
    閉鎖４を側臥位と腹腔鏡に区分
    ①閉鎖４（側臥位）モデル　→　閉鎖４（側臥位）の算定が可能かどうか　((閉鎖４（側臥位）以外のレコード削除)
    ②閉鎖４（腹腔鏡）モデル　→　閉鎖４（腹腔鏡）の算定が可能かどうか　((閉鎖４（腹腔鏡）以外のレコード削除)
    閉鎖4(側臥位）と閉鎖4(腹腔鏡）を、別々のコード（150333210_1、150333210_2)
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_by_category"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 3:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 3]))
        raise Exception
    inpath_si = files[0]
    if not os.path.exists(inpath_si):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_si]))
        raise Exception
    out_dir = files[1]
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 2]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    if "outname" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["outname"]))
        raise Exception
    outname = params["outname"]

    # 診療行為.pickle 読み込み
    df_si = pd.read_pickle(inpath_si)

    # 予測の場合、in_targetsに"150333210_1" "150333210_2"（閉鎖4）コードがないと処理をスキップ
    if model_flag == 0:
        # outname：jsonで指定した閉鎖4コード、in_targets：predicti_receiptで指定したtargets
        # 共通する算定項目コードを抽出する。共通する算定項目コードがない場合、処理をスキップ。
        target_list = list(set(outname) & set(in_targets))
        if len(target_list) == 0:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["s_by_category"]))
            return 0
        # 予測の場合読み込めるように
        df_si = df_si.loc[:, df_si.columns.intersection(columns1)].reindex(columns=columns1)

    # モデルの場合読み込めなければ処理を中断
    elif model_flag == 1:
        # columns1[0],columns1[1] targets:columns1[2]
        try:
            df_si = df_si[columns1]
        except KeyError as ke:
            # 読み込めなかったら　モデル作成不可　予測不可 次のソースへ
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", ke.args))
            return 0

    wkpath = files[2]
    if not os.path.exists(wkpath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [wkpath]))
        return 0

    # 手術麻酔系の名詞抽出したデータ読み込み　"../work/model/1_21/150333210.csv"
    #  ["カルテ番号等", "_date", "側臥位", "腹腔鏡"]のみ使用
    # wkCSV
    try:
        df = pd.read_csv(wkpath, engine='python', encoding="cp932",
                        usecols=columns2, dtype={columns2[0]: 'object'})

    # 読み込めなかったら　モデル作成不可　予測不可
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
        return 0

    # 診療行為と手術麻酔系名詞df マージ
    df_tmp = pd.merge(df_si, df, how='left', on=[columns2[0], columns2[1]])
    df_tmp = df_tmp.fillna(0)

    # 腹腔鏡と側臥位のフラグ
    df_tmp['フラグ'] = df_tmp[[columns2[2], columns2[3]]].sum(axis=1)

    # 腹腔鏡の場合、側臥位の実績があるレコードと腹腔鏡/側臥位のどちらにも区分できない閉鎖４の実績のあるレコードを削除
    # 側臥位の場合、腹腔鏡の実績があるレコードと腹腔鏡/側臥位のどちらにも区分できない閉鎖４の実績のあるレコードを削除
    def Kakou(dropcol, usecol, name):
        out_df = df_tmp[~((df_tmp[dropcol] >= 1) & (df_tmp['フラグ'] == 1))]

        out_tmp1 = out_df[out_df[usecol] >= 1]
        out_tmp2 = out_df[~((out_df[usecol] >= 1) | ((out_df[usecol] == 0) & (out_df[columns1[2]] > 0)))]

        out_df = pd.concat([out_tmp1, out_tmp2])
        out_df.loc[out_df[columns1[2]] > 0, columns1[2]] = 1
        out_df = out_df.rename(columns={columns1[2]: name})

        return out_df

    out_syuzyutu = Kakou(columns2[2], columns2[3], outname[1])
    out_taii = Kakou(columns2[3], columns2[2], outname[0])

    # 側臥位、腹腔鏡それぞれ出力
    outpath_1 = out_dir + outname[0] + '.csv'
    outpath_2 = out_dir + outname[1] + '.csv'
    try:
        out_taii.to_csv(outpath_1, index=False, encoding='cp932')  # 側臥位
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath_1+ ":" +traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    try:
        out_syuzyutu.to_csv(outpath_2, index=False, encoding='cp932')  # 腹腔鏡
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath_2+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_by_category"]))
