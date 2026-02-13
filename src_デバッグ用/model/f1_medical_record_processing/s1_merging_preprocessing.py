# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import glob
from src.model.my_package import text_processing as tp


def call(params, logger, karute_dir):
    """
    電子カルテ　全結合
    """
    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001001", ["s1_merging_processing"]))

    if "files" not in params:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 2:
        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    outpath = files[0]
    wkpath = files[1]
    if not os.path.exists(wkpath):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [wkpath]))
        raise Exception
    if "columns" not in params:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 4:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003011", ["columns", 4]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    columns3 = columns[2]
    columns4 = columns[3]

    # 出力フォルダ作成
    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    # レセプト 全種類結合.csv
    wk_df = pd.read_csv(wkpath, engine='python', dtype='object')

    # karute_dir 配下のファイル名取得
    folder_path_list = glob.glob(karute_dir + '/*.csv')
    _in_df = pd.DataFrame()
    for inpath in folder_path_list:
        # カルテ
        in_df = pd.read_csv(inpath, engine='python', dtype={columns1[0]: 'object', columns1[1]: 'str', columns2[0]: 'object'}, encoding='cp932')

        non_colum = []
        for target_col in columns1+columns4:
            if target_col not in in_df.columns:
                non_colum.append(target_col)

        if len(non_colum)>0:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [inpath + ": " + ' '.join(non_colum)]))
            raise Exception

        _in_df = pd.concat([_in_df, in_df])

    nw_out_df = pd.merge(wk_df, _in_df,on=columns1)

    if len(nw_out_df)==0:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [karute_dir+" (no merge data)"]))
        raise Exception

    nw_out_df = nw_out_df.fillna('')

    # 文字列統一
    for target in columns3:
        # 列の有無チェック
        if target in nw_out_df.columns:
            nw_out_df[target] = [tp.process_pattern3(cr) for cr in nw_out_df[target]]
        else:
            # 設定ファイルに設定されたキー({})がデータに存在しません。"
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", target))
            # 列がなければnw_out_df[targets]空作成 ""
            nw_out_df[target] = ""

    try:
        nw_out_df.to_csv(outpath, encoding='cp932', index=False)
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["s1_merging_processing"]))
