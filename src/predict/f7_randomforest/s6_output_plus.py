# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re

def call(params, logger, out_dir, in_targets):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s6_output_plus"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 1]))
        raise Exception
    inpath = files[0]
    if not os.path.exists(inpath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath]))
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
    _target_list = params["targets"]
    if "outname" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["outname"]))
        raise Exception
    outname = params["outname"]
    if "outcolumns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["outcolumns"]))
        raise Exception
    outcolumns = params["outcolumns"]

    # 共通する算定項目コードを抽出する。
    target_list = list(set(_target_list) & set(in_targets))

    if target_list != 0:

        # inpathの拡張子取得
        root, ext = os.path.splitext(inpath)
        use_col = columns1 + columns2

        # 読み込み
        if ext=='.pickle':
            indf_m = pd.read_pickle(inpath)
            try:
                # カルテ番号、_date,targetを読み込み
                indf_im = indf_m[use_col]
            except KeyError as ke:
                l = lambda x: x[0](x[1]);
                l(logger.format_log_message("AIR002004", ke.args))
                return 0

        elif ext == ".csv":
            try:
                indf_im = pd.read_csv(inpath, engine='python', dtype='object', usecols=use_col)
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                return 0

        for target in target_list:

            # 結果読み込み
            path = out_dir + '/' + target + '.csv'

            # result読み込み　入力チェック
            if not os.path.exists(path):
                # データがないため、処理をスキップします
                l = lambda x: x[0](x[1]);
                l(logger.format_log_message("AIR002002", [path]))
                # 算定項目は実行されません
                l = lambda x: x[0](x[1]);
                l(logger.format_log_message("AIR002001", [target, "targets", target]))
                continue

            # カルテ番号、_date,予測値、target、colistを読み込み
            in_result = pd.read_csv(path, engine='python', dtype='object')

            # マージ
            out_df = pd.merge(in_result, indf_im, on=[columns1[0], columns1[1]],how="left")
            # 出力用の列に格納
            out_df.loc[:, outname[0]] = out_df[columns2[0]]

            out_df = out_df.loc[:, out_df.columns.intersection(outcolumns)].reindex(columns=outcolumns)

            # 出力エラーチェック
            try:
                out_df.to_csv(path, index=False, encoding='cp932')
            except:
                import traceback, sys
                l = lambda x: x[0](x[1]);
                l(logger.format_log_message("AIR003009", [path+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                raise
    # 終了ログ
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s6_output_plus"]))
