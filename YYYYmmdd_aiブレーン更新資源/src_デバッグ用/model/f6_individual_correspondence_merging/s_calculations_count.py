# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re


def call(params, logger, model_path, model_flag, in_targets):
    """
    入院中の算定回数を特徴量ごとに結合
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_calculations_count"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 3:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 3]))
        raise Exception
    indir_nyuin = files[1]
    if not os.path.exists(indir_nyuin):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [indir_nyuin]))
        raise Exception
    basepath = files[2]
    if not os.path.exists(basepath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [basepath]))
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

    # モデルの場合と予測の場合
    if model_flag == 1:
        dir = model_path + files[0]
    elif model_flag == 0:
        dir = files[0]
        target_list = list(set(target_list) & set(in_targets))
        if len(target_list) == 0:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_calculations_count"]))
            return 0

    if not os.path.exists(dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [dir]))
        raise Exception

    # baseCSV:入院期間取得
    try:
        base_df = pd.read_csv(basepath, engine='python', encoding="cp932"
                            , dtype={columns2[0]: 'object'}, usecols=columns2)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
        return 0

    for target in target_list:

        # 特徴量　読み込み
        in_pickle = dir + "/" + target + '.pickle'
        # ファイルがない場合　target skip
        if not os.path.exists(in_pickle):
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [in_pickle]))
            continue

        df = pd.read_pickle(in_pickle)

        # 個別対応　入院中算定回数
        # 対象：入院期間中のみ
        inpath_nyuin = indir_nyuin + target + '_入院中算定回数.csv'

        # ファイルが存在する場合、読み込みと結合処理
        if os.path.exists(inpath_nyuin):
            # nyuinCSV
            try:
                indf = pd.read_csv(inpath_nyuin, engine='python', encoding="cp932"
                                , dtype={columns1[0]: 'object'}, usecols=columns1)
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                continue

            indf_drop = indf.drop_duplicates()
            _base_df = pd.merge(base_df, indf_drop, on=[columns2[0], columns2[1]], how='left', copy=False)
            # 入院期間のみのため、_base_dfに対してhow=left
            new_df = pd.merge(_base_df, df, on=[columns2[0], columns2[1]], how='left', copy=False)
        else:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [inpath_nyuin]))
            continue

        # 出力エラーチェック
        outpath=dir + "/" + target + '.pickle'
        try:
            new_df.to_pickle(outpath)
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

        # 確認用
        # new_df.to_csv(dir + target + '.csv')
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_calculations_count"]))
