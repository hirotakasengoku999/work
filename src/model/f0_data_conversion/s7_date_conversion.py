# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import numpy as np
import os
import glob
import re

def call(params, logger):
    """
    指定するファイルの対象の列を日付に変換
    /0_1/に上書き
    ※前処理ツール（aireceipt_receipt_pp.py）で実施したため不要
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s7_date_conversion"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    dir = files[0]
    if not os.path.exists(dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [dir]))
        raise Exception
    mastre_path = files[1]
    if not os.path.exists(mastre_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [mastre_path]))
        raise Exception
    if "rese_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["rese_list"]))
        raise Exception
    rese_list = params["rese_list"]
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 1]))
        raise Exception
    columns1 = columns[0]

    # 和暦とコードの対応表マスター読み込み　読み込めない場合エラー
    try:
        master_df = pd.read_csv(mastre_path, engine='python', usecols=columns1, dtype='object', encoding="cp932")
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
        raise

    # 生年月日を西暦に変換
    def Seinengappi(date_wareki):
        gappi = str(date_wareki)
        # 空の場合
        if gappi == "nan":
            seireki_gappi = "nan"
        else:
            # コードに対応する年を取得
            target_year = master_df[columns1[1]][master_df[columns1[0]] == gappi[0]].values[0]
            year = int(target_year) + int(gappi[1:3])

            seireki_gappi = str(str(year) + '/' + gappi[3:5] + '/' + gappi[5:7])
        return seireki_gappi

    dirs = glob.glob(dir + "/*")
    for dir in dirs:
        for hoken in ['国保', '社保']:
            for kubun, kubun2 in zip(['ika', 'dpc'], ['RECEIPTC', 'RECEIPTD']):
                for rese_code, target_cols in rese_list:
                    path = dir + '/' + kubun + '/' + kubun2 + hoken + '_' + rese_code + '.csv'

                    # ファイルがあれば処理
                    if os.path.isfile(path):
                        # TODO: 性能改善
                        df = pd.read_csv(path, engine='python', dtype='object', encoding="cp932")
                        for target_col in target_cols:
                            try:
                                df[target_col] = df[target_col].apply(lambda x: Seinengappi(x))
                            except KeyError as ke:
                                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", ke.args))
                                raise

                        try:
                            df.to_csv(path, index=False, encoding='cp932')
                        except:
                            import traceback, sys
                            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [path+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                            raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s7_date_conversion"]))
