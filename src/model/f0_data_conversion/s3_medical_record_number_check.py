# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import numpy as np
import os
import glob


def call(params, logger):
    """
    カルテ番号のクレンジング
    ※前処理ツール（aireceipt_receipt_pp.py）で実施したため不要
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s3_medical_record_number_check"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 1]))
        raise Exception
    dir = files[0]
    if not os.path.exists(dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [dir]))
        raise Exception
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 1]))
        raise Exception
    columns1 = columns[0]
    if "digits" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["digits"]))
        raise Exception
    digits = params["digits"]
    # 型チェック
    if not type(digits) is int:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003001", [digits]))  # 型が int でないエラー
        raise Exception


    dirs = glob.glob(dir + "/*")
    for dir in dirs:
        for kubun in ['ika', 'dpc']:
            files = glob.glob(dir + '/' + kubun + "/*")
            for path in files:
                df = pd.read_csv(path, engine='python', dtype='object', encoding="cp932")

                # AIR003003 columns1[0]がないとき エラー
                if columns1[0] not in df.columns:
                    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", [columns1[0]]))
                    raise Exception
                else:
                    # カルテ番号桁数
                    df[columns1[0]] = df[columns1[0]].str[0:digits]

                    try:
                        df.to_csv(path, index=False, encoding='cp932')
                    except:
                        import traceback, sys
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [path+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s3_medical_record_number_check"]))
