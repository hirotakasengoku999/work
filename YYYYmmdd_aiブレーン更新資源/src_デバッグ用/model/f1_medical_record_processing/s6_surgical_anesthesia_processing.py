# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30
import pandas as pd
import os
import re

def call(params, logger):
    """
    マスタデータの文章と登録キーワードを読み込み、
    各レコードにどの登録キーワードが含まれているかのマップを作成する。
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s6_surgical_anesthesia_processing"]))

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
    out_dir = files[1]
    data_path1 = files[2]
    if not os.path.exists(data_path1):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [data_path1]))
        raise Exception
    data_path2 = files[3]
    if not os.path.exists(data_path2):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [data_path2]))
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
    if "bunsyo_syubetu" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["bunsyo_syubetu"]))
        raise Exception
    bunsyo_syubetu_list = params["bunsyo_syubetu"]
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    targets = params["targets"]

    # 出力フォルダ作成
    os.makedirs(out_dir, exist_ok=True)

    # カルテ全結合データ読み込み
    try:
        wkdf = pd.read_csv(inpath, engine='python', encoding='cp932', dtype='object',
                        usecols=[columns1[0], columns1[1], columns2[0]] + columns3)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
        raise

    wkdf.set_index([columns1[0], columns1[1]], inplace=True)
    wkdf = wkdf[wkdf[columns2[0]].isin(bunsyo_syubetu_list)]

    # wkdfにデータがある場合
    # wkdfにデータがない場合 出力しない
    if len(wkdf) != 0:

        # 対象の列 をまとめる（複数列対応のため
        s_col = columns2[1]
        wkdf[s_col] = ''
        for target_col in columns3:
            wkdf[s_col] = wkdf[s_col].str.cat([wkdf[target_col].fillna('')], sep=' ')

        # file_cate毎にファイルを読み込み
        for c1 in [data_path1, data_path2]:
            for c3 in targets:
                tmp_df = wkdf.copy()
                # /data/surgical_anesthesia
                word_file = "{}/{}.csv".format(c1, c3)
                # ファイルがあれば処理
                if os.path.exists(word_file):
                    try:
                        word_df = pd.read_csv(word_file, encoding='cp932', engine='python',usecols=[columns2[1],columns2[2]])
                    except UnicodeDecodeError:
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [word_file]))
                        raise
                    except ValueError as ve:
                        keys = [word_file]
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [keys]))
                        raise
                    except:
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [word_file]))
                        raise

                    # レコードを患者ID、項目を統合キーワードとしてデータフレームを作成
                    res_df = pd.DataFrame(False, index=tmp_df.index, columns=word_df[columns2[1]].unique())

                    for w, k in zip(word_df[columns2[2]], word_df[columns2[1]]):
                        # 登録キーワードが文章に存在する場合は対応する統合キーワードをTrueにする
                        res = tmp_df[s_col].str.contains(w)
                        res_df[k] = res_df[k] | res
                        # 文章から登録キーワードを削除
                        tmp_df[s_col].replace(w, 'xxxxx', regex=True, inplace=True)

                    tmp = res_df.astype(int)
                    out_df = tmp.groupby(level=[0, 1]).sum()

                    outpath=out_dir + c3 + '.csv'
                    try:
                        out_df.to_csv(outpath, encoding='cp932')
                    except:
                        import traceback, sys
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s6_surgical_anesthesia_processing"]))

