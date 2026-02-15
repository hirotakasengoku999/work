# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30
import pandas as pd
from src.model.my_package import text_structure as ts
import os
import re

def call(params, logger):
    """
    トリアージレベル　個別対応
     """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["k_triage_level"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    inpath = files[0]
    if not os.path.exists(inpath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath]))
        raise Exception
    outpath = files[1]
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 2]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    if "items" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["items"]))
        raise Exception
    item = params["items"]

    # トリアージレベル抽出
    def Toriakakou(wkdf, col):
        items = [item]
        label_name_list = [col]
        # 構造パターン
        pattern = r"【.+?】"
        df_trilevel = wkdf[[columns1[0], columns1[1], col]]
        df_trilevel = df_trilevel.dropna(how='any')

        # item(【トリアージレベル】)の文字列を含むレコードを抽出
        df_trilevel_new = df_trilevel[df_trilevel[col].str.contains(item)]
        df_trilevel_new.reset_index(inplace=True, drop=True)

        # df_trilevel_newのレコード数が0より大きい場合
        if len(df_trilevel_new) != 0:
            # テンプレートテキストを構造化、itemsの内容を抽出
            for x, lbl in enumerate(label_name_list):
                # カルテ内容
                texts = df_trilevel_new[lbl].tolist()
                # 結果格納用作成データフレーム
                result = pd.DataFrame(index=items)
                for i, text in enumerate(texts):
                    sep_text = ts.text_separete_by_items(text, items, pattern)
                    sep_text.name = 1
                    result[i] = sep_text
                result = result.T
                # reslut結合
                df_trilevel_new = pd.concat([df_trilevel_new, result], axis=1)

            # 数字を抽出
            # df_trilevel_new['トリアージレベル'] = df_trilevel_new[item].str.extract(r'(\d*)', expand=False)
            df_trilevel_new['トリアージレベル']= df_trilevel_new[item].apply(lambda x: re.findall(r"\d+", x)[0] if len(re.findall(r"\d+", x))>0 else '')

            df_trilevel_new = df_trilevel_new[[columns1[0], columns1[1], 'トリアージレベル']]
        else:
            df_trilevel_new = pd.DataFrame(columns=[columns1[0], columns1[1], 'トリアージレベル'])

        return df_trilevel_new

    # karuteCSV:カルテ読み込み
    try:
        wkdf = pd.read_csv(inpath, engine='python', encoding='cp932', dtype='object',
                        usecols=[columns1[0], columns1[1]] + columns2)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    # columns2　空白レコード削除
    wkdf = wkdf.dropna(subset=columns2, how='all')

    df_trilevel_new = pd.DataFrame()
    for target_col in columns2:
        wkdf_new = Toriakakou(wkdf, target_col)
        df_trilevel_new = pd.concat([df_trilevel_new, wkdf_new])

    # トリアージレベルが0より大きい場合、'トリアージレベル_フラグ'を1
    df_trilevel_new['トリアージレベル_フラグ'] = 0
    df_trilevel_new.loc[df_trilevel_new['トリアージレベル'] == '', 'トリアージレベル'] = 0
    df_trilevel_new.loc[df_trilevel_new['トリアージレベル'] != 0, 'トリアージレベル_フラグ'] = 1

    out_df = df_trilevel_new[~df_trilevel_new.duplicated()]

    try:
        out_df.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["k_triage_level"]))
