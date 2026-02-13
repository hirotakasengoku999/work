# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import re
import os


def call(params, logger):
    """
        JCSレベル抽出　個別対応
    """
    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001001", ["s_JCS_level"]))

    if "files" not in params:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 4:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003011", ["files", 4]))
        raise Exception
    inpath = files[0]
    if not os.path.exists(inpath):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [inpath]))
        raise Exception
    inpath_rese = files[1]
    if not os.path.exists(inpath_rese):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [inpath_rese]))
        raise Exception
    masterpath = files[2]
    if not os.path.exists(masterpath):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [masterpath]))
        raise Exception
    outpath = files[3]
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
    if "jcs_list" not in params:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003010", ["jcs_list"]))
        raise Exception
    kubun_list = params["jcs_list"]

    # karuteCSV:カルテ読み込み
    try:
        wkdf = pd.read_csv(inpath, engine='python', encoding='cp932', dtype='object',
                        usecols=[columns1[0], columns1[1]] + columns3)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    # jcsCSV:JCSレベル　レセ加工 予定・緊急入院区分コード
    try:
        indf2 = pd.read_csv(inpath_rese, engine='python', dtype='object', encoding="cp932",
                            usecols=columns4)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    # columns3のすべて欠損のレコード削除
    wkdf = wkdf.dropna(subset=columns3, how='all')
    wkdf = wkdf.fillna('')

    wk_jcs = pd.DataFrame()
    # columns2[0]　検索対象の列
    s_col = columns2[0]
    # columns3の要素ごとにfor、ｊｃｓ 単語を含むレコード抽出
    # 「検索対象の列」にrenameして結合
    for target_col_jcs in columns3:
        tmp_jcs = wkdf[wkdf[target_col_jcs].str.contains('ｊｃｓ')]
        tmp_jcs = tmp_jcs.rename(columns={target_col_jcs: s_col})
        wk_jcs = pd.concat([wk_jcs, tmp_jcs])

    jcslevel_str = "JCSレベル_str"
    # 正規表現　ｊｃｓ～文章抽出する
    wk_jcs[jcslevel_str] = wk_jcs[s_col].str.extract(r'(ｊｃｓ.*)', expand=True)
    wk_jcs = wk_jcs[~wk_jcs.duplicated()]
    # 欠損があればfillna
    wk_jcs[jcslevel_str] = wk_jcs[jcslevel_str].fillna('')

    # /data/master/JCS_level.csv　読み込み
    try:
        master_df = pd.read_csv(masterpath, engine='python', encoding="cp932")
    except:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [masterpath]))
        raise

    # マスタからjcs0削除
    collist=[]
    for w,k in zip(master_df['JCSレベル'],master_df['JCS値']):
        collist.append(w)
        # 登録キーワードが文章に存在する場合は対応する統合キーワードをTrueにする
        res = wk_jcs[jcslevel_str].str.contains(w, regex=False)
        wk_jcs[w] =  res.astype(int).replace(1,k)

    wk_jcs['JCS_値'] = wk_jcs[collist].max(axis=1)
    wk_jcs.drop(collist+[columns2[0]], axis=1, inplace=True)

    """
   確認用中間ファイル出力
   wk_outpath:任意の出力フォルダパスを指定
   出力ファイル名：jcs_check.csv
   
   確認・マスタ追加手順
   jcs_check.csvのD列「JCS_値」を0でフィルター
   C列「JCSレベル_str」を確認しｊｃｓ値が0以外でヒットしているレコードを探す。※クリア、清明などは0
   マスタに抽出したい文字列を追加。
    """
    # wk_outpath=""
    # wk_jcs.to_csv(wk_outpath+"/jcs_check.csv", index=False, encoding='cp932')

    # 欠損ゼロ埋め
    wk_jcs['JCS_群'] = 0
    wk_jcs['JCS_値'] = wk_jcs['JCS_値'].fillna(0)

    # 出力列　groupbyで最大値取得
    out_df = wk_jcs[[columns1[0], columns1[1], 'JCS_群', 'JCS_値']].groupby([columns1[0], columns1[1]],
                                                                             as_index=False).max()
    # 対象のjcsレベルのレコードのみ抽出
    df2 = indf2[indf2[columns4[1]].isin(kubun_list)]
    # 入院年月日を_dateにrename
    df2 = df2.rename(columns={columns4[2]: columns1[1]})
    # JCSレベルの文字列を数値に変換
    df2[columns4[1]] = df2[columns4[1]].astype(float)

    # レセとカルテ　マージ
    df = pd.merge(out_df, df2, on=[columns1[0], columns1[1]], how='outer')

    df = df.fillna(0)
    # 電子カルテ'JCS_値'とレセ'ＪＣＳ'columns4[1]の最大値を格納
    df['JCSレベル_値'] = df[['JCS_値', columns4[1]]].max(axis=1)
    # 群はゼロ埋め
    df['JCSレベル_群'] = 0
    # ＪＣＳ値によって群の値を代入
    df.loc[df['JCSレベル_値'] >= 1, 'JCSレベル_群'] = 1
    df.loc[df['JCSレベル_値'] >= 10, 'JCSレベル_群'] = 2
    df.loc[df['JCSレベル_値'] >= 100, 'JCSレベル_群'] = 3

    try:
        df.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["s_JCS_level"]))
