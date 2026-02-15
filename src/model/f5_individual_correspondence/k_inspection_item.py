# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
from dateutil.relativedelta import relativedelta
import os
import re


def call(params, logger):
    """
    検査項目　個別対応
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["k_inspection_item"]))

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

    # karuteCSV:カルテ全結合データ読み込み
    try:
        wkdf = pd.read_csv(inpath, engine='python', encoding='cp932', dtype='object',
                        usecols=[columns1[0], columns1[1], columns2[0]])
    # 読み込めない場合エラー
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    wkdf.loc[:, columns2[0]] = wkdf[columns2[0]].str.lstrip()
    _wkdf = wkdf.dropna(subset=[columns2[0]])
    _wkdf=_wkdf.reset_index(drop=True)

    # 長さチェック
    if len(_wkdf)!=0:

        # 半角空白の個数をカウント max確認
        scount = 0
        for s in _wkdf[columns2[0]].values:
            _scount = s.count(' ')
            if _scount > scount:
                scount = _scount

        # 空白の数分、検査項目を分割
        colm_list = []
        for slength in range(0, scount + 1):
            colm = str(slength + 1) + '_' + columns2[0]
            tmp_v = _wkdf[columns2[0]].str.split(pat=' ', expand=True)[slength]
            _wkdf.loc[:,colm] = tmp_v
            colm_list.append(colm)

        # 検査項目を分割後、一つの列にまとめる
        __wkdf = pd.DataFrame(columns=[columns1[0], columns1[1], columns2[0]])

        for tar_col in colm_list:
            tmp_df = _wkdf[[columns1[0], columns1[1], tar_col]].rename(columns={tar_col: columns2[0]}).copy()
            __wkdf = pd.concat([__wkdf, tmp_df])
        __wkdf = __wkdf.dropna(subset=[columns2[0]])

        df_sinryo = __wkdf.set_index([columns1[0], columns1[1]])
        df_sinryo = pd.get_dummies(df_sinryo[columns2[0]])

        df_sinryo = df_sinryo.groupby(level=[0, 1]).sum()
        df_sinryo[df_sinryo > 1] = 1

        # 検査項目フラグ　記録～2週間後まで　フラグを付与
        # カルテの記録が実際の算定より早いため
        use_col = df_sinryo.columns
        df_sinryo = df_sinryo.reset_index()
        df_sinryo['日付'] = pd.to_datetime(df_sinryo[columns1[1]], format='%Y/%m/%d')
        df_sinryo['2week後日付_dt'] = df_sinryo['日付'].apply(lambda x: x + relativedelta(weeks=2))

        concat_df = pd.DataFrame()
        for i in range(len(df_sinryo)):
            # 入院退院の期間データ作成
            karute_num = df_sinryo.loc[i, columns1[0]]
            start = df_sinryo.loc[i, '日付']
            end = df_sinryo.loc[i, '2week後日付_dt']

            # end = end + datetime.timedelta(days=1)
            tmp_df = pd.DataFrame(index=[pd.to_datetime(start), pd.to_datetime(end)], columns=use_col,
                                  data=[df_sinryo.loc[i, use_col], df_sinryo.loc[i, use_col]])
            tmp_df = tmp_df.resample('D').mean()

            date_df = tmp_df.fillna(method='ffill')
            date_df = date_df.reset_index()
            date_df[columns1[1]] = date_df['index'].astype('str').str[0:10].str.replace('-', '/')
            date_df[columns1[0]] = karute_num
            concat_df = pd.concat([concat_df, date_df])


        concat_df = concat_df.drop("index", axis=1,errors="ignore")

    else:
        concat_df=pd.DataFrame(columns=[columns1[0], columns1[1]])


    # 出力
    try:
        concat_df.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" +traceback.format_exception(*sys.exc_info())[-1]]))
        raise


    # 終了ログ
    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["k_inspection_item"]))
