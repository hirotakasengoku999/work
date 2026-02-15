# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re

def call(params, logger):
    """
    医科レセとDPCレセの入退院日を結合
    同患者で同入院日の記録が存在する場合に日付が長いほうの退院日を選ぶ処理
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s2_merging_DPC_and_IKA"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 6:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 6]))
        raise Exception
    inpath_dpc = files[0]
    if not os.path.exists(inpath_dpc):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_dpc]))
        raise Exception
    inpath_ika = files[1]
    if not os.path.exists(inpath_ika):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_ika]))
        raise Exception
    outpath1 = files[2]
    outpath2 = files[3]
    wk_path = files[4]
    if not os.path.exists(wk_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [wk_path]))
        raise Exception
    kakunin_path = files[5]
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

    # 全患者結合
    wk_df = pd.read_csv(wk_path, engine='python', encoding='cp932', dtype={columns1[0]: 'object'})

    # dpcCSV
    try:
        df_dpc = pd.read_csv(inpath_dpc, engine='python', encoding='cp932', dtype={columns1[0]: 'object'},
                            usecols=columns1)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    # 読み込んだデータフレームが空でない場合実施
    if len(df_dpc)!=0:
        df_dpc = df_dpc.rename(columns={columns1[1]: columns2[1], columns1[2]: columns2[2]})
        df_dpc[columns2[1]] = df_dpc[columns2[1]].str.replace('-', '/')
        df_dpc[columns2[2]] = df_dpc[columns2[2]].str.replace('-', '/')
        df_dpc[columns3[3]] = 'dpc'

    # ikaCSV
    try:
        df_ika = pd.read_csv(inpath_ika, engine='python', encoding='cp932', dtype={columns2[0]: 'object'},
                            usecols=columns2)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise


    # 読み込んだデータフレームが空でない場合実施
    if len(df_ika) != 0:
        df_ika[columns3[3]] = 'ika'

    df = pd.concat([df_dpc, df_ika])

    # 読み込んだデータフレームが空でない場合実施
    if len(df) == 0:
        # 作業ファイルの読み込みに失敗
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003014", [inpath_dpc]))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003014", [inpath_ika]))
        raise Exception

    df[columns1[1]] = pd.to_datetime(df[columns2[1]], format='%Y/%m/%d')
    df[columns1[2]] = pd.to_datetime(df[columns2[2]], format='%Y/%m/%d')

    dfin = df[~df.duplicated()].sort_values(columns2[0])
    df_drop = dfin[dfin.duplicated(subset=[columns2[0], columns2[1]], keep=False)].sort_values(columns2[0])
    df_dup = dfin[~dfin.duplicated(subset=[columns2[0], columns2[1]], keep=False)]

    df_dup2 = pd.DataFrame()
    for karute in set(df_drop[columns2[0]].tolist()):
        tmp_karte = df_drop[df_drop[columns2[0]] == karute]
        for nyuin in set(tmp_karte[columns2[1]].tolist()):
            tmp = tmp_karte[tmp_karte[columns2[1]] == nyuin]
            tmpmax = tmp[tmp[columns1[2]] == max(tmp[columns1[2]])]
            df_dup2 = pd.concat([df_dup2, tmpmax])

    df_new = pd.concat([df_dup, df_dup2])

    # 患者結合
    df_new = df_new.reset_index(drop=True)
    concat_df = pd.DataFrame()
    for line in range(len(df_new)):
        # 入院退院の期間データ作成
        karute_num = df_new.loc[line, columns3[0]]
        out = df_new.loc[line, columns3[2]]
        kubun = df_new.loc[line, columns3[3]]
        start = df_new.loc[line, columns3[4]]
        end = df_new.loc[line, columns3[5]]

        date_df = pd.DataFrame(index=[pd.to_datetime(start), pd.to_datetime(end)])
        date_df[columns3[6]] = 1
        date_df = date_df.resample('D').mean()
        date_df[columns3[6]] = 1
        date_df = date_df.reset_index()
        date_df[columns3[1]] = date_df['index'].astype('str').str[0:10].str.replace('-', '/')
        date_df[columns3[7]] = list(range(1, len(date_df) + 1))
        date_df[columns3[0]] = karute_num
        date_df[columns3[4]] = start
        date_df[columns3[5]] = end
        date_df[columns3[3]] = kubun
        date_df[columns3[2]] = out
        concat_df = pd.concat([concat_df, date_df])

    concat_df_dup = concat_df[~concat_df.duplicated([columns3[0], columns3[1]], keep=False)]

    _dupdrop = concat_df[concat_df.duplicated([columns3[0], columns3[1]], keep=False)]

    # 重複している場合、入院期間が古いほうを採用
    df_dup2 = pd.DataFrame()
    __dupdrop = _dupdrop[[columns3[0], columns3[1]]][~_dupdrop.duplicated(subset=[columns3[0], columns3[1]])]
    for karute, _date in __dupdrop.values:
        _tmp = _dupdrop[(_dupdrop[columns3[0]] == karute) & (_dupdrop[columns3[1]] == _date)]

        # 入院期間チェック:入院日と退院日が同じ場合
        if len(_tmp) == 2:
            _tmp = _tmp.reset_index(drop=True)
            if _tmp.loc[0, columns1[1]] == _tmp.loc[1, columns1[2]]:
                __tmp = _tmp[_tmp[columns1[1]] == min(_tmp[columns1[1]])]
            elif _tmp.loc[0, columns1[2]] == _tmp.loc[1, columns1[1]]:
                __tmp = _tmp[_tmp[columns1[1]] == min(_tmp[columns1[1]])]
            else:
                __tmp = _tmp
        else:
            __tmp = _tmp

        df_dup2 = pd.concat([df_dup2, __tmp])

    # result tmp1
    __out = pd.concat([concat_df_dup, df_dup2])

    # 異常な重複データの処理 (入院開始日がちがうのに、入院期間がかぶってるデータ)
    __out_dup = __out[__out.duplicated([columns3[0], columns3[1]], keep=False)]
    if len(__out_dup) != 0:
        # 重複している場合、古い入院日～新しい退院日
        ___out_dup = __out_dup[[columns3[0], columns3[1]]][~__out_dup.duplicated(subset=[columns3[0], columns3[1]])]
        df_dup2 = ___out_dup.copy()
        # 新しい入退院日を取得
        for karute, _date in ___out_dup.values:
            _tmp = __out_dup[(__out_dup[columns3[0]] == karute) & (__out_dup[columns3[1]] == _date)]

            df_dup2.loc[(df_dup2[columns3[0]] == karute) & (df_dup2[columns3[1]] == _date), columns3[4]] = min(
                _tmp[columns3[4]])
            df_dup2.loc[(df_dup2[columns3[0]] == karute) & (df_dup2[columns3[1]] == _date), columns3[5]] = max(
                _tmp[columns3[5]])
        # 新しい入退院日を取得したデータフレーム
        df_dup2 = df_dup2[[columns3[0], columns3[4], columns3[5]]][
            ~df_dup2.duplicated(subset=[columns3[0], columns3[4], columns3[5]])]

        # result tmp1 __outに含まれる新しい入退院日を取得した患者を削除
        __out_dup_drop = __out_dup[[columns3[0], columns3[4], columns3[5]]][
            ~__out_dup.duplicated(subset=[columns3[0], columns3[4], columns3[5]])]
        for _k, _i, _o in __out_dup_drop.values:
            __out = __out[~((__out[columns3[0]] == _k) & (__out[columns3[4]] == _i) & (__out[columns3[5]] == _o))]

        # 新しい入退院日を取得した患者の入院期間を再算出
        df_new = df_dup2.reset_index(drop=True)
        concat_df = pd.DataFrame()
        for line in range(len(df_new)):
            # 入院退院の期間データ作成
            karute_num = df_new.loc[line, columns3[0]]
            start = df_new.loc[line, columns3[4]]
            end = df_new.loc[line, columns3[5]]

            date_df = pd.DataFrame(index=[pd.to_datetime(start), pd.to_datetime(end)])
            date_df[columns3[6]] = 1
            date_df = date_df.resample('D').mean()
            date_df[columns3[6]] = 1
            date_df = date_df.reset_index()
            date_df[columns3[1]] = date_df['index'].astype('str').str[0:10].str.replace('-', '/')
            date_df[columns3[7]] = list(range(1, len(date_df) + 1))
            date_df[columns3[0]] = karute_num
            date_df[columns3[4]] = start
            date_df[columns3[5]] = end
            concat_df = pd.concat([concat_df, date_df])
    else:
        concat_df = pd.DataFrame()

    # 結合
    __out_concat = pd.concat([__out, concat_df])

    # 確認出力用
    # __out_concat[__out_concat.duplicated([columns3[0], columns3[1]], keep=False)].to_csv(kakunin_path, encoding='cp932')

    out_df = pd.merge(wk_df, __out_concat, on=[columns3[0], columns3[1]])

    # 出力
    try:
        out_df.to_csv(outpath2, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath2+ ":" +traceback.format_exception(*sys.exc_info())[-1]]))
        raise


    #終了ログ
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s2_merging_DPC_and_IKA"]))
