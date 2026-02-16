# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import re
import numpy
import os


def call(params, logger, heap_dir):
    """
    入院中の算定回数をカウント
    対象：'113011710', '190120610','190814010','190128110','113017610', '113017710'
    Memo：ハードコーディング 26行目
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_calculations_count"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 5:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 5]))
        raise Exception
    in_path = files[0]
    if not os.path.exists(in_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_path]))
        raise Exception
    inpath_si = files[1]
    if not os.path.exists(inpath_si):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_si]))
        raise Exception
    period_path = files[2]
    if not os.path.exists(period_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [period_path]))
        raise Exception
    out_dir = files[3]
    outpath_pre = heap_dir + files[4]

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
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    target_list = params["targets"]
    if "nyuin_kaisu_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["nyuin_kaisu_list"]))
        raise Exception
    nyuin_kaisu_list = params["nyuin_kaisu_list"]

    # dfCSV:患者ごとの入退院のデータ
    try:
        df = pd.read_csv(in_path, engine='python', usecols=[columns1[0], columns2[2], columns2[3]],
                        dtype={columns1[0]: 'object'}, encoding="cp932")
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    # 日付↔文字列　変換
    try:
        df[columns2[2]] = pd.to_datetime(df[columns2[2]], format='%Y/%m/%d')
        df[columns2[3]] = pd.to_datetime(df[columns2[3]], format='%Y/%m/%d')
        df[columns2[0]] = df[columns2[2]].dt.strftime('%Y/%m/%d')
        df[columns2[1]] = df[columns2[3]].dt.strftime('%Y/%m/%d')
    except ValueError:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003013", [in_path]))
        raise

    # カルテ番号と入退院日　一覧抽出
    df = df[[columns1[0]] + columns2][~df.duplicated(subset=[columns1[0]] + columns2)].reset_index(
        drop=True)

    # 入力データから、入院年月日と一致するデータを抽出
    # 入力データの日付は1行で出力されている
    df_period = pd.read_csv(period_path, engine='python', encoding="cp932")

    # 入院開始日が対象の年月日の患者の未抽出
    # 入力データの年月
    df["__d"] = df[columns2[0]].str[0:7]
    list_period = []
    for date in df_period.columns:
        list_period.append("{0}/{1}".format(date[0:4], date[4:]))
    df_na = df[df["__d"].isin(list_period)].copy()

    # 診療行為読み込み
    df_si = pd.read_pickle(inpath_si)

    # 予測で使用する中間出力ファイル用
    # カルテ番号、入院年月日、退院年月日
    out_predict_df = df_na[[columns1[0], columns2[0], columns2[1]]].copy()
    # out_predict_df = out_predict_df.set_index([columns1[0], columns2[0], columns2[1]])

    # 算定項目ごとに処理
    for target, nyuin_kaisu in zip(target_list, nyuin_kaisu_list):
        # 中間出力ファイルにtargetの列を作成
        out_predict_df[target] = numpy.nan

        # カルテ番号、_date, taregt
        use_col = [columns1[0], columns1[1], target]
        # 診療行為dfから対象の列を抽出
        target_df = df_si.loc[:, df_si.columns.intersection(use_col)].reindex(columns=use_col)

        # targetコードに1がついている患者のみ入退院年月日を取得
        kanzya_list = target_df[columns1[0]][target_df[target] >= 1]
        df_na_kanzya = df_na[df_na[columns1[0]].isin(kanzya_list)].copy()

        # targetごとの出力用ｄｆ
        out_df = pd.DataFrame()

        np_df_kubun = df_na_kanzya.values
        # np_df_kubunを1行ずつ確認
        for line in np_df_kubun:
            # 入院退院の期間データ作成
            karute_mae = line[0]
            start = line[3]
            end = line[4]

            # 中間出力用の日付文字列データ
            start_str = start.strftime('%Y/%m/%d')
            end_str = end.strftime('%Y/%m/%d')

            # 入院期間中にのデータフレーム作成
            date_df = pd.DataFrame(index=[pd.to_datetime(start), pd.to_datetime(end)])
            # 入院中フラグ 1
            date_df[columns3[0]] = 1
            # start~endの日付のデータを補完
            date_df = date_df.resample('D').mean()
            date_df[columns3[0]] = 1
            date_df = date_df.reset_index()
            # _date取得
            date_df[columns1[1]] = date_df['index'].astype('str').str[0:10].str.replace('-', '/')
            date_df[columns1[0]] = karute_mae
            # 入院中算定回数　初期値1
            date_df[columns3[1]] = 1

            # 診療行為ｄｆtarget_dfとマージ
            tmp_df = pd.merge(target_df, date_df, on=[columns1[0], columns1[1]])
            tmp_df[target] = tmp_df[target].fillna(0)

            # df_count：実績のあるデータのデータフレーム
            df_count = tmp_df[tmp_df[target] >= 1]

            # df_count>=1 =入院期間中に算定のある患者の場合
            # elseの場合処理不要
            if len(df_count) >= 1:

                # 入院中1回以上の制限、かつ実績1回
                if (nyuin_kaisu >= 1) and (len(df_count) == 1):
                    # target算定実績のあるレコードの "入院中算定回数" columns3[1]を0
                    tmp_df.loc[tmp_df[target] >= 1, columns3[1]] = 0

                # 入院中1回の制限、かつ実績が1回以上の場合
                elif (nyuin_kaisu == 1) and (len(df_count) > 1):
                    # a_df:target算定実績のあるレコード
                    a_df = tmp_df[tmp_df[target] >= 1].copy()
                    a_df = a_df.reset_index(drop=True)
                    # 要素0番目の実績のあるレコードの "入院中算定回数" columns3[1]を0
                    a_df.loc[0, columns3[1]] = 0
                    # b_df:target算定実績のないレコード
                    b_df = tmp_df[tmp_df[target] == 0].copy()
                    tmp_df = pd.concat([a_df, b_df])

                # 入院中2回以上の制限、かつ実績2回以上の場合
                elif (nyuin_kaisu >= 2) and (len(df_count) >= 2):
                    # a_df:target算定実績のあるレコード
                    a_df = tmp_df[tmp_df[target] >= 1].copy()
                    a_df = a_df.reset_index(drop=True)
                    # 実績のあるレコードの "入院中算定回数" columns3[1]を要素の数の値を代入
                    a_df[columns3[1]] = range(0, len(df_count))
                    # 入院中算定回数制限より入院中算定回数の値が大きい場合、入院中制限の値を代入
                    a_df.loc[a_df[columns3[1]] > nyuin_kaisu, columns3[1]] = nyuin_kaisu

                    # b_df:target算定実績のないレコード
                    b_df = tmp_df[tmp_df[target] == 0].copy()
                    # nyuin_kaisu(入院中制限回数)
                    b_df.loc[:, columns3[1]] = nyuin_kaisu
                    tmp_df = pd.concat([a_df, b_df])
                else:
                    continue

                out_df = pd.concat([out_df, tmp_df])
                # 中間ファイル出力用
                out_predict_df.loc[((out_predict_df[columns1[0]]==karute_mae)&(out_predict_df[columns2[0]]==start_str)&(out_predict_df[columns2[1]]==end_str)), target] = len(df_count)

        # len(out_df)==0の場合出力しない
        if len(out_df) != 0:
            out_df = out_df.reset_index()
            outpath = str(out_dir + target + '_入院中算定回数.csv')

            try:
                out_df.to_csv(outpath, encoding='cp932', index=False)
            except:
                import traceback, sys
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                raise

    # 中間出力ファイル 出力エラーチェック
    try:
        out_predict_df.to_csv(outpath_pre, encoding='cp932', index=False)
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath_pre+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_calculations_count"]))
