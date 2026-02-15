# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import os
import re
import pandas as pd
from datetime import datetime as dt
import calendar


def call(params, logger):
    """
    DPCレセのみ
    患者の入退院年月日と予定緊急入院区分とJCSを取得
    退院日が空白の場合は月末の日付で補完
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s1_hospitalization_category"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 3:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 3]))
        raise Exception
    in_dir = files[0]
    if not os.path.exists(in_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_dir]))
        raise Exception
    outpath = files[1]
    input_date = files[2]
    if not os.path.exists(input_date):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [input_date]))
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

    # 出力フォルダ作成
    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    # dateの読み込み
    s_target_month_l = pd.read_csv(input_date, engine='python',
                                   encoding='cp932', dtype='object').columns.tolist()

    # 出力用ｄｆ
    out_new_df = pd.DataFrame()
    for s_target_month in s_target_month_l:
        # 月、年、月末取得
        try:
            target_month = dt.strptime(s_target_month, '%Y%m').month
            target_year = dt.strptime(s_target_month, '%Y%m').year
        except ValueError:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003013", [input_date]))
            raise
        _, lastday = calendar.monthrange(target_year, target_month)

        for hoken in ['国保', '社保']:
            # kkCSV:KK読み込み
            kk_path = in_dir + s_target_month + '/dpc/RECEIPTD' + hoken + '_KK.csv'

            # ファイルが存在しなければスキップ
            if not os.path.exists(kk_path):
                # 入力ファイルのデータがないため、処理をスキップします。
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", kk_path))
                continue

            # キーエラー読み込み失敗　スキップ
            try:
                # columns1　 ["カルテ番号等", "予定・緊急入院区分", "ＪＣＳ", "紐づけ番号", "結合キー"]
                kk_df = pd.read_csv(kk_path, engine='python',
                                    usecols=columns1,
                                    encoding="cp932", dtype={columns1[0]: 'object', columns1[2]: 'object'})
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                continue

            #  columns3　["予定入院", "緊急入院", "緊急入院（2以外の場合）"]
            # "予定・緊急入院区分"の数字を置換　1："予定入院"、2:"緊急入院"、3: "緊急入院（2以外の場合）"
            kk_df.loc[:, columns1[1]] = kk_df[columns1[1]].replace({1: columns3[0], 2: columns3[1], 3: columns3[2]})
            set_kk_df = kk_df.set_index([columns1[3], columns1[4]])

            # buCSV:BU読み込み
            bu_path = in_dir + s_target_month + '/dpc/RECEIPTD' + hoken + '_BU.csv'

            # ファイルが存在しなければスキップ
            if not os.path.exists(bu_path):
                # 入力ファイルのデータがないため、処理をスキップします。
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", bu_path))
                continue

            # キーエラー読み込み失敗　スキップ
            try:
                # columns2["今回入院年月日", "今回退院年月日", "紐づけ番号", "結合キー"],
                bu_df = pd.read_csv(bu_path, engine='python', usecols=columns2,
                                    dtype={columns2[0]: 'object'}, encoding="cp932")
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                continue

            #  "今回退院年月日"をstrに変換
            bu_df[columns2[1]] = bu_df[columns2[1]].astype('str')
            bu_df.loc[:, '入院年月日'] = bu_df[columns2[0]]
            bu_df.loc[:, '退院年月日'] = bu_df[columns2[1]]
            set_bu_df = bu_df.set_index([columns1[3], columns1[4]])

            # BU,KKマージ
            bu_kk_df = pd.merge(set_kk_df, set_bu_df, left_index=True, right_index=True)
            m_df = bu_kk_df.reset_index()
            # 重複削除
            new_df = m_df[~m_df.duplicated()]

            # 今回退院年月日が空の場合、月末日を代入
            replace_date = str(target_year) + '/' + s_target_month[4:6] + '/' + str(lastday)
            new_df.loc[:, '退院年月日'] = new_df['退院年月日'].replace('nan', replace_date).replace('NaN', replace_date)
            out_new_df = pd.concat([out_new_df, new_df])

    # データ取得できなかった場合はカラを出力
    if len(out_new_df) != 0:
        # 不要列削除　"紐づけ番号", "結合キー"
        out_new_df.drop([columns1[3], columns1[4]], axis=1, inplace=True)
    else:
        out_new_df = pd.DataFrame(columns=[columns1[0], '入院年月日', '退院年月日', columns2[1]])

    try:
        out_new_df.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s1_hospitalization_category"]))
