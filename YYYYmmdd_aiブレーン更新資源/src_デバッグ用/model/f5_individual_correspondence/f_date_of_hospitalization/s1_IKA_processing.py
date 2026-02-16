# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import datetime
import pandas as pd
from datetime import datetime as dt
import os
import re

def call(params, logger):
    """
    医科レセ
    入院年月日と診療実日数が記録されている患者を抽出
    退院日を計算
    MEMO：入院年月日・診療実日数を用いて，退院年月日を算出
        　※退院年月日当該月の末日の場合、翌月のレセを参照しないと退院したのかどうかが判別不可
        　①入院年月日が当該月の場合　入院年月日＋診療実日数→退院年月日
        　②入院年月日が当該月より前の場合　当該月1日＋診療実日数→退院年月日

    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s1_IKA_processing"]))

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
    if len(columns) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 1]))
        raise Exception
    columns1 = columns[0]

    # date
    s_target_month_l = pd.read_csv(input_date, engine='python',
                                   encoding='cp932', dtype='object').columns.tolist()

    def outday(df):
        # (4/1 + 6=4/6)
        df['退院年月日'] = str(df['入院年月日_dt'] + datetime.timedelta(days=df[columns1[2]] - 1))
        return df

    def outday2(df):
        df['退院年月日'] = str(df['入院年月日_dt2'] + datetime.timedelta(days=df[columns1[2]] - 1))
        return df

    kubun = 'ika'
    tmp_df = pd.DataFrame()
    # 月毎に処理
    for s_target_month in s_target_month_l:
        try:
            target_month = dt.strptime(s_target_month, '%Y%m')
        except ValueError:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003013", [input_date]))
            continue
        target_date = s_target_month[0:4] + '/' + s_target_month[4:7]

        # 国社
        for hoken in ['国保', '社保']:
            inFile_str = in_dir + '/' + s_target_month + '/' + kubun + '/RECEIPTC' + hoken

            # hoCSV:HO
            inpath_ho=inFile_str + '_HO.csv'
            if not os.path.exists(inpath_ho):
                # 入力ファイルのデータがないため、処理をスキップします。
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [inpath_ho]))
                continue

            try:
                df_HO = pd.read_csv(inpath_ho, engine='python', usecols=columns1, encoding='cp932',
                                    dtype={columns1[0]: 'object'})
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                continue

            df_HO[columns1[1]]=df_HO[columns1[1]].astype('str')
            df_new_in = df_HO[df_HO[columns1[1]] != 'nan']
            df_new_in = df_new_in[df_new_in[columns1[1]] != 'NaN']

            # 退院日計算
            # (4/1 + 6=4/6)
            # 入院年月日が当月の場合：入院年月日＋診療実日数
            # 入院年月日が当月以外の場合：当月1日＋診療実日数
            df_new_in.loc[:, '入院年月日_dt'] = pd.to_datetime(df_new_in[columns1[1]])
            df_new_in.loc[:, '入院年月日_month'] = df_new_in[columns1[1]].str[:7]

            # 当月の場合
            df_togetsu = df_new_in[df_new_in['入院年月日_month'] == target_date]
            df_not_togetsu = df_new_in[df_new_in['入院年月日_month'] != target_date]

            # 対象のデータがない場合
            if len(df_not_togetsu) != 0:
                df_not_togetsu=df_not_togetsu.reset_index(drop=True)
                df_not_togetsu.loc[:, '入院年月日_dt2'] = datetime.date(target_month.year, target_month.month, 1)

            # 退院日計算
            df_togetsu_new = df_togetsu.apply(outday, axis=1)
            df_not_togetsu_new = df_not_togetsu.apply(outday2, axis=1)

            outdf_1 = pd.concat([df_togetsu_new, df_not_togetsu_new])
            tmp_df = pd.concat([tmp_df, outdf_1])

    # 読み込めなかった場合空のデータフレーム出力
    if len(tmp_df) != 0:
        # 重複の処理
        df_dup = tmp_df[~tmp_df.duplicated(subset=[columns1[0], columns1[1]], keep=False)]
        df_dup_drop = tmp_df[tmp_df.duplicated(subset=[columns1[0], columns1[1]], keep=False)]

        df_dup2 = pd.DataFrame()
        for karute in set(df_dup_drop[columns1[0]].tolist()):
            tmp = df_dup_drop[df_dup_drop[columns1[0]] == karute]
            tmpmax = tmp[tmp['退院年月日'] == max(tmp['退院年月日'])]
            df_dup2 = pd.concat([df_dup2, tmpmax])

        out_df = pd.concat([df_dup, df_dup2])
        out_df.loc[:, '退院年月日'] = out_df['退院年月日'].str[:10].str.replace('-', '/')
    else:
        out_df = pd.DataFrame(columns=[columns1[0], columns1[1], '退院年月日'])

    try:
        out_df.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    #終了ログ
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s1_IKA_processing"]))