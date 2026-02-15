# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import datetime
from datetime import datetime as dt
import pandas as pd
import os
import re
import calendar
import numpy as np
from pathlib import Path


def call(params, logger,model_path, karte_path):
    '''
    index:日付/カルテ番号 column:コード 1/0 のデータに加工
    '''
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["r_rule_info"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 5:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 5]))
        raise Exception
    in_dir = files[0]
    if not os.path.exists(in_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_dir]))
        raise Exception
    outpath = model_path + files[1]
    input_date = files[2]
    if not os.path.exists(input_date):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [input_date]))
        raise Exception
    cdpath = files[3]
    if not os.path.exists(cdpath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [cdpath]))
        raise Exception
    headerpath= files[4]
    if not os.path.exists(headerpath):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [headerpath]))
        raise Exception
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 4:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 4]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    columns3 = columns[2]
    columns4 = columns[3]
    if "rese_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["rese_list"]))
        raise Exception
    rese_list = params["rese_list"]
    if len(rese_list) < 4:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["rese_list", 4]))
        raise Exception
    rese = rese_list[0]
    rese_code = rese_list[1]
    resekubun = rese_list[2]
    rese_num = rese_list[3]

    # date
    s_target_month_l = pd.read_csv(input_date, engine='python',
                                   encoding='cp932', dtype='object').columns.tolist()

    # headerCSV:レセヘッダー 読み込み　SIの１日の情報～31日の情報
    try:
        header_df = pd.read_csv(headerpath, engine='python', encoding='cp932', dtype='object', usecols=columns4)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    date_info_list = header_df[columns4[0]].tolist()[13:44]
    # 正規表現
    pattern = r'([０１２３４５６７８９0-9]*)'
    date_str_list = [str(int(re.search(pattern, s).group())) for s in date_info_list]
    # 変換リスト
    date_list = [date_info_list, date_str_list]

    # cdCSV:CD_all
    try:
        df_cd = pd.read_csv(cdpath, engine='python', encoding='cp932', dtype='object',
                            usecols=columns3)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
        raise

    # CD_allを対象のレセ項目のコードに限定、コードを対象レセ項目の名称にリネーム
    df_cd_rese = df_cd[df_cd[columns3[2]].str.startswith(rese_num)].copy()
    df_cd_rese = df_cd_rese.rename(columns={columns3[2]: rese_code})

    # 出力フォルダ作成
    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    # 結合用データフレーム作成
    month_df = pd.DataFrame()

    # 月毎に処理
    for s_target_month in s_target_month_l:

        # 日付フォーマットチェック
        # target_year_month = yyyy/mm
        try:
            target_month = dt.strptime(s_target_month, '%Y%m')
        except ValueError:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003013", [input_date]))
            continue
        target_year_month = str(target_month.year) + '/' + str(target_month.month)

        # 対象月の末日を取得
        _, last_day = calendar.monthrange(target_month.year, target_month.month)
        f_fmt = '{}-{}-{}'.format(target_month.year, target_month.month, target_month.day)
        # dummy ： yyyy/mm/1～yyyy/mm/lastdayの1か月のデータフレーム
        dummy = pd.DataFrame(
            {columns2[1]: [pd.to_datetime(f_fmt) + datetime.timedelta(days=i) for i in range(last_day)]})

        for kubun, kubun2 in zip(['dpc', 'ika'], ['RECEIPTD', 'RECEIPTC']):
            for hoken in ['国保', '社保']:

                inFile_str = in_dir + '/' + s_target_month + '/' + kubun + '/' + kubun2 + hoken
                inpath_si = inFile_str + '_' + resekubun + '.csv'

                # ファイルの有無チェック skip
                if not os.path.exists(inpath_si):
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [inpath_si]))
                    continue

                # siCSV
                try:
                    df_si = pd.read_csv(inpath_si, engine='python', encoding='cp932',
                                        dtype=({columns1[0]: 'object', rese_code: 'str'}),
                                        usecols=columns1 + [rese_code] + date_list[0])
                except ValueError as ve:
                    keys = []
                    for arg in ve.args:
                        keys.extend(re.findall(r"\'(.*)\'", arg))
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                    continue

                # 末日が31日以外の場合、列を末日に集約
                if last_day == 28:
                    arr_group = df_si.loc[:, date_info_list[27:31]].values
                    arr_group[np.isnan(arr_group)] = 0
                    arr_sum = np.sum(arr_group, axis=1)
                    df_si[date_info_list[27]] = arr_sum
                elif last_day == 29:
                    arr_group = df_si.loc[:, date_info_list[28:31]].values
                    arr_group[np.isnan(arr_group)] = 0
                    arr_sum = np.sum(arr_group, axis=1)
                    df_si[date_info_list[28]] = arr_sum
                elif last_day == 30:
                    arr_group = df_si.loc[:, date_info_list[29:31]].values
                    arr_group[np.isnan(arr_group)] = 0
                    arr_sum = np.sum(arr_group, axis=1)
                    df_si[date_info_list[29]] = arr_sum

                # date_listでfor
                for colstr, col in zip(date_list[0], date_list[1]):

                    # 月末日まで処理
                    if int(col) <= last_day:
                        month_str = str(target_year_month + '/' + col)
                        tdatetime = dt.strptime(month_str, '%Y/%m/%d')
                        tdatetime = tdatetime.strftime('%Y/%m/%d')
                        day_df = df_si[[rese_code, columns1[0], colstr]].copy()

                        day_df.loc[df_si[colstr] >= 1, colstr] = tdatetime

                        day_df = day_df.rename(columns={colstr: columns2[1]})
                        day_df = day_df.dropna()

                        # dpcの場合CD_allのデータを結合
                        if kubun == 'dpc':
                            df_cd_rese_date = df_cd_rese[df_cd_rese[columns3[1]] == tdatetime]
                            day_df = pd.concat([day_df, df_cd_rese_date])

                        day_df = day_df[day_df[columns2[1]] != 0]
                        month_df = pd.concat([month_df, day_df])

    # month_dfが空だった時の処理　からのデータフレーム出力
    if len(month_df) == 0:
        out_max = pd.DataFrame(columns=[columns2[0], columns2[1]])

    else:
        out_tmp_df = month_df[[columns2[0], columns2[1]]]
        out_tmp_df = out_tmp_df[~out_tmp_df.duplicated()]

        # 最大の日の出力
        out_tmp_df[columns2[1]] = pd.to_datetime(out_tmp_df[columns2[1]])
        out_max = out_tmp_df.groupby([columns2[0]]).max()

    # カルテデータの受付情報読み込み
    karte_path = Path(karte_path)
    karte_list = []
    for karte_file in karte_path.glob('**/*.csv'):
        try:
            karte_list.append(pd.read_csv(karte_file, engine='python', encoding='cp932', dtype='object')[columns2])
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);
            l(logger.format_log_message("AIR003016", keys))
            raise Exception

    karte_df = pd.concat(karte_list)
    karte_df[columns2[1]] = pd.to_datetime(karte_df[columns2[1]])
    karte_max = karte_df.groupby(columns2[0])[columns2[1]].max().reset_index()

    if len(karte_max) > 0:
        out_max = out_max.reset_index()
        tmp_max = pd.concat([out_max, karte_max])
        out_max = tmp_max.groupby(columns2[0])[columns2[1]].max().reset_index()
    out_max[columns2[1]] = out_max[columns2[1]].dt.strftime('%Y/%m/%d')

    try:
        out_max.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["r_rule_info"]))
