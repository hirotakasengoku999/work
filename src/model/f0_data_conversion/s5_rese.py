# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import datetime
from datetime import datetime as dt
import pandas as pd
import re
import os
import calendar
import numpy as np


def call(params, logger):
    '''
    index:日付/カルテ番号 column:コード 1/0 のデータに加工
    '''
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s5_rese"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 6:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 6]))
        raise Exception
    in_dir = files[0]
    if not os.path.exists(in_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_dir]))
        raise Exception
    out_dir = files[1]
    input_date = files[2]
    if not os.path.exists(input_date):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [input_date]))
        raise Exception
    cdpath = files[3]
    if not os.path.exists(cdpath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [cdpath]))
        raise Exception
    outpath_pati = files[4]
    headerpath = files[5]
    if not os.path.exists(headerpath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [headerpath]))
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
    if "merge_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["merge_list"]))
        raise Exception
    merge_list = params["merge_list"]

    # date
    s_target_month_l = pd.read_csv(input_date, engine='python',
                                   encoding='cp932', dtype='object').columns.tolist()

    # レセヘッダー  読み込み　SIの１日の情報～31日の情報
    try:
        header_df = pd.read_csv(headerpath, engine='python',
                                encoding='cp932', dtype='object', usecols=columns4)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
        raise

    date_info_list = header_df[columns4[0]].tolist()[13:44]

    # 正規表現
    pattern = r'([０１２３４５６７８９0-9]*)'
    date_str_list = [str(int(re.search(pattern, s).group())) for s in date_info_list]
    # 変換リスト
    date_list = [date_info_list, date_str_list]

    # CD_all
    try:
        df_cd = pd.read_csv(cdpath, engine='python', encoding='cp932', dtype='object',
                            usecols=columns3)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
        raise

    # 患者の一覧の出力用にカルテ番号等,_dateの空のデータフレーム作成
    new_df_pati = pd.DataFrame(columns=columns2)

    # SI/IY/TO
    for rese, rese_code, resekubun, rese_num in rese_list:

        # レセ項目ごとに出力用のデータフレーム作成
        out_df = pd.DataFrame()

        # CD_allを対象のレセ項目のコードに限定、コードを対象レセ項目の名称にリネーム
        df_cd_rese = df_cd[df_cd[columns3[2]].str.startswith(rese_num)]
        df_cd_rese = df_cd_rese.rename(columns={columns3[2]: rese_code})

        outpath = out_dir + rese + '.pickle'
        # 出力フォルダ
        os.makedirs(out_dir, exist_ok=True)

        # 月毎に処理
        for s_target_month in s_target_month_l:

            # 月ごとの結合用データフレーム作成
            month_df = pd.DataFrame()

            # target_year_month = yyyy/mm
            try:
                target_month = dt.strptime(s_target_month, '%Y%m')
            except ValueError:
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003013", [input_date]))
                raise
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

                    # ファイルチェック：なければWARNIG、skip
                    if not os.path.isfile(inpath_si):
                        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [inpath_si]))
                        continue

                    # 読み込み
                    try:
                        df_si = pd.read_csv(inpath_si, engine='python', encoding='cp932', dtype=({columns1[0]: 'object', rese_code: 'str'}),
                                            usecols=columns1 + [rese_code] + date_list[0])
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
                        raise

                    # ikaの場合、入院年月日が欠損の患者は対象外
                    if kubun == 'ika':
                        df_si = df_si.dropna(subset=[columns1[1]])

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

                            day_df.loc[day_df[colstr] >= 1, colstr] = tdatetime

                            day_df = day_df.rename(columns={colstr: columns2[1]})
                            day_df = day_df.dropna()

                            # dpcの場合CD_allのデータを結合
                            if kubun == 'dpc':
                                df_cd_rese_date = df_cd_rese[df_cd_rese[columns3[1]] == tdatetime]
                                day_df = pd.concat([day_df, df_cd_rese_date])

                            day_df = day_df[day_df[columns2[1]] != 0]
                            month_df = pd.concat([month_df, day_df])

            # この段階でmonth_dfの長さが0場合、continue
            if len(month_df) == 0:
                continue

            # 重複削除
            month_df_dup = month_df[~month_df.duplicated()]
            # ダミー変数作成
            month_df = month_df_dup.set_index([columns1[0], columns2[1]])
            dummy_all = pd.get_dummies(month_df[rese_code], prefix='', prefix_sep='')
            tmp_df = dummy_all.groupby(level=[0, 1]).sum()
            tmp_df[tmp_df >= 2] = 1
            tmp_df = tmp_df.reset_index()

            # 患者×日付(yyyy/mm/1～yyyy/mm/lastday)作成
            conc_df = pd.DataFrame()
            for num in tmp_df[columns1[0]].unique():
                dummy[columns1[0]] = num
                conc_df = pd.concat([conc_df, dummy])
            conc_df[columns2[1]] = conc_df[columns2[1]].dt.strftime('%Y/%m/%d')

            out_df_tmp = pd.merge(conc_df, tmp_df, how='left', on=[columns1[0], columns2[1]])
            out_df = pd.concat([out_df, out_df_tmp])

        # この段階で out_df の長さが0場合、診療行為ならｴﾗｰ、その他warning
        if len(out_df) == 0:
            if rese == '診療行為':
                # 中間ファイルの出力に失敗
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003015", [outpath]))
                raise Exception
            else:
                # データが空のため処理をスキップ
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [outpath]))
                out_df = pd.DataFrame(columns=columns2)

        # 診療行為でコードの結合あり
        if rese == '診療行為':
            merge_target_list = []
            for merge_target in merge_list:

                # コードの有無チェック
                for m_t in merge_target:
                    if m_t not in out_df.columns:
                        out_df[m_t] = 0

                merge_target_list.extend(merge_target)

                merge_target_column = '_'.join(merge_target)

                # merge_targetの列チェック
                # 存在する列のみ使用

                # 入院栄養食事指導料１
                # 悪性腫瘍特異物質治療管理料
                out_df[merge_target_column] = out_df[merge_target].sum(axis=1)
                out_df.loc[out_df[merge_target_column] >= 1, merge_target_column] = 1

            # 列がない場合もエラーにならないパラメータ追加
            out_df = out_df.drop(merge_target_list, axis=1, errors='ignore')

        # 出力　pickle
        try:
            out_df.to_pickle(outpath)
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

        # csv出力する必要なし(確認用)
        # out_df.to_csv(outpathcsv, index=False, encoding='cp932')

        # 患者の一覧
        df_pati = out_df[columns2]
        new_df_pati = pd.merge(new_df_pati, df_pati, on=columns2, how='outer')

    # 全患者の一覧を出力
    try:
        new_df_pati.to_csv(outpath_pati, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath_pati+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s5_rese"]))
