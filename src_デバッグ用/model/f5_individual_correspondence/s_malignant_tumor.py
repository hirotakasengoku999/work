# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import calendar
import os


def call(params, logger):
    """
    113002110_113001310 悪性腫瘍1項目と2項目を結合したコード
    対象のコードcolumns3[0]の同月の算定実績フラグ
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_malignant_tumor"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    in_path = files[0]
    if not os.path.exists(in_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_path]))
        raise Exception
    outpath = files[1]
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

    # 日付
    def daterange(_start, _end):
        for n in range((_end - _start).days):
            yield _start + datetime.timedelta(n)

    # 診療行為　項目名がない可能性があるので、空データで続行
    indf_sid = pd.read_pickle(in_path)
    colm=columns1 + columns3
    df_tar = indf_sid.loc[:, indf_sid.columns.intersection(colm)].reindex(columns=colm)
    df_tar[columns3[0]] = df_tar[columns3[0]].fillna(0)

    df_tar2 = df_tar.copy()
    # 文字列を日付フォーマットに変換
    df_tar2.loc[:, 'index_dt'] = pd.to_datetime(df_tar.loc[:, columns1[1]], format='%Y/%m/%d')
    # 文字列日付をyyyy/mmの年月データに変換し別の列を生成
    df_tar2.loc[:, 'index_dt2'] = df_tar.loc[:, columns1[1]].str[0:7]
    # 出力列：同月算定回数を0埋め
    df_tar2.loc[:, columns2[0]] = 0

    # columns3[0]：target 算定実績があるカルテ番号でカルテ番号、年月のレコード抽出
    target_karute = df_tar2[[columns1[0], 'index_dt2']][df_tar2[columns3[0]] >= 1].copy()
    # 重複削除
    target_karute_dup = target_karute[~target_karute.duplicated()]

    # 長さチェック
    if len(target_karute_dup)!=0:

        # 格納用データフレーム
        df_new_flag = pd.DataFrame()
        # target_karute_dupを1レコードずつ確認
        for karute, month in zip(target_karute_dup[columns1[0]], target_karute_dup['index_dt2']):
            # 月の月末日を取得
            _, lastday = calendar.monthrange(int(month[0:4]), int(month[5:7]))

            # df_tar_setのカルテ番号と年月が一致するレコードを抽出
            df_tar_karute = df_tar2[(df_tar2[columns1[0]] == karute) & (df_tar2["index_dt2"] == month)].reset_index(drop=True)
            # columns3[0]:targetの算定のあった日付をlistで取得
            tar_date = df_tar_karute['index_dt'][df_tar_karute.loc[:, columns3[0]] >= 1].tolist()

            # 取得した日付の日にちをtar_dayとして、月末日と比較
            # 日付チェック
            tar_day = tar_date[0].day
            if lastday == tar_day:
                # 対象が月末の場合はスキップ
                continue
            else:
                # 算定の次の日から月末までフラグ
                # start:算定のあった日付の次の日
                start = tar_date[0] + datetime.timedelta(days=1)
                # end:算定のあった月の次の月の1日
                end = pd.to_datetime(
                    (start.replace(day=1) + relativedelta(months=1, days=-1)).date()) + datetime.timedelta(
                    days=1)
                # strat~endまで1日ずつ確認
                for i in daterange(start, end):
                    date = i.strftime('%Y/%m/%d')
                    # columns3[0]:targetの値が1以上の場合
                    if df_tar_karute[columns3[0]][df_tar_karute[columns1[1]] == date].values[0] >= 1:
                        # 値を+1
                        df_tar_karute.loc[df_tar_karute[columns1[1]] == date, columns2[0]] = \
                            df_tar_karute[columns2[0]][df_tar_karute[columns1[1]] == date].values[0] + 1
                    else:
                        # columns3[0]:targetの値が0の場合、
                        df_tar_karute.loc[df_tar_karute[columns1[1]] == date, columns2[0]] = 1

                # 結合
                df_new_flag = pd.concat([df_new_flag, df_tar_karute])

        # 不要な列削除
        df_new_flag_drop = df_new_flag.drop([columns3[0], 'index_dt', 'index_dt2'], axis=1,errors="ignore")
        # マージ
        indf_si_copy = pd.merge(df_tar, df_new_flag_drop, on=[columns1[0], columns1[1]], how='left')

    else:
        indf_si_copy=pd.DataFrame(columns=[columns1[0], columns1[1], columns2[0]])

    try:
        indf_si_copy.to_csv(outpath, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_malignant_tumor"]))
