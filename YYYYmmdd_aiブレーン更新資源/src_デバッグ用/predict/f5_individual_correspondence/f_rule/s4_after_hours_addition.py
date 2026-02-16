# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
from time import strptime, strftime
import os
import glob
import re

def call(params, logger, karute_dir, in_targets):
    """
    時間外の手術記録を抽出。
    対象：入院初日のみ
    背反項目：columns4[2]"150371390"
    月～金曜　18:00～21:59,6:00~7:59、土曜　12：00～21: 59　※祝日除く
    """
    # 開始ログ
    l = lambda x: x[0](x[1]);
    l(logger.format_log_message("AIR001001", ["s4_after_hours_addition"]))

    if "files" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 4:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["files", 4]))
        raise Exception
    outpath = files[0]
    master_path = files[1]
    if not os.path.exists(master_path):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [master_path]))
        raise Exception
    nyuin_path = files[2]
    if not os.path.exists(nyuin_path):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [nyuin_path]))
        raise Exception
    inpath_si = files[3]
    if not os.path.exists(inpath_si):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [inpath_si]))
        raise Exception
    if "columns" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 4:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["columns", 4]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    columns3 = columns[2]
    columns4 = columns[3]
    if "target" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["target"]))
        raise Exception
    target = params["target"]

    if "rule_time_weekday" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["rule_time_weekday"]))
        raise Exception
    rule_time_weekday = params["rule_time_weekday"]
    if len(rule_time_weekday) < 2:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["rule_time_weekday", 2]))
        raise Exception
    rule_time_weekday_1 = rule_time_weekday[0]
    rule_time_weekday_2 = rule_time_weekday[1]

    if "rule_time_saturday" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["rule_time_saturday"]))
        raise Exception
    rule_time_saturday = params["rule_time_saturday"]
    if len(rule_time_saturday) < 1:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["rule_time_saturday", 1]))
        raise Exception
    rule_time_saturday_1 = rule_time_saturday[0]

    # 出力フォルダの作成
    os.makedirs(outpath, exist_ok=True)

    # target：jsonで指定したtarget、in_targets：predicti_receiptで指定したtargets
    # in_targetsにtargetがあれば、処理を実施
    if target in in_targets:

        # 患者の入院開始日のデータ読み込み
        try:
            nyuindf = pd.read_csv(nyuin_path, engine='python', encoding="cp932", usecols=columns3,
                                  dtype={columns3[0]: 'object'})
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
            raise

        # 入院初日のみ対象
        nyuindf = nyuindf.rename(columns={columns3[1]: columns1[1]})
        nyuindf = nyuindf[~nyuindf.duplicated()]
        nyuindf[columns1[1]] = nyuindf[columns1[1]].str.replace("-", "/")

        # 休日祝日マスタ
        try:
            syukudf = pd.read_csv(master_path, engine='python', encoding="cp932", usecols=columns2)
        except UnicodeDecodeError:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [master_path]))
            raise
        except ValueError as ve:
            keys = [master_path]
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", [keys]))
            raise
        except:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [master_path]))
            raise

        # 日付フォーマット修正
        try:
            syukudf[columns1[1]] = pd.to_datetime(syukudf[columns2[0]], format='%Y/%m/%d')
            syukudf[columns1[1]] = syukudf[columns1[1]].dt.strftime('%Y/%m/%d')
        except ValueError:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003013", [master_path]))
            raise

        # 電子カルテ読み込み
        # karute_dir 配下のファイル名取得
        # predict_receipt.pyでkarute_dir配下にファイルが存在することは確認済み
        folder_path_list = glob.glob(karute_dir + '*')
        wkdf = pd.DataFrame()
        for karute_path in folder_path_list:
            # カルテファイル読み込み
            try:
                in_df = pd.read_csv(karute_path, engine='python', encoding="cp932",
                                    usecols=columns1, dtype={columns1[0]: 'object'})
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
                raise

            wkdf = pd.concat([wkdf, in_df])

        # 空白除外
        df_trilevel = wkdf.dropna(subset=[columns1[2]])
        df_trilevel=df_trilevel.reset_index()

        # 長さチェック
        if len(df_trilevel)!=0:
            df_trilevel.loc[:, columns1[2]] = df_trilevel[columns1[2]].astype('str')
            df_trilevel.loc[:, columns1[2]] = df_trilevel[columns1[2]].str.strip()
            df_trilevel = df_trilevel[df_trilevel[columns1[2]] != 'NaT']

            # 時間、曜日変換 フォーマットチェック
            df_trilevel['開始時間_dt'] = pd.to_datetime(df_trilevel[columns1[2]], format='%H:%M')
            df_trilevel['index_dt'] = pd.to_datetime(df_trilevel[columns1[1]], format='%Y/%m/%d')
            df_trilevel['曜日_num'] = df_trilevel['index_dt'].dt.dayofweek  # 月曜始まり（int）

            # {0: '月', 1: '火', 2: '水', 3: '木', 4: '金', 5: '土', 6: '日'}
            # 平日:1,土曜:2,日曜:3
            df_trilevel['平日休日フラグ'] = df_trilevel['曜日_num'].replace({0: 1, 1: 1, 2: 1,
                                                                    3: 1, 4: 1, 5: 2, 6: 3})

            new_df = pd.merge(df_trilevel, syukudf, on=columns1[1], how='left')

            # 祝日
            new_df['国民の祝日・休日月日'] = new_df['国民の祝日・休日月日'].fillna(0)
            new_df.loc[new_df['国民の祝日・休日月日'] != 0, '平日休日フラグ'] = 4
            new_df['算定漏れ確率'] = 0

            # 一行ずつ時間外判断
            def zikangai_check(l_df):
                flag = 0

                # 平日休日フラグ=2　の場合=土曜
                if l_df['平日休日フラグ'] == 2:
                    # 12：00～21: 59
                    t1 = strptime(rule_time_saturday_1[0], '%H:%M:%S')
                    t2 = strptime(rule_time_saturday_1[1], '%H:%M:%S')
                    nn = strptime(l_df[columns1[2]], '%H:%M')

                    if t1 <= nn <= t2:
                        flag = 1

                elif l_df['平日休日フラグ'] == 1:
                    # 6:00～7:59
                    t1 = strptime(rule_time_weekday_1[0], '%H:%M:%S')
                    t2 = strptime(rule_time_weekday_1[1], '%H:%M:%S')
                    # 18:00～21:59
                    t1_2 = strptime(rule_time_weekday_2[0], '%H:%M:%S')
                    t2_2 = strptime(rule_time_weekday_2[1], '%H:%M:%S')
                    nn = strptime(l_df[columns1[2]], '%H:%M')

                    if (t1 <= nn <= t2) | (t1_2 <= nn <= t2_2):
                        flag = 1
                return flag

            new_df['算定漏れ確率'] = new_df.T.apply(lambda x: zikangai_check(x))
            # 入院初日の患者のデータと結合
            syonitidf = pd.merge(nyuindf, new_df, on=[columns1[0], columns1[1]])

            # 背反項目　診療行為.pickle読み込み
            inData = pd.read_pickle(inpath_si)
            # 対象の列がない場合も抽出する
            inData = inData.loc[:, inData.columns.intersection(columns4)].reindex(columns=columns4)
            inData[columns4[2]] = inData[columns4[2]].fillna(0)

            outdf = pd.merge(syonitidf, inData, on=[columns4[0], columns4[1]])
            outdf.loc[outdf[columns4[2]] >= 1, '算定漏れ確率'] = 0

            # len(out_df)!=0の場合に出力
            if len(outdf) != 0:
                out_p=outpath + target + ".csv"
                try:
                    outdf.to_csv(out_p, index=False, encoding='cp932')
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [out_p+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise
        else:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [columns1[2]]))

    # 終了ログ
    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["s4_after_hours_addition"]))