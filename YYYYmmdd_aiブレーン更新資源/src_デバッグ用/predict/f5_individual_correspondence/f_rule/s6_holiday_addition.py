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
    祝日の手術記録を抽出。
    22:00~24:00、00:00~5:59”””以外”””＝日曜の6:00～21:59　＋祝も
    """
    # 開始ログ
    l = lambda x: x[0](x[1]);
    l(logger.format_log_message("AIR001001", ["s6_holiday_addition"]))

    if "files" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 2:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    outpath = files[0]
    master_path = files[1]
    if not os.path.exists(master_path):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [master_path]))
        raise Exception
    if "columns" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 2:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["columns", 2]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    if "target" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["target"]))
        raise Exception
    target = params["target"]

    if "rule_time_horiday" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["rule_time_horiday"]))
        raise Exception
    rule_time_horiday = params["rule_time_horiday"]
    if len(rule_time_horiday) < 1:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["rule_time_horiday", 1]))
        raise Exception
    rule_time_horiday_1 = rule_time_horiday[0]

    # 出力フォルダの作成
    os.makedirs(outpath, exist_ok=True)

    # target：jsonで指定したtarget、in_targets：predicti_receiptで指定したtargets
    # in_targetsにtargetがあれば、処理を実施
    if target in in_targets:

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
        if len(df_trilevel) != 0:

            df_trilevel.loc[:, columns1[2]] = df_trilevel[columns1[2]].str.strip()

            df_trilevel.loc[:, '開始時間_dt'] = pd.to_datetime(df_trilevel[columns1[2]], format='%H:%M')

            df_trilevel.loc[:, 'index_dt'] = pd.to_datetime(df_trilevel[columns1[1]], format='%Y/%m/%d')
            df_trilevel.loc[:, '曜日_num'] = df_trilevel['index_dt'].dt.dayofweek  # 月曜始まり（int）

            # {0: '月', 1: '火', 2: '水', 3: '木', 4: '金', 5: '土', 6: '日'}
            # 手術実施日が日曜日（※祝日以外）　日曜のみ2
            df_trilevel.loc[:, '平日休日フラグ'] = df_trilevel['曜日_num'].replace({0: 1, 1: 1, 2: 1,
                                                                           3: 1, 4: 1, 5: 1, 6: 2})

            # 休日祝日マスタ結合
            new_df = pd.merge(df_trilevel, syukudf, on=columns1[1], how='left')
            new_df[columns2[0]] = new_df[columns2[0]].fillna(0)

            # 手術実施日が日曜日（※祝日以外）　祝日を3で上書き
            new_df.loc[new_df[columns2[0]] != 0, '平日休日フラグ'] = 3
            new_df['算定漏れ確率'] = 0

            # 休日加算ルール
            def zikangai_check(l_df):
                flag = 0

                if l_df['平日休日フラグ'] >= 2:
                    # 22:00~24:00、00:00~5:59”””以外”””
                    # ＝日曜の6:00～21:59　＋祝も
                    t1 = strptime(rule_time_horiday_1[0], '%H:%M:%S')
                    t2 = strptime(rule_time_horiday_1[1], '%H:%M:%S')

                    nn = strptime(l_df[columns1[2]], '%H:%M')
                    if t1 <= nn <= t2:
                        flag = 1

                return flag

            new_df['算定漏れ確率'] = new_df.T.apply(lambda x: zikangai_check(x))

            # len(new_df)!=0の場合に出力
            if len(new_df) != 0:
                out_p=outpath + target + ".csv"
                try:
                    new_df.to_csv(out_p, index=False, encoding='cp932')
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [out_p+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise
        else:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [columns1[2]]))

    # 終了ログ
    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["s6_holiday_addition"]))
