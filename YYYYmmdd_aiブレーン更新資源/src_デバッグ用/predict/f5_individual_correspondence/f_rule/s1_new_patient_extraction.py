# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import calendar
import os
import re
from datetime import datetime as dt

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from pathlib import Path


def call(params, logger, heap_dir, outdir, in_targets, karte_path):
    '''
    index:日付/カルテ番号 column:コード 1/0 のデータに加工
    4310401→20180401に変換
    '''

    # 開始ログ
    l = lambda x: x[0](x[1]);
    l(logger.format_log_message("AIR001001", ["s1_new_patient_extraction"]))

    if "files" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 9:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["files", 9]))
        raise Exception
    in_dir = files[0]
    if not os.path.exists(in_dir):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [in_dir]))
        raise Exception
    out_dir = files[1]
    input_date = files[2]
    if not os.path.exists(input_date):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [input_date]))
        raise Exception
    cdpath = files[3]
    if not os.path.exists(cdpath):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [cdpath]))
        raise Exception
    headerpath = files[4]
    if not os.path.exists(headerpath):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [headerpath]))
        raise Exception
    rule_modelp_path = heap_dir + files[5]
    if not os.path.exists(rule_modelp_path):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [rule_modelp_path]))
        raise Exception
    code_path = files[6]
    if not os.path.exists(code_path):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [code_path]))
        raise Exception
    master_path = files[7]
    if not os.path.exists(master_path):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [master_path]))
    name_path = files[8]
    if not os.path.exists(name_path):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [name_path]))
    if "columns" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 6:# 716_初診料_再診予約対応 len(columns) < 5 -> len(columns) < 6
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["columns", 5]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    columns3 = columns[2]
    columns4 = columns[3]
    columns5 = columns[4]
    columns6 = columns[5]
    if "rese_list" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["rese_list"]))
        raise Exception
    rese_list = params["rese_list"]
    if len(rese_list) < 4:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["rese_list", 4]))
        raise Exception
    rese = rese_list[0]
    rese_code = rese_list[1]
    resekubun = rese_list[2]
    rese_num = rese_list[3]
    if "target" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["target"]))
        raise Exception
    target = params["target"]
    # 716_初診料_再診予約対応
    if "uketuke_bunsyo_syubetu" not in params:
        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR003010", ["uketuke_bunsyo_syubetu"]))
        raise Exception
    uketuke_bunsyo_syubetu = params["uketuke_bunsyo_syubetu"]
    if "reservation_type_column" not in params:
        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR003010", ["reservation_type_column"]))
        raise Exception
    if "excluded_reservation_types" not in params:
        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR003010", ["excluded_reservation_types"]))
        raise Exception
    excluded_reservation_types = params["excluded_reservation_types"]
    # 753_初診料_他保険算定対応
    if "other_ins_bunsyo_syubetu" not in params:
        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR003010", ["other_ins_bunsyo_syubetu"]))
        raise Exception
    other_ins_bunsyo_syubetu = params["other_ins_bunsyo_syubetu"]
    if "other_ins_column" not in params:
        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR003010", ["other_ins_column"]))
        raise Exception
    other_ins_column = params["other_ins_column"]
    reservation_type_column = params["reservation_type_column"]
    if "drops" not in params:
        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR003010", ["drops"]))
        raise Exception
    drops = params["drops"]
    if "outcolumns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["outcolumns"]))
        raise Exception
    outcolumns = params["outcolumns"]

    # target：jsonで指定したtarget、in_targets：predicti_receiptで指定したtargets
    # in_targetsにtargetがあれば、処理を実施。
    if target in in_targets:

        # 出力フォルダの作成
        os.makedirs(out_dir, exist_ok=True)

        # date
        s_target_month_l = pd.read_csv(input_date, engine='python',
                                       encoding='cp932', dtype='object').columns.tolist()

        # 診療科コード読み込み
        indf_h = pd.read_csv(code_path, engine='python', dtype='object',encoding='cp932')

        # 氏名読み込み
        indf_n = pd.read_csv(name_path, engine='python', dtype='object',encoding='cp932')

        # masetr読み込み
        try:
            indf_m = pd.read_csv(master_path, engine='python', usecols=columns5, dtype='object',encoding='cp932')
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

        # targetの名称抽出
        target_name = indf_m[columns5[1]][indf_m[columns5[0]] == target].values

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

        # CD_all 読み込み
        try:
            df_cd = pd.read_csv(cdpath, engine='python', encoding='cp932', dtype='object',
                                usecols=columns2)
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
            raise

        # CD_allを対象のレセ項目のコードに限定、コードを対象レセ項目の名称にリネーム
        df_cd_rese = df_cd[df_cd[columns2[2]].str.startswith(rese_num)]
        df_cd_rese = df_cd_rese.rename(columns={columns2[2]: rese_code})

        # 結合用データフレーム作成
        month_df = pd.DataFrame()

        # 月毎に処理
        for s_target_month in s_target_month_l:
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

            for kubun, kubun2 in zip(['dpc', 'ika'], ['RECEIPTD', 'RECEIPTC']):
                for hoken in ['国保', '社保']:

                    inFile_str = in_dir + '/' + s_target_month + '/' + kubun + '/' + kubun2 + hoken
                    inpath_si = inFile_str + '_' + resekubun + '.csv'

                    # ファイルチェック：なければWARNIG、skip
                    if not os.path.exists(inpath_si):
                        l = lambda x: x[0](x[1]);
                        l(logger.format_log_message("AIR002002", [inpath_si]))
                        continue

                    # inpath_si 読み込み
                    try:
                        df_si = pd.read_csv(inpath_si, engine='python', encoding='cp932',
                                            dtype=({columns1[0]: 'object', rese_code: 'str'}),
                                            usecols=columns1 + [rese_code] + date_list[0])
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
                        raise

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
                        df_si[date_info_list[30]] = arr_sum

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
                                df_cd_rese_date = df_cd_rese[df_cd_rese[columns2[1]] == tdatetime]
                                day_df = pd.concat([day_df, df_cd_rese_date])

                            day_df = day_df[day_df[columns2[1]] != 0]
                            month_df = pd.concat([month_df, day_df])

        # 作業ファイルの読み込みに失敗
        if len(month_df) == 0:
            # データがないためスキップ、算定項目({})は実行されません。
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [in_dir]))
            return 0
        else:
            # 初診料の算定実績抽出
            out_syosin_df = month_df[month_df[rese_code] == target]
            out_syosin_df = out_syosin_df[[columns2[0], columns2[1]]]
            out_syosin_df[target] = 1
            # 初診料inData
            out_syosin_df = out_syosin_df[~out_syosin_df.duplicated()]
            drops_df = month_df[month_df[rese_code].isin(drops)]

            out_tmp_df = month_df[[columns2[0], columns2[1]]]
            out_tmp_df = out_tmp_df[~out_tmp_df.duplicated()]

            # 最小の日を抽出
            out_tmp_df[columns2[1]] = pd.to_datetime(out_tmp_df[columns2[1]])
            out_max = out_tmp_df.groupby([columns2[0]]).min()
            out_max.loc[:, columns2[1]] = out_max[columns2[1]].dt.strftime('%Y/%m/%d')
            out_max.loc[:, '日付_dt'] = pd.to_datetime(out_max[columns2[1]], format='%Y/%m/%d')
            out_max.loc[:, '4ヵ月前日付_dt'] = out_max['日付_dt'].apply(lambda x: x - relativedelta(months=4))
            out_max.reset_index(inplace=True)

            # モデル中間ファイル , "../work/heap/"+rule/rule_info.csv
            try:
                df_model = pd.read_csv(rule_modelp_path, engine='python', encoding='cp932', usecols=[columns2[0], columns2[1]], dtype={columns2[0]: 'object'})
            except UnicodeDecodeError:
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [rule_modelp_path]))
                raise
            except ValueError as ve:
                keys = [rule_modelp_path]
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [keys]))
                raise
            except:
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [rule_modelp_path]))
                raise

            # 読み込んだデータがから
            if len(df_model) == 0:
                # データがないためスキップ、算定項目({})は実行されません。
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [rule_modelp_path]))
                return 0

            df_model.loc[:, '日付_dt'] = pd.to_datetime(df_model[columns2[1]], format='%Y/%m/%d')

            # カルテの受付情報
            karte_list = []
            if uketuke_bunsyo_syubetu and reservation_type_column:
                columns6.append(reservation_type_column)
            # 753_初診料_他保険算定対応
            if other_ins_bunsyo_syubetu and other_ins_column:
                columns6.append(other_ins_column)
            for karte_file in Path(karte_path).glob('**/*.csv'):
                karte_list.append(pd.read_csv(karte_file, engine='python', encoding='cp932',dtype='object'))
            karte_df = pd.concat(karte_list)

            # 716_初診料_再診予約対応 予約患者のリストを作成
            karte_use_cols = [c for c in karte_df.columns if c in columns6]
            karte_df = karte_df[karte_use_cols]
            excluded_patient_dict = {}
            if uketuke_bunsyo_syubetu and reservation_type_column and reservation_type_column in karte_use_cols and len(excluded_reservation_types) > 0:
                excluded_df = karte_df[karte_df[reservation_type_column].isin(excluded_reservation_types)]
                excluded_patient_dict = dict(zip(excluded_df[columns6[0]], excluded_df[reservation_type_column]))

            # 753_初診料_他保険算定対応 他保険で算定済みの患者リストを作成
            other_ins_patient_list = []
            if other_ins_bunsyo_syubetu and other_ins_column and other_ins_column in karte_use_cols:
                other_ins_patient_list = list(
                    karte_df[
                        (karte_df[columns6[2]] == other_ins_bunsyo_syubetu) &
                        (karte_df[other_ins_column] == target)
                    ][columns6[0]].unique()
                )
            karte_df = karte_df[[columns6[0], columns6[1]]]

            # カルテ番号等列でグループ化し、最小日付を取得する
            karte_min = karte_df.groupby(columns6[0])[columns6[1]].min().reset_index()
            # 重複する行を削除する
            karte_min = karte_min.drop_duplicates()
            karte_min.loc[:, '日付_dt'] = pd.to_datetime(karte_min[columns2[1]],format='%Y/%m/%d')

            # 出力用のデータフレーム
            out_df = out_max[[columns2[0], columns2[1]]]
            out_df = out_df.set_index([columns2[0], columns2[1]])
            out_df['算定漏れ確率'] = np.nan
            out_df['過去実績日付'] = np.nan

            # 対象月の患者を1件ずつ確認
            for kaute in out_max[columns2[0]].unique():

                df_mdoel_dt = df_model[df_model[columns2[0]] == kaute]
                df_pre_dt = out_max[out_max[columns2[0]] == kaute]

                # 716_初診料_再診予約対応 条件に予約患者のリストに含まれていない場合を追加
                if not kaute in excluded_patient_dict.keys():
                    flag = 0
                    # モデル中間ファイルに患者が存在する場合
                    if len(df_mdoel_dt) > 0:
                        model_dt = df_mdoel_dt['日付_dt'].values[0]
                        pre_4month_dt = df_pre_dt['4ヵ月前日付_dt'].values[0]

                        # 2019/4/5の実績の4カ月前の日付2018/12/5に対して、実際過去2019/2/23に実績があった場合、flag=0
                        flag = 1 if pre_4month_dt >= model_dt else 0
                        model_dt_str = pd.to_datetime(str(model_dt)).strftime('%Y/%m/%d')

                    # モデル中間ファイルに患者が存在しない場合、過去に診療実績がないため、flag=1
                    else:
                        flag = 1
                        model_dt_str = np.nan

                    # 予測月のカルテ受付情報に来院していればflag = 0
                    if flag == 1 and len(karte_min) > 0:
                        df_karte_pre_dt = karte_min[
                            karte_min[columns2[0]] == kaute]
                        if len(df_karte_pre_dt) > 0:
                            karte_pre_dt = df_karte_pre_dt['日付_dt'].values[0]
                            receipt_pre_dt = df_pre_dt['日付_dt'].values[0]
                            flag = 0 if receipt_pre_dt > karte_pre_dt else flag

                    # 初診料算定漏れと判断されたが、背反項目が同日に算定されていれば算定漏れ確率0にする
                    if flag == 1:
                        work_kaute_df = out_max[out_max[columns2[0]] == kaute]
                        work_date = work_kaute_df.loc[work_kaute_df.index[0], "_date"]
                        work_drops_df = drops_df[(drops_df[columns2[0]] == kaute) & (drops_df[columns2[1]] == work_date)]
                        if len(work_drops_df) > 0:
                            flag = 0
                else:
                    flag = 0
                    model_dt_str = excluded_patient_dict[kaute]

                out_df.loc[(kaute, df_pre_dt[columns2[1]].values[0]), '算定漏れ確率'] = flag
                out_df.loc[(kaute, df_pre_dt[columns2[1]].values[0]), '過去実績日付'] = model_dt_str

            # 結果ファイル作成
            out_df = out_df.reset_index()
            # 算定根拠
            konkyo_column = columns3[1] + '_1'
            out_df[konkyo_column] = out_df.apply(lambda x: "前回診療日（" + str(x["過去実績日付"]).replace('nan', '') + "）", axis=1)

            # 診療行為結合
            _wk_df = pd.merge(out_df, out_syosin_df, on=[columns2[0], columns2[1]], how='left')
            # 753_初診料_他保険算定対応 他保険で算定済みの患者は算定実績に1を設定
            _wk_df.loc[_wk_df[columns2[0]].isin(other_ins_patient_list), target] = 1
            _wk_df.loc[_wk_df[target] >= 1, target] = 1
            _wk_df = _wk_df.fillna(0)

            _wk_df = _wk_df.loc[:, [columns2[0], columns2[1], target, '算定漏れ確率', konkyo_column]]
            data = _wk_df.values
            sort_2 = sorted(data, key=lambda x: x[3], reverse=True)
            sort_3 = sorted(sort_2, key=lambda x: x[2])
            out_df_new = pd.DataFrame(sort_3, columns=_wk_df.columns)

            # 診療科と結合
            out_df_new = pd.merge(out_df_new, indf_h, how='left', on=[columns2[0]])
            # 氏名と結合
            out_df_new = pd.merge(out_df_new, indf_n, how='left', on=[columns2[0]])

            # 1/0を100.00 or 0.00に変換
            out_df_new.loc[:, '算定漏れ確率'] = out_df_new['算定漏れ確率'].fillna(0).map('{:.2%}'.format).astype("str").str.replace("%", "")
            # 出力フォーマット
            out_df_new.loc[:, columns3[0]] = target_name
            out_df_new.loc[:, columns3[4]] = out_df_new[target]

            out_df_new = out_df_new.loc[:, out_df_new.columns.intersection(outcolumns)].reindex(columns=outcolumns)
            out_df_new = out_df_new.fillna(-1)

            # 出力エラーチェック
            out_path = outdir + target + '.csv'
            try:
                out_df_new.to_csv(out_path, index=False, encoding='cp932')
            except:
                import traceback, sys
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003009", [out_path+ ":" +traceback.format_exception(*sys.exc_info())[-1]]))
                raise

    # 終了ログ
    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["s1_new_patient_extraction"]))
