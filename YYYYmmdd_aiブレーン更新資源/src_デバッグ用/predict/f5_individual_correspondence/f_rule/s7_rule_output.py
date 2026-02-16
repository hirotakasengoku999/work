# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re

def call(params, logger, outdir, in_targets):
    # 開始ログ
    l = lambda x: x[0](x[1]);
    l(logger.format_log_message("AIR001001", ["s7_rule_output"]))

    if "files" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 5:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["files", 5]))
        raise Exception
    rule_dir = files[0]
    if not os.path.exists(rule_dir):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [rule_dir]))
        raise Exception
    inpath_si = files[1]
    if not os.path.exists(inpath_si):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [inpath_si]))
        raise Exception
    master_path = files[2]
    if not os.path.exists(master_path):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [master_path]))
    code_path = files[3]
    if not os.path.exists(code_path):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [code_path]))
    name_path = files[4]
    if not os.path.exists(name_path):
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003012", [name_path]))
    if "targets" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["target"]))
        raise Exception
    _target_list = params["targets"]
    if "columns" not in params:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 3:
        l = lambda x: x[0](x[1]);
        l(logger.format_log_message("AIR003011", ["columns", 3]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    columns3 = columns[2]
    if "outcolumns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["outcolumns"]))
        raise Exception
    outcolumns = params["outcolumns"]

    # _target_list：jsonで指定したtargets、in_targets：predicti_receiptで指定したtargets
    # 共通する算定項目コードを抽出する。共通する算定項目コードがない場合、処理をスキップ。
    target_list = list(set(_target_list) & set(in_targets))
    if len(target_list) > 0:
        # 診療科コード読み込み
        indf_h = pd.read_csv(code_path, engine='python', dtype='object',encoding='cp932')

        # 氏名読み込み
        indf_n = pd.read_csv(name_path, engine='python', dtype='object',encoding='cp932')

        # masetr読み込み
        try:
            indf_m = pd.read_csv(master_path, engine='python', usecols=columns2, dtype='object',encoding='cp932')
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

        # 診療行為.pickle読み込み
        inData = pd.read_pickle(inpath_si)
        usec = [columns1[0], columns1[1]] + target_list
        # 対象の列がない場合も抽出する
        inData = inData.loc[:, inData.columns.intersection(usec)].reindex(columns=usec)
        inData[target_list] = inData[target_list].fillna(0)

        for taretNO in target_list:

            # targetの名称抽出
            target_name = indf_m[columns2[1]][indf_m[columns2[0]] == taretNO].values

            in_path = rule_dir + taretNO + '.csv'
            if not os.path.exists(in_path):
                # データがないため、処理をスキップします。
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [in_path]))
                continue

            try:
                wkdf = pd.read_csv(in_path, engine='python', encoding="cp932",
                                   usecols=columns1, dtype={columns1[0]: 'object'})
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
                raise

            # 曜日
            wkdf['index_dt'] = pd.to_datetime(wkdf[columns1[1]], format='%Y/%m/%d')
            wkdf['曜日_num'] = wkdf['index_dt'].dt.dayofweek  # 月曜始まり（int）
            wkdf['曜日'] = wkdf['曜日_num'].replace({0: '月', 1: '火', 2: '水', 3: '木', 4: '金', 5: '土', 6: '日'})
            # {0: '月', 1: '火', 2: '水', 3: '木', 4: '金', 5: '土', 6: '日'}

            # 算定根拠
            konkyo_column = columns3[1] + '_1'
            if taretNO == "150371590":
                wkdf[konkyo_column] = wkdf.apply(
                    lambda x: "手術開始時間が時間外（[" + x['曜日'] + ']' + x[columns1[1]] + ' ' + x[columns1[3]] + "）", axis=1)
            elif taretNO == "150371290":
                wkdf[konkyo_column] = wkdf.apply(
                    lambda x: "手術開始日が日曜日または祝日（年末年始含む）（[" + x['曜日'] + ']' + x[columns1[1]] + ' ' + x[columns1[3]] + "）",
                    axis=1)
            elif taretNO == "150371490":
                wkdf[konkyo_column] = wkdf.apply(
                    lambda x: "手術開始時間が深夜（" + x[columns1[3]] + "）", axis=1)
            else:
                wkdf[konkyo_column] = wkdf.apply(lambda x: x[columns1[3]], axis=1)

            _wk_df = pd.merge(wkdf, inData[[columns1[0], columns1[1], taretNO]], on=[columns1[0], columns1[1]])
            _wk_df.loc[_wk_df[taretNO] >= 1, taretNO] = 1
            _wk_df = _wk_df.fillna(0)

            _wk_df = _wk_df.loc[:, [columns1[0], columns1[1], taretNO, columns1[2], konkyo_column]]
            data = _wk_df.values
            sort_1 = sorted(data, key=lambda x: x[3], reverse=True)
            sort_2 = sorted(sort_1, key=lambda x: x[2])
            out_df_new = pd.DataFrame(sort_2, columns=_wk_df.columns)

            # 算定もれ根拠修正
            out_df_new.loc[out_df_new[columns1[2]] == 0, konkyo_column] = -1

            #診療科と結合
            out_df_new = pd.merge(out_df_new, indf_h, how='left', on=[columns1[0], columns1[1]])
            # 氏名と結合
            out_df_new = pd.merge(out_df_new, indf_n, how='left', on=[columns1[0]])

            # 1/0を100.00 or 0.00に変換
            out_df_new.loc[:, columns1[2]] = out_df_new[columns1[2]].fillna(0).map('{:.2%}'.format).astype(
                "str").str.replace("%", "")
            # 出力フォーマット
            out_df_new.loc[:, columns3[0]] = target_name
            out_df_new.loc[:, columns3[4]] = out_df_new[taretNO]

            out_df_new = out_df_new.loc[:, out_df_new.columns.intersection(outcolumns)].reindex(columns=outcolumns)
            out_df_new = out_df_new.fillna(-1)

            # 出力エラーチェック
            out_path = outdir + taretNO + '.csv'
            try:
                out_df_new.to_csv(out_path, index=False, encoding='cp932')
            except:
                import traceback, sys
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003009", [out_path+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                raise

    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["s7_rule_output"]))
