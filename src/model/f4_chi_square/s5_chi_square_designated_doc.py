# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import numpy as np
import scipy as sp
import scipy.stats
import os
import re
import collections

def call(params, logger):
    """
    ⑨カルテ全体　のX二乗検　実施
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s5_chi_square_designated_doc"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 5:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 5]))
        raise Exception
    inpath_si = files[0]
    if not os.path.exists(inpath_si):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_si]))
        raise Exception
    base_path = files[1]
    if not os.path.exists(base_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [base_path]))
        raise Exception
    inDir = files[2]
    if not os.path.exists(inDir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inDir]))
        raise Exception
    outDir = files[3]
    wkdir = files[4]
    if not os.path.exists(wkdir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [wkdir]))
        raise Exception
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 4:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 4]))
        raise Exception
    columns2 = columns[1]
    columns1 = columns[0]
    columns3 = columns[2]
    columns4 = columns[3]
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    target_list = params["targets"]
    if "bunsyo_syubetu" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["bunsyo_syubetu"]))
        raise Exception
    bunsyo_list = params["bunsyo_syubetu"]
    if "specific_target_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["specific_target_list"]))
        raise Exception
    specific_target_list = params["specific_target_list"]
    if "p_value" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["p_value"]))
        raise Exception
    p_value = params["p_value"]

    # targetlistがカラの場合
    if len(target_list) != 0:

        # baseCSV
        try:
            df_head = pd.read_csv(base_path, engine='python', dtype={columns1[0]: 'object'}
                                , usecols=[columns1[0], columns1[1], columns1[2]])
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
            raise
        df_head = df_head[df_head[columns1[2]] == 1][[columns1[0], columns1[1]]]

        # 診療行為読み込み
        df_si = pd.read_pickle(inpath_si)
        # target_listからspecific_target_listと重複する項目を抽出
        spe_targets = list(set(target_list) & set(specific_target_list))
        # 診療行為dfの項目と target_list の重複している項目を抽出
        si_targets = list(set(df_si.columns) & set(target_list))
        # x二乗を実施する項目のlist
        chi_list = si_targets + spe_targets

        # 診療行為項目抽出
        df_si = df_si[[columns2[0], columns2[1]] + si_targets]
        new_df_si = pd.merge(df_head, df_si, on=[columns2[0], columns2[1]], how='left', copy=False)

        # カルテ 文書
        for i in range(len(target_list)):
            # 同じ要素番号の値を抽出
            b = bunsyo_list[i]  # list
            target = target_list[i]  # str

            # X二乗実施項目にない場合
            # 項目の有無チェック(chi_list)
            if target not in chi_list:
                # 算定項目は実行されません
                l = lambda x: x[0](x[1])
                l(logger.format_log_message("AIR002001", [target, "targets", target]))
                # ない場合次のfor
                continue

            # 実績の有無チェック
            if target not in specific_target_list:
                if new_df_si[target].sum() == 0:
                    # 算定項目は実行されません
                    l = lambda x: x[0](x[1])
                    l(logger.format_log_message("AIR002001", [target, "targets", target]))
                    # ない場合次のfor
                    continue

            # specific_target_listに算定項目が含まれる場合
            if target in specific_target_list:

                spe_path = wkdir + target + '.csv'
                # ファイル有無　なければ contin messe
                if not os.path.isfile(spe_path):
                    # データがないため、処理をスキップします
                    l = lambda x: x[0](x[1])
                    l(logger.format_log_message("AIR002002", [spe_path]))
                    # ない場合次のfor
                    continue

                # specificCSV
                try:
                    df_specific = pd.read_csv(spe_path, engine='python', dtype={columns2[0]: 'object'}, usecols=[columns2[0], columns2[1], target])
                except ValueError as ve:
                    keys = []
                    for arg in ve.args:
                        keys.extend(re.findall(r"\'(.*)\'", arg))
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                    continue

                new_df_si_spec = pd.merge(df_specific, df_head, on=[columns2[0], columns2[1]], copy=False)

                if new_df_si_spec[target].sum() == 0:
                    # 算定項目は実行されません
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002001", [target, "targets", target]))
                    # ない場合次のfor
                    continue

            # 出力フォルダ作成
            os.makedirs(outDir + target + '/karute/', exist_ok=True)

            # 文書種別ごとに処理
            for filname in b:

                filepath = inDir + filname + '_形態素解析.csv'
                # ファイル有無　なければ contin messe
                if not os.path.isfile(filepath):
                    # データがないため、処理をスキップします
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [filepath]))
                    # ない場合次のfor
                    continue

                # karuteCSV
                try:
                    karute_df = pd.read_csv(filepath, engine='python', dtype={columns3[0]: 'object'},
                                            usecols=[columns3[0], columns3[1], columns3[2]])
                except ValueError as ve:
                    keys = []
                    for arg in ve.args:
                        keys.extend(re.findall(r"\'(.*)\'", arg))
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                    continue
                in_df_len = len(karute_df)

                karute_df[columns3[2]] = karute_df[columns3[2]].apply(lambda x: x.split(' '))
                l_1d = karute_df[columns3[2]].values.tolist()
                l_2d = [e for inner_list in l_1d for e in inner_list]

                l_2nd_tupl = collections.Counter(l_2d)
                target_col_list = [i for i, v in l_2nd_tupl.items() if v > 2]
                if "" in target_col_list:
                    target_col_list.remove('')

                # 診療行為と結合
                df_me_tmp = pd.merge(new_df_si, karute_df, on=[columns3[0], columns3[1]], how='left', copy=False)

                df_me = df_me_tmp.copy()
                # specific_target_listに算定項目が含まれる場合
                if target in specific_target_list:
                    df_me = pd.merge(new_df_si_spec, karute_df, on=[columns2[0], columns2[1]], copy=False, how='left')

                i = 0
                out_df = pd.DataFrame(columns=columns4)

                c_df = df_me[[target, columns3[2]]].copy()
                c_df[columns3[2]] = c_df[columns3[2]].fillna('')
                c_df[target] = c_df[target].fillna(0)
                c_df[target][c_df[target] > 0] = 1

                for use_col in target_col_list:
                    tmp = c_df.copy()
                    tmp[columns3[2]] = tmp[columns3[2]].apply(lambda x: use_col in x).astype(np.float32)
                    if sum(tmp[columns3[2]]) == 0:
                        continue

                    # クロス集計
                    crossed = pd.crosstab(tmp[target], tmp[columns3[2]])

                    a = crossed.iloc[0, 0]  # 算定なし/なし
                    b = crossed.iloc[0, 1]  # 算定なし/あり
                    c = crossed.iloc[1, 0]  # 算定あり/なし
                    d = crossed.iloc[1, 1]  # 算定あり/あり

                    D_d = ((c + d) * (b + d)) / (a + b + c + d)

                    # カイ二乗値、p値,自由度
                    x2, p, dof, expected = sp.stats.chi2_contingency(crossed)  # x2:カイ二乗値.p:確率

                    if p < p_value and d > D_d:
                        flags = 1
                        out_df.loc[i, columns4] = filname, use_col, p, (b + d) / in_df_len, flags
                        i = i + 1

                outpath = str(outDir + target + '/karute/' + filname + '_X二乗.csv')

                try:
                    out_df.to_csv(outpath, index=False, encoding='cp932')
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s5_chi_square_designated_doc"]))
