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

def call(params, logger, model_path):
    """
    ②～⑥X二乗検　実施
    レセプト：傷病、医薬品、特定器材
    電子カルテ：タイトル有無、文書有無
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s2_chi_square_not_si"]))

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
    outDir = model_path + files[2]
    inpickle_dir = files[3]
    if not os.path.exists(inpickle_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpickle_dir]))
        raise Exception
    wkdir = files[4]
    if not os.path.exists(wkdir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [wkdir]))
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
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    target_list = params["targets"]
    if "specific_target_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["specific_target_list"]))
        raise Exception
    specific_target_list = params["specific_target_list"]
    if "p_value" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["p_value"]))
        raise Exception
    p_value_list = params["p_value"]
    if "use_word" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["use_word"]))
        raise Exception
    use_word_list = params["use_word"]
    if "x2_item_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["x2_item_list"]))
        raise Exception
    x2_item_list = params["x2_item_list"]

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
    df_me = pd.merge(df_head, df_si, on=[columns2[0], columns2[1]], how='left', copy=False)
    for x2_item in x2_item_list:
        # x2itemCSV:対象のpickle有無確認
        pickle_path=inpickle_dir + x2_item + '.pickle'

        if not os.path.exists(pickle_path):
            # データがないため、処理をスキップします
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [pickle_path]))
            continue
        # pickle読み込み
        df = pd.read_pickle(pickle_path)

        # データフレームが空の場合 skip
        if len(df)==0:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [pickle_path]))
            continue

        target_col_list = [s for s in df.columns if (columns2[0] not in s) and (columns2[1] not in s)]

        df_tmp_si = pd.merge(df_me, df, on=[columns2[0], columns2[1]], how='left', copy=False)

        for target, p_value, use_word in zip(target_list, p_value_list, use_word_list):

            # X二乗実施項目にない場合
            if target not in chi_list:
                # 算定項目は実行されません
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002001", [target, "targets", target]))
                # ない場合次のfor
                continue

            # 実績の有無確認
            if target not in specific_target_list:
                if df_me[target].sum() == 0:
                    # 算定項目は実行されません
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002001", [target, "targets", target]))
                    # ない場合次のfor
                    continue

            # 出力フォルダ作成
            os.makedirs(outDir + "/" + target, exist_ok=True)

            df_tmp = df_tmp_si.copy()

            # specific_target_listに算定項目が含まれる場合
            if target in specific_target_list:
                spe_path = wkdir + target + '.csv'
                # ファイル有無　なければ contin messe
                if not os.path.isfile(spe_path):
                    # データがないため、処理をスキップします
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [spe_path]))
                    # ない場合次のfor
                    continue

                # specificCSV
                try:
                    df_specific = pd.read_csv(spe_path, engine='python', dtype={columns2[0]: 'object'},
                                            usecols=[columns2[0], columns2[1], target])
                except ValueError as ve:
                    keys = []
                    for arg in ve.args:
                        keys.extend(re.findall(r"\'(.*)\'", arg))
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                    continue

                df_me_spec = pd.merge(df_specific, df_head, on=[columns2[0], columns2[1]], copy=False)
                df_tmp = pd.merge(df_me_spec, df, on=[columns2[0], columns2[1]], copy=False, how='left')

                if df_tmp[target].sum()==0:
                    # 算定項目は実行されません
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002001", [target, "targets", target]))
                    # ない場合次のfor
                    continue
            i = 0
            df_p_flag = pd.DataFrame(columns=columns3)
            for use_col in target_col_list:
                target_df = df_tmp[[target, use_col]]

                # 値を1に置換
                c_df_v = target_df.values
                c_df_float = c_df_v.astype(np.float32)
                c_df_float[np.isnan(c_df_float)] = 0
                cross_df = np.where(c_df_float >= 1, 1, c_df_float)

                sum_v = sum(cross_df[:, 1])

                if sum_v == 0:
                    continue

                # クロス集計
                crossed = pd.crosstab(cross_df[:, 0], cross_df[:, 1])
                crossed.columns = ['算定されない', '算定された']
                crossed.index = ['なし', 'あり']

                a = crossed.iloc[0, 0]
                b = crossed.iloc[0, 1]
                c = crossed.iloc[1, 0]
                d = crossed.iloc[1, 1]

                D_d = ((c + d) * (b + d)) / (a + b + c + d)

                # カイ二乗値、p値,自由度
                x2, p, dof, expected = sp.stats.chi2_contingency(crossed)  # x2:カイ二乗値.p:確率

                if p < p_value and d > D_d:
                    flags = 1
                    df_p_flag.loc[i, columns3] = use_col, p, flags
                    i = i + 1

            indf_im_v = df_p_flag.values
            im_sort = sorted(indf_im_v, key=lambda x: (x[1]))
            im_sort_df = pd.DataFrame(im_sort, columns=df_p_flag.columns)
            top_df = im_sort_df.iloc[0:use_word, :]

            koumoku = top_df[columns3[0]].tolist()

            if len(koumoku) != 0:
                im_sort_df = pd.DataFrame(columns=koumoku)
                # 説明変数ファイル出力エラーチェック
                outpath = outDir + "/" + target + '/' + x2_item + '_説明変数.csv'
                try:
                    im_sort_df.to_csv(outpath, index=False, encoding='cp932')
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s2_chi_square_not_si"]))
