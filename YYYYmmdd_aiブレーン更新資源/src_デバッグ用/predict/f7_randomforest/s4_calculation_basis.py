# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pickle
import numpy as np
import pandas as pd
import os
import lime
import lime.lime_tabular
import src.model.my_package.accuracy_rate as ar


def call(params, logger, model_path, in_targets):
    """
    lime
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s4_calculation_basis_calculation"]))

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
    model_dir = model_path + files[1] + "/"
    if not os.path.exists(model_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [model_dir]))
        raise Exception
    out_dir = files[2]
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(files) < 3:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 3]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    columns3 = columns[2]
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    target_list = params["targets"]

    if "outnumber" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["outnumber"]))
        raise Exception
    outnumber = params["outnumber"]
    if "limenumber" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["limenumber"]))
        raise Exception
    limenumber_l = params["limenumber"]
    if "f_value" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["f_value"]))
        raise Exception
    f_value = params["f_value"]

    # f_valueとtarget_listの長さが一致しない場合
    if len(f_value)!=len(target_list):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003003", ["target_list","f_value"]))
        raise Exception

    # 出力フォルダ作成
    os.makedirs(out_dir, exist_ok=True)

    for target,limenumber,threshold in zip(target_list,limenumber_l,f_value):

        # in_targetsになければ実施しない
        if target not in in_targets:
            continue

        # csv読み込み 予測結果
        test_path = in_dir + '/' + target + '_result.csv'
        # warning 算定項目は実施されません　continue　出力しない
        if not os.path.exists(test_path):
            # データがないため、処理をスキップします
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [test_path]))
            # 算定項目は実行されません
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002001", [target,"targets",target]))
            continue

        in_test = pd.read_csv(test_path, engine='python', dtype={columns1[0]: 'object', columns1[2]: 'float64'})

        # 予測値がF値の最大の閾値以上かつ、実績０の患者を抽出
        __test = in_test[(in_test[columns1[2]] >= float(threshold)) & (in_test[target] == 0)]
        # 予測値を降順ソートして設定ファイルで指定した件数抽出
        __test = __test.sort_values(columns1[2], ascending=False)
        test = __test.reset_index(drop=True)

        if len(test) > limenumber:
            test = test.iloc[range(limenumber), :]

        if len(test) == 0:
            # 算定項目は実行されません
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002001", [target,"targets",target]))
            # 出力しない
            continue

        # 特徴量読み込み 入力チェック
        model_pickle_path=model_dir + target + '.pickle'
        if not os.path.exists(model_pickle_path):
            # データがないため、処理をスキップします
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [model_pickle_path]))
            # 算定項目は実行されません
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002001", [target,"targets",target]))
            continue

        train = pd.read_pickle(model_pickle_path)

        # model読み込み　入力チェック
        model_sav_path = model_dir + target + '_result.sav'
        if not os.path.exists(model_sav_path):
            # データがないため、処理をスキップします
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [model_sav_path]))
            # 算定項目は実行されません
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002001", [target, "targets", target]))
            continue
        rf = pickle.load(open(model_sav_path, 'rb'))

        # 説明変数
        feature_names = [col for col in train.columns if col not in columns1 + [target]]

        # 型変換
        train = train.fillna(0)
        test = test.fillna(0)
        test[feature_names] = test[feature_names].astype(float)
        train[feature_names] = train[feature_names].astype(float)
        train[[target]] = train[[target]].astype(float)
        test[[target]] = test[[target]].astype(float)

        np.random.seed(1)
        explainer = lime.lime_tabular.LimeTabularExplainer(train[feature_names].values, feature_names=feature_names,
                                                           class_names=[target], verbose=True, mode='regression')

        def OutLime(x):
            result_list = explainer.explain_instance(x[feature_names].values, rf.predict,
                                                     num_features=len(feature_names)).as_list()
            result_list_new = [s for s in result_list if '>' in s[0]]
            result_list_new = [s for s in result_list_new if s[1] >= 0]

            if len(result_list_new) <= outnumber:
                result_list_new = result_list_new + [('', '')] * outnumber

            return result_list_new[0:outnumber]

        # 1行ずつLIME算出
        out_df = test.apply(OutLime, axis=1)

        # 寄与度データフレーム格納
        _out_df = test[[columns1[0], columns1[1]]]
        out_df.name = '寄与度'
        _out_df = pd.concat([_out_df, out_df],axis=1)

        out_lime_df = pd.DataFrame()
        for length in range(len(_out_df)):
            out_lime_df_tmp = pd.DataFrame(columns=[columns3[0], columns3[1]], data=out_df[length])
            out_lime_df_tmp.loc[:, columns1[0]] = _out_df.loc[length, columns1[0]]
            out_lime_df_tmp.loc[:, columns1[1]] = _out_df.loc[length, columns1[1]]
            out_lime_df_tmp.loc[:, columns3[2]] = list(range(1, outnumber + 1))

            out_lime_df = pd.concat([out_lime_df, out_lime_df_tmp])

        out_lime_df[columns3[0]] = out_lime_df[columns3[0]].str.replace(' > 0.00', '')

        outpath=out_dir + target + '_lime.csv'
        try:
            out_lime_df.to_csv(outpath, index=False, encoding='cp932')
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s4_calculation_basis_calculation"]))
