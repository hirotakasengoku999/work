# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import pickle
import os


def call(params, logger, model_path, in_targets):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s2_randomforest_prediction"]))

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
    model_dir = model_path + files[1]+"/"
    if not os.path.exists(model_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [model_dir]))
        raise Exception
    out_dir = files[2]
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 1]))
        raise Exception
    columns1 = columns[0]
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    target_list = params["targets"]
    # 共通する算定項目コードを抽出する。
    target_list = list(set(target_list) & set(in_targets))

    # 出力フォルダ作成
    os.makedirs(out_dir, exist_ok=True)

    for coln in target_list:
        # modelのpath　チェック
        inmodel = model_dir + coln + '_result.sav'
        # warning 算定項目は実施されません
        if not os.path.exists(inmodel):
            continue

        # モデル特徴量のヘッダー（順）を取得
        v_path = model_dir + coln + '.pickle'
        # warning 算定項目は実施されません
        if not os.path.exists(v_path):
            continue
        modeldata = pd.read_pickle(v_path)
        modeldata_col = modeldata.columns

        # 予測用　特徴量読み込み
        in_path = in_dir + coln + '.pickle'
        # warning 算定項目は実施されません
        if not os.path.exists(in_path):
            continue
        inData = pd.read_pickle(in_path)
        inData = inData.loc[:, inData.columns.intersection(modeldata_col)].reindex(columns=modeldata_col)

        inData = inData.fillna(0)

        # パラメータを追加する
        idcol = columns1[0]
        obj = coln
        # 目的変数、説明変数の設定
        data = pd.DataFrame()
        target = None

        # 空白とか
        for key, x_base in inData.iteritems():
            # ID列は除外
            if key == idcol:
                continue

            x = x_base.copy()
            # ｆloatに変換できるか確認
            try:
                x = x.astype(float)
            except ValueError:
                # 目的変数が数値で無い場合、処理を分類に切り替え
                if key != obj:
                    continue

            mode = 'r'

            # 説明変数と目的変数を分ける
            if key != obj:
                data[key] = pd.to_numeric(x_base, errors='coerce')
            elif mode == 'r':
                target = pd.to_numeric(x_base, errors='coerce')

        # 説明変数、目的変数が無い場合終了
        if len(data.columns) == 0:
            continue

        # 数値に変換した目的変数と説明変数の行列を連結する
        data[obj] = target
        data = data.dropna()
        # 再び目的変数と説明変数に分ける
        target = data[obj].copy()
        data = data.drop(obj, axis=1)

        testinData = inData

        # 出力データを作成
        outData = testinData.copy()

        X_test = data.iloc[:len(testinData) + 1]
        Y_test = target.iloc[:len(testinData) + 1]

        loaded_model = pickle.load(open(inmodel, 'rb'))

        try:
            result = loaded_model.predict(X_test)
        except ValueError:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [in_path]))
            continue

        predict = result
        outData[columns1[1]] = predict
        outData_crlf = outData.replace('\r', '\n')

        outpath=out_dir + coln + '_result.csv'
        try:
            outData_crlf.to_csv(outpath, index=None, header=True, encoding='cp932')
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s2_randomforest_prediction"]))

