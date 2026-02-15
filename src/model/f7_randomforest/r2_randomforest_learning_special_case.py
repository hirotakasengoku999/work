# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30


import sys, traceback
import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from datetime import datetime
import pickle


def call(params, logger, out_dir):
    """
    モデル作成・保存＞sav
    重要度出力＞csv
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["r2_randomforest_learning_special_case"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 1]))
        raise Exception
    Dir = out_dir + files[0] + "/"
    if not os.path.exists(Dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [Dir]))
        raise Exception

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
    if "max_depth" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["max_depth"]))
        raise Exception
    max_depth = params["max_depth"]
    if "n_estimators" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["n_estimators"]))
        raise Exception
    n_estimators = params["n_estimators"]
    if "option_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["option_list"]))
        raise Exception
    count_list = params["option_list"]

    # 実績あり/入院中算定回数1回以上のデータを削除
    def NyuinCount(df, taret, count):
        inData = df[~((df[taret] == 1) & (df['入院中算定回数'] >= count))]
        inData = inData.reset_index(drop=True)
        return inData

    for coln, count in zip(target_list, count_list):
        # pickle有無確認
        inFilePath = Dir + coln + '.pickle'
        if not os.path.exists(inFilePath):
            # 算定項目は実行されません
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [inFilePath]))
            continue

        # データ読み込み
        f = open(inFilePath, 'rb')
        inData_in = pickle.load(f)
        inData = inData_in.fillna(0)

        # 個別対応
        inData = NyuinCount(inData, coln, count)

        idcol = columns1[0]
        obj = coln

        ##乱数のシード
        random_state = 1

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

            # 説明変数と目的変数を分ける
            if key != obj:
                data[key] = pd.to_numeric(x_base, errors='coerce')
            else:
                target = pd.to_numeric(x_base, errors='coerce')

        # 数値に変換した目的変数と説明変数の行列を連結する
        data[obj] = target
        # 欠損値が含まれる行を削除する
        data = data.dropna()

        # 再び目的変数と説明変数に分ける
        target = data[obj].copy()
        data = data.drop(obj, axis=1)

        importance = pd.DataFrame(index=data.columns)
        trainData = inData

        X_train = data.iloc[:len(trainData) + 1]
        y_train = target.iloc[:len(trainData) + 1]

        clf = RandomForestRegressor(max_depth=max_depth, n_estimators=n_estimators,
                                    random_state=1, criterion='mse')

        try:
            clf.fit(X_train, y_train)
        except:
            # 算定項目は実行されません
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002001", [coln,"targets",coln]))
            continue

        # モデルファイル出力エラーチェック
        outpath = Dir + coln + '_result.sav'
        try:
            pickle.dump(clf, open(outpath, 'wb'))
        except:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003009", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

        importance["重要度"] = clf.feature_importances_
        imp_mean = importance.mean(axis=1)
        imp_mean.name = '重要度'
        imp_mean_crlf = imp_mean.replace('\r', '\n')

        # 重要度ファイル出力エラーチェック
        outpath = Dir + coln + '_importance.csv'
        try:
            imp_mean_crlf.to_csv(outpath, index_label='項目名', header=True, encoding='cp932')
        except:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003009", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["r2_randomforest_learning_special_case"]))
