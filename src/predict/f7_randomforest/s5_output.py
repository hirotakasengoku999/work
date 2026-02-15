# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re

def call(params, logger, model_path, out_dir, in_targets):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s5_output"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 7:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 7]))
        raise Exception
    in_dir = files[0]
    if not os.path.exists(in_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_dir]))
        raise Exception
    lime_dir = files[1]
    if not os.path.exists(lime_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [lime_dir]))
        raise Exception
    imp_dir = model_path + files[2] + "/"
    if not os.path.exists(imp_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [imp_dir]))
        raise Exception
    master_path = files[3]
    if not os.path.exists(master_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [master_path]))
        raise Exception
    code_path = files[4]
    if not os.path.exists(code_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [code_path]))
        raise Exception
    model_dir = files[5]
    if not os.path.exists(model_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [model_dir]))
        raise Exception
    name_path = files[6]
    if not os.path.exists(name_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [name_path]))
        raise Exception
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 4:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 4]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    columns3 = columns[2]
    columns4 = columns[3]
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    _target_list = params["targets"]
    if "outcolumns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["outcolumns"]))
        raise Exception
    outcolumns = params["outcolumns"]

    # 共通する算定項目コードを抽出する。
    check_target=[]
    for target, col_list in _target_list:
        check_target.append(target)
    target_list = list(set(check_target) & set(in_targets))

    # 特徴量から個別対応抽出
    def OutColum(df, indf_im, collist):
        use_col = [columns4[0], columns1[1]] + collist
        indf_im = indf_im[use_col]
        return pd.merge(df, indf_im, on=[columns4[0], columns1[1]])

    os.makedirs(out_dir, exist_ok=True)

    # masetr
    try:
        indf_m = pd.read_csv(master_path, engine='python', usecols=columns2, dtype='object', encoding="cp932")
    except UnicodeDecodeError:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [master_path]))
        raise
    except ValueError as ve:
        keys = [master_path]
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003008", [keys]))
        raise
    except:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [master_path]))
        raise

    # 診療科読み込み
    indf_h = pd.read_csv(code_path, engine='python', dtype='object', encoding="cp932")
    # 氏名読み込み
    indf_n = pd.read_csv(name_path, engine='python', dtype='object', encoding="cp932")

    for _target in target_list:
        for target, col_list in _target_list:
            if target == _target:
                outpath = out_dir + '/' + target + '.csv'

                ############# 予測結果
                # result読み込み　入力チェック
                result_path =in_dir + '/' + target + '_result.csv'
                if not os.path.exists(result_path):
                    # データがないため、処理をスキップします
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [result_path]))
                    # 算定項目は実行されません
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002001", [target, "targets", target]))
                    continue
                try:
                    # カルテ番号、_date,予測値、target、colistを読み込み
                    in_result = pd.read_csv(result_path, engine='python', dtype={columns1[0]: 'object'}, encoding="cp932")
                except ValueError as ve:
                    keys = []
                    for arg in ve.args:
                        keys.extend(re.findall(r"\'(.*)\'", arg))
                    l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
                    raise

                # columns抽出
                usecols=columns1 + [target] + col_list
                in_result = in_result.loc[:, in_result.columns.intersection(usecols)].reindex(columns=usecols)

                # 個別付加情報　抽出
                _in_result_copy = in_result.copy()

                # targetの名称抽出
                target_name_ori = target.replace('_1', '').replace('_2', '')
                target_name = indf_m[columns2[1]][indf_m[columns2[0]] == target_name_ori].values

                if len(target_name) > 0:
                    in_result.loc[:, columns4[3]] = target_name

                in_result = in_result.reindex(columns=[columns1[0],columns1[1],target,columns1[2],columns4[3]])
                tmp_result = in_result.values
                sort_1 = sorted(tmp_result, key=lambda x: x[3], reverse=True)
                sort_2 = sorted(sort_1, key=lambda x: x[2])
                out_df = pd.DataFrame(sort_2, columns=in_result.columns)
                out_df[columns4[4]] = out_df[columns1[2]].map('{:.2%}'.format).astype("str").str.replace("%", "")
                out_df.loc[:,columns4[5]] = out_df[target]

                #############  lime
                # lime結果読み込み　入力チェック
                lime_path = lime_dir + target + '_lime.csv'
                # ファイルが存在する場合処理
                if os.path.isfile(lime_path):
                    try:
                        in_lime = pd.read_csv(lime_path, engine='python', dtype='object',usecols=columns3, encoding="cp932")
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
                        raise

                    # limeの結果がからの場合
                    # マスタと結合
                    _in_lime = pd.merge(in_lime, indf_m.rename(columns={columns2[0]: columns3[3]}), on=[columns3[3]],
                                        how='left')
                    _in_lime[columns2[1]] = _in_lime[columns2[1]].fillna(0)  # 欠損0埋め
                    _in_lime.loc[_in_lime[columns2[1]] != 0, columns3[3]] = _in_lime[columns2[1]][_in_lime[columns2[1]] != 0]

                    _in_lime = _in_lime.fillna(-1)
                    tmp_lime = _in_lime[[columns3[0], columns3[1]]][
                        ~_in_lime[[columns3[0], columns3[1]]].duplicated()].set_index(
                        [columns3[0], columns3[1]])
                    for rank in sorted(_in_lime[columns3[2]].unique()):
                        tmp_rank = _in_lime[_in_lime[columns3[2]] == rank].set_index([columns3[0], columns3[1]])
                        tmp_rank = tmp_rank[[columns3[3], columns3[4]]].rename(
                            columns={columns3[3]:  columns4[6] + '_' +str(rank), columns3[4]:  columns4[6] + '_'+columns3[4] +'_' +str(rank)})
                        tmp_lime = pd.concat([tmp_lime, tmp_rank], axis=1)
                    # output用に列名取得
                    lime_columns = tmp_lime.columns.tolist()
                    tmp_lime = tmp_lime.reset_index()

                    # lime 予測結果　結合
                    out_df = pd.merge(out_df, tmp_lime, on=[columns1[0], columns1[1]], how='left')

                # 診療科　予測結果 結合
                out_df = pd.merge(out_df, indf_h, how='left', on=[columns1[0], columns1[1]])
                # 氏名　結合
                out_df = pd.merge(out_df, indf_n, how='left', on=[columns1[0]])

                # collistの個別対応指定がある場合
                if len(col_list) >= 1:
                    # 特徴量にある、付加情報を抽出
                    out_df = OutColum(out_df, _in_result_copy, col_list)
                out_df = out_df.loc[:, out_df.columns.intersection(outcolumns)].reindex(columns=outcolumns)
                out_df = out_df.fillna(-1)

                # 出力エラーチェック
                try:
                    out_df.to_csv(outpath, index=False, encoding='cp932')
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003009", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise
    # 終了ログ
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s5_output"]))
