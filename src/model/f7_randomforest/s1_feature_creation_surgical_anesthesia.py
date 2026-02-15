# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re


def call(params, logger, model_dir, model_flag, in_targets):
    """
    手術麻酔系　特徴量作成
    診療行為、カルテ有無、オーダ名詞、文書名詞

    閉鎖5,3以外：外部ファイルで指定した診療行為を
    閉鎖5,3：X二乗検定で選別した診療行為
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s1_feature_creation_surgical_anethesia"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    # inDir, outDir, hedDir, baseDir, meisiDir, hedDir2
    files = params["files"]
    if len(files) < 7:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 7]))
        raise Exception
    inDir = files[0]
    if not os.path.exists(inDir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inDir]))
        raise Exception
    hedDir = files[2]
    if not os.path.exists(hedDir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [hedDir]))
        raise Exception
    base_path = files[3]
    if not os.path.exists(base_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [base_path]))
        raise Exception
    hedDir2 = model_dir + files[4]
    if not os.path.exists(hedDir2):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [hedDir2]))
        raise Exception
    karuteDir = files[5]
    if not os.path.exists(karuteDir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [karuteDir]))
        raise Exception
    wkdir = files[6]
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
    _target_list = params["targets"]
    if "bunsyo_syubetu" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["bunsyo_syubetu"]))
        raise Exception
    bunsyo_syubetu = params["bunsyo_syubetu"]
    if "folder_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["folder_list"]))
        raise Exception
    folder_list = params["folder_list"]
    if "specific_target_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["specific_target_list"]))
        raise Exception
    specific_target_list = params["specific_target_list"]

    # モデルの場合
    if model_flag == 1:
        # 出力フォルダ
        outDir = model_dir + files[1]

        # traget_list
        target_list = _target_list.copy()

        # 入院患者のみ
        try:
            in_Base_df = pd.read_csv(base_path, engine='python', dtype={columns1[0]: 'object'}
                                    , usecols=[columns1[0], columns1[1], columns1[2]], encoding="cp932")
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
            raise

        in_Base_df = in_Base_df[in_Base_df[columns1[2]] == 1][[columns1[0], columns1[1]]]

    # 予測の場合
    elif model_flag == 0:
        # 出力フォルダ
        outDir = files[1]

        # 全患者読み込み
        try:
            in_Base_df = pd.read_csv(base_path, engine='python', dtype={columns1[0]: 'object'}
                                    , usecols=[columns1[0], columns1[1]], encoding="cp932")
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
            raise

        # _target_list：jsonで指定したtargets、in_targets：predicti_receiptで指定したtargets
        # 共通する算定項目コードを抽出する。共通する算定項目コードがない場合、処理をスキップ。
        target_list = list(set(_target_list) & set(in_targets))
        if len(target_list) < 0:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_by_category"]))
            return 0

    # 算定項目ごとに処理
    for target in target_list:
        break_flag=0

        for folder in folder_list:
            # fileチェック　continue
            inpath = inDir + folder + ".pickle"

            # 診療行為の場合break
            if folder == '診療行為':
                if not os.path.exists(inpath):
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [inpath]))
                    break_flag=1
                    break
            else:
                if not os.path.exists(inpath):
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [inpath]))
                    continue
            df = pd.read_pickle(inpath)

            head_path = hedDir2 + "/" + target + '/' + folder + '_説明変数.csv'

            if folder == '診療行為':

                # 手術系麻酔系指定の診療行為コード読み込み
                hedpath = hedDir + target + '.csv'
                if os.path.isfile(hedpath):
                    try:
                        code_df = pd.read_csv(hedpath, engine='python',usecols=[columns2[0]], dtype={columns2[0]: 'object'}, encoding='cp932')
                    except UnicodeDecodeError:
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [hedpath]))
                        raise
                    except ValueError as ve:
                        keys = [hedpath]
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [keys]))
                        raise
                    except:
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [hedpath]))
                        raise

                    l1_l2_and=code_df[columns2[0]].tolist()
                # 手術系麻酔系指定の診療行為コード読み込みできない場合、X二乗のファイルがあれば読み込み
                elif os.path.isfile(head_path):
                    # X二乗診療行為コード
                    try:
                        l1_l2_and = pd.read_csv(head_path, engine='python', encoding="cp932").columns.tolist()
                    except:
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [head_path]))
                        raise
                else:
                    l1_l2_and = []

                # specific_target_listに算定項目が含まれる場合
                if target in specific_target_list:
                    colm = [columns3[0], columns3[1]] + l1_l2_and
                    new_df = df.loc[:, df.columns.intersection(colm)].reindex(columns=colm)

                    spe_path=wkdir + target + '.csv'
                    if not os.path.exists(spe_path):
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [spe_path]))
                        break_flag = 1
                        break

                    # speCSV
                    try:
                        # columns
                        df_specific = pd.read_csv(spe_path, engine='python', dtype={columns3[0]: 'object'},
                                                usecols=[columns3[0], columns3[1], target], encoding="cp932")
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                        break_flag = 1
                        break

                    new_df_m1 = pd.merge(df_specific, in_Base_df, on=[columns3[0], columns3[1]], copy=False)
                    new_df_m = pd.merge(new_df_m1, new_df, on=[columns3[0], columns3[1]], how='left', copy=False)
                else:
                    colm = [columns3[0], columns3[1], target] + l1_l2_and
                    new_df = df.loc[:, df.columns.intersection(colm)].reindex(columns=colm)
                    new_df_m = pd.merge(in_Base_df, new_df, on=[columns3[0], columns3[1]], how='left', copy=False)

            elif folder == '文書有無':
                colm = [columns3[0], columns3[1]] + bunsyo_syubetu
                drop_df = df.loc[:, df.columns.intersection(colm)].reindex(columns=colm)
                new_df_m = pd.merge(new_df_m, drop_df, on=[columns3[0], columns3[1]], how='left', copy=False)

            else:
                if os.path.isfile(head_path):
                    try:
                        colm = [columns3[0], columns3[1]] + pd.read_csv(head_path, engine='python', encoding="cp932").columns.tolist()
                    except:
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [head_path]))
                        raise
                    drop_df = df.loc[:, df.columns.intersection(colm)].reindex(columns=colm)
                    new_df_m = pd.merge(new_df_m, drop_df, on=[columns3[0], columns3[1]], how='left', copy=False)

        if break_flag==0:

            # カルテ名詞
            # specific_target_listに算定項目が含まれる場合
            if target in specific_target_list:
                target_karute = target.split('_')[0]
            else:
                target_karute = target

            path = karuteDir + target_karute + '.csv'
            # pathチェック　存在する場合処理
            if os.path.exists(path):
                indf = pd.read_csv(path, engine='python', encoding="cp932", dtype={columns3[0]: 'object'})
                drop_df = indf.rename(columns=lambda s: str('カルテ_' + s) if s != columns3[0] and s != columns3[1] else s)
                new_df_m = pd.merge(new_df_m, drop_df, on=[columns3[0], columns3[1]], how='left', copy=False)

            out_df = new_df_m.set_index([columns3[0], columns3[1]])
            out_df[out_df >= 2] = 1
            out_df = out_df.reset_index()

            # 出力エラーチェック
            outpath = outDir + "/" + target + '.pickle'
            try:
                out_df.to_pickle(outpath)
            except:
                import traceback, sys
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                raise

            # 確認用
            # out_df.to_csv(outDir + 'csv/' + target + '_特徴量.csv')
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s1_feature_creation_surgical_anethesia"]))
