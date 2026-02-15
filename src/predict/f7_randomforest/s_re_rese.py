# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re

def call(params, logger):
    '''
    カルテ情報 REから診療科名、患者氏名を抽出
    '''
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_re_rese"]))

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
    out_path = files[1]
    code_path = files[2]
    if not os.path.exists(code_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [code_path]))
        raise Exception
    input_date = files[3]
    if not os.path.exists(input_date):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [input_date]))
        raise Exception
    nyuin_path = files[4]
    if not os.path.exists(nyuin_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [nyuin_path]))
        raise Exception
    out_path_all = files[5]
    out_path_name = files[6]
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

    # 出力フォルダの作成
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # date 月末取得
    target_month_l = pd.read_csv(input_date, engine='python',
                               encoding='cp932', dtype='object').columns.tolist()
    # マスタ　診療科コード
    try:
        _master = pd.read_csv(code_path, engine='python', dtype='object', encoding="cp932")
    except:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [code_path]))
        raise

    # 入退院一覧
    _nyuin = pd.read_csv(nyuin_path, engine='python', dtype='object', encoding="cp932")
    _nyuin[columns2[0]] = _nyuin[columns2[0]].str.replace("-", "/")

    tmp_df = pd.DataFrame()
    tmp_df_all = pd.DataFrame()
    tmp_d_name = pd.DataFrame()
    # 入力フォルダ取得
    for target_month in target_month_l:
        folder_path = in_dir + target_month
        for kubun, kubun2 in zip(['dpc', 'ika'], ['RECEIPTD', 'RECEIPTC']):
            for hoken in ['国保', '社保']:

                # 医科の場合
                if kubun == "ika":
                    # RE読み込み
                    re_usecol = [columns1[0], columns1[1], columns1[2], columns1[4], columns1[5]] # レセプト種別(columns1[5])の読み込み追加
                    re_path = folder_path + '/' + kubun + '/' + kubun2 + hoken + "_RE.csv"

                    # ファイル有無チェック　続ける
                    if not os.path.exists(re_path):
                        continue
                    try:
                        re_df = pd.read_csv(re_path, engine='python', usecols=re_usecol,
                                            dtype={columns1[0]: 'object', columns1[1]: 'object'}, encoding="cp932")
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR002004", keys))
                        continue

                    # 氏名抽出
                    re_df_name = re_df[[columns1[0], columns1[4]]]
                    tmp_d_name = pd.concat([tmp_d_name, re_df_name])

                    # 診療科が空白の場合レコード削除
                    re_df = re_df.dropna(subset=[columns1[1]])

                    # HO読み込み
                    ho_path = folder_path + '/' + kubun + '/' + kubun2 + hoken + "_HO.csv"
                    # ファイル有無チェック　続ける
                    if not os.path.exists(ho_path):
                        continue
                    try:
                        ho_df = pd.read_csv(ho_path, engine='python', usecols=columns3, encoding="cp932",
                                            dtype={columns3[0]: 'object'})
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR002004", keys))
                        continue

                    ho_df[columns3[0]] = ho_df[columns3[0]].astype('str')
                    ho_df = ho_df[ho_df[columns3[0]] != 'nan']
                    ho_df = ho_df[ho_df[columns3[0]] != 'NaN']
                    new_df = pd.merge(re_df, ho_df, on=[columns1[2]])

                    # 全患者用 カルテ番号で、後ろのレコードを残して重複削除
                    re_df_all=re_df[[columns1[0], columns1[1], columns1[5]]][~re_df.duplicated([columns1[0]], keep="last")] # columns1[5](レセプト種別)を追加

                elif kubun == "dpc":
                    # RE読み込み
                    re_usecol = [columns1[0], columns1[1], columns1[2], columns1[3], columns1[4], columns1[5]] # レセプト種別(columns1[5])の読み込み追加
                    re_path = folder_path + '/' + kubun + '/' + kubun2 + hoken + "_RE.csv"

                    # ファイル有無チェック　続ける
                    if not os.path.exists(re_path):
                        continue
                    try:
                        re_df = pd.read_csv(re_path, engine='python', usecols=re_usecol, # usecols=columns1 => re_usecolに変更
                                            dtype={columns1[0]: 'object', columns1[1]: 'object'}, encoding="cp932")
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                        continue

                    # 氏名抽出
                    re_df_name = re_df[[columns1[0], columns1[4]]]
                    tmp_d_name = pd.concat([tmp_d_name, re_df_name])

                    # 診療科が空白の場合レコード削除
                    re_df=re_df.dropna(subset=[columns1[1]])

                    bu_path = folder_path + '/' + kubun + '/' + kubun2 + hoken + "_BU.csv"

                    # ファイル有無チェック　続ける
                    if not os.path.exists(bu_path):
                        continue
                    try:
                        bu_df = pd.read_csv(bu_path, engine='python', usecols=columns4,
                                            dtype={columns4[0]: 'object'}, encoding="cp932")
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]); l(logger.format_log_message("AIR002004", keys))
                        continue

                    bu_df = bu_df.rename(columns={columns4[0]: columns3[0]})
                    new_df = pd.merge(re_df, bu_df, on=[columns1[2], columns1[3]])
                    # 全患者用 カルテ番号で、後ろのレコードを残して重複削除
                    re_df_all = re_df[[columns1[0], columns1[1], columns1[5]]][~re_df.duplicated([columns1[0]], keep="last")] # columns1[5](レセプト種別)を追加

                _new_df = new_df[[columns1[0], columns1[1], columns1[5], columns3[0]]] # columns1[5](レセプト種別)を追加
                tmp_df = pd.concat([tmp_df, _new_df])
                tmp_df_all = pd.concat([tmp_df_all, re_df_all])

    # データを一つも読み込めない場合、warnig 空のデータ出力
    if len(tmp_df) == 0:
        _tmp_df_all = pd.DataFrame(columns=[columns1[0], columns1[1], columns1[5]])  # columns1[5](レセプト種別)を追加
        out_df = pd.DataFrame(columns=[columns1[0], columns2[1], columns1[1], columns1[5]]) # columns1[5](レセプト種別)を追加
        tmp_d_name = pd.DataFrame(columns=[columns1[0],columns1[4]])
        # "データがないため、処理をスキップします
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", ["s_re_rese"]))
    else:
        # 入院年月日とカルテ番号が重複したレコードは、後ろのレコードを残して削除
        df_dup2 = tmp_df[~tmp_df.duplicated([columns1[0], columns3[0]], keep="last")]
        _tmp_df_all = tmp_df_all[~tmp_df_all.duplicated([columns1[0]], keep="last")]
        tmp_d_name = tmp_d_name[~tmp_d_name.duplicated([columns1[0]], keep="last")]

        # マスタとマージ
        dup_df = pd.merge(df_dup2, _master, on=columns1[1])
        dup_df = dup_df.rename(columns={columns3[0]: columns2[0]})
        _tmp_df_all = pd.merge(_tmp_df_all, _master, on=columns1[1])
        # 入外を変換
        dup_df[columns1[6]] = dup_df[columns1[5]].astype(str).str[-1:].apply(lambda x: '外来' if int(x) % 2 == 0 else '入院') # dup_dfの入外変換
        _tmp_df_all[columns1[6]] = _tmp_df_all[columns1[5]].astype(str).str[-1:].apply(lambda x: '外来' if int(x) % 2 == 0 else '入院') # _tmp_df_allの入外変換
        # 入院期間中のデータとマージ
        out_df = pd.merge(dup_df, _nyuin, on=[columns1[0], columns2[0]])

        out_df[columns1[1]] = out_df[columns2[2]]

        out_df_2=out_df[[columns1[0], columns2[1], columns1[1], columns1[6]]] # columns1[6](入外)を追加
        out_df_2 = out_df_2.reset_index(drop=True)
        out_df_2.loc[:,columns2[1]] = out_df_2[columns2[1]].str[0:7]
        out_df_2 = out_df_2[~out_df_2.duplicated()]
        out_df=pd.concat([out_df,out_df_2])

        _tmp_df_all[columns1[1]] = _tmp_df_all[columns2[2]]

    try:
        out_df[[columns1[0], columns2[1], columns1[1], columns1[6]]].to_csv(out_path, index=False, encoding='cp932') # 出力
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [out_path+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    try:
        _tmp_df_all[[columns1[0], columns1[1], columns1[6]]].to_csv(out_path_all, index=False, encoding='cp932') #出力
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [out_path_all+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    try:
        tmp_d_name.to_csv(out_path_name, index=False, encoding='cp932')
    except:
        import traceback, sys
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [out_path_name+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
        raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_re_rese"]))
