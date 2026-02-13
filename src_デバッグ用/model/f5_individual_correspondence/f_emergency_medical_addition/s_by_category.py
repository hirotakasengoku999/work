# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re


def call(params, logger, model_flag, in_targets=[]):
    """
    救急医療管理加算1に対して区分ごとに対応する個別対応

    救急医療管理加算1に対して入院開始日のみのデータに対応する個別対応
    ①救急医療管理加算1(ア)モデル　→　(ア)の算定が可能かどうか　((ア)以外のレコード削除)
    ②救急医療管理加算1(イ)モデル　→　(イ)の算定が可能かどうか　((イ)以外のレコード削除)
    ③救急医療管理加算1(ウ)モデル　→　(ウ)の算定が可能かどうか　((ウ)以外のレコード削除)
    ④救急医療管理加算1(カ)モデル　→　(カ)の算定が可能かどうか　((カ)以外のレコード削除)
    ⑤救急医療管理加算1(ク)モデル　→　(ク)の算定が可能かどうか　((ク)以外のレコード削除)
    ⑥救急医療管理加算1(ケ)モデル　→　(ケ)の算定が可能かどうか　((ケ)以外のレコード削除)
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s_by_category"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 4:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 4]))
        raise Exception
    in_dir = files[0]
    if not os.path.exists(in_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [in_dir]))
        raise Exception
    out_dir = files[1]
    inpath_si = files[2]
    if not os.path.exists(inpath_si):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_si]))
        raise Exception
    input_date = files[3]
    if not os.path.exists(input_date):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [input_date]))
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
    if "commentcode_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["commentcode_list"]))
        raise Exception
    code_list = params["commentcode_list"]

    # 出力フォルダ作成
    os.makedirs(out_dir, exist_ok=True)

    # モデル予測チェック：予測の場合、in_targetsに"82"コードがないと処理をスキップ
    if model_flag == 0:
        # code_list：jsonで指定したコメントコード、in_targets：predicti_receiptで指定したtargets
        # 共通する算定項目コードを抽出する。共通する算定項目コードがない場合、処理をスキップ。
        target_list = list(set(code_list) & set(in_targets))
        if len(target_list) <= 0:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR001002", ["s_by_category"]))
            return 0

    # date
    s_target_month_l = pd.read_csv(input_date, engine='python',
                                   encoding='cp932', dtype='object').columns.tolist()

    # 結合用データフレームを作成
    tmp_df = pd.DataFrame()
    for s_target_month in s_target_month_l:
        for hoken in ['国保', '社保']:
            for kubun, kubun2 in zip(['ika', 'dpc'], ['RECEIPTC', 'RECEIPTD']):

                # ikaとdpcで読み込むカラムが異なる
                if kubun == 'ika':
                    usecols = columns1
                elif kubun == 'dpc':
                    usecols = columns2

                # coCSV:コメントコード抽出
                co_path = in_dir + s_target_month + '/' + kubun + '/' + kubun2 + hoken + '_CO.csv'
                if not os.path.exists(co_path):
                    # 入力ファイルのデータがないため、処理をスキップします。
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [co_path]))
                    continue

                try:
                    co_df = pd.read_csv(co_path, engine='python', usecols=usecols, encoding="cp932"
                                        , dtype='object')
                except ValueError as ve:
                    keys = []
                    for arg in ve.args:
                        keys.extend(re.findall(r"\'(.*)\'", arg))
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                    continue

                # 日付修正
                # ikaの場合、入院年月日が欠損の患者は対象外
                if kubun == 'ika':
                    co_df[columns1[2]] = co_df[columns1[2]].astype("str")
                    co_df = co_df[co_df[columns1[2]] != "nan"]

                # buCSV:dpcの場合、BU(今回入院年月日の情報とCOの情報を結合)
                elif kubun == 'dpc':
                    # BU
                    nyuin_path = in_dir + s_target_month + '/' + kubun + '/' + kubun2 + hoken + '_BU.csv'
                    if not os.path.exists(nyuin_path):
                        # 入力ファイルのデータがないため、処理をスキップします。
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [nyuin_path]))
                        continue

                    try:
                        nyuin_df = pd.read_csv(nyuin_path, engine='python', usecols=columns3, dtype='object', encoding="cp932")
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                        continue

                    nyuin_df[columns1[2]] = nyuin_df[columns3[0]]
                    co_df = pd.merge(co_df, nyuin_df, on=[columns3[1], columns3[2]])

                # [カルテ番号等,コメントコード,入院年月日]のみ抽出
                co_df = co_df[columns1]

                # コメントコードの列をダミー変数に変換
                co_df = co_df.set_index([columns1[0], columns1[2]])
                co_df_dummy = pd.get_dummies(co_df, prefix='', prefix_sep='')
                # code_listで指定したコードのみ使用
                co_new_df = co_df_dummy.loc[:, co_df_dummy.columns.intersection(code_list)].reindex(columns=code_list)
                co_new_df.reset_index(inplace=True)

                co_group = co_new_df.groupby([columns1[0], columns1[2]], as_index=False).sum()
                co_group = co_group.fillna(0)

                tmp_df = pd.concat([tmp_df, co_group])

    # モデル・予測 return 0
    if len(tmp_df)==0:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", ["s_by_category"]))
        return 0

    indf_code = tmp_df.groupby([columns1[0], columns1[2]], as_index=False).sum()
    # リネーム：入院年月日→_date
    indf_code = indf_code.rename(columns={columns1[2]: columns4[1]})

    # 診療行為.pickle読み込み
    indf = pd.read_pickle(inpath_si)
    # 救急医療管理加算２の算定が0件の場合エラーで中断してしまうため、列追加
    if not columns4[3] in indf.columns:
        indf[columns4[3]] = 0
    # 読み込めるように
    df = indf.loc[:, [columns4[0], columns4[1], columns4[2], columns4[3]]]

    # 結合
    new_df = pd.merge(df, indf_code, how='left', on=[columns4[0], columns4[1]])

    new_df = new_df.fillna(0)
    # 対象のコードごとに出力
    for cod in code_list:
        # code_listからcodを除外
        non_target = [i for i in code_list if i != cod]
        # non_targetのsumを”対象外”列を作成
        new_df['対象外'] = new_df[non_target].sum(axis=1)

        # codにフラグがあるレコード、または、"対象外"が0かつ救急1の実績がないレコードを抽出
        # →救急1をコメントコードで区分できない場合はレコード削除
        # →non_target以外のコードの実績＆救急1の実績があるレコードを削除
        out_df = new_df[(new_df[cod] >= 1) | ((new_df['対象外'] == 0) & (new_df[columns4[2]] == 0))]

        # モデルの場合 model_flag=1,予測の場合 model_flag=0
        if model_flag == 1:
            out_df = out_df[~(out_df[columns4[3]] >= 1)]

        # リネーム：救急１-> cod
        out_df = out_df.rename(columns={cod: cod + '_old', columns4[2]: cod})

        outpath=out_dir + cod + '.csv'
        try:
            out_df.to_csv(outpath, encoding='cp932')
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s_by_category"]))

