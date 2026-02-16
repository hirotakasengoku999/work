# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import glob
import os


# Pattern1：リストで指定した単語と"項目名称"の単語が一致すれば、"項目名称"の値を"TOOL固定情報２"の値に置換
def Pattern1(df, word_list):
    df_target = df[df['項目名称'].isin(word_list)]
    df_not_target = df[~df['項目名称'].isin(word_list)]

    if len(df_target) > 0:
        df_target['項目名称'] = df_target['TOOL固定情報２']
        df_new = pd.concat([df_target, df_not_target])
    else:
        df_new = df_not_target
    return df_new


# Pattern2：リストで指定した単語と"TOOL固定情報２"の単語が一致すれば、"TOOL固定情報２"の値と"項目名称"の値を結合する
def Pattern2(df, word_list):
    df_target = df[df['TOOL固定情報２'].isin(word_list)]
    df_not_target = df[~df['TOOL固定情報２'].isin(word_list)]

    if len(df_target) > 0:
        df_target['項目名称'] = df_target['TOOL固定情報２'].str.cat(df_target['項目名称'])
        df_new = pd.concat([df_target, df_not_target])
    else:
        df_new = df_not_target
    return df_new


# Pattern3：'TOOL固定情報２'に記載されている指定した単語を列として生成する（？）
def Pattern3(df, word_colum):
    s_target = df['項目名称'][df['TOOL固定情報２'] == word_colum]
    s_target.name = word_colum
    if len(s_target) > 0:
        df_new = pd.concat([df, s_target], axis=1)
    else:
        df_new = df
    return df_new


def call(folder_path, out_dir):
    """
    ①項目名称が”あり”の場合、’TOOL固定情報２’の値を置換
    ②’TOOL固定情報２’が”JCS”の場合、項目名称の値を上書き

    Parameters
    :param folder_path: '3_work/wk/1_1/'
    :param out_dir: '3_work/wk/1_2/'
    """
    # 入力ファイル
    file_list = glob.glob(folder_path + 'EXDIOD*')
    for in_path in file_list:
        filename_str = os.path.basename(in_path)
        outpath = out_dir + filename_str

        in_df = pd.read_csv(in_path, engine='python', encoding="cp932", dtype='object')

        # 前処理
        in_df = in_df.fillna('nan')
        dup_df = in_df[~in_df.duplicated()]
        dup_df.項目名称 = dup_df.項目名称.str.strip()
        dup_df['TOOL固定情報２'] = dup_df['TOOL固定情報２'].str.strip()

        dup_df = Pattern1(dup_df, ['あり', '有', 'あり'])
        dup_df = Pattern2(dup_df, ['JCS'])
        dup_df = Pattern3(dup_df, '手術開始時間')

        dup_df.to_csv(outpath, index=False, encoding='cp932')


# 単体テスト用コマンド呼び出し関数
if __name__ == "__main__":
    import sys

    args = sys.argv
    # call(*args[1:])

    work_date_dir = "D:\mom\model/3_work/wk"
    in_date_dir = "D:\mom\model/1_input/"
    # 'EXDIDH' or 'EXDIDC' or 'EXDIOD'
    call(work_date_dir + '/1_1/', work_date_dir + '/1_2/')
