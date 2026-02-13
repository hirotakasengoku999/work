# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os


def call(folder_path1, folder_path2, out_dir):
    """
    電子カルテ　全結合

    Parameters
    :param folder_path: '3_work/wk/1_1/'
    :param '3_work/wk/1_1/': '3_work/wk/1_2/'
    :param out_dir: '3_work/wk/1_3/'
    """

    karute = 'カルテ番号等'
    data_column_name = '_date'

    # 入力ファイル
    # 'EXDIDH' or 'EXDIDC' or 'EXDIOD'
    file_list = ['EXDIDH', 'EXDIOD', 'EXDIDC']

    # 出力フォルダの作成
    os.makedirs(out_dir, exist_ok=True)
    outpath = out_dir + '/カルテ_全結合.csv'

    for file in file_list:

        # 入力パス
        in_path = folder_path2 + file + '.csv'
        on_column = ['患者番号', '文書番号', '文書版数', '文書日付', '文書種別']

        if file in ['EXDIDH', 'EXDIDC']:
            in_path = folder_path1 + file + '.csv'
            on_column = ['患者番号', '文書番号', '文書版数']

        in_df = pd.read_csv(in_path, engine='python', dtype={'患者番号': 'object', '文書版数': 'int'}, encoding='cp932')

        if file == 'EXDIDH':
            base_df = in_df.copy()
        else:
            base_df = pd.merge(base_df, in_df, on=on_column, how='left')

    out_df = base_df.drop(['文書番号', '文書版数', '入外区分'], axis=1)
    nw_out_df = pd.DataFrame()
    target_colum = list(set(out_df.columns) - set(['患者番号', '文書日付', '文書種別']))
    tmp_df = out_df.fillna('')
    for target in target_colum:
        t_df = tmp_df[['患者番号', '文書日付', '文書種別', target]]
        t_df = t_df[~t_df.duplicated()]
        nw_out_df = pd.concat([nw_out_df, t_df])

    nw_out_df = nw_out_df.rename(columns={'文書日付': data_column_name})

    nw_out_df['カルテ内容'] = nw_out_df['文書情報'].str.cat(nw_out_df['項目名称'], na_rep='', sep=' ')

    nw_out_df.to_csv(outpath, encoding='cp932', index=False)

# 単体テスト用コマンド呼び出し関数
if __name__ == "__main__":
    import sys

    args = sys.argv
    # call(*args[1:])

    work_date_dir = "D:\mom\model/3_work/wk"
    in_date_dir = "D:\mom\model/1_input"
    call(work_date_dir + '/1_1/', work_date_dir + '/1_2/', work_date_dir + '/1_3/')
