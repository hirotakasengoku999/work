# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import glob


def call(folder_path, out_dir, prefix):
    '''
    1_input/電子カルテ/[prefix]～　ファイルを読み込み
    ヘッダー付与、日付修正、患者番号修正
    期間抽出のハードコーディングはレセ結合で解決しそうなので削除しました。

    Parameters
    :param iniDir: '1_input/電子カルテ/'
    :param out_dir: '3_work/wk/1_1/'
    :param prefix: 'EXDIDH' or 'EXDIDC' or 'EXDIOD'
    '''

    # 入力ファイル
    file_list = glob.glob(folder_path + '電子カルテ/' + prefix + '*')
    print(file_list)
    # 出力フォルダの作成
    os.makedirs(out_dir, exist_ok=True)

    # prefixごとに読み込みの列変更
    if prefix == 'EXDIDH':
        usecol = [1, 2, 3, 6, 7, 8, 23, 177, 183]
        indf_columns = ['患者番号', '文書番号', '文書版数', '文書種別', 'タイトル', '文書日付',
                        '入外区分', '発生者職種', '更新者職種']
    elif prefix == 'EXDIDC':
        # usecol = [1, 2, 3, 7]
        usecol = [0, 1, 2, 3]
        indf_columns = ['患者番号', '文書番号', '文書版数', '文書情報']
    elif prefix == 'EXDIOD':
        usecol = [1, 2, 3, 6, 9, 13, 25]
        indf_columns = ['患者番号', '文書番号', '文書版数', '文書日付', '文書種別', '項目名称', 'TOOL固定情報２']

    # 日付修正、患者番号修正
    out_df = pd.DataFrame()
    outpath = out_dir + prefix + '.csv'

    for in_path in file_list:
        filename_str = os.path.basename(in_path)

        in_df = pd.read_csv(in_path, engine='python', header=None, usecols=usecol)
        in_df.columns = indf_columns

        if prefix == 'EXDIDH':
            in_df = in_df[in_df['入外区分'] == 2]
            in_df['タイトル'] = in_df['タイトル'].str.replace('　', ' ').str.replace(r' ', '')

        if prefix in ['EXDIDH', 'EXDIOD']:
            # テスト実行の際はコメント▽
            # in_df['文書日付'] = in_df['文書日付'] / 1000
            # in_df['文書日付'] = in_df['文書日付'].astype('str')
            # in_df['文書日付'] = in_df['文書日付'].str[0:8]
            # テスト実行の際は不要
            in_df['文書日付'] = pd.to_datetime(in_df['文書日付'], format='%Y/%m/%d')
            in_df['文書日付'] = in_df['文書日付'].dt.strftime('%Y/%m/%d')

        in_df['患者番号'] = in_df['患者番号'].astype('str')
        in_df['患者番号'] = in_df['患者番号'].str.replace('\.0', '')
        in_df['患者番号'] = in_df['患者番号'].str.zfill(10)

        if prefix == 'EXDIDC':
            in_df['文書情報'] = in_df['文書情報'].str.replace('<[A-Z]+NO>.+?</[A-Z]+NO>', ' ').str.replace('<.+?>', ' ') \
                .str.replace('[A-Z0-9]+\.[A-Z0-9]+\.(jpg|png)', 'jpg') \
                .str.replace('amp;', ' ').str.replace('&lt;/DIV&gt;', ' ') \
                .str.replace('&lt;DIV&gt;', ' ').str.replace('&lt;BR&gt;BP;', ' ').str.replace('&lt;/DIV&gt;', ' ') \
                .str.replace('apos;', ' ').str.replace('quot;', ' ').str.replace('nbsp;', ' ').str.replace('&gt;', ' ') \
                .str.replace('"', '').str.replace("'", '').str.replace("'", '') \
                .str.replace("TRUE", ' ').str.replace("FALSE", ' ').str.replace("　", ' ').str.replace("&", ' ') \
                .str.replace("lt;BR", ' ').str.replace("(オーダ番号：[0-9]+)", ' ').str.replace("lt;", ' ') \
                .str.replace("(認証結果：○)", ' ')
            in_df['文書情報'] = in_df['文書情報'].str.replace(r'(\s)+', ' ')

        out_df = pd.concat([out_df, in_df])
    out_df.to_csv(outpath, index=False, encoding='cp932')


# 単体テスト用コマンド呼び出し関数
if __name__ == "__main__":
    import sys

    args = sys.argv
    # call(*args[1:])

    work_date_dir = "D:\mom\model/3_work/wk"
    in_date_dir = "D:\mom\model/1_input/"
    # 'EXDIDH' or 'EXDIDC' or 'EXDIOD'
    call(in_date_dir, work_date_dir + '/1_1/', 'EXDIDH')
    call(in_date_dir, work_date_dir + '/1_1/', 'EXDIDC')
    call(in_date_dir, work_date_dir + '/1_1/', 'EXDIOD')
