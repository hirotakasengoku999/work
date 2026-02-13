# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import glob
import os

data_column_name = "_date"
karute = 'カルテ番号等'


# 複数列を１つの列に結合
def Grouping(df, out_colum, target_list):
    if out_colum in target_list:
        target_list.remove(out_colum)
    else:
        df[out_colum] = ''

    for target_col in target_list:
        df[out_colum] = df[out_colum].str.cat([df[target_col].fillna('')], sep=' ')
    df.drop(target_list, axis=1, inplace=True)
    return df


# 列名変更
def RenameColum(df, target_list):
    return df.rename(columns=target_list)


def call(Karute_dir, out_dir):
    '''
    ①列名変更
    ③列結合
    ②使用する列抽出

    受付2019→(再診・初診) 最終診療日　初診料で使用　,受付日→index
    DVT予防法2019→測定日→index 測定値１→カルテ内容
    リハビリ計画書2019→リハビリテーション総合計画評価料１ 初回入力日→index
    介護支援連携指導2019→初回入力日→index
    リハビリ記録2019→文書　使用 カルテ日付-> index
    医師記録2019→文書　使用  カルテ日付-> index
    手術歴2019→文書　使用  日付-> index
    手術記録2019→文書　使用  手術日-> index　'手術記事': 'カルテ内容', '測定値１': 'カルテ内容'
    服薬指導記録2019→文書　使用  手術日-> index
    検査結果2019→使用  受付日-> index
    看護記録2019→使用  カルテ日付-> index
    '''


    # 入力ファイル
    path = str(Karute_dir + '/*')
    file_listc = glob.glob(path)

    os.makedirs(out_dir, exist_ok=True)

    for path_1 in file_listc:

        basename = os.path.basename(path_1).replace('.csv', '')
        # if basename != '検査結果2019':
        #     continue

        outpath = out_dir + basename + '.csv'
        df = pd.read_csv(path_1, engine='python', encoding="cp932",
                         dtype={'患者コード': 'object', '患者ID': 'object'})

        # 不要な列削除
        df = df.loc[:, ~(df.columns.str.startswith('Unnamed'))]

        # 共通
        df = RenameColum(df, {'患者コード': karute, '患者ID': karute, '日付': data_column_name,
                              '初回入力日': data_column_name, '測定日': data_column_name,
                              'カルテ日付': data_column_name, '受付日': data_column_name,
                              '手術日': data_column_name})

        if basename == '手術歴2019':
            df = Grouping(df, 'カルテ内容', ['術式1', '術式直接入力', '使用機器1', '使用機器2'])
            df = RenameColum(df, {'開始時間': '手術開始時間'})
            df = df[[karute, data_column_name, '手術開始時間', '体位', 'カルテ内容']]

        elif basename == '手術記録2019':
            df = Grouping(df, 'カルテ内容', ['手術記事', '病名１', '病名直接入力'])


        elif basename in ['リハビリ記録2019', '看護記録2019', '服薬指導記録2019', '医師記録2019']:
            df = Grouping(df, 'カルテ内容', ['サブタイトル', 'カルテ内容'])
            df = df[[karute, data_column_name, 'カルテ内容', '診療科']]


        elif basename == '介護支援連携指導2019':
            df = RenameColum(df, {'文書': '介護支援連携指導_文書'})
            df = df[[karute, data_column_name, '介護支援連携指導_文書', '診療科']]
            #### 空白削除
            df['介護支援連携指導_文書'] = df['介護支援連携指導_文書'].str.replace(" ", "").str.replace("　", "")

        elif basename == 'DVT予防法2019':
            df = RenameColum(df, {'測定値１': 'カルテ内容'})
            df = df[[karute, data_column_name, 'カルテ内容']]

        elif basename == '検査結果2019':
            df = Grouping(df, '検査項目', ['検査項目', '結果値'])
            df = df[[karute, data_column_name, '検査項目', '診療科']]
            #### 空白削除
            df['検査項目'] = df['検査項目'].str.replace(" ", "").str.replace("　", "")

        elif basename == 'リハビリ計画書2019':
            df = df[[karute, data_column_name, '診療科']]

        df = df[~df.duplicated()]
        df[data_column_name] = pd.to_datetime(df[data_column_name], format='%Y/%m/%d')
        df[data_column_name] = df[data_column_name].dt.strftime('%Y/%m/%d')

        df.to_csv(outpath, index=False, encoding='cp932')


# 単体テスト用コマンド呼び出し関数
if __name__ == "__main__":
    import sys

    args = sys.argv
    # call(*args[1:])

    work_date_dir = "D:\mom\model/3_work" + '/' + 'wk2'
    work_dir = "D:\mom\model/1_input"
    call(work_dir + '/電子カルテ_医誠会/', work_date_dir + '/1_1/')
