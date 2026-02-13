# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import glob

data_column_name = "_date"

def call(folder_path, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    karute = 'カルテ番号等'

    # 入力ファイル
    path = str(folder_path + '/*')
    file_list = glob.glob(path)

    tmp_df = pd.DataFrame()
    for in_path in file_list:
        basename = os.path.basename(in_path).replace('.csv', '')

        df = pd.read_csv(in_path, engine='python', dtype='str')
        df = df.fillna('')

        if basename == '手術歴2019':
            df = df.dropna(subset=['手術開始時間'])
            df['手術開始時間'] = df['手術開始時間'].str.strip()
            df['手術開始時間'] = pd.to_datetime(df['手術開始時間'], format='%H:%M')

        target_colum = list(set(df.columns) - set([karute, data_column_name]))

        for target in target_colum:
            t_df = df[[karute, data_column_name, target]]
            t_df = t_df[~t_df.duplicated()]

            if basename == '手術歴2019' and target == '手術開始時間':
                result = t_df.groupby([karute, data_column_name])[target].max()
                result = result.dt.strftime('%H:%M')

            else:
                result = (t_df.groupby([karute, data_column_name])[target]
                          .apply(list)
                          .apply(lambda x: sorted(x))
                          .apply(' '.join)
                          )

            result = result.reset_index()
            result['文書種別'] = basename
            tmp_df = pd.concat([tmp_df, result])

    out_df = pd.DataFrame()
    target_colum = list(set(tmp_df.columns) - set([karute, data_column_name, '文書種別']))
    tmp_df = tmp_df.fillna('')
    for target in target_colum:
        t_df = tmp_df[[karute, data_column_name, '文書種別', target]]
        t_df = t_df[~t_df.duplicated()]

        result = (t_df.groupby([karute, data_column_name, '文書種別'])[target]
                  .apply(list)
                  .apply(lambda x: sorted(x))
                  .apply(' '.join)
                  )
        result.name = target
        out_df = pd.concat([out_df, result], axis=1)

    out_df.reset_index(inplace=True)

    out_path = out_dir + 'カルテ_全結合.csv'
    out_df.to_csv(out_path, index=False)


# 単体テスト用コマンド呼び出し関数
if __name__ == "__main__":
    import sys

    args = sys.argv
    # call(*args[1:])

    work_date_dir = "D:\mom\model/3_work" + '/' + 'wk2'
    work_dir = "D:\mom\model/1_input"
    call(work_date_dir + '/1_1/', work_date_dir + '/1_3/')
