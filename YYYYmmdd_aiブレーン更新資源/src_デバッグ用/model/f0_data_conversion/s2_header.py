# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import glob

def call(params,logger):
    '''
    情報区分、月ごとに対応するヘッダー情報を付与
    患者ごとに紐づけ番号を付与
    '''
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s2_header"]))

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
    head_path = files[2]
    if not os.path.exists(head_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [head_path]))
        raise Exception
    input_date = files[3]
    if not os.path.exists(input_date):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [input_date]))
        raise Exception
    if "key" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["key"]))
        raise Exception
    key = params["key"]  # list
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 1]))
        raise Exception
    columns1 = columns[0]

    # date
    s_target_month = pd.read_csv(input_date, engine='python',
                                 encoding='cp932', dtype='object').columns.tolist()

    # header
    try:
        head_df = pd.read_csv(head_path, engine='python', header=0, encoding='cp932')
    except:
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [head_path]))
        raise

    # 月ごとに処理
    for target_month2 in s_target_month:
        out_path = out_dir + target_month2 + '/ika/'

        # 出力フォルダ作成
        os.makedirs(out_path, exist_ok=True)

        for kubun in ['国保', '社保']:
            file_path = in_dir + target_month2 + '/ika/RECEIPTC' + kubun
            file_list = glob.glob(file_path + '*')

            # 抽出したファイル len(file_list)=0の場合、処理をスキップ
            if len(file_list) == 0:
                # 対象のデータ（フォルダ）が空
                l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [file_path]))

            else:
                # 先にREの処理を実施
                re_path = file_path + '_RE.csv'
                if not os.path.exists(re_path):
                    # 入力ファイルの読み込みに失敗
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [re_path]))
                    continue

                df_re = pd.read_csv(re_path, engine='python', index_col=None, encoding='cp932', dtype='object')

                # ヘッダー付与
                len_dfre = len(df_re.columns)
                header_list = head_df['RE'].tolist()

                # ヘッダーの長さ
                len_head = len(header_list)
                if len_dfre - len_head == 1:
                    header_list = header_list + key
                elif len_dfre - len_head > 1:
                    header_list = header_list + [''] * (len_dfre - len_head - 1) + key
                else:
                    # 入力ファイルとヘッダーが一致しない場合,エラー
                    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003005", [re_path, head_path]))
                    raise Exception
                df_re.columns = header_list

                # 出力
                out_str = out_path + '/RECEIPTC' + kubun + '_RE.csv'
                try:
                    df_re.to_csv(out_str, index=False, encoding='cp932')
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [out_str+ ":" +traceback.format_exception(*sys.exc_info())[-1]]))
                    raise

                # RE以外取得
                df_re_karute = df_re[columns1 + key]
                file_list = [s for s in file_list if '_RE.csv' not in s]

                for file in file_list:
                    df = pd.read_csv(file, engine='python', index_col=None, encoding='cp932', dtype='object')

                    df_ID = df['0'].unique()[0]

                    # ヘッダー付与
                    len_dfre = len(df.columns)
                    header_list = head_df[df_ID].tolist()

                    len_head = len(header_list)
                    if len_dfre - len_head == 1:
                        header_list = header_list + key
                    elif len_dfre - len_head > 1:
                        header_list = header_list + [''] * (len_dfre - len_head - 1) + key
                    else:
                        # 入力ファイルとヘッダーが一致しない場合,エラー
                        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003005", [file, head_path]))
                        raise Exception

                    df.columns = header_list

                    out_df = pd.merge(df, df_re_karute, on=key)

                    # 出力
                    file_name = os.path.basename(file)
                    out_str = out_path + '/' + file_name

                    try:
                        out_df.to_csv(out_str, index=False, encoding='cp932')
                    except:
                        import traceback, sys
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [out_str + ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                        raise


    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s2_header"]))
