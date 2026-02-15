# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import re
import os
import glob


def call(params, logger, model_path):
    """
    X二乗の結果に項目を追加

    -追加対象項目-(/data/receipt_code/　配下すべて)
      113011210', '医療機器安全管理料', 'si'
      113013810', '夜間休日救急搬送医学管理料', si
      113013710', '院内トリアージ実施料', si
      190101770', '難病患者等入院診療加算', 'sy', 'iy'
      820100018', '救急医療管理加算１', 'si', 'iy'
      820100024', '救急医療管理加算１', 'si'
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s7_after_chi_squared_add_del"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    dir = model_path + files[0]
    if not os.path.exists(dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [dir]))
        raise Exception
    master_dir = files[1]
    if not os.path.exists(master_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [master_dir]))
        raise Exception
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 1:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 1]))
        raise Exception
    columns1 = columns[0]

    # X二乗後のフォルダ
    target_add_list = glob.glob(master_dir + '/add_*.csv')
    target_del_list = glob.glob(master_dir + '/del_*.csv')

    # 追加
    for path in target_add_list + target_del_list:
        filname_list = os.path.basename(path).replace('.csv', '').split('_')

        # masterCSV:追加項目　読み込み
        try:
            master_df = pd.read_csv(path, usecols=columns1, engine='python', encoding="cp932", dtype='object')
        except UnicodeDecodeError:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [path]))
            continue
        except ValueError as ve:
            keys = [path]
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", [keys]))
            continue
        except:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [path]))
            continue

        master_l_list = [master_df[columns1[0]].tolist()]

        dir_check = dir + "/" + filname_list[2] + '/'

        # 読み込みファイルの分類
        if filname_list[1] == 'si':
            rese_list = ['診療行為']
        elif filname_list[1] == 'iy':
            rese_list = ['医薬品']
        elif filname_list[1] == 'sy':
            rese_list = ['傷病']
        elif filname_list[1] == 'to':
            rese_list = ['特定器材']
        elif filname_list[1] == 'bunsyo':
            rese_list = ['文書有無']
        elif filname_list[1] == 'title':
            rese_list = ['タイトル有無']
        elif filname_list[1] == 'karute':
            rese_list = ['karute/karute_all']
            dir_check = dir + "/" + filname_list[2] + '/'
        elif filname_list[1] == 'word':
            master_l_list = []
            rese_list = master_df[columns1[0]].unique()
            for code in rese_list:
                tmp_master_list = master_df[master_df[columns1[0]] == code][columns1[1]].tolist()
                master_l_list.append(tmp_master_list)
            rese_list = 'karute/' + rese_list
            dir_check = dir + "/" + filname_list[2] + '/'
        else:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [path]))
            continue

        for rese, master_list in zip(rese_list, master_l_list):
            val_path = dir_check + rese + '_説明変数.csv'
            if os.path.exists(val_path):
                x2_list = pd.read_csv(val_path, engine='python', encoding="cp932").columns.tolist()
            elif os.path.exists(dir_check):
                x2_list = []
            else:
                # 算定項目のフォルダがなければスキップ
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [val_path]))
                continue

            # 対象の項目の有無
            if filname_list[0] == 'add':
                result1 = list(set(x2_list) & set(master_list))
                result2 = list(set(master_list) - set(result1))
                out_list = x2_list + result2
            elif filname_list[0] == 'del':
                out_list = list(set(x2_list) - set(master_list))
            outdf = pd.DataFrame(columns=out_list)

            # 説明変数ファイル出力エラーチェック
            try:
                outdf.to_csv(val_path, index=False, encoding='cp932')
            except:
                import traceback, sys
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [val_path+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s7_after_chi_squared_add_del"]))
