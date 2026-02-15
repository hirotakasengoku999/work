# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import numpy as np
import scipy as sp
import scipy.stats
import os
import glob
import re
import collections


# 1/0データ作成
def DataKakou(df, target, l_2d,del_l):
    _fileheader = list(set(l_2d))
    fileheader = [e for e in _fileheader if e not in del_l]
    s_list = df.columns.tolist() + fileheader
    _new_df = df.loc[:, df.columns.intersection(s_list)].reindex(columns=s_list)

    def Kakou(x):
        target_lis_w = set(fileheader) & set(x[target])
        x[target_lis_w] = 1
        return x

    new_df = _new_df.apply(Kakou, axis=1)
    new_df.drop(target, axis=1, inplace=True)
    return new_df

def call(params, logger, model_path):
    """
    ⑦文書種別ごと　のX二乗検　実施
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s4_chi_square_all_doc"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 7:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 6]))
        raise Exception
    inpath_si = files[0]
    if not os.path.exists(inpath_si):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath_si]))
        raise Exception
    base_path = files[1]
    if not os.path.exists(base_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [base_path]))
        raise Exception
    inDir = files[2]
    if not os.path.exists(inDir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inDir]))
        raise Exception
    outDir = model_path + files[3]
    wkdir = files[4]
    if not os.path.exists(wkdir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [wkdir]))
        raise Exception
    outpathall = files[5]
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    master_dir = files[6]
    if not os.path.exists(master_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [master_dir]))
    columns = params["columns"]
    if len(columns) < 5:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 5]))
        raise Exception
    columns2 = columns[1]
    columns1 = columns[0]
    columns3 = columns[2]
    columns4 = columns[3]
    columns5 = columns[4]
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    target_list = params["targets"]
    if "specific_target_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["specific_target_list"]))
        raise Exception
    specific_target_list = params["specific_target_list"]
    if "p_value" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["p_value"]))
        raise Exception
    p_value_list = params["p_value"]
    if "use_word" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["use_word"]))
        raise Exception
    use_word_list = params["use_word"]
    if "x2_item" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["x2_item"]))
        raise Exception
    filname = params["x2_item"]

    # targetlistがカラの場合　処理しない
    if len(target_list) != 0:

        # 特徴のあるすべての単語
        all_koumoku = []
        # 削除する単語
        del_koumoku = []

        # 名詞追加
        # X二乗後のフォルダ
        target_add_list = glob.glob(master_dir + '/add_*.csv')
        target_del_list = glob.glob(master_dir + '/del_*.csv')

        # 追加
        for path in target_add_list + target_del_list:
            filname_list = os.path.basename(path).replace('.csv', '').split('_')

            if filname_list[1] == "karute":
                if filname_list[2] in target_list:
                    # masterCSV:追加項目　読み込み
                    try:
                        master_df = pd.read_csv(path, engine='python',usecols=columns5, encoding="cp932", dtype='object')
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

                    w_list = master_df[master_df.columns[0]].tolist()
                    if filname_list[0] == "add":
                        all_koumoku.extend(w_list)
                    elif filname_list[0] == "del":
                        del_koumoku.extend(w_list)

        # baseCSV
        try:
            df_head = pd.read_csv(base_path, engine='python', dtype={columns1[0]: 'object'}
                                , usecols=[columns1[0], columns1[1], columns1[2]])
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
            raise
        df_head = df_head[df_head[columns1[2]] == 1][[columns1[0], columns1[1]]]

        # 診療行為読み込み
        df_si = pd.read_pickle(inpath_si)
        # target_listからspecific_target_listと重複する項目を抽出
        spe_targets = list(set(target_list) & set(specific_target_list))
        # 診療行為dfの項目と target_list の重複している項目を抽出
        si_targets = list(set(df_si.columns) & set(target_list))
        # x二乗を実施する項目のlist
        chi_list = si_targets + spe_targets

        # 診療行為項目抽出
        df_si = df_si[[columns2[0], columns2[1]] + si_targets]
        new_df_si = pd.merge(df_head, df_si, on=[columns2[0], columns2[1]], how='left', copy=False)

        # カルテ 文書読み込み
        filepathlist = glob.glob(inDir + '/*')
        karute_df_tmp = pd.DataFrame(columns=[columns3[0], columns3[1], columns3[2]])
        for filepath in filepathlist:

            # karuteCSV
            try:
                in_df = pd.read_csv(filepath, engine='python', dtype={columns3[0]: 'object'},
                                    usecols=[columns3[0], columns3[1], columns3[2]])
                basename = os.path.basename(filepath).replace("_形態素解析.csv", "")
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                continue

            # karute_df_tmp = pd.concat([karute_df_tmp, in_df])
            karute_df_tmp = pd.merge(karute_df_tmp, in_df,on=[columns3[0], columns3[1]],how="outer",suffixes=("",basename))

        """
        karute_df = (karute_df_tmp.groupby([columns3[0], columns3[1]])[columns3[2]]
                     .apply(list)
                     .apply(lambda x: sorted(x))
                     .apply(' '.join)
                     ).reset_index()
        """

        if len(karute_df_tmp.columns)>3:
            target_col_re=[ x for x in karute_df_tmp.columns if x not in [columns3[0], columns3[1]]]
            karute_df_tmp[columns3[2]]=""
            for t_col in target_col_re:
                karute_df_tmp[t_col] = karute_df_tmp[t_col].fillna("")
                karute_df_tmp[columns3[2]]=karute_df_tmp[columns3[2]].str.cat(karute_df_tmp[t_col], sep=' ')

        karute_df=karute_df_tmp[[columns3[0], columns3[1], columns3[2]]].copy()
        karute_df[columns3[2]] = karute_df[columns3[2]].apply(lambda x: x.split(' '))
        l_1d = karute_df[columns3[2]].values.tolist()
        l_2d=[e for inner_list in l_1d for e in inner_list]

        l_2nd_tupl = collections.Counter(l_2d)
        target_col_list = [i for i, v in l_2nd_tupl.items() if v > 2]
        # target_col_list = list(set(l_2d))
        if "" in target_col_list:
            target_col_list.remove('')

        # 診療行為と結合
        df_me_tmp = pd.merge(new_df_si, karute_df, on=[columns3[0], columns3[1]], how='left', copy=False)


        for target, p_value, use_word in zip(target_list, p_value_list, use_word_list):

            # X二乗実施項目にない場合
            # 項目の有無チェック (chi_list)
            if target not in chi_list:
                # 算定項目は実行されません
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002001", [target, "targets", target]))
                # ない場合次のfor
                continue

            # 実績の有無チェック
            if target not in specific_target_list:
                if new_df_si[target].sum() == 0:
                    # 算定項目は実行されません
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002001", [target, "targets", target]))
                    # ない場合次のfor
                    continue

            # 出力フォルダ作成
            os.makedirs(outDir + "/" + target + '/karute/', exist_ok=True)

            df_me = df_me_tmp.copy()

            # specific_target_listに算定項目が含まれる場合
            if target in specific_target_list:

                spe_path = wkdir + target + '.csv'
                # ファイル有無　なければ contin messe
                if not os.path.isfile(spe_path):
                    # データがないため、処理をスキップします
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [spe_path]))
                    # ない場合次のfor
                    continue

                try:
                    df_specific = pd.read_csv(spe_path, engine='python', dtype={columns2[0]: 'object'},
                                            usecols=[columns2[0], columns2[1], target])
                except ValueError as ve:
                    keys = []
                    for arg in ve.args:
                        keys.extend(re.findall(r"\'(.*)\'", arg))
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                    continue

                new_df_si_spec = pd.merge(df_specific, df_head, on=[columns2[0], columns2[1]], copy=False)
                df_me = pd.merge(new_df_si_spec, karute_df, on=[columns2[0], columns2[1]], copy=False, how='left')

                if df_me[target].sum() == 0:
                    # 算定項目は実行されません
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002001", [target, "targets", target]))
                    # ない場合次のfor
                    continue

            df_p_flag = pd.DataFrame(columns=columns4)

            c_df = df_me[[target, columns3[2]]].copy()
            c_df[columns3[2]] = c_df[columns3[2]].fillna('')
            c_df[target] = c_df[target].fillna(0)
            c_df.loc[c_df[target] > 0, target] = 1

            i = 0
            for use_col in target_col_list:

                x = c_df[columns3[2]].apply(lambda x: use_col in x).astype(np.float32)

                if sum(x) == 0:
                    continue

                # クロス集計
                crossed = pd.crosstab(c_df[target], x)

                a = crossed.iloc[0, 0]
                b = crossed.iloc[0, 1]
                c = crossed.iloc[1, 0]
                d = crossed.iloc[1, 1]

                D_d = ((c + d) * (b + d)) / (a + b + c + d)

                # カイ二乗値、p値,自由度
                x2, p, dof, expected = sp.stats.chi2_contingency(crossed)  # x2:カイ二乗値.p:確率

                if p < p_value and d > D_d:
                    flags = 1
                    df_p_flag.loc[i, columns4] = use_col, p, flags
                    i = i + 1

            indf_im_v = df_p_flag.values
            im_sort = sorted(indf_im_v, key=lambda x: (x[1]))
            im_sort_df = pd.DataFrame(im_sort, columns=df_p_flag.columns)
            top_df = im_sort_df.iloc[0:use_word, :]

            koumoku = top_df[columns4[0]].tolist()

            if len(koumoku) != 0:
                all_koumoku.extend(koumoku)

                im_sort_df = pd.DataFrame(columns=koumoku)
                # 説明変数ファイル出力エラーチェック
                outpath = outDir + "/" + target + '/karute/' + filname + '_説明変数.csv'
                try:
                    im_sort_df.to_csv(outpath, index=False, encoding='cp932')
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise

        # 1/0データ作成
        new_df = DataKakou(karute_df, columns3[2], all_koumoku,del_koumoku)

        try:
            new_df.to_csv(outpathall, encoding='cp932', index=False)
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015",[outpathall + ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s4_chi_square_all_doc"]))
