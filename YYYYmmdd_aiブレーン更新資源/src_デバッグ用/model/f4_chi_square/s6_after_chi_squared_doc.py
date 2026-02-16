# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30
import pandas as pd
import re
import os
import glob

# 1/0データ作成
def DataKakou(df, target, l_2d):
    fileheader = list(set(l_2d))
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
    X二乗で残った項目を抽出
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s6_after_chi_squared_doc"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 5:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 5]))
        raise Exception
    inDir = files[0]
    # パスが存在しない可能性もあるためraise不要
    if not os.path.exists(inDir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002002", [inDir]))
    outDir = model_path + files[1]
    in_df_path = files[2]
    if not os.path.exists(in_df_path):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [in_df_path]))
        raise Exception
    out_df_path = files[3]
    master_dir = files[4]
    if not os.path.exists(master_dir):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [master_dir]))
    if not os.path.exists(out_df_path):
        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003012", [out_df_path]))
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
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    targets = params["targets"]
    if "use_word" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["use_word"]))
        raise Exception
    use_word_list = params["use_word"]

    # targetlistがカラじゃない場合、indirがある場合
    if len(targets) != 0 and os.path.exists(inDir):

        # X二乗後のフォルダ
        target_add_list = glob.glob(master_dir + '/add_*.csv')
        target_del_list = glob.glob(master_dir + '/del_*.csv')

        add_df=pd.DataFrame(columns=columns4)
        del_df=pd.DataFrame(columns=columns4)
        # 追加 除外
        for path in target_add_list + target_del_list:
            filname_list = os.path.basename(path).replace('.csv', '').split('_')
            if filname_list[1] == "word":
                if filname_list[2] in targets:
                    # masterCSV:追加項目　読み込み
                    try:
                        master_df = pd.read_csv(path, engine='python',usecols=columns4, encoding="cp932", dtype='object')
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

                    if filname_list[0] == "add":
                        add_df = pd.concat([add_df, master_df])
                    elif filname_list[0] == "del":
                        del_df=pd.concat([del_df, master_df])

        for target, use_word in zip(targets, use_word_list):
            # 出力フォルダ作成
            # os.makedirs(outDir + "/" + target + '/karute/', exist_ok=True) ★ここでフォルダ作成すると算定件数０件でもフォルダが作成されてしまい、後続のadd_delでエラーになってしまうためコメントアウト

            target_folpath = inDir + target
            # 算定項目ごとのフォルダ
            target_filepath_list = glob.glob(target_folpath + '/karute/*')

            x2_df = pd.DataFrame()
            for filepath in target_filepath_list:

                # karuteCSV
                try:
                    df_p = pd.read_csv(filepath, engine='python', encoding="cp932",
                                    usecols=[columns2[0], columns2[1], columns2[2], columns2[3], columns2[4]],
                                    dtype={columns2[2]: 'float', columns2[3]: 'float'})
                except ValueError as ve:
                    keys = []
                    for arg in ve.args:
                        keys.extend(re.findall(r"\'(.*)\'", arg))
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                    continue

                df_p_flag = df_p[df_p[columns2[4]] == 1]
                if len(df_p_flag) != 0:
                    x2_df = pd.concat([x2_df, df_p_flag])

            # x2_dfがからの場合出力せず
            if len(x2_df) == 0:
                continue

            x2_df = x2_df.loc[:, [columns2[0], columns2[1], columns2[2], columns2[3], columns2[4]]]
            indf_im_v = x2_df.values
            im_sort = sorted(indf_im_v, key=lambda x: (x[3], -x[2]))
            im_sort_df = pd.DataFrame(im_sort, columns=x2_df.columns)
            top500_df = im_sort_df.iloc[0:use_word, :]

            for koumoku in top500_df.項目名.unique():
                koumoku_meisi = top500_df[top500_df[columns2[0]] == koumoku].単語
                im_sort_df = pd.DataFrame(columns=koumoku_meisi)

                # 説明変数ファイル出力エラーチェック
                outpath = outDir + "/" + target + '/karute/' + str(koumoku) + '_説明変数.csv'
                try:
                    # outpathの１つ上のパスが存在しなければ作成
                    outpath_parent = os.path.dirname(outpath)
                    if not os.path.exists(outpath_parent):
                        os.makedirs(outpath_parent)
                    im_sort_df.to_csv(outpath, index=False, encoding='cp932')
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise

        # 形態素解析のファイル取得
        target_filepath_list = glob.glob(in_df_path + '/*')
        for filepath in target_filepath_list:
            # 形態素解析ｄｆ読み込み
            try:
                df_keitaiso = pd.read_csv(filepath, engine='python', encoding="cp932", usecols=columns3, dtype={columns3[0]: 'object', columns3[3]: 'str'})
            except ValueError as ve:
                keys = []
                for arg in ve.args:
                    keys.extend(re.findall(r"\'(.*)\'", arg))
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                continue

            basename = os.path.basename(filepath).replace("_形態素解析.csv", "")

            # targetごとにファイルチェック
            all_word_list=[]
            for target in targets:
                x2_path = outDir + "/" + target + '/karute/' + basename+"_説明変数.csv"
                if os.path.isfile(x2_path):
                    # X二乗結果
                    try:
                        l1_l2_and = pd.read_csv(x2_path, engine='python', encoding="cp932").columns.tolist()
                    except:
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [x2_path]))
                        raise
                    all_word_list.extend(l1_l2_and)

            if len(all_word_list) !=0:
                # 追加
                all_word_list.extend(add_df[add_df[columns4[0]]==basename][columns4[1]].tolist())
                # 除外
                all_word_list = [e for e in all_word_list if e not in del_df[del_df[columns4[0]]==basename][columns4[1]].tolist()]

                # 1/0データ作成
                new_df = DataKakou(df_keitaiso, columns3[3], all_word_list)

                try:
                    new_df.to_csv(out_df_path + basename + ".csv", encoding='cp932', index=False)
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [out_df_path + basename + ".csv" + ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise

    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s6_after_chi_squared_doc"]))
