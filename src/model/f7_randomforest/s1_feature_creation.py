# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os
import re
import glob

def call(params, logger, model_dir, model_flag, in_targets):
    """
    特徴量作成
    診療行為、特定器材、傷病、医薬品
    カルテ有無、タイトル有無、文書名詞
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s1_feature_creation"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 10:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 10]))
        raise Exception
    inDir = files[0]
    if not os.path.exists(inDir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inDir]))
        raise Exception
    inDir_kyukyu = files[2]
    if not os.path.exists(inDir_kyukyu):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inDir_kyukyu]))
        raise Exception
    hedDir = model_dir + files[3] + "/"
    if not os.path.exists(hedDir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [hedDir]))
        raise Exception
    base_path = files[4]
    if not os.path.exists(base_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [base_path]))
        raise Exception
    karuteDir = files[5]
    if not os.path.exists(karuteDir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [karuteDir]))
        raise Exception
    # 個別処理のパス
    si_path = files[6]
    if not os.path.exists(si_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [si_path]))
        raise Exception
    mastre_path = files[7]
    if not os.path.exists(mastre_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [mastre_path]))
        raise Exception
    hospitalization_category_path = files[8]
    if not os.path.exists(hospitalization_category_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [hospitalization_category_path]))
        raise Exception
    nyuin_path = files[9]
    if not os.path.exists(nyuin_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [nyuin_path]))
        raise Exception

    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(columns) < 3:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["columns", 3]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    columns3 = columns[2]

    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    target_list = params["targets"]
    if "folder_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["folder_list"]))
        raise Exception
    folder_list = params["folder_list"]
    if "kyukyu_list" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["kyukyu_list"]))
        raise Exception
    kyukyu_list = params["kyukyu_list"]
    if "option_pattern" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["option_pattern"]))
        raise Exception
    option_pattern = params["option_pattern"]
    if "x2_item" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["x2_item"]))
        raise Exception
    karute_basename = params["x2_item"]

    # モデルの場合
    if model_flag == 1:
        # 出力フォルダ
        outDir = model_dir + files[1]

        # baseCSV:入院患者のデータのみ読み込み
        try:
            in_Base_df = pd.read_csv(base_path, engine='python', dtype={columns1[0]: 'object'}
                                    , usecols=[columns1[0], columns1[1], columns1[2]])
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
            raise

        in_Base_df = in_Base_df[in_Base_df[columns1[2]] == 1][[columns1[0], columns1[1]]]

    # 予測の場合
    elif model_flag == 0:
        # 出力フォルダ
        outDir = files[1]

        # 全患者読み込み
        try:
            in_Base_df = pd.read_csv(base_path, engine='python', dtype={columns1[0]: 'object'}
                                     , usecols=[columns1[0], columns1[1]])
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
            raise

    ### 個別関数
    # monthly 月でグルーピング
    def Month(df):
        df['index_dt'] = df[columns1[1]].str[0:7]
        drop_col = [columns1[0], 'index_dt']
        week_df_group = df.groupby(drop_col, as_index=False).sum()
        week_df_1 = week_df_group.loc[:, drop_col]
        week_df_2 = week_df_group.drop(drop_col, axis=1)
        week_df_2[week_df_2 >= 1] = 1
        out_df = pd.concat([week_df_1, week_df_2], axis=1)
        out_df = out_df.rename(columns={'index_dt': columns1[1]})
        return out_df

    # weekly 週でグルーピング（日～土） #修正必要　年区別できていない
    def Week(df):
        df['index_dt'] = pd.to_datetime(df[columns1[1]])

        # 年
        df['YY'] = df[columns1[1]].str[0:4]  # 年の情報
        df['WW'] = (df['index_dt'] + pd.DateOffset(days=1)).dt.week  # 第何週か
        df['YOUBI'] = df['index_dt'].dt.dayofweek  # 曜日の情報
        # {0: '月', 1: '火', 2: '水', 3: '木', 4: '金', 5: '土', 6: '日'}

        # 週の最終日の日付取得
        sunday = df[['YY', 'WW', 'YOUBI', columns1[1]]].groupby(['WW', 'YY'], as_index=False).max()
        sunday = sunday[~sunday.duplicated()]

        # 第何週,年,カルテ番号でグルーピング
        week_df_group = df.groupby([columns1[0], 'YY', 'WW'], as_index=False).sum()

        week_df_group = pd.merge(week_df_group, sunday[[columns1[1], 'WW', 'YY']], on=['WW', 'YY'], how='left')
        week_df_group.drop(['WW', 'YOUBI', 'YY'], axis=1, inplace=True)
        week_df_1 = week_df_group[[columns1[0], columns1[1]]]
        week_df_2 = week_df_group.drop([columns1[0], columns1[1]], axis=1)
        week_df_2[week_df_2 >= 1] = 1

        out_df = pd.concat([week_df_1, week_df_2], axis=1)
        return out_df

    # general_patient 一般病棟入院基本料の算定患者のみ抽出
    def IppanByoto(df):
        sidf = pd.read_pickle(si_path)
        try:
            df_mastre = pd.read_csv(mastre_path, encoding="cp932", engine='python', dtype='object',usecols=[columns2[0]])
        except UnicodeDecodeError:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [mastre_path]))
            raise
        except ValueError as ve:
            keys = [mastre_path]
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", [keys]))
            raise
        except:
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [mastre_path]))
            raise

        df_m_list = df_mastre[columns2[0]].tolist()
        s_list = [columns1[0], columns1[1]] + df_m_list

        df_new = sidf.loc[:, sidf.columns.intersection(s_list)].reindex(columns=s_list).fillna(0)
        # df_new = sidf.loc[:, s_list].fillna(0)
        df_new['sum'] = df_new[df_m_list].sum(axis=1)
        df_k = df_new[df_new['sum'] >= 1]
        target_user = df_k[columns1[0]].unique()
        out_df = df[df[columns1[0]].isin(target_user)]
        return out_df

    # discharged_patient 退院済み患者のみ
    def Taiin(df):
        try:
            df_n = pd.read_csv(base_path, engine='python', encoding="cp932",
                            dtype={columns1[0]: 'object'},
                            usecols=columns1)
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
            raise

        # dpcは今回退院年月日が空白の場合まだ退院していない
        df_n[columns1[3]] = df_n[columns1[3]].fillna('na')
        # 今回退院年月日の記録があるのは”区分”がdpcの患者のみ
        # dpc患者かつnaのレコードを削除
        df_n_new = df_n[~((df_n[columns1[4]] == 'dpc') & (df_n[columns1[3]] == 'na'))]
        df_n_new = df_n_new[df_n_new[columns1[2]] == 1]
        out_df = pd.merge(df, df_n_new[[columns1[0], columns1[1]]], on=[columns1[0], columns1[1]])
        return out_df

    # 入院期間中でまとめる calculation
    # para 初期値＝入院期間中　数値指定した場合入院初日からpara日
    def NyuinkikanChu(df):
        # 入院期間のデータフレーム
        try:
            df_in = pd.read_csv(nyuin_path, engine='python', encoding='cp932', dtype={columns1[0]: 'object'}
                                , usecols=[columns1[0], columns1[1], columns3[2], columns3[3]])
        except ValueError as ve:
            keys = []
            for arg in ve.args:
                keys.extend(re.findall(r"\'(.*)\'", arg))
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003016", keys))
            raise

        # 日付↔文字列　変換
        try:
            df_in[columns3[2]] = pd.to_datetime(df_in[columns3[2]], format='%Y/%m/%d')
            df_in[columns3[3]] = pd.to_datetime(df_in[columns3[3]], format='%Y/%m/%d')
            df_in[columns3[0]] = df_in[columns3[2]].dt.strftime('%Y/%m/%d')
            df_in[columns3[1]] = df_in[columns3[3]].dt.strftime('%Y/%m/%d')
        except ValueError:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003013", [nyuin_path]))
            raise

        # カルテ番号と入退院日　一覧抽出
        __new_df = df_in[[columns1[0]] + columns3][~df_in.duplicated(subset=[columns1[0]] + columns3)].reset_index(
            drop=True)

        # 1行ずつ確認
        for coli in range(len(__new_df)):

            # カルテ番号、入院日、退院日を取得
            karute = __new_df.loc[coli, columns1[0]]
            startstr = __new_df.loc[coli, columns3[0]]
            # endstr = __new_df.loc[coli, columns3[1]]
            start = __new_df.loc[coli, columns3[2]]
            end = __new_df.loc[coli, columns3[3]]

            # カルテ番号、入院日、退院日を取得一致する入院期間を df_in から抽出
            date_df = df_in[[columns1[0], columns1[1]]][
                (df_in[columns1[0]] == karute) & (df_in[columns3[2]] == start) & (
                        df_in[columns3[3]] == end)].reset_index(drop=True)
            fill_df = pd.merge(date_df, df, on=[columns1[0], columns1[1]])

            # 長さチェック
            if len(fill_df) == 0:
                continue

            group_df = fill_df.fillna(0)
            group_df = group_df.groupby([columns1[0]]).sum()
            group_df[group_df >= 1] = 1
            group_df = group_df.reset_index()
            group_df[columns1[1]] = startstr

            if coli == 0:
                out_df = group_df
            else:
                out_df.loc[coli, :] = group_df.loc[0, :]

        return out_df

    # DPC患者のみ。入院初日 first_hospitalization
    def NyuinkiSyoniti(df):
        indf = pd.read_csv(hospitalization_category_path, engine='python', encoding="cp932",
                           dtype={columns1[0]: 'object'})
        indf_new = indf.drop_duplicates()
        new_df = pd.merge(df, indf_new, on=[columns1[0], columns1[1]], copy=False)
        # 入院初日のみに絞る項目
        out_df = new_df[(new_df["予定入院"] >= 1) | (new_df["緊急入院"] >= 1) | (new_df["緊急入院（2以外の場合）"] >= 1)]
        out_df = out_df.drop(["予定入院", "緊急入院", "緊急入院（2以外の場合）"], axis=1)
        return out_df

    ### 個別関数

    # 出力フォルダ作成
    os.makedirs(outDir, exist_ok=True)

    # targetごと
    for target, option_list in zip(target_list, option_pattern):

        # 予測の場合
        if model_flag==0:
            # _target_list：jsonで指定したtargets、in_targets：predicti_receiptで指定したtargets
            # 共通する算定項目コードを抽出する。共通する算定項目コードがない場合、処理をスキップ。
            if target not in in_targets:
                continue

        if model_flag==1:
            if not os.path.exists(hedDir + target + '/'):
                continue

        for folder in folder_list:

            # fileチェック　continue
            inpath = inDir + folder + ".pickle"

            # 診療行為の場合break
            if folder == '診療行為':
                if not os.path.exists(inpath):
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [inpath]))
                    break
            else:
                if not os.path.exists(inpath):
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [inpath]))
                    continue

            # pickle読み込み
            indf = pd.read_pickle(inpath)

            # 説明変数
            hed_path = hedDir + target + '/' + folder + '_説明変数.csv'

            # 救急医療管理加算のコードの場合
            if target in kyukyu_list:
                if folder == '診療行為':
                    _si_path=inDir_kyukyu + target + '.csv'

                    # 診療行為が読み込めない場合break　次の算定項目へ
                    if not os.path.exists(_si_path):
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002002", [inpath]))
                        break

                    # siCSV
                    try:
                        indf_kyukyu = pd.read_csv(_si_path, engine='python', encoding="cp932",
                                                dtype={columns1[0]: 'object'},
                                                usecols=[columns1[0], columns1[1], target])
                    except ValueError as ve:
                        keys = []
                        for arg in ve.args:
                            keys.extend(re.findall(r"\'(.*)\'", arg))
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR002004", keys))
                        break

                    new_df_m = pd.merge(in_Base_df, indf_kyukyu, on=[columns1[0], columns1[1]], copy=False)


                if os.path.isfile(hed_path):
                    try:
                        use_col = pd.read_csv(hed_path, engine='python', encoding="cp932").columns.tolist()
                    except:
                        l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [hed_path]))
                        raise

                else:
                    continue

                usec = [columns1[0], columns1[1]] + use_col
                indf = indf.loc[:, indf.columns.intersection(usec)].reindex(columns=usec)
                new_df_m = pd.merge(new_df_m, indf, on=[columns1[0], columns1[1]], how='left', copy=False)

            else:
                if folder == '診療行為':
                    if os.path.isfile(hed_path):
                        try:
                            use_col = pd.read_csv(hed_path, engine='python', encoding="cp932").columns.tolist()
                        except:
                            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [hed_path]))
                            raise
                    else:
                        use_col = []

                    usec = [columns1[0], columns1[1], target] + use_col
                    indf = indf.loc[:, indf.columns.intersection(usec)].reindex(columns=usec)
                    new_df_m = pd.merge(in_Base_df, indf, on=[columns1[0], columns1[1]], how='left', copy=False)
                else:
                    if os.path.isfile(hed_path):
                        try:
                            use_col = pd.read_csv(hed_path, engine='python', encoding="cp932").columns.tolist()
                        except:
                            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [hed_path]))
                            raise
                    else:
                        continue

                    usec = [columns1[0], columns1[1]] + use_col
                    indf = indf.loc[:, indf.columns.intersection(usec)].reindex(columns=usec)
                    new_df_m = pd.merge(new_df_m, indf, on=[columns1[0], columns1[1]], how='left', copy=False)

        # カルテ
        karute_path = hedDir +target + '/karute/' + karute_basename + '_説明変数.csv'
        if os.path.isfile(karute_path):
            try:
                head_df = pd.read_csv(karute_path, engine='python', encoding="cp932")
            except:
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [karute_path]))
                raise
            colm = [columns1[0], columns1[1]] + head_df.columns.tolist()

            if model_flag == 0:
                karute_file_list = glob.glob(karuteDir + '*')
                karute_all_df = pd.DataFrame()
                # カルテのｄｆ
                for karute_path in karute_file_list:
                    df = pd.read_csv(karute_path, engine='python', encoding="cp932", dtype={columns1[0]: 'object'})
                    df = df.loc[:, df.columns.intersection(colm)].reindex(columns=colm)
                    karute_all_df = pd.concat([karute_all_df, df])

                if len(karute_all_df)!=0:
                    karute_all_df = karute_all_df.groupby([columns1[0], columns1[1]], as_index=False).sum()

            elif model_flag == 1:
                karute_all_df = pd.read_csv(karuteDir + "allカルテ.csv", engine='python', encoding="cp932", dtype={columns1[0]: 'object'})

            karute_all_df = karute_all_df.loc[:, karute_all_df.columns.intersection(colm)].reindex(columns=colm)


            new_df = karute_all_df.rename(
                columns=lambda s: str(karute_basename + '_' + s) if s != columns1[0] and s != columns1[1] else s)
            new_df_m = pd.merge(new_df_m, new_df, on=[columns1[0], columns1[1]], how='left', copy=False)

        else:
            path = str(hedDir+ target + '/karute/' + '*')
            order_file_list = glob.glob(path)
            for order in order_file_list:
                basename = os.path.basename(order).replace(str('_説明変数.csv'), '')

                # 説明変数
                try:
                    head_df = pd.read_csv(order, engine='python', encoding="cp932")
                except:
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003008", [order]))
                    raise

                df = pd.read_csv(karuteDir + basename + '.csv', engine='python', encoding="cp932",
                                 dtype={columns1[0]: 'object'})

                colm = [columns1[0], columns1[1]] + head_df.columns.tolist()
                new_df = df.loc[:, df.columns.intersection(colm)].reindex(columns=colm)
                new_df = new_df.rename(
                    columns=lambda s: str(basename + '_' + s) if s != columns1[0] and s != columns1[1] else s)
                new_df_m = pd.merge(new_df_m, new_df, on=[columns1[0], columns1[1]], how='left', copy=False)

        out_df = new_df_m.set_index([columns1[0], columns1[1]])
        out_df[out_df >= 2] = 1
        out_df = out_df.reset_index()

        # 組み合わせチェック
        # 4つのうち2つ以上一致したら⇒エラーとして処理 or listの最初の要素のみ実施するか
        date_option = ['monthly', 'weekly', 'calculation', 'first_hospitalization']
        l1_l2_and = set(option_list) & set(date_option)
        if len(l1_l2_and) >= 2:
            # 最初の要素のほうを採用
            l_len = len(option_list) - 1
            tmp_list = set(option_list) - l1_l2_and
            for y in l1_l2_and:
                if l_len > option_list.index(y):
                    list_str = y
            option_list = [list_str] + list(tmp_list)
            # warning
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR002001", [target, "option_pattern", l1_l2_and - {list_str}]))

        for option in option_list:
            if option == 'monthly':
                out_df = Month(out_df)
            elif option == 'weekly':
                out_df = Week(out_df)
            elif option == 'calculation':
                out_df = NyuinkikanChu(out_df)
            elif option == 'first_hospitalization':
                out_df = NyuinkiSyoniti(out_df)
            elif option == 'discharged_patient':
                out_df = Taiin(out_df)
            elif option == 'general_patient':
                out_df = IppanByoto(out_df)

        # 出力エラーチェック
        outpath=outDir + "/" + target + '.pickle'
        try:
            out_df.to_pickle(outpath)
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

        # 確認用
        # out_df.to_csv(outDir + 'csv/' + target + '_特徴量.csv')
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s1_feature_creation"]))