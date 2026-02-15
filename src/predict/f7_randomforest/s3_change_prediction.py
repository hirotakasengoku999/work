# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import pandas as pd
import os


def call(params, logger, in_targets):
    '''
    条件に応じて、予測結果を強制的に変更しする

    ○背反項目
    113011210　医療機器安全管理料（生命維持管理装置使用）→190137910,呼吸ケアチーム加算
    150333010　閉鎖循環式全身麻酔３（伏臥位）→　150332910, 閉鎖循環式全身麻酔３（麻酔困難な患者）
    150233410　閉鎖循環式全身麻酔５（その他）→　150328210, 閉鎖循環式全身麻酔５（麻酔困難な患者）
    150333210　閉鎖循環式全身麻酔４　→　150333110, 閉鎖循環式全身麻酔４（麻酔困難な患者
    113013810　夜間休日救急搬送医学管理料　→　113013710, 院内トリアージ実施料
    113013710, 院内トリアージ実施料　→ 113013810　夜間休日救急搬送医学管理料

    ○背反項目　複数項目（以下それぞれ背反）
    認知症ケア加算１（１４日以内の期間）	190193110
    認知症ケア加算１（１５日以上の期間）	190193210
    認知症ケア加算１（１５日以上の期間）身体的拘束実施	190193610
    認知症ケア加算１（１４日以内の期間）身体的拘束実施	190193510

    ○対象の術式がなければ強制0%
    150339310　自動吻合器加算
    150339210　自動縫合器加算
    150286990　超音波凝固切開装置等加算
    '''
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s3_change_prediction"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 3:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 3]))
        raise Exception
    dir = files[0]
    if not os.path.exists(dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [dir]))
        raise Exception
    si_path = files[1]
    if not os.path.exists(si_path):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [si_path]))
        raise Exception
    ope_dir = files[2]
    if not os.path.exists(ope_dir):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [ope_dir]))
        raise Exception
    if "columns" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["columns"]))
        raise Exception
    columns = params["columns"]
    if len(files) < 2:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 2]))
        raise Exception
    columns1 = columns[0]
    columns2 = columns[1]
    if "targets" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["targets"]))
        raise Exception
    target_list = params["targets"]
    if "drops" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["drops"]))
        raise Exception
    drop_list = params["drops"]
    if "option_pattern" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["option_pattern"]))
        raise Exception
    option_pattern = params["option_pattern"]
    # 共通する算定項目コードを抽出する。
    target_list_1 = target_list[0]
    target_list_2 = target_list[1]
    target_list_3 = target_list[2]
    target_list_4 = target_list[3]
    drop_list_1 = drop_list[0]
    drop_list_2 = drop_list[1]
    drop_list_3 = drop_list[2]
    drop_list_4 = drop_list[3]
    option_pattern_1=option_pattern[0]
    option_pattern_2=option_pattern[1]
    option_pattern_3=option_pattern[2]
    option_pattern_4=option_pattern[3]

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

    # 予測結果　読み込み
    def Input(filname):
        inpath = dir + filname + '_result.csv'
        # ファイルが存在しない場合　warning 算定項目は実施されません
        if not os.path.exists(inpath):
            return 0
        # ファイルが存在する場合、csv読み込み
        else:
            return pd.read_csv(inpath, engine='python', dtype={columns1[0]: 'object'})

    # 出力
    def Output(outdf, filname):
        outpath = dir + '/' + filname + '_result.csv'
        # お客様がexcelで表示するファイルのため、SJISで出力
        try:
            outdf.to_csv(outpath, index=False, encoding='cp932')
        except:
            import traceback, sys
            l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
            raise

    # ○背反項目
    def Check(targetNo, dropNo,option):
        indf = Input(targetNo)

        # 読み込めなかった場合 return 0
        if not isinstance(indf, pd.DataFrame):
            return 0

        try:
            # 特徴量に対象の項目がある場合
            indf[dropNo] = indf[dropNo].fillna(0).astype('int')
            indf.loc[indf[dropNo] == 1, columns1[2]] = 0
            new_df = indf.copy()
        except KeyError:
            # 特徴量に対象の項目がない場合、診療行為マージ
            usec = [columns1[1], columns1[0], dropNo]
            df_tmp = indf_s.loc[:, indf_s.columns.intersection(usec)].reindex(columns=usec)
            df_tmp[dropNo] = df_tmp[dropNo].fillna(0).astype('int')

            if option == 'monthly':
                df_tmp = Month(df_tmp)
            elif option == 'weekly':
                df_tmp = Week(df_tmp)

            new_df = pd.merge(indf, df_tmp, on=[columns1[1], columns1[0]], how='left')
            new_df[dropNo] = new_df[dropNo].fillna(0)
            new_df[columns2[0]] = new_df[columns1[2]]
            new_df.loc[new_df[dropNo] >= 1, columns1[2]] = 0
            new_df.sum()
        Output(new_df, targetNo)

    # ○対象の術式がなければ強制0%
    def Check2(targetNo):
        indf = Input(targetNo)

        # 読み込めなかった場合 return 0
        if not isinstance(indf, pd.DataFrame):
            return 0

        # 対象の術式　ファイル有無確認　ない場合 return 0
        ope_path = ope_dir + targetNo + '.csv'
        if not os.path.exists(ope_path):
            return 0

        syuzyutu_target = pd.read_csv(ope_path, engine='python', dtype={'コード': 'object'})
        l1_l2_and = list(set(syuzyutu_target.コード.tolist()) & set(indf.columns.tolist()))
        indf[l1_l2_and] = indf[l1_l2_and].fillna(0)
        indf["対象術式_有無"] = indf[l1_l2_and].sum(axis=1)
        indf[columns2[0]] = indf[columns1[2]]
        indf.loc[indf["対象術式_有無"] == 0, columns1[2]] = 0
        Output(indf, targetNo)

    # ○背反項目 個別対応
    def Check3(targetNo, dropNo):
        indf = Input(targetNo)

        # 読み込めなかった場合 return 0
        if not isinstance(indf, pd.DataFrame):
            return 0

        indf[dropNo] = indf[dropNo].fillna(0).astype('int')
        indf[columns2[0]] = indf[columns1[2]]
        indf.loc[indf[dropNo] == 1, columns1[2]] = 0
        Output(indf, targetNo)

    # ○背反項目　複数項目（以下それぞれ背反）
    def Check4(targetNo, dropNo_list, option):
        # 予測結果
        indf = Input(targetNo)

        # 読み込めなかった場合 return 0
        if not isinstance(indf, pd.DataFrame):
            return 0

        indf = indf.drop(dropNo_list, axis=1, errors='ignore')
        usec = [columns1[1], columns1[0]] + dropNo_list
        df_tmp = indf_s.loc[:, indf_s.columns.intersection(usec)].reindex(columns=usec)

        df_tmp[dropNo_list] = df_tmp[dropNo_list].fillna(0).astype('int')

        if option == 'monthly':
            df_tmp = Month(df_tmp)
        elif option == 'weekly':
            df_tmp = Week(df_tmp)

        df_tmp = df_tmp.fillna(0)
        df_tmp['sum'] = df_tmp[dropNo_list].sum(axis=1)
        new_df = pd.merge(indf, df_tmp, on=[columns1[1], columns1[0]], how='left')
        new_df[columns2[0]] = new_df[columns1[2]]
        new_df.loc[new_df['sum'] >= 1, columns1[2]] = 0
        Output(new_df, targetNo)

    # 診療行為
    indf_s = pd.read_pickle(si_path)

    # ターゲットごとに処理
    for target, drop,option in zip(target_list_1, drop_list_1,option_pattern_1):
        # in_targetsにあれば実施
        if target in in_targets:
            Check(target, drop,option)

    for target, drop,option in zip(target_list_4, drop_list_4,option_pattern_4):
        # in_targetsにあれば実施
        if target in in_targets:
            Check4(target, drop,option)

    for target, drop in zip(target_list_3, drop_list_3):
        # in_targetsにあれば実施
        if target in in_targets:
            Check3(target, drop)

    for target in target_list_2:
        # in_targetsにあれば実施
        if target in in_targets:
            Check2(target)

    # 終了ログ
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s3_change_prediction"]))
