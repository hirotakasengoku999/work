# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import re
import MeCab
import pandas as pd
import os

# 形態素解析
def Mecab_keitaio(texts):
    # 不要キゴウ　変換
    data_list = {"@": "", ",": " ", "-": "", "＊": " ",
                 "⑥": " ", "②": " ", "③": " ", "①": " ",
                 ".": " ", "）": " ", "┏": " ", "*": "",
                 "↑": "", "：": " ", "+": " ", "＋": " ",
                 "…": "", "[": "", "～": "", "%": "",
                 "↓": "", "±": "", "(": " ", "④": "",
                 "⑦": "", "⑤": "", "=": "", "－": "",
                 "⑧": "", "&": " ", "＆": "", "?": "",
                 "⑨": "", "!": "", "○": "", "┬": "",
                 "＞": "", "？": "", "→": " ", ";": "",
                 "【": " ", "＃": "", "〉": "", "●": "",
                 "「": "", "■": "", "^": "", "！": "",
                 "＜": "", "】": " ", ")": " ", "（": " ",
                 "\t": " ", "\n": " "}
    table = texts.maketrans(data_list)
    text_kakou = texts.translate(table)

    # mecab
    mecab = MeCab.Tagger()
    parse = mecab.parse(text_kakou)
    lines = parse.split('\n')
    items = [re.sub('\t', ',', line) for line in lines]
    items_split = [item.split(',') for item in items]

    # 「名詞」かつ「一般 or サ変接続 or 固有名詞」で、「固有名詞の場合は、一般 or 組織 or 地域 or 人名 で [名]以外」
    meisi_list = [item[0]
                  for item in items_split
                  if (item[0] not in ('EOS', '', 'ー', '"', ":", 'ｆｏｎｔｓｉｚｅ') and
                      (item[1] == '名詞' and (item[2] == '一般' or item[2] == 'サ変接続' or
                                            (item[2] == '固有名詞' and (
                                                    item[3] == '一般' or item[3] == '組織' or item[3] == '地域'
                                                    or item[3] == '人名') and (item[4] != '名')))))]

    set_meisi = set(meisi_list)
    koumoku_text = ' '.join(map(str, set_meisi))
    return koumoku_text


# 1/0データ作成
def DataKakou(df, target):
    df[target] = df[target].apply(lambda x: x.split(' '))
    l_1d = df[target].values.tolist()
    l_2d = []
    for l in l_1d:
        l_2d = l_2d + l
    fileheader = list(set(l_2d))

    s_list=df.columns.tolist()+fileheader
    _new_df = df.loc[:, df.columns.intersection(s_list)].reindex(columns=s_list)
    # new_df = pd.DataFrame(columns=fileheader)
    # df = pd.concat([df, new_df], axis=1)
    
    def Kakou(x):
        x[x[target]] = 1
        return x

    new_df = _new_df.apply(Kakou, axis=1)
    new_df.drop(target, axis=1, inplace=True)
    return new_df


def call(params, logger, model_flag):
    """
    形態素解析
    """
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["s5_morphological_analysis_processing"]))

    if "files" not in params:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ["files"]))
        raise Exception
    files = params["files"]
    if len(files) < 3:
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003011", ["files", 3]))
        raise Exception
    inpath = files[0]
    if not os.path.exists(inpath):
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003012", [inpath]))
        raise Exception
    out_dir1 = files[1]
    out_dir2 = files[2]
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

    # 出力フォルダ作成
    os.makedirs(out_dir1,exist_ok=True)
    os.makedirs(out_dir2,exist_ok=True)

    bunsyo_syubetu = columns2[0]
    out_colname = columns2[1]
    target_colums = columns2[2]

    # カルテ全結合データ読み込み
    try:
        wkdf = pd.read_csv(inpath, engine='python', encoding='cp932', dtype='object',
                        usecols=[columns1[0], columns1[1], bunsyo_syubetu] + columns3)
    except ValueError as ve:
        keys = []
        for arg in ve.args:
            keys.extend(re.findall(r"\'(.*)\'", arg))
        l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003016", keys))
        raise

    # 空白レコード削除
    wkdf = wkdf.dropna(subset=columns3, how='all')

    # columns3を指定の列にまとめる（複数列対応のため
    wkdf[target_colums] = ''
    for target in columns3:
        wkdf[target_colums] = wkdf[target_colums].str.cat(wkdf[target], sep=' ', na_rep='')
    tmp_df1 = wkdf[[columns1[0], columns1[1], target_colums, bunsyo_syubetu]].copy()
    tmp_df1 = tmp_df1[tmp_df1[target_colums] != '']

    # 文書種別ごとに処理、文書種別のユニーク
    for bunsyo in tmp_df1[bunsyo_syubetu].unique():
        # 対象の文書種別のレコードを抽出
        wk_df = tmp_df1.loc[tmp_df1[bunsyo_syubetu] == bunsyo].copy()

        # 形態素解析
        wk_df[out_colname] = wk_df[target_colums].apply(Mecab_keitaio)
        wk_df = wk_df[[columns1[0], columns1[1], bunsyo_syubetu, out_colname]]
        wk_df = wk_df[wk_df[out_colname] != '']

        # wk_df = (wk_df.groupby([columns1[0], columns1[1], bunsyo_syubetu])[out_colname]
        #          .apply(list)
        #          .apply(lambda x: sorted(x))
        #          .apply(' '.join)
        #          ).reset_index()

        # 形態素解析後　1/0データに変換
        if len(wk_df) > 0:

            # # モデルのみ
            # if model_flag==1:
            #     # 3個以上非欠損値の列以外削除
            #     tmp_df_wk = wk_df[out_colname].apply(lambda x: x.split(' '))
            #     l_1d = tmp_df_wk.values.tolist()
            #     # l_2d = []
            #     # for l in l_1d:
            #     #     l_2d = l_2d + l
            #     l_2d = [e for inner_list in l_1d for e in inner_list]
            #
            #     l_2nd_tupl = collections.Counter(l_2d)
            #     drop_itm = [i for i, v in l_2nd_tupl.items() if v < 3]
            #
            #     # drop_imetを削除
            #     result_list=[]
            #     for l in l_1d:
            #         drop_itm_2=set(drop_itm) & set(l)
            #         result_list.append(" ".join(list(drop_itm_2 ^ set(l))))
            #     wk_df[out_colname]=result_list


            outpath = out_dir1 + bunsyo + '_形態素解析.csv'
            try:
                wk_df.to_csv(outpath, encoding='cp932', index=False)
            except:
                import traceback, sys
                l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath + ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                raise

            # 予測の場合のみ処理
            if model_flag==0:
                new_df = DataKakou(wk_df, out_colname)

                outpath = out_dir2 + bunsyo + '.csv'

                try:
                    new_df.to_csv(outpath, encoding='cp932', index=False)
                except:
                    import traceback, sys
                    l = lambda x: x[0](x[1]);l(logger.format_log_message("AIR003015", [outpath+ ":" + traceback.format_exception(*sys.exc_info())[-1]]))
                    raise


    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["s5_morphological_analysis_processing"]))
